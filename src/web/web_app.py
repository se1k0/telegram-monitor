import logging
import json
import time
import os
import multiprocessing
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from flask import Flask, render_template, jsonify, request, redirect, url_for, flash, send_from_directory, abort, session, Response
from flask_cors import CORS
from dotenv import load_dotenv
from src.database.models import Token, Message, TelegramChannel, TokensMark
from functools import wraps
import threading
import asyncio
from functools import wraps
import platform

# 辅助函数：确保数值转换正确
def to_decimal_or_float(value):
    """将值转换为float，处理None和转换错误，始终返回有效数值"""
    if value is None:
        return 0.0
    try:
        # 先尝试直接转换
        result = float(value)
        return result
    except (ValueError, TypeError):
        # 如果是字符串，尝试移除逗号等格式字符再转换
        if isinstance(value, str):
            try:
                clean_value = value.replace(',', '').strip()
                return float(clean_value) if clean_value else 0.0
            except (ValueError, TypeError):
                return 0.0
        return 0.0

# 添加异步支持装饰器
def async_route(f):
    """
    装饰器，用于在Flask中支持异步路由处理函数
    将异步函数转换为同步函数，供Flask调用
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        # 获取或创建事件循环
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # 如果当前线程没有事件循环，创建一个新的
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # 运行异步函数并返回结果
        try:
            result = loop.run_until_complete(f(*args, **kwargs))
            return result
        except Exception as e:
            logger.error(f"异步路由执行错误: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return jsonify({"success": False, "error": f"异步处理错误: {str(e)}"}), 500
    
    return wrapper

# 缓存和速率限制相关
# 使用简单的内存字典实现缓存，生产环境可考虑使用Redis
API_CACHE = {}  # 格式: {cache_key: {'data': data, 'timestamp': timestamp}}
API_LOCKS = {}  # 格式: {cache_key: lock_object}
API_LOCK = threading.Lock()  # 全局锁，用于保护API_LOCKS和API_CACHE的并发访问
CACHE_CLEANUP_INTERVAL = 300  # 缓存清理间隔，单位秒（5分钟）
CACHE_MAX_AGE = 300  # 缓存最大保存时间，单位秒（5分钟）

# 在开发环境中修改路径
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.database.db_handler import extract_promotion_info
import config.settings as config

# 加载环境变量
load_dotenv()

app = Flask(__name__)
# 从环境变量中读取密钥，如果不存在则使用默认值（仅用于开发）
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'telegram-monitor-dev-key')
CORS(app)

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../logs/web_app.log'))),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 创建数据库引擎和会话
# 使用数据库工厂获取适配器
try:
    logger.info("在Web应用程序中使用Supabase适配器")
    from src.database.db_factory import get_db_adapter
    db_adapter = get_db_adapter()
except Exception as e:
    logger.error(f"初始化数据库连接时出错: {str(e)}")
    import traceback
    logger.error(traceback.format_exc())

def handle_error(error_message, status_code=500):
    """通用错误处理函数，返回友好的错误页面"""
    logger.error(error_message)
    
    # 在开发模式下显示完整的错误信息，否则显示简单的错误消息
    if app.debug:
        import traceback
        traceback.print_exc()
        detailed_error = traceback.format_exc()
    else:
        detailed_error = None
    
    # 尝试渲染错误模板
    try:
        return render_template('error.html', 
                               error_message=error_message,
                               detailed_error=detailed_error,
                               year=datetime.now().year), status_code
    except Exception:
        # 如果模板不存在，则返回简单的错误文本
        return f"系统错误: {error_message}", status_code

def format_market_cap(value):
    """格式化市值显示"""
    try:
        if value is None:
            return "$0.00"
        if isinstance(value, str):
            try:
                value = float(value.replace(',', ''))
            except:
                return "$0.00"
        # 格式化显示，使用符号而不是中文字
        if value >= 1000000000:  # 十亿 (B)
            return f"${value/1000000000:.2f}B"
        elif value >= 1000000:   # 百万 (M)
            return f"${value/1000000:.2f}M"
        elif value >= 1000:      # 千 (K)
            return f"${value/1000:.2f}K"
        return f"${value:.2f}"
    except Exception as e:
        logger.error(f"市值格式化错误: {value}, 错误: {str(e)}")
        return "$0.00"


def get_db_connection():
    """创建数据库连接"""
    from src.database.db_factory import get_db_adapter
    logger.info("使用Supabase适配器创建数据库连接")
    return get_db_adapter()


def get_dexscreener_url(chain: str, contract: str) -> str:
    """生成 DexScreener URL"""
    if chain == 'SOL':
        return f"https://dexscreener.com/solana/{contract}"
    elif chain == 'ETH':
        return f"https://dexscreener.com/ethereum/{contract}"
    elif chain == 'BSC':
        return f"https://dexscreener.com/bsc/{contract}"
    else:
        return f"https://dexscreener.com/{chain.lower()}/{contract}"

app.jinja_env.globals.update(
    format_market_cap=format_market_cap,
    get_dexscreener_url=get_dexscreener_url
)

def get_system_stats():
    """获取系统统计数据"""
    default_stats = {
        'active_channels_count': 0,
        'message_count': 0,
        'token_count': 0,
        'last_update': "未知",
        'channels': [],
    }
    
    try:
        # 使用Supabase适配器获取数据，避免不必要的查询
        from supabase import create_client
        import config.settings as config
        
        # 获取Supabase配置
        supabase_url = config.SUPABASE_URL
        supabase_key = config.SUPABASE_KEY
        
        if not supabase_url or not supabase_key:
            logger.error("缺少 SUPABASE_URL 或 SUPABASE_KEY 配置")
            return default_stats
            
        # 创建Supabase客户端
        supabase = create_client(supabase_url, supabase_key)
        
        # 1. 只获取活跃频道数量，不需要完整的频道数据
        try:
            # 只计数，不获取完整数据
            active_channels_response = supabase.table('telegram_channels').select('id', count='exact').eq('is_active', True).execute()
            active_channels_count = active_channels_response.count if hasattr(active_channels_response, 'count') else 0
        except Exception as e:
            logger.error(f"获取活跃频道计数时出错: {str(e)}")
            active_channels_count = 0
            
        # 2. 获取代币数量和消息数量
        try:
            # 获取代币数量
            tokens_count_response = supabase.table('tokens').select('id', count='exact').execute()
            token_count = tokens_count_response.count if hasattr(tokens_count_response, 'count') else 0
            
            # 获取消息数量
            messages_count_response = supabase.table('messages').select('id', count='exact').execute()
            message_count = messages_count_response.count if hasattr(messages_count_response, 'count') else 0
            
            # 获取最后更新时间
            last_update_response = supabase.table('tokens').select('latest_update').order('latest_update', desc=True).limit(1).execute()
            last_update = last_update_response.data[0]['latest_update'] if hasattr(last_update_response, 'data') and last_update_response.data else "未知"
        except Exception as e:
            logger.error(f"获取Supabase数据统计时出错: {str(e)}")
            token_count = 0
            message_count = 0
            last_update = "未知"
        
        # 首页只需要统计数量，不需要获取完整的频道数据列表
        return {
            'active_channels_count': active_channels_count,
            'message_count': message_count,
            'token_count': token_count,
            'last_update': last_update,
            'channels': [], # 首页不需要完整的频道数据，置空以提高性能
        }
    except Exception as e:
        logger.error(f"获取系统统计数据时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        # 返回默认值以防止页面崩溃
        return default_stats


@app.route('/')
def index():
    """首页"""
    try:
        # 获取查询参数
        chain_filter = request.args.get('chain', 'all')
        search_query = request.args.get('search', '')
        is_ajax = request.args.get('ajax', '0') == '1'  # 是否是AJAX请求
        
        # 检查是否是获取新token的请求
        check_new = request.args.get('check_new', '0') == '1'
        last_id = request.args.get('last_id', '')
        
        # 使用Supabase适配器处理AJAX请求
        if is_ajax or check_new:
            logger.info("处理AJAX请求")
            try:
                from supabase import create_client
                # 不要重新导入config模块，直接使用全局config
                
                supabase_url = config.SUPABASE_URL
                supabase_key = config.SUPABASE_KEY
                
                if not supabase_url or not supabase_key:
                    logger.error("缺少 SUPABASE_URL 或 SUPABASE_KEY 配置")
                    return jsonify({"success": False, "error": "数据库配置不完整"})
                
                # 创建 Supabase 客户端
                supabase = create_client(supabase_url, supabase_key)
                
                # 处理检查新token的请求
                if check_new:
                    # 获取最后看到的token ID
                    last_id = request.args.get('last_id', '0')
                    
                    try:
                        # 如果有lastId，则查询比last_token更新的数据
                        if last_id and last_id.isdigit() and int(last_id) > 0:
                            # 先获取该ID的token的first_update时间
                            last_token_result = supabase.table('tokens').select('first_update').eq('id', last_id).execute()
                            
                            if hasattr(last_token_result, 'data') and last_token_result.data:
                                last_token_time = last_token_result.data[0].get('first_update')
                                if last_token_time:
                                    logger.info(f"查询first_update > {last_token_time}的数据")
                                    
                                    # 构建基本查询
                                    query = supabase.table('tokens').select('*')
                                    
                                    # 添加时间过滤条件 - 使用gt(greater than)操作符
                                    query = query.gt('first_update', last_token_time)
                                    
                                    # 应用链过滤
                                    if chain_filter and chain_filter.lower() != 'all':
                                        query = query.eq('chain', chain_filter.upper())
                                    
                                    # 应用搜索过滤（如果有搜索条件）
                                    if search_query:
                                        # 使用Supabase支持的ilike操作和or_查询
                                        query = query.or_(f"token_symbol.ilike.%{search_query}%,contract.ilike.%{search_query}%")
                                    
                                    # 按first_update降序排序
                                    query = query.order('first_update', desc=True)
                                    
                                    # 执行查询
                                    result = query.execute()
                                    
                                    # 处理结果
                                    new_tokens = []
                                    if hasattr(result, 'data') and result.data:
                                        raw_tokens = result.data
                                        logger.info(f"找到 {len(raw_tokens)} 个新token")
                                        
                                        # 处理每个token
                                        for token in raw_tokens:
                                            processed_token = process_token_data(token)
                                            new_tokens.append(processed_token)
                                    
                                    # 返回结果
                                    return jsonify({
                                        "success": True,
                                        "new_tokens": new_tokens,
                                        "count": len(new_tokens)
                                    })
                            else:
                                logger.warning(f"未找到ID={last_id}的token")
                                return jsonify({
                                    "success": False,
                                    "error": f"未找到ID={last_id}的token",
                                    "count": 0
                                })
                        else:
                            logger.warning("没有有效的last_id参数")
                            return jsonify({
                                "success": False,
                                "error": "没有有效的last_id参数",
                                "count": 0
                            })
                            
                    except Exception as e:
                        logger.error(f"执行新token查询出错: {str(e)}")
                        import traceback
                        logger.error(traceback.format_exc())
                        return jsonify({"success": False, "error": f"查询新token失败: {str(e)}"})
                
                # 非check_new请求的处理
                return jsonify({"success": False, "error": "不支持的请求类型"})
            except Exception as e:
                logger.error(f"AJAX请求处理出错: {str(e)}")
                return jsonify({"success": False, "error": str(e)})
        
        # 获取系统统计数据
        stats = get_system_stats()
        
        # 获取可用链列表
        available_chains = ['eth', 'bsc', 'sol']  # 默认支持的链
        
        return render_template(
            'index.html', 
            stats=stats,
            chain_filter=chain_filter, 
            search_query=search_query,
            available_chains=available_chains,
            year=datetime.now().year
        )
    except Exception as e:
        logger.error(f"首页渲染时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return handle_error(str(e))

@app.route('/api/tokens/stream')
@async_route
async def stream_tokens():
    """流式返回代币列表，支持分页、排序和过滤"""
    try:
        # 获取查询参数
        chain = request.args.get('chain', 'all')
        search = request.args.get('search', '')
        last_id = request.args.get('last_id', '0')
        batch_size = int(request.args.get('batch_size', '20'))  # 默认加载20条
        
        # 获取数据库连接
        db = get_db_connection()
        
        # 构建查询条件
        filters = {}
        if chain and chain.lower() != 'all':
            filters['chain'] = chain.upper()
            
        # 获取总记录数 - 使用db适配器的方法
        total_count = 0
        try:
            # 执行count查询
            count_result = await db.execute_query(
                'tokens',
                'select',
                fields=['id'],
                filters={} if chain.lower() == 'all' else {'chain': chain.upper()}
            )
            
            # 计算总记录数
            if count_result and isinstance(count_result, list):
                total_count = len(count_result)
                
            logger.info(f"数据库中共有 {total_count} 条token记录")
        except Exception as e:
            logger.error(f"获取token总数出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            # 即使出错也继续执行，不影响主功能
        
        # 关键修复：确保分页正确工作
        # 收到last_id后，我们需要获取比这个ID更旧的数据(first_update更小的数据)
        # 前端是按照先展示最新数据，然后下拉加载更旧数据的方式工作的
        
        # 如果last_id存在，我们需要先获取这个token的first_update时间
        last_token_time = None
        if last_id and last_id.isdigit() and int(last_id) > 0:
            try:
                # 获取last_id对应的token
                last_token_result = await db.execute_query(
                    'tokens',
                    'select',
                    fields=['first_update'],
                    filters={'id': int(last_id)}
                )
                
                if last_token_result and isinstance(last_token_result, list) and len(last_token_result) > 0:
                    last_token_time = last_token_result[0].get('first_update')
                    logger.info(f"找到ID={last_id}的token，first_update时间为{last_token_time}")
            except Exception as e:
                logger.error(f"获取last_token_time时出错: {str(e)}")
        
        # 构建查询 - 如果有last_token_time，查询比这个时间更早的数据
        query_filters = filters.copy()
        if last_token_time:
            # 查询比last_token_time更早的数据(first_update更小的值)
            query_filters['first_update'] = ('<', last_token_time)
            logger.info(f"查询first_update < {last_token_time}的数据")
        
        # 获取代币数据 - 按照首次发现时间降序排序
        logger.info(f"加载token数据: last_id={last_id}, chain={chain}, batch_size={batch_size}, 按first_update降序排序")
        tokens = await db.execute_query(
            'tokens', 
            'select', 
            filters=query_filters, 
            limit=batch_size,
            order_by={'first_update': 'desc'}  # 按照首次发现时间降序排列(新的在前)
        )
        
        # 处理查询结果
        processed_tokens = []
        
        # 如果tokens不为空，处理数据
        if tokens and isinstance(tokens, list):
            # 处理代币数据
            for token in tokens:
                if isinstance(token, dict):
                    # 如果有搜索条件，则在应用层过滤
                    if search:
                        # 在token_symbol和contract字段中搜索，不区分大小写
                        token_symbol = token.get('token_symbol', '').lower() if token.get('token_symbol') else ''
                        contract = token.get('contract', '').lower() if token.get('contract') else ''
                        search_lower = search.lower()
                        
                        # 如果匹配则添加到结果
                        if search_lower in token_symbol or search_lower in contract:
                            processed_token = process_token_data(token)
                            processed_tokens.append(processed_token)
                    else:
                        # 无搜索条件，直接处理
                        processed_token = process_token_data(token)
                        processed_tokens.append(processed_token)
        
        # 获取下一个ID - 确保有数据时才获取
        next_id = 0
        if processed_tokens and len(processed_tokens) > 0:
            # 获取当前批次中最后一个token的ID
            last_token = processed_tokens[-1]
            if isinstance(last_token, dict) and 'id' in last_token:
                next_id = last_token['id']
                logger.info(f"设置next_id为当前批次最后一个token的ID: {next_id}")
        
        # 判断是否还有更多数据
        # 如果没有返回任何数据，说明没有更多了
        if len(processed_tokens) == 0:
            has_more = False
            logger.info("查询无结果，没有更多数据了")
        else:
            # 如果返回的数据少于请求的批次大小，可能没有更多数据了，但仍需检查
            has_more = len(processed_tokens) >= batch_size
        
        # 当返回结果小于批次大小但不为0时，检查是否真的没有更多数据
        if has_more == False and len(processed_tokens) > 0 and len(processed_tokens) < batch_size:
            try:
                # 查询是否还有更早的数据
                # 使用当前批次最后一个token的first_update时间
                last_time = processed_tokens[-1].get('first_update')
                if last_time:
                    earlier_check = await db.execute_query(
                        'tokens', 
                        'select', 
                        fields=['id'],
                        filters={
                            **filters,
                            'first_update': ('<', last_time)
                        },
                        limit=1
                    )
                    
                    # 如果还有更早的数据，设置has_more=True
                    if earlier_check and len(earlier_check) > 0:
                        has_more = True
                        logger.info(f"检测到还有更早的数据，设置has_more=True")
            except Exception as e:
                logger.error(f"检查更早数据时出错: {str(e)}")
                # 出错时保守处理，假设没有更多数据
                has_more = False
        
        # 关键修复：删除特殊情况处理，避免无限循环
        # 当没有数据时，不再返回最新的数据，而是返回空数据并设置has_more=False
            
        logger.info(f"返回 {len(processed_tokens)} 条token数据，has_more={has_more}")
        
        # 返回JSON响应
        return jsonify({
            'success': True,
            'tokens': processed_tokens,
            'next_id': next_id,
            'has_more': has_more,
            'batch_size': batch_size,
            'total_count': total_count
        })
        
    except Exception as e:
        logger.error(f"流式获取代币数据时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': f"流式获取代币数据时出错: {str(e)}"
        }), 500

@app.route('/channels')
def channels():
    """社群信息页面，显示所有频道和群组信息"""
    try:
        # 使用 ChannelManager 获取数据
        from src.core.channel_manager import ChannelManager
        channel_manager = ChannelManager()
        
        # 获取所有频道
        all_channels = channel_manager.get_all_channels()
        
        # 获取活跃频道数量
        active_channels = channel_manager.get_active_channels()
        active_channels_count = len(active_channels) if active_channels else 0
        
        # 最后更新时间使用当前时间
        last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 渲染模板
        return render_template(
            'channels.html',
            channels=all_channels,
            active_channels_count=active_channels_count,
            last_update=last_update,
            year=datetime.now().year
        )
    except Exception as e:
        logger.error(f"社群信息页面请求处理错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return handle_error(f"处理社群信息页面请求时出错: {str(e)}")


@app.route('/statistics')
def statistics():
    """统计分析页面，显示系统统计数据和图表"""
    try:
        # 获取系统统计数据，处理已经在 get_system_stats 函数中完成
        stats = get_system_stats()
        
        # 从Supabase获取代币分布数据
        try:
            from supabase import create_client
            supabase_url = config.SUPABASE_URL
            supabase_key = config.SUPABASE_KEY
            
            if not supabase_url or not supabase_key:
                logger.error("缺少 SUPABASE_URL 或 SUPABASE_KEY 配置")
                raise ValueError("数据库配置不完整")
                
            # 创建Supabase客户端
            supabase = create_client(supabase_url, supabase_key)
            
            # 使用原生SQL查询获取代币分布
            # 修改：不再使用exec_sql函数，改用Supabase SDK原生方法
            # 获取所有代币记录
            tokens_response = supabase.table('tokens').select('chain').execute()
            
            if hasattr(tokens_response, 'data') and tokens_response.data:
                # 手动计数每个链的代币数量
                chain_counts = {}
                for token in tokens_response.data:
                    chain = token.get('chain')
                    if chain:
                        chain_counts[chain] = chain_counts.get(chain, 0) + 1
                
                chains = list(chain_counts.keys())
                counts = [chain_counts[chain] for chain in chains]
                
                chart_data = {
                    'chains': chains,
                    'counts': counts
                }
            else:
                # 没有数据时使用默认值
                chart_data = {
                    'chains': ['ETH', 'BSC', 'SOL'],  # 默认支持的链
                    'counts': [0, 0, 0]  # 暂时没有数据
                }
        except Exception as e:
            logger.error(f"获取链分布数据失败: {str(e)}")
            # 使用默认数据
            chart_data = {
                'chains': ['ETH', 'BSC', 'SOL'],  # 默认支持的链
                'counts': [0, 0, 0]  # 暂时没有数据
            }
        
        # 渲染模板
        return render_template(
            'statistics.html',
            active_channels_count=stats['active_channels_count'],
            message_count=stats['message_count'],
            token_count=stats['token_count'],
            last_update=stats['last_update'],
            channels=stats['channels'],
            chart_data=chart_data,
            year=datetime.now().year
        )
    except Exception as e:
        logger.error(f"统计分析页面请求处理错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return handle_error(f"处理统计分析页面请求时出错: {str(e)}")


@app.route('/token/mention_details/<int:channel_id>/<chain>/<contract>')
def token_mention_details(channel_id, chain, contract):
    """显示特定频道中代币的提及详情"""
    try:
        # 保存当前URL到session，用于导航
        current_url = request.url
        # 检查是否是从消息详情页来的
        is_from_message = False
        referer = request.headers.get('Referer', '')
        if referer and '/message/' in referer:
            is_from_message = True
            session['last_message_detail_url'] = referer
            
        # 使用Supabase获取数据
        from supabase import create_client
        
        supabase_url = config.SUPABASE_URL
        supabase_key = config.SUPABASE_KEY
        
        if not supabase_url or not supabase_key:
            logger.error("缺少SUPABASE_URL或SUPABASE_KEY配置")
            return handle_error("数据库配置不完整", 500)
            
        # 创建Supabase客户端
        supabase = create_client(supabase_url, supabase_key)
        
        # 获取频道信息
        channel_response = supabase.table('telegram_channels').select('*').eq('channel_id', channel_id).limit(1).execute()
        channel = channel_response.data[0] if hasattr(channel_response, 'data') and channel_response.data else None
        
        if not channel:
            return handle_error(f"未找到频道: ID {channel_id}")
            
        # 获取代币信息
        token_response = supabase.table('tokens').select('*').eq('chain', chain.upper()).eq('contract', contract).limit(1).execute()
        token = token_response.data[0] if hasattr(token_response, 'data') and token_response.data else None
        
        if not token:
            return handle_error(f"未找到代币: {chain}/{contract}")
            
        # 查询该频道中代币的提及记录
        mentions_response = supabase.table('tokens_mark').select('*')\
            .eq('chain', chain.upper())\
            .eq('contract', contract)\
            .eq('channel_id', channel_id)\
            .order('mention_time', desc=True)\
            .execute()
            
        mentions = mentions_response.data if hasattr(mentions_response, 'data') else []
        
        # 转换为字典列表
        mention_data = []
        for mention in mentions:
            mention_data.append({
                'id': mention.get('id'),
                'chain': mention.get('chain'),
                'token_symbol': mention.get('token_symbol'),
                'market_cap': mention.get('market_cap'),
                'market_cap_formatted': format_market_cap(mention.get('market_cap')),
                'mention_time': mention.get('mention_time'),
                'message_id': mention.get('message_id')
            })
        
        # 渲染模板
        return render_template(
            'token_mention_details.html',
            token=token,
            channel=channel,
            mentions=mention_data,
            is_from_message=is_from_message
        )
                
    except Exception as e:
        logger.error(f"获取代币提及详情失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return handle_error(f"获取代币提及详情失败: {str(e)}")


def start_web_server(host='0.0.0.0', port=5000, debug=False):
    """
    启动Web服务器
    临时方案：无论什么系统都只用http，Linux下启用https的代码已注释
    """
    try:
        logger.info(f"正在启动Web服务器: {host}:{port}")
        logger.info(f"数据库配置: {config.DATABASE_URI}")
        
        # 检查是否使用Supabase
        if not config.DATABASE_URI.startswith('supabase://'):
            logger.error("未使用Supabase数据库，请检查配置")
            logger.error(f"当前DATABASE_URI: {config.DATABASE_URI}")
            logger.error("DATABASE_URI应以'supabase://'开头")
            return None
        
        # 初始化Supabase适配器
        logger.info("Web服务器使用Supabase数据库")
        try:
            from src.database.db_factory import get_db_adapter
            db_adapter = get_db_adapter()
            if db_adapter:
                logger.info("Supabase适配器初始化成功")
            else:
                logger.error("无法初始化Supabase适配器")
                return None
        except Exception as e:
            logger.error(f"初始化Supabase适配器时出错: {str(e)}")
            return None
        
        is_windows = platform.system() == 'Windows'
        
        # 在Windows环境下使用线程，在Linux环境下使用多进程
        if is_windows:
            logger.info("Windows环境：使用线程启动Web服务器")
            def run_flask_app():
                global app
                try:
                    logger.info(f"Flask线程启动: {host}:{port}")
                    app.run(host=host, port=port, debug=debug, ssl_context=None)
                except Exception as e:
                    logger.error(f"Flask线程崩溃: {str(e)}")
                    import traceback
                    logger.error(traceback.format_exc())
            import threading
            thread = threading.Thread(target=run_flask_app)
            thread.daemon = True
            thread.start()
            logger.info(f"已使用线程启动Web服务器")
            return thread
        else:
            # 临时方案：Linux环境也只用http，https相关代码已注释
            # logger.info("Linux环境：使用多进程启动Web服务器，启用HTTPS")
            # ssl_context = ('/home/ubuntu/certs/server.crt', '/home/ubuntu/certs/server.key')
            # def run_flask_server_with_ssl(host, port, debug):
            #     global app
            #     app.run(host=host, port=port, debug=debug, ssl_context=ssl_context)
            # import multiprocessing
            # process = multiprocessing.Process(target=run_flask_server_with_ssl, args=(host, port, debug))
            # process.daemon = True
            # try:
            #     process.start()
            #     logger.info(f"Web服务器已启动，进程ID: {process.pid}")
            #     return process
            # except Exception as multi_error:
            #     logger.error(f"使用多进程启动失败: {str(multi_error)}")
            #     # 回退到线程方式
            #     logger.info("回退到线程方式启动")
            #     def run_flask_app():
            #         global app
            #         app.run(host=host, port=port, debug=debug, ssl_context=ssl_context)
            #     import threading
            #     thread = threading.Thread(target=run_flask_app)
            #     thread.daemon = True
            #     thread.start()
            #     logger.info(f"已使用线程启动Web服务器")
            #     return thread
            # 临时方案：直接用http
            import multiprocessing
            def run_flask_server_http(host, port, debug):
                global app
                app.run(host=host, port=port, debug=debug)
            process = multiprocessing.Process(target=run_flask_server_http, args=(host, port, debug))
            process.daemon = True
            try:
                process.start()
                logger.info(f"Web服务器已启动，进程ID: {process.pid}")
                return process
            except Exception as multi_error:
                logger.error(f"使用多进程启动失败: {str(multi_error)}")
                # 回退到线程方式
                logger.info("回退到线程方式启动")
                def run_flask_app():
                    global app
                    app.run(host=host, port=port, debug=debug)
                import threading
                thread = threading.Thread(target=run_flask_app)
                thread.daemon = True
                thread.start()
                logger.info(f"已使用线程启动Web服务器")
                return thread
    except Exception as e:
        logger.error(f"启动Web服务器时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None

# 添加全局函数用于多进程启动Flask (Linux环境)
def run_flask_server(host, port, debug):
    """
    在新进程中运行Flask服务器的全局函数
    
    Args:
        host: 主机地址
        port: 端口号
        debug: 是否启用调试模式
    """
    global app
    
    # 在新进程中设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../logs/web_app.log'))),
            logging.StreamHandler()
        ]
    )
    
    try:
        logger.info(f"Flask进程启动: {host}:{port}")
        app.run(host=host, port=port, debug=debug, ssl_context=None)
    except Exception as e:
        logger.error(f"Flask进程崩溃: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())


# 添加全局错误处理器
@app.errorhandler(404)
def page_not_found(e):
    """处理404错误"""
    return handle_error("请求的页面不存在，请检查URL是否正确", 404)

@app.errorhandler(500)
def internal_server_error(e):
    """处理500错误"""
    return handle_error("服务器内部错误，请稍后再试", 500)


@app.route('/api/token_market_history/<chain>/<contract>')
@async_route
async def api_token_market_history(chain, contract):
    """获取代币市值历史数据和提及统计API，并更新关键数据"""
    try:
        # 定义缓存键
        cache_key = f"{chain}_{contract}"
        # 缓存时间（秒）- 降低到30秒，让数据更及时刷新
        cache_duration = 30  # 30秒
        current_time = time.time()
        
        # 检查是否明确要求不使用缓存
        force_refresh = request.args.get('refresh', '0') == '1'
        
        # 标记是否需要在后台更新数据
        need_background_update = False
        
        # 首先检查是否存在缓存数据且未过期
        cached_data = None
        with API_LOCK:
            if cache_key in API_CACHE and not force_refresh:
                cache_data = API_CACHE[cache_key]
                # 如果缓存有效且未过期，直接返回缓存数据
                if current_time - cache_data['timestamp'] < cache_duration:
                    logger.info(f"返回缓存数据: {chain}/{contract}")
                    return jsonify(cache_data['data'])
                else:
                    # 缓存已过期，但我们可以暂时使用它，同时标记需要更新
                    cached_data = cache_data['data']
                    need_background_update = True
                    logger.info(f"缓存已过期，准备从数据库获取数据: {chain}/{contract}")
            else:
                # 没有缓存或要求强制刷新，需要更新
                need_background_update = True
            
            # 获取或创建此代币的锁
            if cache_key not in API_LOCKS:
                API_LOCKS[cache_key] = threading.Lock()
        
        # 尝试获取锁
        token_lock = API_LOCKS[cache_key]
        lock_acquired = token_lock.acquire(blocking=False)
        
        # 如果无法获取锁（意味着另一个请求正在处理相同的代币）
        if not lock_acquired:
            # 如果有过期的缓存，先返回过期的缓存
            if cached_data:
                logger.info(f"无法获取锁，返回过期的缓存数据: {chain}/{contract}")
                return jsonify(cached_data)
                
            # 如果有其他缓存（可能是其他线程刚更新的），返回它
            with API_LOCK:
                if cache_key in API_CACHE:
                    logger.info(f"无法获取锁，返回其他线程更新的缓存数据: {chain}/{contract}")
                    return jsonify(API_CACHE[cache_key]['data'])
            
            # 如果没有任何缓存，告知用户稍后重试
            logger.info(f"无法获取锁且无缓存，等待其他请求完成: {chain}/{contract}")
            return jsonify({"success": False, "error": "系统正在处理相同的请求，请稍后重试"})
        
        try:
            # 使用Supabase适配器
            try:
                from supabase import create_client
                supabase_url = config.SUPABASE_URL
                supabase_key = config.SUPABASE_KEY
                
                # 创建Supabase客户端
                supabase = create_client(supabase_url, supabase_key)
                
                # 获取代币基本信息
                token_response = supabase.table('tokens').select('*').eq('chain', chain).eq('contract', contract).limit(1).execute()
                token = token_response.data[0] if hasattr(token_response, 'data') and token_response.data and len(token_response.data) > 0 else None
                
                if not token:
                    return jsonify({"success": False, "error": f"未找到代币 {chain}/{contract}"})
                
                # 从数据库获取代币数据
                logger.info(f"从数据库获取代币数据: {chain}/{contract}")
                
                # 获取代币提及历史
                mentions_response = supabase.table('tokens_mark').select('*').eq('chain', chain).eq('contract', contract).order('mention_time', desc=True).execute()
                mentions = mentions_response.data if hasattr(mentions_response, 'data') else []
                
                # 格式化提及历史数据
                history = []
                channel_stats = {}  # 用于统计各频道的提及情况
                
                for mention in mentions:
                    channel_id = mention.get('channel_id')
                    mention_time = mention.get('mention_time')
                    market_cap = mention.get('market_cap')
                    
                    if channel_id and mention_time:
                        try:
                            # 获取频道信息
                            channel_response = supabase.table('telegram_channels').select('*').eq('channel_id', channel_id).limit(1).execute()
                            channel = channel_response.data[0] if hasattr(channel_response, 'data') and channel_response.data and len(channel_response.data) > 0 else None
                            
                            channel_name = channel.get('channel_name') if channel else '未知频道'
                            member_count = channel.get('member_count') if channel else 0
                            
                            # 添加到历史记录
                            history.append({
                                'mention_time': mention_time,
                                'market_cap': market_cap,
                                'channel_id': channel_id,
                                'channel_name': channel_name,
                                'member_count': member_count
                            })
                            
                            # 更新频道统计
                            if channel_id not in channel_stats:
                                channel_stats[channel_id] = {
                                    'channel_id': channel_id,
                                    'channel_name': channel_name,
                                    'member_count': member_count,
                                    'mention_count': 0,
                                    'first_mention_time': None,
                                    'first_market_cap': None
                                }
                            
                            # 增加提及次数
                            channel_stats[channel_id]['mention_count'] += 1
                            
                            # 更新首次提及时间和市值
                            if not channel_stats[channel_id]['first_mention_time'] or mention_time < channel_stats[channel_id]['first_mention_time']:
                                channel_stats[channel_id]['first_mention_time'] = mention_time
                                channel_stats[channel_id]['first_market_cap'] = market_cap
                        except Exception as e:
                            logger.error(f"处理频道提及数据错误: {str(e)}")
                            continue
                
                # 处理token数据
                processed_token = process_token_data(token)
                
                # 构建响应数据
                response_data = {
                    "success": True,
                    "token": processed_token,
                    "history": sorted(history, key=lambda x: x['mention_time']) if history else [],
                    "channel_stats": list(channel_stats.values()) if channel_stats else [],
                    "data_source": "database",
                    "refresh_timestamp": time.time()
                }
                
                # 更新缓存
                with API_LOCK:
                    API_CACHE[cache_key] = {
                        'data': response_data,
                        'timestamp': time.time()
                    }
                
                # 如果需要后台更新数据 - 总是在后台更新，确保数据始终是最新的
                # 但显式请求刷新时优先获取实时数据
                if need_background_update and not force_refresh:
                    # 启动后台任务更新数据
                    background_task = asyncio.create_task(
                        update_token_data_background(chain, contract, cache_key)
                    )
                    # 不等待后台任务完成
                    logger.info(f"已启动后台任务更新代币数据: {chain}/{contract}")
                # 如果请求强制刷新，同步方式执行数据更新
                elif force_refresh:
                    # 修改：强制刷新时，先执行同步更新，确保获取到最新数据
                    logger.info(f"强制刷新: 同步更新代币数据: {chain}/{contract}")
                    try:
                        # 直接更新代币数据（同步等待完成）
                        await update_token_data_background(chain, contract, cache_key)
                        logger.info(f"强制刷新: 同步更新完成: {chain}/{contract}")
                        
                        # 重新从数据库获取最新数据
                        with API_LOCK:
                            if cache_key in API_CACHE:
                                # 直接返回更新后的缓存
                                return jsonify(API_CACHE[cache_key]['data'])
                        
                        # 如果缓存中没有数据，重新从数据库获取
                        db_adapter = get_db_adapter()
                        token = await db_adapter.execute_query(
                            'tokens',
                            'select',
                            filters={
                                'chain': chain,
                                'contract': contract
                            }
                        )
                        if isinstance(token, list) and len(token) > 0:
                            # 重新处理token数据
                            token = token[0]
                            token_processed = process_token_data(token)
                            
                            # 重新获取历史数据和提及数据
                            # 这里复用之前准备的数据，但确保更新了最新的token数据
                            response_data["token"] = token_processed
                            # 更新缓存
                            with API_LOCK:
                                API_CACHE[cache_key] = {
                                    'data': response_data,
                                    'timestamp': time.time()
                                }
                    except Exception as e:
                        logger.error(f"强制刷新时出错: {str(e)}")
                        import traceback
                        logger.error(traceback.format_exc())
                        # 出错时，仍然尝试返回准备好的数据
                
                # 返回数据
                return jsonify(response_data)
                
            except Exception as e:
                logger.error(f"获取代币数据时出错: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                return jsonify({"success": False, "error": f"获取代币数据时出错: {str(e)}"})
                
        finally:
            # 释放锁
            token_lock.release()
            
    except Exception as e:
        logger.error(f"处理代币市值历史请求时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({"success": False, "error": f"处理请求时出错: {str(e)}"})


@app.route('/message/<chain>/<int:message_id>')
def message_detail(chain, message_id):
    """显示特定消息的详情页面"""
    try:
        # 获取当前URL并保存到session
        current_url = request.url
        session['last_message_detail_url'] = current_url
        
        # 使用Supabase获取数据
        from supabase import create_client
        
        supabase_url = config.SUPABASE_URL
        supabase_key = config.SUPABASE_KEY
        
        if not supabase_url or not supabase_key:
            logger.error("缺少SUPABASE_URL或SUPABASE_KEY配置")
            return handle_error("数据库配置不完整", 500)
            
        # 创建Supabase客户端
        supabase = create_client(supabase_url, supabase_key)
        
        # 获取消息数据 - 同时检查指定链和UNKNOWN链
        message_response = supabase.table('messages').select('*').or_(f'chain.eq.{chain},chain.eq.UNKNOWN').eq('message_id', message_id).limit(1).execute()
        message = message_response.data[0] if hasattr(message_response, 'data') and message_response.data else None
        
        if not message:
            return handle_error(f"未找到消息: {chain}/{message_id}")
            
        # 获取频道数据
        channel = None
        if message.get('channel_id'):
            channel_response = supabase.table('telegram_channels').select('*').eq('channel_id', message.get('channel_id')).limit(1).execute()
            channel = channel_response.data[0] if hasattr(channel_response, 'data') and channel_response.data else None
        
        # 检查是否有相关代币标记
        tokens = []
        token_mark_response = supabase.table('tokens_mark').select('*').eq('chain', chain).eq('message_id', message_id).execute()
        if hasattr(token_mark_response, 'data') and token_mark_response.data:
            # 提取所有唯一的合约地址
            contract_set = set()
            for token_mark in token_mark_response.data:
                if token_mark.get('contract'):
                    contract_set.add(token_mark.get('contract'))
                
            # 获取完整的代币数据
            for contract in contract_set:
                token_response = supabase.table('tokens').select('*').eq('chain', chain).eq('contract', contract).limit(1).execute()
                if hasattr(token_response, 'data') and token_response.data and len(token_response.data) > 0:
                    token = token_response.data[0]
                    # 格式化市值
                    token['market_cap_formatted'] = format_market_cap(token.get('market_cap'))
                    tokens.append(token)
        
        # 渲染模板
        return render_template(
            'message_detail.html',
            message=message,
            channel=channel,
            tokens=tokens,
            has_tokens=len(tokens) > 0
        )
        
    except Exception as e:
        logger.error(f"获取消息详情失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return handle_error(f"获取消息详情失败: {str(e)}")


@app.route('/media/<path:filename>')
def serve_media(filename):
    """提供媒体文件服务"""
    try:
        # 创建媒体目录的绝对路径
        media_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../media'))
        
        # 如果路径以media/开头，则移除此前缀以避免路径重复
        if filename.startswith('media/'):
            filename = filename[6:]  # 移除"media/"前缀
            
        logger.info(f"尝试提供媒体文件: {filename}，从目录: {media_dir}")
        
        # 将所有路径分隔符标准化为操作系统风格
        norm_filename = os.path.normpath(filename)
        
        # 检查文件是否存在，如果不存在，尝试添加常见的图片/视频扩展名
        file_path = os.path.join(media_dir, norm_filename)
        if os.path.exists(file_path):
            # 将路径分解为目录和文件名部分
            subdir, base_filename = os.path.split(norm_filename)
            full_dir = os.path.join(media_dir, subdir)
            logger.info(f"找到原始文件: {base_filename}，从目录: {full_dir}")
            return send_from_directory(full_dir, base_filename)
            
        # 文件不存在，尝试添加扩展名
        dirname, basename = os.path.split(norm_filename)
        
        # 尝试常见的图片/视频扩展名
        for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.mp4', '.webm']:
            test_filename = basename + ext
            if dirname:
                test_filepath = os.path.join(media_dir, dirname, test_filename)
                subdir = dirname
            else:
                test_filepath = os.path.join(media_dir, test_filename)
                subdir = ''
                
            logger.info(f"尝试查找文件: {test_filepath}")
            
            if os.path.exists(test_filepath):
                full_dir = os.path.join(media_dir, subdir)
                logger.info(f"找到媒体文件: {test_filename}，从目录: {full_dir}")
                return send_from_directory(full_dir, test_filename)
        
        # 如果所有尝试都失败，记录并返回错误
        logger.warning(f"找不到媒体文件: {norm_filename}，已尝试所有常见扩展名")
        return handle_error("无法找到所请求的媒体文件", 404)
    
    except Exception as e:
        logger.error(f"提供媒体文件时出错: {str(e)}")
        return handle_error("无法加载所请求的媒体文件", 404)


# 定时清理过期缓存的函数
def cleanup_expired_cache():
    """定期清理过期的API缓存，防止内存泄漏"""
    global API_CACHE
    while True:
        try:
            # 每隔一段时间执行一次清理
            time.sleep(CACHE_CLEANUP_INTERVAL)
            
            # 获取当前时间
            current_time = time.time()
            keys_to_remove = []
            
            # 加锁访问缓存
            with API_LOCK:
                # 查找过期的缓存项
                for key, cache_item in API_CACHE.items():
                    if current_time - cache_item['timestamp'] > CACHE_MAX_AGE:
                        keys_to_remove.append(key)
                
                # 移除过期的缓存项
                for key in keys_to_remove:
                    del API_CACHE[key]
                    # 同时也可以清理不再需要的锁
                    if key in API_LOCKS:
                        del API_LOCKS[key]
                        
            if keys_to_remove:
                logger.info(f"已清理 {len(keys_to_remove)} 个过期的API缓存项")
        except Exception as e:
            logger.error(f"清理缓存时发生错误: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

# 启动缓存清理线程
cache_cleanup_thread = threading.Thread(target=cleanup_expired_cache, daemon=True)
cache_cleanup_thread.start()
logger.info("已启动API缓存清理线程")

# 辅助函数：处理token数据
def process_token_data(token):
    """处理代币数据，添加额外的显示信息"""
    try:
        # 确保first_update_formatted有值
        first_update = token.get('first_update_formatted', '')
        if not first_update:
            first_update = token.get('first_update', '')
        
        # 处理首次更新时间，计算经过的天数
        days_since_first = None
        if first_update and isinstance(first_update, str):
            try:
                # 记录原始日期字符串，帮助调试
                logger.debug(f"处理首次更新时间: {first_update}")
                
                # 将ISO格式时间转换为datetime对象，确保有时区信息
                # 处理常见的ISO格式，确保Z被替换为+00:00
                if 'Z' in first_update:
                    first_update = first_update.replace('Z', '+00:00')
                elif 'T' in first_update and not any(x in first_update for x in ['+', '-', 'Z']):
                    # 如果有T分隔符但没有时区信息，添加UTC时区
                    first_update = first_update + '+00:00'
                
                # 尝试解析日期
                try:
                    dt = datetime.fromisoformat(first_update)
                except ValueError:
                    # 尝试其他格式
                    formats = [
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%d %H:%M",
                        "%Y-%m-%d"
                    ]
                    for fmt in formats:
                        try:
                            dt = datetime.strptime(first_update, fmt)
                            break
                        except ValueError:
                            continue
                    else:
                        raise ValueError(f"无法解析日期: {first_update}")
                
                logger.debug(f"转换后的datetime对象: {dt}, 时区信息: {dt.tzinfo}")
                
                # 确保dt是offset-aware的
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                    logger.debug(f"添加UTC时区后: {dt}")
                
                # 获取当前时间，确保也是offset-aware的
                now = datetime.now(timezone.utc)
                logger.debug(f"当前UTC时间: {now}")
                
                # 计算到当前时间的天数差
                delta = now - dt
                days_since_first = delta.days
                logger.debug(f"计算的天数差: {days_since_first}天")
                
                # 格式化首次推荐时间显示
                if days_since_first < 1:
                    # 不足一天显示 <1d
                    first_update_display = "<1d"
                else:
                    # 显示具体天数
                    first_update_display = f"{days_since_first}d"
                
                logger.debug(f"格式化后的首次推荐显示: {first_update_display}")
            except Exception as e:
                logger.warning(f"处理首次更新时间出错: {str(e)}, 原始值: {first_update}")
                # 如果无法解析日期，使用原始值
                first_update_display = first_update
        else:
            # 没有有效的首次更新时间
            first_update_display = '未知'
        
        # 获取原始市值数值，确保为数值类型
        market_cap_value = token.get('market_cap', 0)
        try:
            if isinstance(market_cap_value, str):
                market_cap_value = float(market_cap_value.replace(',', ''))
            else:
                market_cap_value = float(market_cap_value) if market_cap_value is not None else 0
        except:
            market_cap_value = 0
            
        # 获取交易量原始值，确保为数值类型
        volume_1h_value = token.get('volume_1h', 0)
        try:
            if isinstance(volume_1h_value, str):
                volume_1h_value = float(volume_1h_value.replace(',', ''))
            else:
                volume_1h_value = float(volume_1h_value) if volume_1h_value is not None else 0
        except:
            volume_1h_value = 0
            
        # 基础数据处理
        token_data = {
            'id': token.get('id', 0),  # 添加id字段
            'name': token.get('name', ''),
            'token_symbol': token.get('token_symbol', ''),  # 添加token_symbol字段
            'symbol': token.get('symbol', ''),
            'chain': token.get('chain', ''),
            'contract': token.get('contract', ''),
            'market_cap': market_cap_value,  # 保留原始数值，供前端JS处理
            'market_cap_formatted': format_market_cap(market_cap_value),  # 添加格式化后的市值
            'first_market_cap': token.get('first_market_cap', market_cap_value),  # 添加首次市值，如果没有则使用当前市值
            'price': format_number(token.get('price', 0)),
            'volume_1h': volume_1h_value,  # 保留原始值，供前端JS处理
            'volume_1h_formatted': _format_volume(volume_1h_value),  # 使用新的格式化函数
            'volume_24h': format_number(token.get('volume_24h', 0)),
            'holders': format_number(token.get('holders', 0)),
            'holders_count': token.get('holders_count', 0),  # 添加原始持有者数量
            'latest_update': token.get('latest_update', ''),
            'isSol': token.get('chain', '').upper() == 'SOL',
            'first_update_original': first_update or '未知',  # 保存原始首次更新时间
            'first_update_formatted': first_update_display,  # 使用新的首次推荐显示方式
            'days_since_first': days_since_first,  # 添加天数信息
            'buys_1h': token.get('buys_1h', 0),
            'sells_1h': token.get('sells_1h', 0),
            'community_reach': token.get('community_reach', 0),
            'spread_count': token.get('spread_count', 0),
            'change_pct_value': token.get('change_pct_value', 0),
            'change_percentage': token.get('change_percentage', '0.00%'),
            'image_url': token.get('image_url', ''),  # 添加图片URL
            'likes_count': token.get('likes_count', 0),  # 添加点赞数
            'first_update': token.get('first_update', ''),
        }
        
        # 添加社交链接
        token_data.update({
            'twitter': token.get('twitter', ''),
            'website': token.get('website', ''),
            'telegram': token.get('telegram', ''),
        })
        
        # 添加其他链接
        token_data.update({
            'dexscreener_url': get_dexscreener_url(token.get('chain', ''), token.get('contract', '')),
            'twitter_search_url': f"https://x.com/search?q=({token.get('name', '')}%20OR%20{token.get('contract', '')})&src=typed_query&f=live",
        })
        
        # 如果是Solana代币，添加特定链接
        if token.get('chain', '').upper() == 'SOL':
            token_data.update({
                'axiom_url': f"https://axiom.trade/meme/{token.get('contract', '')}",
                'pumpfun_url': f"https://pump.fun/coin/{token.get('contract', '')}",
            })
        
        # 添加通用链接
        token_data.update({
            'debot_url': f"https://debot.ai/token/{token.get('chain', '').lower()}/{token.get('contract', '')}",
            'gmgn_url': f"https://gmgn.ai/{token.get('chain', '').lower()}/token/{token.get('contract', '')}",
        })
        
        return token_data
    except Exception as e:
        logger.error(f"处理代币数据时出错: {str(e)}")
        return token

# 辅助函数：格式化数字
def format_number(value):
    """将数值格式化为带千位分隔符的字符串"""
    try:
        if value is None:
            return "0"
        return f"{float(value):,.2f}"
    except (ValueError, TypeError):
        return "0"

# 新增的后台更新函数
async def update_token_data_background(chain, contract, cache_key):
    """
    在后台更新代币数据
    
    Args:
        chain: 区块链标识
        contract: 代币合约
        cache_key: 缓存键
    """
    logger.info(f"开始后台更新代币数据: {chain}/{contract}")
    
    try:
        # 获取数据库适配器
        from src.database.db_factory import get_db_adapter
        db_adapter = get_db_adapter()
        
        # 获取当前代币数据
        token = None
        token_result = await db_adapter.execute_query(
            'tokens',
            'select',
            filters={
                'chain': chain,
                'contract': contract
            }
        )
        
        if isinstance(token_result, list) and len(token_result) > 0:
            token = token_result[0]
        else:
            logger.warning(f"后台更新: 未找到代币 {chain}/{contract}")
            return
        
        # 获取当前的市值和其他数据作为基准
        current_market_cap = token.get('market_cap')
        market_cap_1h = token.get('market_cap_1h')
        holders_count = token.get('holders_count')
        buys_1h = token.get('buys_1h')
        sells_1h = token.get('sells_1h')
        volume_1h = token.get('volume_1h')
        
        # 尝试通过DEX Screener API更新市场数据
        # 使用单独变量标记各种数据是否已经更新
        updated_market_cap = None
        updated_liquidity = None
        updated_holders_count = None
        updated_buys_1h = None
        updated_sells_1h = None
        updated_volume_1h = None
        updated_community_reach = None
        updated_spread_count = None
        
        # 更新市场数据和交易数据
        try:
            from src.api.dex_screener_api import get_token_pools
            from src.api.token_market_updater import _normalize_chain_id
            
            normalized_chain = _normalize_chain_id(chain)
            if normalized_chain:
                pools = get_token_pools(normalized_chain, contract)
                
                # 修正API返回数据的处理
                # API返回的是数组而不是包含'pairs'字段的对象
                if pools and isinstance(pools, list) and len(pools) > 0:
                    # 提取交易数据
                    buys_1h = 0
                    sells_1h = 0
                    volume_1h = 0
                    volume_24h = 0
                    
                    # 直接遍历返回的数组
                    for pair in pools:
                        # 提取1小时交易数据
                        if 'txns' in pair and 'h1' in pair['txns']:
                            h1_data = pair['txns']['h1']
                            buys_1h += h1_data.get('buys', 0)
                            sells_1h += h1_data.get('sells', 0)
                        
                        # 提取交易量数据
                        if 'volume' in pair:
                            if 'h1' in pair['volume']:
                                volume_1h += float(pair['volume']['h1'] or 0)
                            if 'h24' in pair['volume']:
                                volume_24h += float(pair['volume']['h24'] or 0)
                    
                    # 更新交易数据
                    txn_data = {}
                    if buys_1h > 0:
                        txn_data['buys_1h'] = buys_1h
                    if sells_1h > 0:
                        txn_data['sells_1h'] = sells_1h
                    if volume_1h > 0:
                        txn_data['volume_1h'] = volume_1h
                    if volume_24h > 0:
                        txn_data['volume_24h'] = volume_24h
                    
                    if txn_data:
                        txn_result = await db_adapter.execute_query(
                            'tokens',
                            'update',
                            data=txn_data,
                            filters={'chain': chain, 'contract': contract}
                        )
                        txn_updated = True
                        logger.info(f"成功更新 {token.get('token_symbol')} 的交易数据")
                # 记录API结果调试信息
                else:
                    logger.warning(f"从DEX API获取到的数据格式不正确或为空: {pools}")
                    if pools:
                        logger.debug(f"API返回类型: {type(pools)}, 内容: {pools[:200]}...")
        except Exception as e:
            logger.error(f"后台更新: 获取代币市场数据时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
        
        # 计算价格变化百分比
        if current_market_cap is not None and market_cap_1h is not None and market_cap_1h > 0:
            try:
                change_pct = (float(current_market_cap) - float(market_cap_1h)) / float(market_cap_1h) * 100
                change_percentage = f"{change_pct:+.2f}%"
                logger.info(f"后台更新: 计算涨跌幅: {change_pct:+.2f}%")
                
                # 保存计算结果到数据库专用字段，确保数据类型正确
                updated_data = {}
                updated_data['last_calculation_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # timestamp类型
            except Exception as e:
                logger.error(f"后台更新: 计算涨跌幅时出错: {str(e)}")
        
        # 准备更新数据
        updated_data = {}

        # 更改：确保正确处理market_cap和market_cap_1h的更新
        if updated_market_cap is not None and updated_market_cap > 0:
            # 保存当前市值到market_cap_1h字段，然后更新market_cap为新值
            updated_data['market_cap_1h'] = current_market_cap
            updated_data['market_cap'] = updated_market_cap
            updated_data['market_cap_formatted'] = _format_market_cap(updated_market_cap)  # 同时更新格式化的市值
            logger.info(f"后台更新: 从 {current_market_cap} 更新市值为 {updated_market_cap}, 旧值保存到market_cap_1h")
        
        # 更新其他字段（如果有新数据）
        if updated_holders_count is not None:
            updated_data['holders_count'] = updated_holders_count
        
        if updated_buys_1h is not None:
            updated_data['buys_1h'] = updated_buys_1h
        
        if updated_sells_1h is not None:
            updated_data['sells_1h'] = updated_sells_1h
        
        if updated_volume_1h is not None:
            updated_data['volume_1h'] = updated_volume_1h
        
        if updated_liquidity is not None:
            updated_data['liquidity'] = updated_liquidity
            
        # 更新社群覆盖数据
        if updated_community_reach is not None:
            updated_data['community_reach'] = updated_community_reach
        
        if updated_spread_count is not None:
            updated_data['spread_count'] = updated_spread_count
        
        # 更新最后更新时间
        updated_data['latest_update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 执行数据库更新
        if updated_data:
            # 只更新主表数据，不再记录任何历史数据
            update_result = await db_adapter.execute_query(
                'tokens',
                'update',
                data=updated_data,
                filters={
                    'chain': chain,
                    'contract': contract
                }
            )
            if isinstance(update_result, dict) and update_result.get('error'):
                logger.error(f"后台更新: 更新数据库失败: {update_result.get('error')}")
            else:
                logger.info(f"后台更新: 成功更新代币数据: {chain}/{contract}")
                if cache_key in API_CACHE:
                    with API_LOCK:
                        processed_token = process_token_data({**token, **updated_data})
                        API_CACHE[cache_key]['data']['token'].update(processed_token)
                        API_CACHE[cache_key]['timestamp'] = time.time()
                    logger.info(f"后台更新: 更新了API缓存数据: {cache_key}")
        else:
            logger.info(f"后台更新: 没有需要更新的数据: {chain}/{contract}")
    except Exception as e:
        logger.error(f"后台更新: 更新过程中发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

# 格式化市值的辅助函数
def _format_market_cap(market_cap: float) -> str:
    """格式化市值显示"""
    if market_cap is None:
        return "N/A"
    
    if market_cap >= 1000000000:  # 十亿 (B)
        return f"${market_cap/1000000000:.2f}B"
    elif market_cap >= 1000000:   # 百万 (M)
        return f"${market_cap/1000000:.2f}M"
    elif market_cap >= 1000:      # 千 (K)
        return f"${market_cap/1000:.2f}K"
    return f"${market_cap:.2f}"

# 新增：格式化交易量的辅助函数
def _format_volume(volume: float) -> str:
    """格式化交易量显示，与前端formatVolume保持一致"""
    if volume is None or volume == 0:
        return "$0.00"
    
    if volume >= 1000000000:  # 十亿 (B)
        return f"${volume/1000000000:.2f}B"
    elif volume >= 1000000:   # 百万 (M)
        return f"${volume/1000000:.2f}M"
    elif volume >= 1000:      # 千 (K)
        return f"${volume/1000:.2f}K"
    return f"${volume:.2f}"

@app.route('/api/refresh_tokens', methods=['POST'])
@async_route
async def api_refresh_tokens():
    """强制刷新所有代币数据或特定代币数据"""
    try:
        # 获取请求参数
        data = request.get_json()
        if data is None:
            data = {}  # 如果请求体为空，使用空字典
            
        chain = data.get('chain')
        contract = data.get('contract')
        
        logger.info(f"收到刷新代币请求: data={data}")
        
        # 使用数据库适配器
        from src.database.db_factory import get_db_adapter
        db_adapter = get_db_adapter()
        
        # 记录刷新请求
        if chain and contract:
            logger.info(f"刷新特定代币: {chain}/{contract}")
            filters = {
                'chain': chain,
                'contract': contract
            }
        else:
            logger.info("刷新所有代币")
            filters = {}
        
        try:
            # 获取token数据
            tokens = await db_adapter.execute_query('tokens', 'select', filters=filters)
            
            if not tokens or not isinstance(tokens, list):
                logger.warning("未找到代币数据")
                return jsonify({"success": False, "error": "未找到代币数据"})
                
            logger.info(f"找到 {len(tokens)} 个代币需要刷新")
            
            # 启动实际的数据更新任务
            update_tasks = []
            for token in tokens:
                token_chain = token.get('chain')
                token_contract = token.get('contract')
                if token_chain and token_contract:
                    # 为每个代币创建缓存键
                    cache_key = f"{token_chain}_{token_contract}"
                    # 先清除缓存
                    with API_LOCK:
                        if cache_key in API_CACHE:
                            del API_CACHE[cache_key]
                            logger.info(f"已清除代币缓存: {cache_key}")
                    
                    # 实际更新代币数据（从DEX Screener等获取最新数据）
                    update_task = asyncio.create_task(
                        update_token_data_background(token_chain, token_contract, cache_key)
                    )
                    update_tasks.append(update_task)
                    logger.info(f"已启动代币数据更新任务: {token_chain}/{token_contract}")
            
            # 等待所有更新任务初始化（但不等待完成）
            await asyncio.sleep(0.1)
            
            # 重新处理token数据用于响应
            processed_tokens = []
            for token in tokens:
                try:
                    processed_token = process_token_data(token)
                    processed_tokens.append(processed_token)
                except Exception as e:
                    logger.error(f"处理代币时出错: {str(e)}")
                    continue
            
            logger.info(f"成功启动 {len(update_tasks)} 个代币更新任务")
            return jsonify({
                "success": True,
                "message": "数据更新中，请稍后刷新页面查看最新数据",
                "count": len(processed_tokens),
                "data": processed_tokens if len(processed_tokens) < 10 else f"共更新{len(processed_tokens)}个代币"
            })
            
        except Exception as e:
            logger.error(f"执行数据库查询时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return jsonify({"success": False, "error": f"数据库操作失败: {str(e)}"})
        
    except Exception as e:
        logger.error(f"刷新代币数据时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({"success": False, "error": f"处理请求时出错: {str(e)}"})

@app.route('/api/token_detail/<chain>/<contract>')
@async_route
async def api_token_detail(chain, contract):
    """获取代币详细信息API，包括基本信息、市场数据和提及历史"""
    try:
        logger.info(f"获取代币详细信息: {chain}/{contract}")
        
        # 设置响应超时控制
        start_time = time.time()
        timeout_seconds = 20  # 整个函数的最大执行时间
        
        # 检查缓存
        cache_key = f"detail_{chain}_{contract}"
        force_refresh = request.args.get('refresh', '0') == '1'
        current_time = time.time()
        
        # 强制清除所有可能的缓存键，确保不使用旧缓存
        with API_LOCK:
            # 清除可能的不同格式的缓存键
            possible_keys = [
                f"detail_{chain}_{contract}",
                f"token_detail:{chain}:{contract}",
                f"{chain}_{contract}"
            ]
            for key in possible_keys:
                if key in API_CACHE:
                    logger.info(f"清除缓存: {key}")
                    del API_CACHE[key]
        
        # 使用数据库适配器获取数据
        from src.database.db_factory import get_db_adapter
        db_adapter = get_db_adapter()
        logger.info(f"已获取数据库适配器: {db_adapter.__class__.__name__}")
        
        # 获取代币基本信息
        logger.info(f"正在查询代币基本信息: chain={chain}, contract={contract}")
        try:
            # 添加超时控制
            token_result = await asyncio.wait_for(
                db_adapter.execute_query(
                    'tokens',
                    'select',
                    filters={'chain': chain, 'contract': contract},
                    limit=1
                ),
                timeout=10  # 最多等待10秒
            )
        except asyncio.TimeoutError:
            logger.error(f"查询代币基本信息超时: {chain}/{contract}")
            return jsonify({
                "success": False, 
                "error": "查询代币信息超时，请稍后重试",
                "timeout": True
            }), 408
        
        logger.info(f"代币查询结果: {token_result != None}, 类型: {type(token_result)}, 长度: {len(token_result) if token_result else 0}")
        
        token = token_result[0] if token_result and len(token_result) > 0 else None
        
        if not token:
            logger.warning(f"未找到代币: {chain}/{contract}")
            return jsonify({"success": False, "error": f"未找到代币 {chain}/{contract}"}), 404
        
        # 处理token数据，确保格式一致
        processed_token = process_token_data(token)
        logger.info(f"已处理token数据: {processed_token.get('token_symbol')}")
        
        # 检查是否已经接近超时阈值，如果是，直接返回简化结果
        if time.time() - start_time > timeout_seconds * 0.6:
            logger.warning(f"处理时间过长，返回简化结果: {time.time() - start_time:.2f}秒")
            return jsonify({
                "success": True,
                "token": processed_token,
                "mention_history": [],
                "channel_stats": [],
                "market_cap_history": [],
                "timestamp": time.time(),
                "simplified": True,
                "elapsed_time": time.time() - start_time
            })
        
        # 获取代币提及历史
        logger.info(f"正在查询代币提及历史")
        try:
            # 添加超时控制
            mentions_result = await asyncio.wait_for(
                db_adapter.execute_query(
                    'tokens_mark',
                    'select',
                    filters={'chain': chain, 'contract': contract},
                    order_by={'mention_time': 'desc'},
                    limit=20
                ),
                timeout=6  # 最多等待6秒
            )
        except asyncio.TimeoutError:
            logger.error(f"查询提及历史超时: {chain}/{contract}")
            # 返回简化结果，只包含基本token信息
            return jsonify({
                "success": True,
                "token": processed_token,
                "mention_history": [],
                "channel_stats": [],
                "market_cap_history": [],
                "timestamp": time.time(),
                "timeout": True
            })
        
        logger.info(f"提及历史查询结果: {mentions_result != None}, 长度: {len(mentions_result) if mentions_result else 0}")
        
        mentions = mentions_result if mentions_result else []
        
        # 格式化提及历史数据
        mention_history = []
        channel_stats = {}  # 用于统计各频道的提及情况
        channel_cache = {}  # 缓存频道信息，避免重复查询
        
        # 限制处理的记录数量
        max_mentions = min(10, len(mentions))
        logger.info(f"将处理前 {max_mentions} 条提及记录")
        
        for i in range(max_mentions):
            mention = mentions[i]
            channel_id = mention.get('channel_id')
            mention_time = mention.get('mention_time')
            market_cap = mention.get('market_cap')
            message_id = mention.get('message_id')
            
            # 检查是否已接近超时
            if time.time() - start_time > timeout_seconds * 0.8:
                logger.warning(f"处理提及记录时间过长，提前中断: {time.time() - start_time:.2f}秒")
                break
            
            if channel_id and mention_time:
                try:
                    # 优先从缓存中获取频道信息
                    if channel_id in channel_cache:
                        channel = channel_cache[channel_id]
                        channel_name = channel.get('channel_name') if channel else '未知频道'
                        member_count = channel.get('member_count') if channel else 0
                    else:
                        # 获取频道信息
                        try:
                            channel_result = await asyncio.wait_for(
                                db_adapter.execute_query(
                                    'telegram_channels',
                                    'select',
                                    filters={'channel_id': channel_id},
                                    limit=1
                                ),
                                timeout=3  # 每个频道查询最多3秒
                            )
                            
                            channel = channel_result[0] if channel_result and len(channel_result) > 0 else None
                            channel_cache[channel_id] = channel  # 缓存频道信息
                            
                            channel_name = channel.get('channel_name') if channel else '未知频道'
                            member_count = channel.get('member_count') if channel else 0
                        except asyncio.TimeoutError:
                            logger.warning(f"查询频道信息超时: {channel_id}")
                            channel_name = '未知频道'
                            member_count = 0
                    
                    # 添加到历史记录
                    mention_history.append({
                        'mention_time': mention_time,
                        'market_cap': market_cap,
                        'market_cap_formatted': format_market_cap(market_cap) if market_cap else '$0',
                        'channel_id': channel_id,
                        'channel_name': channel_name,
                        'member_count': member_count,
                        'message_id': message_id,
                        'chain': chain
                    })
                    
                    # 更新频道统计
                    if channel_id not in channel_stats:
                        channel_stats[channel_id] = {
                            'channel_id': channel_id,
                            'channel_name': channel_name,
                            'member_count': member_count,
                            'mention_count': 0,
                            'first_mention_time': None,
                            'first_market_cap': None
                        }
                    
                    # 增加提及次数
                    channel_stats[channel_id]['mention_count'] += 1
                    
                    # 更新首次提及时间和市值
                    if not channel_stats[channel_id]['first_mention_time'] or mention_time < channel_stats[channel_id]['first_mention_time']:
                        channel_stats[channel_id]['first_mention_time'] = mention_time
                        channel_stats[channel_id]['first_market_cap'] = market_cap
                except Exception as e:
                    logger.error(f"处理频道提及数据错误: {str(e)}")
                    continue
        
        # 检查是否已接近超时
        if time.time() - start_time > timeout_seconds * 0.85:
            logger.warning(f"市值历史处理前已接近超时: {time.time() - start_time:.2f}秒")
            # 构建简化响应数据，不处理市值历史
            response_data = {
                "success": True,
                "token": processed_token,
                "mention_history": mention_history,
                "channel_stats": list(channel_stats.values()),
                "market_cap_history": [],
                "timestamp": time.time(),
                "simplified": True,
                "elapsed_time": time.time() - start_time
            }
            
            logger.info(f"由于接近超时，返回简化数据，不包含市值历史")
            return jsonify(response_data)
        
        # 构建市值历史数据，用于绘制图表
        logger.info("构建市值历史数据")
        
        # 确保所有提及历史记录都有有效的mention_time
        valid_mentions = []
        for mention in mention_history:
            if mention.get('mention_time'):
                valid_mentions.append(mention)
            else:
                logger.warning(f"跳过无效的提及记录: 缺少mention_time")
        
        # 按时间排序
        try:
            # 确保所有mention_time都是相同的格式
            for mention in valid_mentions:
                if isinstance(mention['mention_time'], str):
                    try:
                        # 尝试转换为datetime对象以确保格式一致
                        dt = datetime.fromisoformat(mention['mention_time'].replace('Z', '+00:00'))
                        mention['mention_time'] = dt.isoformat()
                    except ValueError:
                        logger.warning(f"无法解析时间格式: {mention['mention_time']}")
            
            valid_mentions.sort(key=lambda x: x['mention_time'])
            logger.info(f"已排序 {len(valid_mentions)} 条提及历史数据")
        except Exception as e:
            logger.error(f"排序提及历史时出错: {str(e)}")
            # 出错时不排序，保持原始顺序
        
        # 提取市值历史数据
        market_cap_history = []
        for mention in valid_mentions:
            if mention.get('market_cap'):
                try:
                    market_cap_history.append({
                        'time': mention['mention_time'],
                        'value': float(mention['market_cap'])
                    })
                except (ValueError, TypeError) as e:
                    logger.warning(f"处理市值历史数据出错: {str(e)}, market_cap={mention.get('market_cap')}")
        
        logger.info(f"生成了 {len(market_cap_history)} 条市值历史数据")
        
        # 记录总处理时间
        elapsed_time = time.time() - start_time
        logger.info(f"总处理时间: {elapsed_time:.2f}秒")
        
        # 构建响应数据
        response_data = {
            "success": True,
            "token": processed_token,
            "mention_history": valid_mentions,  # 使用有效的提及历史记录
            "channel_stats": list(channel_stats.values()),
            "market_cap_history": market_cap_history,
            "timestamp": time.time(),
            "elapsed_time": elapsed_time
        }
        
        # 更新缓存
        with API_LOCK:
            API_CACHE[cache_key] = {
                'data': response_data,
                'timestamp': time.time()
            }
            logger.info(f"已更新缓存: {cache_key}")
        
        logger.info("准备返回token详情数据")
        return jsonify(response_data)
    
    except asyncio.CancelledError:
        logger.error(f"请求被取消: {chain}/{contract}")
        return jsonify({
            "success": False, 
            "error": "请求被取消，可能是由于客户端断开连接",
            "cancelled": True
        }), 499
        
    except Exception as e:
        logger.error(f"获取代币详情时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({
            "success": False, 
            "error": f"获取代币详情时出错: {str(e)}",
            "error_traceback": traceback.format_exc() if app.debug else None
        }), 500

@app.route('/api/refresh_token/<chain>/<contract>', methods=['POST'])
@async_route
async def api_refresh_token(chain, contract):
    """
    刷新单个代币的所有数据
    包括：市值、价格、交易量、持有者数量等
    """
    logger.info(f"接收到刷新代币请求: {chain}/{contract}")
    
    try:
        # 获取数据库适配器
        from src.database.db_factory import get_db_adapter
        db_adapter = get_db_adapter()
        
        # 获取代币当前数据，用于后续比较和日志
        token_data = await db_adapter.execute_query(
            'tokens',
            'select',
            filters={'chain': chain, 'contract': contract},
            limit=1
        )
        
        if not token_data or len(token_data) == 0:
            return jsonify({
                "success": False,
                "error": "未找到代币",
                "chain": chain,
                "contract": contract
            }), 404
            
        token = token_data[0]
        token_symbol = token.get('token_symbol', '未知代币')
        current_market_cap = token.get('market_cap', 0)
        
        logger.info(f"开始全面刷新代币 {token_symbol} ({chain}/{contract})")
        
        # 1. 更新市场数据（市值、价格等）
        market_updated = False
        market_error = None
        try:
            from src.api.token_market_updater import update_token_market_data_async
            market_result = await update_token_market_data_async(chain, contract)
            
            # 检查是否返回deleted标志，表示代币已被删除
            if isinstance(market_result, dict) and market_result.get('deleted', False):
                logger.info(f"代币 {token_symbol} ({chain}/{contract}) 在DEX上不存在，已被删除")
                deleted_info = market_result.get('deleted_info', {})
                
                # 返回已删除的响应
                return jsonify({
                    "success": True,
                    "deleted": True,
                    "token_symbol": token_symbol,
                    "chain": chain,
                    "contract": contract,
                    "message": "代币在DEX上不存在，已从数据库中删除",
                    "deleted_info": deleted_info
                })
                
            if isinstance(market_result, dict) and not market_result.get('error'):
                market_updated = True
                logger.info(f"成功更新 {token_symbol} 的市场数据")
            else:
                market_error = market_result.get('error', '更新市场数据失败')
                logger.error(f"更新 {token_symbol} 市场数据失败: {market_error}")
        except Exception as e:
            market_error = str(e)
            logger.error(f"更新 {token_symbol} 市场数据时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            
        # 2. 更新交易数据（交易量、买卖等）
        txn_updated = False
        txn_error = None
        try:
            # 使用DEX Screener API获取交易数据
            from src.api.dex_screener_api import get_token_pools
            from src.api.token_market_updater import _normalize_chain_id
            
            normalized_chain = _normalize_chain_id(chain)
            if normalized_chain:
                pools = get_token_pools(normalized_chain, contract)
                
                # 检查是否返回空结果，表示代币不存在
                if not pools or (isinstance(pools, list) and len(pools) == 0):
                    logger.warning(f"DEX API返回空结果，代币 {token_symbol} ({chain}/{contract}) 可能不存在")
                    
                    # 调用删除函数
                    try:
                        from src.api.token_market_updater import delete_token_data
                        logger.info(f"代币 {token_symbol} ({chain}/{contract}) 在DEX上不存在，将从数据库中删除")
                        
                        delete_result = await delete_token_data(chain, contract, double_check=True)
                        if delete_result['success']:
                            deleted_info = delete_result.get('deleted_token_data', {})
                            logger.info(f"成功删除无效代币 {token_symbol} ({chain}/{contract}) 及其相关数据")
                            
                            # 返回已删除的响应
                            return jsonify({
                                "success": True,
                                "deleted": True,
                                "token_symbol": token_symbol,
                                "chain": chain,
                                "contract": contract,
                                "message": "代币在DEX上不存在，已从数据库中删除",
                                "deleted_info": deleted_info
                            })
                        else:
                            logger.error(f"删除无效代币失败: {delete_result.get('error', '未知错误')}")
                            txn_error = f"代币在DEX上不存在，删除失败: {delete_result.get('error', '未知错误')}"
                    except Exception as delete_error:
                        logger.error(f"删除无效代币时出错: {str(delete_error)}")
                        txn_error = f"代币在DEX上不存在，删除时出错: {str(delete_error)}"
                
                # 修正API返回数据的处理
                # API返回的是数组而不是包含'pairs'字段的对象
                elif pools and isinstance(pools, list) and len(pools) > 0:
                    # 提取交易数据
                    buys_1h = 0
                    sells_1h = 0
                    volume_1h = 0
                    volume_24h = 0
                    
                    # 直接遍历返回的数组
                    for pair in pools:
                        # 提取1小时交易数据
                        if 'txns' in pair and 'h1' in pair['txns']:
                            h1_data = pair['txns']['h1']
                            buys_1h += h1_data.get('buys', 0)
                            sells_1h += h1_data.get('sells', 0)
                        
                        # 提取交易量数据
                        if 'volume' in pair:
                            if 'h1' in pair['volume']:
                                volume_1h += float(pair['volume']['h1'] or 0)
                            if 'h24' in pair['volume']:
                                volume_24h += float(pair['volume']['h24'] or 0)
                    
                    # 更新交易数据
                    txn_data = {}
                    if buys_1h > 0:
                        txn_data['buys_1h'] = buys_1h
                    if sells_1h > 0:
                        txn_data['sells_1h'] = sells_1h
                    if volume_1h > 0:
                        txn_data['volume_1h'] = volume_1h
                    if volume_24h > 0:
                        txn_data['volume_24h'] = volume_24h
                    
                    if txn_data:
                        txn_result = await db_adapter.execute_query(
                            'tokens',
                            'update',
                            data=txn_data,
                            filters={'chain': chain, 'contract': contract}
                        )
                        txn_updated = True
                        logger.info(f"成功更新 {token_symbol} 的交易数据")
                # 记录API结果调试信息
                else:
                    logger.warning(f"从DEX API获取到的数据格式不正确或为空: {pools}")
                    if pools:
                        logger.debug(f"API返回类型: {type(pools)}, 内容: {pools[:200]}...")
            else:
                txn_error = f"不支持的链: {chain}"
                logger.warning(txn_error)
        except Exception as e:
            txn_error = str(e)
            logger.error(f"更新 {token_symbol} 交易数据时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            
        # 3. 更新社区数据（传播次数、社区覆盖等）
        community_updated = False
        community_error = None
        try:
            # 先获取token_id
            token_id = token.get('id')
            if token_id:
                # 导入社区数据更新函数
                try:
                    from src.utils.update_reach import update_token_community_reach_async
                    community_result = await update_token_community_reach_async(token_symbol)
                    
                    if community_result.get('success', False):
                        community_updated = True
                        logger.info(f"成功更新 {token_symbol} 的社区数据")
                    else:
                        community_error = community_result.get('error', '更新社区数据失败')
                        logger.warning(f"更新 {token_symbol} 社区数据失败: {community_error}")
                except ImportError as e:
                    community_error = f"社区数据更新模块不可用: {str(e)}"
                    logger.error(f"导入社区数据更新函数失败: {str(e)}")
            else:
                community_error = "未找到token_id，无法更新社区数据"
                logger.warning(f"未找到 {token_symbol} 的ID，跳过社区数据更新")
        except Exception as e:
            community_error = str(e)
            logger.error(f"更新 {token_symbol} 社区数据时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
        
        # 获取更新后的代币数据
        updated_token_data = await db_adapter.execute_query(
            'tokens',
            'select',
            filters={'chain': chain, 'contract': contract},
            limit=1
        )
        
        updated_token = updated_token_data[0] if updated_token_data and len(updated_token_data) > 0 else {}
        
        # 确保社区数据被正确包含在返回结果中
        if community_updated and community_result and community_result.get('success'):
            # 显式设置社区数据字段，确保它们被包含在返回数据中
            updated_token['community_reach'] = community_result.get('community_reach', updated_token.get('community_reach', 0))
            updated_token['spread_count'] = community_result.get('spread_count', updated_token.get('spread_count', 0))
            logger.info(f"已将社区数据添加到响应中: 社群覆盖={updated_token['community_reach']}, 传播次数={updated_token['spread_count']}")
        
        # 检查市值是否更新
        new_market_cap = updated_token.get('market_cap')
        market_cap_change = 0
        if current_market_cap is not None and new_market_cap is not None and current_market_cap > 0 and new_market_cap > 0:
            market_cap_change = ((new_market_cap - current_market_cap) / current_market_cap) * 100
            logger.info(f"{token_symbol} 市值变化: {current_market_cap} -> {new_market_cap} ({market_cap_change:+.2f}%)")
        
        # 确保保留之前的涨跌幅数据，如果没有新数据
        if 'change_pct_value' not in updated_token or updated_token.get('change_pct_value') == 0:
            updated_token['change_pct_value'] = token.get('change_pct_value', 0)
            updated_token['change_percentage'] = token.get('change_percentage', '0.00%')
            logger.info(f"保留 {token_symbol} 之前的涨跌幅数据: {updated_token['change_percentage']}")
        
        # 计算新的涨跌幅并添加到返回数据中（如果有市值和1小时前市值）
        market_cap_1h = updated_token.get('market_cap_1h')
        market_cap = updated_token.get('market_cap')
        if market_cap is not None and market_cap_1h is not None and market_cap > 0 and market_cap_1h > 0:
            # 计算涨跌幅
            change_pct = ((market_cap - market_cap_1h) / market_cap_1h) * 100
            # 更新到token数据中
            updated_token['change_pct_value'] = change_pct
            updated_token['change_percentage'] = f"{'+' if change_pct > 0 else ''}{change_pct:.2f}%"
            logger.info(f"计算 {token_symbol} 的新涨跌幅: {updated_token['change_percentage']}")
        else:
            logger.info(f"无法计算涨跌幅: market_cap={market_cap}, market_cap_1h={market_cap_1h}")
        
        # 确保保留之前的交易量数据，如果没有新数据
        if 'volume_1h' not in updated_token or updated_token.get('volume_1h', 0) == 0:
            updated_token['volume_1h'] = token.get('volume_1h', 0)
            updated_token['volume_1h_formatted'] = token.get('volume_1h_formatted', '$0.00')
            logger.info(f"保留 {token_symbol} 之前的交易量数据: {updated_token['volume_1h_formatted']}")
        else:
            # 格式化交易量数据
            volume_1h = updated_token.get('volume_1h')
            # 确保volume_1h不是None且大于0
            if volume_1h is not None and volume_1h > 0:
                updated_token['volume_1h_formatted'] = _format_volume(volume_1h)
                logger.info(f"格式化 {token_symbol} 的新交易量: {updated_token['volume_1h_formatted']}")
            else:
                # 如果是None或0，设置默认格式化值
                updated_token['volume_1h'] = 0
                updated_token['volume_1h_formatted'] = '$0.00'
                logger.info(f"交易量数据为空或为0，设置默认值")
            
        # 更新缓存数据
        cache_key = f"token_detail:{chain}:{contract}"
        with API_LOCK:
            if cache_key in API_CACHE:
                API_CACHE[cache_key]['data'] = updated_token
                API_CACHE[cache_key]['timestamp'] = time.time()
                logger.info(f"已更新缓存: {cache_key}")
        
        # 返回处理结果
        return jsonify({
            "success": market_updated or txn_updated or community_updated,
            "token": updated_token,
            "refresh_results": {
                "market": {
                    "updated": market_updated,
                    "error": market_error
                },
                "transaction": {
                    "updated": txn_updated,
                    "error": txn_error
                },
                "community": {
                    "updated": community_updated,
                    "error": community_error
                }
            },
            "market_cap_change": {
                "previous": current_market_cap,
                "current": new_market_cap,
                "percent": market_cap_change
            },
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"刷新代币数据时发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({
            "success": False,
            "error": str(e),
            "chain": chain,
            "contract": contract
        }), 500

if __name__ == '__main__':
    # 确保必要的目录存在
    os.makedirs('./logs', exist_ok=True)
    os.makedirs('./data', exist_ok=True)
    os.makedirs('./media', exist_ok=True)
    
    # 确保静态资源目录存在
    static_img_dir = os.path.join(app.static_folder, 'img')
    os.makedirs(static_img_dir, exist_ok=True)
    
    # 创建默认的图像文件（如果不存在）
    default_img_path = os.path.join(static_img_dir, 'image-not-found.png')
    if not os.path.exists(default_img_path):
        logger.info(f"默认图像文件不存在，将创建一个简单的占位图像: {default_img_path}")
        try:
            # 这里可以添加代码来创建一个简单的图像
            # 或者显示一条消息，提醒用户需要手动添加图像
            logger.warning("请手动添加默认图像文件: image-not-found.png")
        except Exception as e:
            logger.error(f"处理默认图像时出错: {str(e)}")
    
    # 临时方案：无论什么系统都只用http，Linux下启用https的代码已注释
    # if platform.system().lower() == 'linux':
    #     # Linux环境，启用自签名证书
    #     ssl_context = ('/home/ubuntu/certs/server.crt', '/home/ubuntu/certs/server.key')
    #     logger.info('Linux环境，使用HTTPS启动Flask')
    #     app.run(host='0.0.0.0', port=5000, debug=True, ssl_context=ssl_context)
    # else:
    #     # Windows等其它环境，保持http
    #     logger.info('非Linux环境，使用HTTP启动Flask')
    app.run(host='0.0.0.0', port=5000, debug=True) 