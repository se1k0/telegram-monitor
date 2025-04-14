import logging
import json
import time
import os
import multiprocessing
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from flask import Flask, render_template, jsonify, request, redirect, url_for, flash, send_from_directory, abort, session
from flask_cors import CORS
from dotenv import load_dotenv
from src.database.models import Token, Message, TelegramChannel, TokensMark
from functools import wraps
import threading
import asyncio

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
        # 使用Supabase适配器
        from src.database.db_factory import get_db_adapter
        from src.core.channel_manager import ChannelManager
        db_adapter = get_db_adapter()
        
        # 使用ChannelManager获取活跃频道
        channel_manager = ChannelManager()
        channels = channel_manager.get_active_channels()
        active_channels_count = len(channels) if channels else 0
        
        # 使用Supabase适配器获取消息和代币数量
        try:
            # 获取代币数量
            from supabase import create_client
            supabase_url = config.SUPABASE_URL
            supabase_key = config.SUPABASE_KEY
            supabase = create_client(supabase_url, supabase_key)
            
            # 获取代币数量
            tokens_count_response = supabase.table('tokens').select('id', count='exact').execute()
            token_count = tokens_count_response.count if hasattr(tokens_count_response, 'count') else 0
            
            # 获取消息数量
            messages_count_response = supabase.table('messages').select('id', count='exact').execute()
            message_count = messages_count_response.count if hasattr(messages_count_response, 'count') else 0
            
            # 获取最后更新时间
            last_update_response = supabase.table('tokens').select('latest_update').order('id', desc=True).limit(1).execute()
            last_update = last_update_response.data[0]['latest_update'] if hasattr(last_update_response, 'data') and last_update_response.data else "未知"
        except Exception as e:
            logger.error(f"获取Supabase数据统计时出错: {str(e)}")
            token_count = 0
            message_count = 0
            last_update = "未知"
        
        return {
            'active_channels_count': active_channels_count,
            'message_count': message_count,
            'token_count': token_count,
            'last_update': last_update,
            'channels': channels or [],
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
                    # 构建查询检索比last_id更新的token
                    new_tokens_query = supabase.table('tokens').select('*')
                    
                    # 应用筛选条件
                    if chain_filter and chain_filter.lower() != 'all':
                        new_tokens_query = new_tokens_query.eq('chain', chain_filter)
                        
                    # 应用搜索条件
                    if search_query:
                        new_tokens_query = new_tokens_query.or_(f"token_symbol.ilike.%{search_query}%,contract.ilike.%{search_query}%")
                    
                    # 如果有last_id，查询比它更新的token
                    if last_id and last_id.isdigit():
                        new_tokens_query = new_tokens_query.gt('id', int(last_id))
                    
                    # 按最新更新时间排序并限制数量
                    new_tokens_query = new_tokens_query.order('id', desc=True).limit(10)
                    
                    # 执行查询
                    new_tokens_response = new_tokens_query.execute()
                    
                    # 处理新token数据
                    new_tokens = []
                    if hasattr(new_tokens_response, 'data'):
                        for token in new_tokens_response.data:
                            if token is None:
                                continue
                                
                            # 处理token数据，与原代码相同
                            processed_token = process_token_data(token)
                            new_tokens.append(processed_token)
                    
                    # 返回JSON响应
                    return jsonify({
                        'success': True,
                        'new_tokens': new_tokens
                    })
            except Exception as e:
                logger.error(f"AJAX请求处理出错: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                return jsonify({"success": False, "error": str(e)})
        
        # 仅获取可用的链信息，不预加载代币数据
        try:
            from supabase import create_client
            
            supabase_url = config.SUPABASE_URL
            supabase_key = config.SUPABASE_KEY
            
            if supabase_url and supabase_key:
                # 创建 Supabase 客户端
                supabase = create_client(supabase_url, supabase_key)
                
                # 仅获取可用的链信息
                chains_response = supabase.table('tokens').select('chain').execute()
                available_chains = []
                if hasattr(chains_response, 'data'):
                    # 提取唯一的链名称
                    chain_values = [item.get('chain') for item in chains_response.data if item.get('chain')]
                    available_chains = list(set(chain_values))
                else:
                    available_chains = ['ETH', 'BSC', 'SOL']  # 默认值
            else:
                logger.warning("缺少 SUPABASE_URL 或 SUPABASE_KEY 配置，使用默认链列表")
                available_chains = ['ETH', 'BSC', 'SOL']  # 默认值
                
        except Exception as e:
            logger.warning(f"获取链数据时出错，使用默认值: {str(e)}")
            available_chains = ['ETH', 'BSC', 'SOL']  # 默认值
        
        # 立即渲染模板，不预加载代币数据
        return render_template(
            'index.html',
            tokens=[],  # 传递空列表，不预加载数据
            available_chains=available_chains,
            chain_filter=chain_filter,
            search_query=search_query,
            year=datetime.now().year
        )
            
    except Exception as e:
        logger.error(f"首页渲染时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return handle_error(f"页面加载出错: {str(e)}", 500)


@app.route('/api/tokens/stream')
def stream_tokens():
    """流式获取代币数据的API，每次返回一条代币数据"""
    try:
        # 获取查询参数
        chain_filter = request.args.get('chain', 'all')
        search_query = request.args.get('search', '')
        offset = request.args.get('offset', 0, type=int)
        batch_size = request.args.get('batch_size', 5, type=int)  # 每次获取的数量，默认5条
        last_id = request.args.get('last_id', 0, type=int)  # 上一次请求的最后一个token ID
        
        logger.info(f"流式获取代币数据: offset={offset}, batch_size={batch_size}, last_id={last_id}")
        
        # 使用Supabase获取数据
        from supabase import create_client
        
        supabase_url = config.SUPABASE_URL
        supabase_key = config.SUPABASE_KEY
        
        if not supabase_url or not supabase_key:
            logger.error("缺少SUPABASE_URL或SUPABASE_KEY配置")
            return jsonify({"success": False, "error": "数据库配置不完整"})
        
        # 创建Supabase客户端
        supabase = create_client(supabase_url, supabase_key)
        
        # 构建查询
        query = supabase.table('tokens').select('*')
        
        # 应用筛选条件
        if chain_filter and chain_filter.lower() != 'all':
            query = query.eq('chain', chain_filter)
            
        # 应用搜索条件
        if search_query:
            query = query.or_(f"token_symbol.ilike.%{search_query}%,contract.ilike.%{search_query}%")
        
        # 如果有last_id，从last_id之后的记录开始获取
        if last_id > 0:
            query = query.lt('id', last_id)  # 获取ID小于last_id的记录
        
        # 按最新更新时间排序并限制数量
        query = query.order('latest_update', desc=True).limit(batch_size)
        
        # 执行查询
        response = query.execute()
        
        # 处理代币数据
        tokens = []
        min_id = 0  # 记录最小ID，用于下次请求
        
        if hasattr(response, 'data'):
            for token in response.data:
                if token is None:
                    continue
                    
                # 处理token数据
                processed_token = process_token_data(token)
                tokens.append(processed_token)
                
                # 更新最小ID
                token_id = token.get('id', 0)
                if token_id > 0 and (min_id == 0 or token_id < min_id):
                    min_id = token_id
        
        # 检查是否还有更多数据
        has_more = len(tokens) >= batch_size
        
        return jsonify({
            'success': True,
            'tokens': tokens,
            'has_more': has_more,
            'next_id': min_id if has_more else 0
        })
        
    except Exception as e:
        logger.error(f"流式获取代币数据时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': f"获取数据失败: {str(e)}"
        })

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
    
    Args:
        host: 主机地址
        port: 端口号
        debug: 是否启用调试模式
        
    Returns:
        Web服务器进程或线程
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
        
        # 检测操作系统环境
        import platform
        is_windows = platform.system() == 'Windows'
        
        # 在Windows环境下使用线程，在Linux环境下使用多进程
        if is_windows:
            logger.info("Windows环境：使用线程启动Web服务器")
            
            def run_flask_app():
                global app
                try:
                    logger.info(f"Flask线程启动: {host}:{port}")
                    app.run(host=host, port=port, debug=debug)
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
            # 创建多进程 (Linux环境)
            logger.info("Linux环境：使用多进程启动Web服务器")
            
            # 使用全局函数而非本地函数，解决pickling问题
            process = multiprocessing.Process(target=run_flask_server, args=(host, port, debug))
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
        app.run(host=host, port=port, debug=debug)
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
        
        # 获取消息数据
        message_response = supabase.table('messages').select('*').eq('chain', chain).eq('message_id', message_id).limit(1).execute()
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
    """处理token数据，格式化价格和时间等信息"""
    try:
        # 创建处理后的token对象
        processed_token = dict(token)
        
        # 确保始终设置基本字段，避免None值
        processed_token['change_pct_value'] = to_decimal_or_float(token.get('change_pct_value', 0)) or 0
        processed_token['change_percentage'] = token.get('change_percentage', "0.00%") or "0.00%"
        processed_token['market_cap'] = to_decimal_or_float(token.get('market_cap', 0)) or 0
        processed_token['volume_1h'] = to_decimal_or_float(token.get('volume_1h', 0)) or 0
        
        # 确保社群数据为整数类型，并处理None值和错误转换情况
        original_community_reach = token.get('community_reach')
        original_spread_count = token.get('spread_count')
        
        try:
            processed_token['community_reach'] = int(float(token.get('community_reach', 0) or 0))
        except (ValueError, TypeError):
            processed_token['community_reach'] = 0
            
        try:
            processed_token['spread_count'] = int(float(token.get('spread_count', 0) or 0))
        except (ValueError, TypeError):
            processed_token['spread_count'] = 0
            
        # 添加详细调试日志
        logger.info(f"处理token数据: ID={token.get('id')}, Chain={token.get('chain')}, Symbol={token.get('token_symbol')}")
        logger.info(f"原始社群数据: community_reach={original_community_reach}({type(original_community_reach).__name__}), spread_count={original_spread_count}({type(original_spread_count).__name__})")
        logger.info(f"处理后社群数据: community_reach={processed_token['community_reach']}, spread_count={processed_token['spread_count']}")
        
        processed_token['buys_1h'] = int(to_decimal_or_float(token.get('buys_1h', 0)) or 0)
        processed_token['sells_1h'] = int(to_decimal_or_float(token.get('sells_1h', 0)) or 0)
        processed_token['holders_count'] = int(to_decimal_or_float(token.get('holders_count', 0)) or 0)
        
        # 处理市值格式化
        if processed_token['market_cap'] > 0:
            processed_token['market_cap_formatted'] = format_market_cap(processed_token['market_cap'])
        else:
            processed_token['market_cap_formatted'] = "$0.00"
        
        # 处理价格变化百分比
        change_pct = to_decimal_or_float(token.get('price_change_24h')) or to_decimal_or_float(token.get('last_calculated_change_pct')) or 0
        processed_token['change_pct_value'] = change_pct
        if change_pct > 0:
            processed_token['change_percentage'] = f"+{change_pct:.2f}%"
        else:
            processed_token['change_percentage'] = f"{change_pct:.2f}%"
        
        # 处理1小时交易量格式化
        if processed_token['volume_1h'] > 0:
            processed_token['volume_1h_formatted'] = format_market_cap(processed_token['volume_1h'])
        else:
            processed_token['volume_1h_formatted'] = "$0.00"
        
        # 处理首次更新时间格式化
        if token.get('first_update'):
            try:
                # 尝试解析时间格式
                dt = datetime.fromisoformat(token['first_update'].replace('Z', '+00:00'))
                processed_token['first_update_formatted'] = dt.strftime('%Y-%m-%d %H:%M:%S')
            except (ValueError, TypeError, AttributeError):
                processed_token['first_update_formatted'] = str(token.get('first_update', "未知"))
        else:
            processed_token['first_update_formatted'] = "未知"
        
        # 处理流动性数据
        liquidity = to_decimal_or_float(token.get('liquidity', 0)) or 0
        processed_token['liquidity'] = format_number(liquidity)
        
        # 添加调试日志
        logger.debug(f"处理后的token数据: 社群覆盖人数={processed_token['community_reach']}, 传播次数={processed_token['spread_count']}")
        
        return processed_token
    except Exception as e:
        logger.error(f"处理token数据时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        # 返回带基本字段的字典，确保页面不会崩溃
        return {
            'chain': token.get('chain', ''),
            'contract': token.get('contract', ''),
            'token_symbol': token.get('token_symbol', '未知'),
            'market_cap': 0,
            'market_cap_formatted': "$0.00",
            'change_pct_value': 0,
            'change_percentage': "0.00%",
            'volume_1h': 0,
            'volume_1h_formatted': "$0.00",
            'community_reach': 0,
            'spread_count': 0,
            'buys_1h': 0,
            'sells_1h': 0,
            'holders_count': 0,
            'first_update_formatted': "未知",
            'liquidity': "0"
        }

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
            pools = get_token_pools(chain.lower(), contract)
            
            if pools and 'pairs' in pools and pools['pairs']:
                # 汇总所有交易对的交易数据
                buys_1h = 0
                sells_1h = 0
                volume_1h = 0
                
                # 获取市值和流动性数据
                market_cap = 0
                liquidity = 0
                
                for pair in pools['pairs']:
                    if 'txns' in pair and 'h1' in pair['txns']:
                        h1_data = pair['txns']['h1']
                        buys_1h += h1_data.get('buys', 0)
                        sells_1h += h1_data.get('sells', 0)
                        
                    if 'volume' in pair and 'h1' in pair['volume']:
                        volume_1h += float(pair['volume']['h1'] or 0)
                        
                    # 提取市值和流动性数据
                    if 'fdv' in pair:
                        current_pair_cap = float(pair['fdv'] or 0)
                        if current_pair_cap > market_cap:
                            market_cap = current_pair_cap
                        
                    if 'liquidity' in pair and 'usd' in pair['liquidity']:
                        liquidity += float(pair['liquidity']['usd'] or 0)
                
                updated_buys_1h = buys_1h
                updated_sells_1h = sells_1h
                updated_volume_1h = volume_1h
                
                if market_cap > 0:
                    updated_market_cap = market_cap
                    logger.info(f"后台更新: 更新市值: ${market_cap}")
                    
                if liquidity > 0:
                    updated_liquidity = liquidity
                    logger.info(f"后台更新: 更新流动性: ${liquidity}")
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
                updated_data['last_calculated_change_pct'] = to_decimal_or_float(change_pct)  # numeric类型
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
            # 记录历史数据到token_history表
            try:
                # 在更新数据库前，创建历史记录
                history_data = {
                    'chain': chain,
                    'contract': contract,
                    'token_symbol': token.get('token_symbol', ''),
                    'timestamp': datetime.now().isoformat(),  # 将datetime对象转换为ISO格式字符串
                    'market_cap': updated_market_cap or current_market_cap,
                    'price': token.get('price'),
                    'liquidity': updated_liquidity or token.get('liquidity'),
                    'volume_24h': token.get('volume_24h'),
                    'volume_1h': updated_volume_1h or token.get('volume_1h'),
                    'holders_count': updated_holders_count or token.get('holders_count'),
                    'buys_1h': updated_buys_1h or token.get('buys_1h'),
                    'sells_1h': updated_sells_1h or token.get('sells_1h'),
                    'community_reach': updated_community_reach or token.get('community_reach'),
                    'spread_count': updated_spread_count or token.get('spread_count'),
                    'market_cap_change_pct': to_decimal_or_float(change_pct) if 'change_pct' in locals() else None,
                    'price_change_pct': token.get('price_change_24h')
                }
                
                # 插入历史记录
                history_result = await db_adapter.execute_query(
                    'token_history',
                    'insert',
                    data=history_data
                )
                
                if isinstance(history_result, dict) and history_result.get('error'):
                    logger.error(f"后台更新: 记录历史数据失败: {history_result.get('error')}")
                else:
                    logger.info(f"后台更新: 成功记录历史数据")
            except Exception as e:
                logger.error(f"后台更新: 记录历史数据时出错: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
            
            # 更新主表数据
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
                
                # 更新缓存
                if cache_key in API_CACHE:
                    with API_LOCK:
                        # 获取处理后的token数据
                        processed_token = process_token_data({**token, **updated_data})
                        # 更新缓存中的token数据部分
                        API_CACHE[cache_key]['data']['token'].update(processed_token)
                        API_CACHE[cache_key]['timestamp'] = time.time()  # 刷新时间戳
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

@app.route('/api/refresh_tokens', methods=['POST'])
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

@app.route('/api/token_history/<chain>/<contract>')
async def api_token_history(chain, contract):
    """获取代币历史数据API，支持时间范围过滤和数据聚合"""
    try:
        # 记录请求开始
        logger.info(f"收到历史数据请求: {chain}/{contract}")
        
        # 获取查询参数
        start_date = request.args.get('start_date')  # 开始日期，格式：YYYY-MM-DD
        end_date = request.args.get('end_date')      # 结束日期，格式：YYYY-MM-DD
        interval = request.args.get('interval', 'day') # 数据聚合间隔：hour, day, week, month
        metrics = request.args.get('metrics', 'market_cap,price,community_reach,spread_count')  # 需要的指标
        
        logger.info(f"历史数据请求参数: start_date={start_date}, end_date={end_date}, interval={interval}, metrics={metrics}")
        
        try:
            # 获取数据库适配器
            from src.database.db_factory import get_db_adapter
            db_adapter = get_db_adapter()
            
            # 获取代币基本信息
            token = await db_adapter.get_token_by_contract(chain, contract)
            
            if not token:
                logger.warning(f"未找到代币: {chain}/{contract}")
                return jsonify({"success": False, "error": f"未找到代币 {chain}/{contract}"})
            
            # 解析需要的指标
            metrics_list = metrics.split(',')
            valid_metrics = ['market_cap', 'price', 'liquidity', 'volume_24h', 'volume_1h', 
                          'holders_count', 'buys_1h', 'sells_1h', 'community_reach', 
                          'spread_count', 'market_cap_change_pct', 'price_change_pct']
            
            # 过滤有效的指标
            metrics_list = [m for m in metrics_list if m in valid_metrics]
            
            if not metrics_list:
                metrics_list = ['market_cap', 'price', 'community_reach', 'spread_count']  # 默认指标
            
            logger.info(f"使用的指标: {metrics_list}")
            
            # 构建过滤条件
            filters = {
                'chain': chain,
                'contract': contract
            }
            
            if start_date:
                # 过滤条件添加，实际筛选在后面处理
                filters['start_date'] = start_date
            
            if end_date:
                # 过滤条件添加，实际筛选在后面处理
                filters['end_date'] = end_date
            
            # 查询历史数据
            try:
                result = await db_adapter.execute_query(
                    'token_history',
                    'select',
                    filters={'chain': chain, 'contract': contract}
                )
                logger.info(f"查询历史数据结果: 获取到 {len(result) if result else 0} 条记录")
            except Exception as e:
                logger.error(f"查询历史数据失败: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                result = []
            
            # 如果没有历史数据，使用当前数据
            if not result or not isinstance(result, list) or len(result) == 0:
                logger.warning(f"无历史数据，使用当前数据: {chain}/{contract}")
                current_data = {
                    "time_interval": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                
                # 添加当前值
                for metric in metrics_list:
                    current_data[metric] = to_decimal_or_float(token.get(metric, 0))
                    
                history_data = [current_data]
            else:
                # 手动处理日期过滤和聚合
                filtered_result = []
                
                # 解析起止日期
                start_datetime = None
                end_datetime = None
                try:
                    if start_date:
                        start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
                    if end_date:
                        end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
                        end_datetime = end_datetime.replace(hour=23, minute=59, second=59)
                except ValueError as e:
                    logger.error(f"日期格式无效: {str(e)}")
                    return jsonify({"success": False, "error": f"日期格式无效: {str(e)}"})
                
                logger.info(f"过滤日期: {start_datetime} 到 {end_datetime}")
                
                # 筛选日期范围
                for record in result:
                    if 'timestamp' in record:
                        try:
                            # 处理不同格式的时间戳
                            if isinstance(record['timestamp'], str):
                                record_time = datetime.strptime(record['timestamp'].split('.')[0], '%Y-%m-%d %H:%M:%S')
                            elif isinstance(record['timestamp'], datetime):
                                record_time = record['timestamp']
                            else:
                                logger.warning(f"无效的时间戳格式: {type(record['timestamp'])} - {record['timestamp']}")
                                continue
                                
                            # 应用日期过滤
                            if start_datetime and record_time < start_datetime:
                                continue
                            if end_datetime and record_time > end_datetime:
                                continue
                                
                            # 通过筛选的记录
                            filtered_result.append(record)
                        except Exception as e:
                            logger.error(f"处理时间戳时出错: {str(e)}")
                            continue
                
                logger.info(f"筛选后的记录数: {len(filtered_result)}")
                
                # 按照interval进行聚合
                grouped_data = {}
                
                for record in filtered_result:
                    if 'timestamp' not in record:
                        continue
                        
                    timestamp = record['timestamp']
                    if isinstance(timestamp, str):
                        timestamp = datetime.strptime(timestamp.split('.')[0], '%Y-%m-%d %H:%M:%S')
                    
                    # 根据interval确定聚合键
                    if interval == 'hour':
                        group_key = timestamp.strftime('%Y-%m-%d %H:00:00')
                    elif interval == 'day':
                        group_key = timestamp.strftime('%Y-%m-%d 00:00:00')
                    elif interval == 'week':
                        # 计算周开始日期
                        week_start = timestamp - timedelta(days=timestamp.weekday())
                        group_key = week_start.strftime('%Y-%m-%d 00:00:00')
                    elif interval == 'month':
                        group_key = timestamp.strftime('%Y-%m-01 00:00:00')
                    else:
                        group_key = timestamp.strftime('%Y-%m-%d 00:00:00')  # 默认按天
                    
                    # 初始化分组
                    if group_key not in grouped_data:
                        grouped_data[group_key] = {
                            'time_interval': group_key,
                            'count': 0
                        }
                        for metric in metrics_list:
                            grouped_data[group_key][metric] = 0
                    
                    # 累加指标值
                    grouped_data[group_key]['count'] += 1
                    for metric in metrics_list:
                        if metric in record and record[metric] is not None:
                            try:
                                metric_value = to_decimal_or_float(record[metric])
                                grouped_data[group_key][metric] += metric_value
                            except Exception as e:
                                logger.error(f"处理指标值出错: {metric}={record[metric]}, 错误: {str(e)}")
                
                # 计算平均值
                history_data = []
                for group_key, group_data in grouped_data.items():
                    if group_data['count'] > 0:
                        for metric in metrics_list:
                            if metric in group_data:
                                group_data[metric] = group_data[metric] / group_data['count']
                        # 删除计数字段
                        del group_data['count']
                        history_data.append(group_data)
                
                # 按时间排序
                history_data.sort(key=lambda x: x['time_interval'])
                
                logger.info(f"生成了 {len(history_data)} 条历史数据点")
            
            # 处理token数据
            processed_token = process_token_data(token)
            
            # 构建响应数据
            response_data = {
                "success": True,
                "token": processed_token,
                "history": history_data,
                "interval": interval,
                "metrics": metrics_list,
                "timestamp": time.time()
            }
            
            # 返回数据
            return jsonify(response_data)
                
        except Exception as e:
            logger.error(f"获取代币历史数据时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return jsonify({"success": False, "error": f"获取代币历史数据时出错: {str(e)}"})
            
    except Exception as e:
        logger.error(f"处理代币历史数据请求时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({"success": False, "error": f"处理请求时出错: {str(e)}"})

@app.route('/api/token_detail/<chain>/<contract>')
async def api_token_detail(chain, contract):
    """获取代币详细信息API，包括基本信息、市场数据和提及历史"""
    try:
        logger.info(f"获取代币详细信息: {chain}/{contract}")
        
        # 检查缓存
        cache_key = f"detail_{chain}_{contract}"
        force_refresh = request.args.get('refresh', '0') == '1'
        current_time = time.time()
        
        # 缓存时间（秒）- 30秒
        cache_duration = 30
        
        # 首先检查是否存在缓存数据且未过期
        cached_data = None
        with API_LOCK:
            if cache_key in API_CACHE and not force_refresh:
                cache_data = API_CACHE[cache_key]
                # 如果缓存有效且未过期，直接返回缓存数据
                if current_time - cache_data['timestamp'] < cache_duration:
                    logger.info(f"返回详情缓存数据: {chain}/{contract}")
                    return jsonify(cache_data['data'])
        
        # 使用Supabase适配器获取数据
        from supabase import create_client
        
        supabase_url = config.SUPABASE_URL
        supabase_key = config.SUPABASE_KEY
        
        if not supabase_url or not supabase_key:
            logger.error("缺少SUPABASE_URL或SUPABASE_KEY配置")
            return jsonify({"success": False, "error": "数据库配置不完整"})
            
        # 创建Supabase客户端
        supabase = create_client(supabase_url, supabase_key)
        
        # 获取代币基本信息
        token_response = supabase.table('tokens').select('*').eq('chain', chain).eq('contract', contract).limit(1).execute()
        token = token_response.data[0] if hasattr(token_response, 'data') and token_response.data and len(token_response.data) > 0 else None
        
        if not token:
            logger.warning(f"未找到代币: {chain}/{contract}")
            return jsonify({"success": False, "error": f"未找到代币 {chain}/{contract}"})
        
        # 处理token数据，确保格式一致
        processed_token = process_token_data(token)
        
        # 获取代币提及历史
        mentions_response = supabase.table('tokens_mark').select('*').eq('chain', chain).eq('contract', contract).order('mention_time', desc=True).limit(20).execute()
        mentions = mentions_response.data if hasattr(mentions_response, 'data') else []
        
        # 格式化提及历史数据
        mention_history = []
        channel_stats = {}  # 用于统计各频道的提及情况
        
        for mention in mentions:
            channel_id = mention.get('channel_id')
            mention_time = mention.get('mention_time')
            market_cap = mention.get('market_cap')
            message_id = mention.get('message_id')
            
            if channel_id and mention_time:
                try:
                    # 获取频道信息
                    channel_response = supabase.table('telegram_channels').select('*').eq('channel_id', channel_id).limit(1).execute()
                    channel = channel_response.data[0] if hasattr(channel_response, 'data') and channel_response.data and len(channel_response.data) > 0 else None
                    
                    channel_name = channel.get('channel_name') if channel else '未知频道'
                    member_count = channel.get('member_count') if channel else 0
                    
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
        
        # 构建市值历史数据，用于绘制图表
        # 按时间排序
        mention_history.sort(key=lambda x: x['mention_time'] if isinstance(x['mention_time'], str) else x['mention_time'].isoformat())
        
        # 提取市值历史数据
        market_cap_history = []
        for mention in mention_history:
            if mention.get('market_cap'):
                market_cap_history.append({
                    'time': mention['mention_time'] if isinstance(mention['mention_time'], str) else mention['mention_time'].isoformat(),
                    'value': float(mention['market_cap'])
                })
        
        # 构建响应数据
        response_data = {
            "success": True,
            "token": processed_token,
            "mention_history": mention_history,
            "channel_stats": list(channel_stats.values()),
            "market_cap_history": market_cap_history,
            "timestamp": time.time()
        }
        
        # 更新缓存
        with API_LOCK:
            API_CACHE[cache_key] = {
                'data': response_data,
                'timestamp': time.time()
            }
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"获取代币详情时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({"success": False, "error": f"获取代币详情时出错: {str(e)}"})

@app.route('/api/refresh_token/<chain>/<contract>', methods=['POST'])
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
                
                if pools and 'pairs' in pools and pools['pairs']:
                    # 提取交易数据
                    buys_1h = 0
                    sells_1h = 0
                    volume_1h = 0
                    volume_24h = 0
                    
                    for pair in pools['pairs']:
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
        
        # 检查市值是否更新
        new_market_cap = updated_token.get('market_cap', 0)
        market_cap_change = 0
        if current_market_cap and current_market_cap > 0 and new_market_cap and new_market_cap > 0:
            market_cap_change = ((new_market_cap - current_market_cap) / current_market_cap) * 100
            logger.info(f"{token_symbol} 市值变化: {current_market_cap} -> {new_market_cap} ({market_cap_change:+.2f}%)")
        
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
    
    # 直接启动应用，不使用多进程/线程
    app.run(host='0.0.0.0', port=5000, debug=True) 