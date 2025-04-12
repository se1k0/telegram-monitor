import logging
import json
import time
import os
import multiprocessing
from datetime import datetime, timezone, timedelta
from flask import Flask, render_template, jsonify, request, redirect, url_for, flash, send_from_directory, abort, session
from flask_cors import CORS
from dotenv import load_dotenv
from src.database.models import Token, Message, TelegramChannel, TokensMark
from functools import wraps
import threading

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
        
        # 使用Supabase适配器获取代币数据
        logger.info("使用Supabase获取首页数据")
        try:
            from supabase import create_client
            # 不要重新导入config模块，直接使用全局config
            
            supabase_url = config.SUPABASE_URL
            supabase_key = config.SUPABASE_KEY
            
            if not supabase_url or not supabase_key:
                logger.error("缺少 SUPABASE_URL 或 SUPABASE_KEY 配置")
                return handle_error("数据库配置不完整", 500)
            
            # 创建 Supabase 客户端
            supabase = create_client(supabase_url, supabase_key)
            
            # 构建查询
            query = supabase.table('tokens').select('*')
            
            # 应用筛选条件
            if chain_filter and chain_filter.lower() != 'all':
                query = query.eq('chain', chain_filter)
                
            # 应用搜索条件（有限支持）
            if search_query:
                query = query.or_(f"token_symbol.ilike.%{search_query}%,contract.ilike.%{search_query}%")
            
            # 获取最近更新的代币并限制数量
            query = query.order('latest_update', desc=True).limit(20)
            
            # 执行查询
            response = query.execute()
            
            if not hasattr(response, 'data'):
                logger.error("Supabase查询未返回data字段")
                return handle_error("查询数据失败", 500)
            
            # 获取可用的链
            chains_response = supabase.table('tokens').select('chain').execute()
            available_chains = []
            if hasattr(chains_response, 'data'):
                # 提取唯一的链名称
                chain_values = [item.get('chain') for item in chains_response.data if item.get('chain')]
                available_chains = list(set(chain_values))
            else:
                available_chains = ['ETH', 'BSC', 'SOL']  # 默认值
            
            # 处理代币数据
            tokens = []
            for token in response.data:
                # 检查token是否为None
                if token is None:
                    logger.warning("发现None类型的token对象，已跳过")
                    continue
                    
                # 处理token对象，提取需要的字段
                token_dict = {
                    'id': token.get('id'),
                    'chain': token.get('chain'),
                    'token_symbol': token.get('token_symbol'),
                    'contract': token.get('contract'),
                    'first_update': token.get('first_update'),
                    'latest_update': token.get('latest_update'),
                    'image_url': token.get('image_url'),
                    'market_cap': format_market_cap(token.get('market_cap')),
                    'liquidity': token.get('liquidity'),
                    'dexscreener_url': token.get('dexscreener_url') or get_dexscreener_url(token.get('chain'), token.get('contract')),
                    'telegram_url': token.get('telegram_url'),
                    'twitter_url': token.get('twitter_url'),
                    'website_url': token.get('website_url'),
                    'holders_count': token.get('holders_count') or '未知',
                    'buys_1h': token.get('buys_1h') or 0,
                    'sells_1h': token.get('sells_1h') or 0,
                    'volume_1h': format_market_cap(token.get('volume_1h')) if token.get('volume_1h') else '$0',
                    'spread_count': f"{token.get('spread_count') or 0}次",
                    'community_reach': f"{token.get('community_reach') or 0}人"
                }
                
                # 计算涨跌幅
                try:
                    # 使用Token.market_cap和Token.market_cap_1h直接计算涨跌幅
                    market_cap_1h = token.get('market_cap_1h')
                    if market_cap_1h and float(market_cap_1h) > 0:
                        # 使用一小时前的市值和当前市值计算涨跌幅
                        current_market_cap = token.get('market_cap') or 0
                        change_pct = (float(current_market_cap) - float(market_cap_1h)) / float(market_cap_1h) * 100
                        token_dict['change_percentage'] = f"{change_pct:+.2f}%"
                        token_dict['change_pct_value'] = change_pct
                        
                        # 从字符串转换成 CSS 类
                        if change_pct > 0:
                            token_dict['change_class'] = 'positive-change'
                        elif change_pct < 0:
                            token_dict['change_class'] = 'negative-change'
                        else:
                            token_dict['change_class'] = 'neutral-change'
                    else:
                        token_dict['change_percentage'] = '0.00%'
                        token_dict['change_class'] = 'neutral-change'
                        token_dict['change_pct_value'] = 0
                except Exception as e:
                    logger.error(f"计算涨跌幅时出错: {token.get('token_symbol')}, {str(e)}")
                    token_dict['change_percentage'] = '0.00%'
                    token_dict['change_class'] = 'neutral-change'
                    token_dict['change_pct_value'] = 0
                
                tokens.append(token_dict)
            
            # 获取系统统计数据
            system_stats = get_system_stats()
            
            # 渲染模板
            return render_template('index.html', 
                                tokens=tokens,
                                available_chains=available_chains,
                                selected_chain=chain_filter,
                                search_query=search_query,
                                system_stats=system_stats,
                                year=datetime.now().year)
                                
        except Exception as e:
            logger.error(f"使用Supabase获取数据时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return handle_error(f"获取数据失败: {str(e)}", 500)
            
    except Exception as e:
        logger.error(f"首页渲染时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return handle_error(f"页面加载出错: {str(e)}", 500)


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
def api_token_market_history(chain, contract):
    """获取代币市值历史数据和提及统计API，并更新关键数据"""
    try:
        # 定义缓存键
        cache_key = f"{chain}_{contract}"
        # 缓存时间（秒）
        cache_duration = 60  # 1分钟
        current_time = time.time()
        
        # 首先检查是否存在缓存数据且未过期
        with API_LOCK:
            if cache_key in API_CACHE:
                cache_data = API_CACHE[cache_key]
                # 如果缓存有效且未过期，直接返回缓存数据
                if current_time - cache_data['timestamp'] < cache_duration:
                    logger.info(f"返回缓存数据: {chain}/{contract}")
                    return jsonify(cache_data['data'])
            
            # 获取或创建此代币的锁
            if cache_key not in API_LOCKS:
                API_LOCKS[cache_key] = threading.Lock()
            
            # 尝试获取锁
            token_lock = API_LOCKS[cache_key]
            lock_acquired = token_lock.acquire(blocking=False)
            
            # 如果无法获取锁（意味着另一个请求正在处理相同的代币），再次检查缓存
            if not lock_acquired:
                if cache_key in API_CACHE:
                    logger.info(f"无法获取锁，返回缓存数据: {chain}/{contract}")
                    return jsonify(API_CACHE[cache_key]['data'])
                else:
                    # 如果没有缓存，则告知用户稍后重试
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
                token = token_response.data[0] if hasattr(token_response, 'data') and token_response.data else None
                
                if not token:
                    return jsonify({"success": False, "error": f"未找到代币 {chain}/{contract}"})
                
                # 更新代币数据
                logger.info(f"开始更新代币数据: {chain}/{contract}")
                
                try:
                    # 1. 尝试更新代币持有者数量 - 可能需要调用外部API
                    logger.info("更新持有者数量")
                    updated_holders_count = None
                    
                    if chain == 'SOL':
                        # 如果是Solana链，尝试使用DAS API获取持有者信息
                        try:
                            from src.api.das_api import get_token_holders_info
                            holders_count, _ = get_token_holders_info(contract)
                            if holders_count:
                                updated_holders_count = holders_count
                                logger.info(f"通过DAS API获取到持有者数量: {holders_count}")
                        except Exception as e:
                            logger.error(f"DAS API获取持有者信息失败: {str(e)}")
                    else:
                        # 其他链可以添加对应的API调用
                        logger.info(f"暂未实现{chain}链的持有者数量更新")
                    
                    # 2. 更新交易数据 - 调用DEX Screener API
                    logger.info("更新交易数据")
                    updated_buys_1h = None
                    updated_sells_1h = None
                    updated_volume_1h = None
                    updated_market_cap = None
                    updated_liquidity = None
                    
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
                                    market_cap = float(pair['fdv'] or 0)
                                    
                                if 'liquidity' in pair and 'usd' in pair['liquidity']:
                                    liquidity += float(pair['liquidity']['usd'] or 0)
                            
                            updated_buys_1h = buys_1h
                            updated_sells_1h = sells_1h
                            updated_volume_1h = volume_1h
                            
                            if market_cap > 0:
                                updated_market_cap = market_cap
                                logger.info(f"更新市值: ${market_cap}")
                                
                            if liquidity > 0:
                                updated_liquidity = liquidity
                                logger.info(f"更新流动性: ${liquidity}")
                            
                            logger.info(f"更新交易数据: 买入={buys_1h}, 卖出={sells_1h}, 交易量=${volume_1h}")
                    except Exception as e:
                        logger.error(f"DEX Screener API获取交易数据失败: {str(e)}")
                    
                    # 3. 更新社群数据
                    logger.info("更新社群数据")
                    # 重新计算社群覆盖人数和传播次数
                    try:
                        # 计算传播次数 - 代币在所有群组中被提及的次数
                        spread_count_response = supabase.table('tokens_mark').select('id', count='exact').eq('chain', chain).eq('contract', contract).execute()
                        updated_spread_count = spread_count_response.count if hasattr(spread_count_response, 'count') else 0
                        
                        # 计算社群覆盖人数 - 涉及该代币的所有群组成员总数
                        # 先获取所有提到该代币的频道ID
                        channel_ids_response = supabase.table('tokens_mark').select('channel_id').eq('chain', chain).eq('contract', contract).execute()
                        if hasattr(channel_ids_response, 'data') and channel_ids_response.data:
                            # 提取唯一的频道ID
                            unique_channel_ids = set()
                            for item in channel_ids_response.data:
                                if item.get('channel_id'):
                                    unique_channel_ids.add(item.get('channel_id'))
                            
                            # 获取这些频道的成员数并求和
                            if unique_channel_ids:
                                community_reach = 0
                                for channel_id in unique_channel_ids:
                                    channel_response = supabase.table('telegram_channels').select('member_count').eq('channel_id', channel_id).limit(1).execute()
                                    if hasattr(channel_response, 'data') and channel_response.data:
                                        member_count = channel_response.data[0].get('member_count', 0)
                                        community_reach += member_count
                                
                                updated_community_reach = community_reach
                                logger.info(f"更新社群数据: 覆盖人数={community_reach}, 传播次数={updated_spread_count}")
                    except Exception as e:
                        logger.error(f"更新社群数据失败: {str(e)}")
                    
                    # 4. 计算市值涨跌幅
                    logger.info("计算市值涨跌幅")
                    # 获取当前市值（如果已经通过API更新，使用新值）
                    current_market_cap = updated_market_cap if updated_market_cap is not None else token.get('market_cap')
                    
                    # 获取1小时前市值
                    market_cap_1h = token.get('market_cap_1h')
                    
                    # 计算涨跌幅
                    change_pct = 0
                    change_percentage = '0.00%'
                    
                    if current_market_cap is not None and market_cap_1h is not None and market_cap_1h > 0:
                        change_pct = (float(current_market_cap) - float(market_cap_1h)) / float(market_cap_1h) * 100
                        change_percentage = f"{change_pct:+.2f}%"
                        logger.info(f"涨跌幅: {change_percentage}")
                    
                    # 5. 更新数据库中的代币数据
                    update_data = {}
                    
                    # 只更新有值的字段
                    if updated_holders_count is not None:
                        update_data['holders_count'] = updated_holders_count
                    
                    if updated_buys_1h is not None:
                        update_data['buys_1h'] = updated_buys_1h
                    
                    if updated_sells_1h is not None:
                        update_data['sells_1h'] = updated_sells_1h
                    
                    if updated_volume_1h is not None:
                        update_data['volume_1h'] = updated_volume_1h
                    
                    if updated_market_cap is not None:
                        update_data['market_cap'] = updated_market_cap
                    
                    if updated_liquidity is not None:
                        update_data['liquidity'] = updated_liquidity
                    
                    if 'updated_community_reach' in locals() and updated_community_reach is not None:
                        update_data['community_reach'] = updated_community_reach
                    
                    if 'updated_spread_count' in locals() and updated_spread_count is not None:
                        update_data['spread_count'] = updated_spread_count
                    
                    # 更新市值1小时数据，用于下次计算涨跌幅
                    if current_market_cap is not None:
                        update_data['market_cap_1h'] = current_market_cap
                    
                    # 更新最后更新时间
                    update_data['latest_update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    # 执行更新
                    if update_data:
                        logger.info(f"更新代币数据: {update_data}")
                        supabase.table('tokens').update(update_data).eq('chain', chain).eq('contract', contract).execute()
                    
                    # 重新获取更新后的数据
                    updated_token_response = supabase.table('tokens').select('*').eq('chain', chain).eq('contract', contract).limit(1).execute()
                    token = updated_token_response.data[0] if hasattr(updated_token_response, 'data') and updated_token_response.data else token
                    
                except Exception as e:
                    logger.error(f"更新代币数据过程中出错: {str(e)}")
                    import traceback
                    logger.error(traceback.format_exc())
                
                # 获取代币提及历史
                # 通过tokens_mark表获取
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
                        # 获取频道信息
                        channel_response = supabase.table('telegram_channels').select('*').eq('channel_id', channel_id).limit(1).execute()
                        channel = channel_response.data[0] if hasattr(channel_response, 'data') and channel_response.data else None
                        
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
                
                # 获取当前代币的市场数据
                current_data = {
                    'token_symbol': token.get('token_symbol'),
                    'contract': token.get('contract'),
                    'chain': token.get('chain'),
                    'market_cap': token.get('market_cap'),
                    'market_cap_formatted': format_market_cap(token.get('market_cap')),
                    'liquidity': token.get('liquidity'),
                    'buys_1h': token.get('buys_1h') or 0,
                    'sells_1h': token.get('sells_1h') or 0,
                    'volume_1h': token.get('volume_1h') or 0,
                    'volume_1h_formatted': format_market_cap(token.get('volume_1h')),
                    'holders_count': token.get('holders_count') or 0,
                    'community_reach': token.get('community_reach') or 0,
                    'spread_count': token.get('spread_count') or 0
                }
                
                # 添加涨跌幅数据
                if 'change_pct' in locals() and 'change_percentage' in locals():
                    current_data['change_percentage'] = change_percentage
                    current_data['change_pct_value'] = change_pct
                    
                    # 设置样式类
                    if change_pct > 0:
                        current_data['change_class'] = 'positive-change'
                    elif change_pct < 0:
                        current_data['change_class'] = 'negative-change'
                    else:
                        current_data['change_class'] = 'neutral-change'
                
                # 构建响应数据
                response_data = {
                    "success": True,
                    "token": current_data,
                    "history": sorted(history, key=lambda x: x['mention_time']),
                    "channel_stats": list(channel_stats.values())
                }
                
                # 更新缓存
                with API_LOCK:
                    API_CACHE[cache_key] = {
                        'data': response_data,
                        'timestamp': time.time()
                    }
                
                return jsonify(response_data)
                
            except Exception as e:
                logger.error(f"获取代币市值历史数据出错: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                return jsonify({"success": False, "error": str(e)})
        finally:
            # 确保锁被释放
            token_lock.release()
            
    except Exception as e:
        logger.error(f"处理API请求时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({"success": False, "error": str(e)})


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