import logging
import sqlite3
import json
import time
import os
import multiprocessing
from datetime import datetime, timezone, timedelta
from flask import Flask, render_template, jsonify, request, redirect, url_for, flash, send_from_directory, abort, session
from flask_cors import CORS
from sqlalchemy import create_engine, func, desc, and_, or_
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from src.database.models import engine, Token, Message, TelegramChannel, TokensMark
from src.core.channel_manager import ChannelManager
import config.settings as config
import urllib.parse

# 在开发环境中修改路径
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.database.db_handler import extract_promotion_info

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

# 创建数据库会话
engine = create_engine(config.DATABASE_URI)
Session = sessionmaker(bind=engine)

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
    return sqlite3.connect(os.path.join('./data', 'telegram_messages.db'))


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
    session = None
    default_stats = {
        'active_channels_count': 0,
        'message_count': 0,
        'token_count': 0,
        'last_update': "未知",
        'channels': [],
    }
    
    try:
        session = Session()
        
        # 获取活跃频道数
        active_channels_count = session.query(TelegramChannel).filter_by(is_active=True).count()
        
        # 获取消息数
        message_count = session.query(Message).count()
        
        # 获取代币数
        token_count = session.query(Token).count()
        
        # 获取最后更新时间
        last_update = session.query(Token.latest_update).order_by(Token.latest_update.desc()).first()
        last_update = last_update[0] if last_update else "未知"
        
        # 获取活跃频道列表
        channels = session.query(TelegramChannel).filter_by(is_active=True).all()
        
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
    finally:
        if session:
            session.close()


@app.route('/')
def index():
    """首页"""
    # 从数据库获取系统统计和最近代币
    session = Session()
    try:
        # 获取查询参数
        chain_filter = request.args.get('chain', 'all')
        search_query = request.args.get('search', '')
        
        # 构建查询
        query = session.query(Token)
        
        # 应用筛选条件
        if chain_filter and chain_filter.lower() != 'all':
            query = query.filter(Token.chain == chain_filter)
            
        # 应用搜索条件
        if search_query:
            # 在代币符号、合约地址和名称中搜索
            query = query.filter(
                or_(
                    Token.token_symbol.ilike(f"%{search_query}%"),
                    Token.contract.ilike(f"%{search_query}%")
                )
            )
        
        # 获取最近的代币，限制20个
        recent_tokens = query.order_by(Token.latest_update.desc()).limit(20).all()
        
        # 获取所有可用的Chain
        available_chains = [r[0] for r in session.query(Token.chain).distinct().all()]
        
        # 处理代币数据
        tokens = []
        for token in recent_tokens:
            # 检查token是否为None
            if token is None:
                logger.warning("发现None类型的token对象，已跳过")
                continue
                
            # 处理token对象，提取需要的字段
            token_dict = {
                'id': token.id,
                'chain': token.chain,
                'token_symbol': token.token_symbol,
                'contract': token.contract,
                'first_update': token.first_update,
                'latest_update': token.latest_update,
                'image_url': getattr(token, 'image_url', None),
                'market_cap': format_market_cap(token.market_cap),
                'liquidity': token.liquidity,
                'dexscreener_url': token.dexscreener_url,
                'telegram_url': token.telegram_url,
                'twitter_url': token.twitter_url,
                'website_url': token.website_url,
                'holders_count': token.holders_count or '未知',
                'buys_1h': token.buys_1h or 0,
                'sells_1h': token.sells_1h or 0,
                'volume_1h': format_market_cap(token.volume_1h) if token.volume_1h else 0,
                'spread_count': f"{token.spread_count or 0}次",
                'community_reach': f"{token.community_reach or 0}人"
            }
            
            # 计算涨跌幅
            try:
                # 使用Token.market_cap和Token.market_cap_1h直接计算涨跌幅
                if token.market_cap_1h and token.market_cap_1h > 0:
                    # 使用一小时前的市值和当前市值计算涨跌幅
                    change_pct = ((token.market_cap or 0) - token.market_cap_1h) / token.market_cap_1h * 100
                    token_dict['change_percentage'] = f"{change_pct:+.2f}%"
                    token_dict['change_pct_value'] = change_pct
                    token_dict['is_profit'] = change_pct >= 0
                else:
                    # 如果没有一小时前的市值记录，退回到使用first_market_cap
                    if token.first_market_cap and token.first_market_cap > 0:
                        change_pct = ((token.market_cap or 0) - token.first_market_cap) / token.first_market_cap * 100
                        token_dict['change_percentage'] = f"{change_pct:+.2f}%"
                        token_dict['change_pct_value'] = change_pct
                        token_dict['is_profit'] = change_pct >= 0
                    else:
                        token_dict['change_percentage'] = "0%"
                        token_dict['change_pct_value'] = 0
                        token_dict['is_profit'] = True
            except Exception as e:
                logger.error(f"计算代币 {token.chain}/{token.contract} 涨跌幅时出错: {str(e)}")
                # 出错时使用默认值
                token_dict['change_percentage'] = "0%"
                token_dict['change_pct_value'] = 0
                token_dict['is_profit'] = True
                
            tokens.append(token_dict)
        
        # 使用system_stats填充上下文
        stats = get_system_stats()
        
        return render_template('index.html', 
                               tokens=tokens,
                               stats=stats,
                               chain_filter=chain_filter,
                               search_query=search_query,
                               available_chains=available_chains,
                               year=datetime.now().year)
    except Exception as e:
        logger.error(f"首页请求处理错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return handle_error(f"处理首页请求时出错: {str(e)}")
    finally:
        session.close()


@app.route('/channels')
def channels():
    """社群信息页面，显示所有频道和群组信息"""
    try:
        session = Session()
        
        # 获取所有频道信息
        channels = session.query(TelegramChannel).order_by(TelegramChannel.chain).all()
        
        # 获取活跃频道数量
        active_channels_count = session.query(TelegramChannel).filter_by(is_active=True).count()
        
        # 获取最后更新时间
        last_update = session.query(Token.latest_update).order_by(Token.latest_update.desc()).first()
        last_update = last_update[0] if last_update else "未知"
        
        # 渲染模板
        return render_template(
            'channels.html',
            channels=channels,
            active_channels_count=active_channels_count,
            last_update=last_update,
            year=datetime.now().year
        )
        
    except Exception as e:
        logger.error(f"社群信息页面请求处理错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return handle_error(f"处理社群信息页面请求时出错: {str(e)}")
    finally:
        session.close()


@app.route('/statistics')
def statistics():
    """统计分析页面，显示系统统计数据和图表"""
    try:
        session = Session()
        
        # 获取系统统计数据
        stats = get_system_stats()
        
        # 获取代币分布数据
        chain_counts = session.query(Token.chain, func.count(Token.id)).group_by(Token.chain).all()
        
        # 准备图表数据
        chart_data = {
            'chains': [chain for chain, _ in chain_counts],
            'counts': [count for _, count in chain_counts]
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
    finally:
        session.close()


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
        
        db_session = Session()
        
        # 获取频道信息
        channel = db_session.query(TelegramChannel).filter(TelegramChannel.channel_id == channel_id).first()
        
        # 获取代币信息
        token = db_session.query(Token).filter(
            Token.chain == chain.upper(),
            Token.contract == contract
        ).first()
        
        if not token:
            return handle_error(f"未找到代币: {chain}/{contract}")
            
        # 查询该频道中代币的提及记录
        mentions = db_session.query(
            TokensMark
        ).filter(
            TokensMark.chain == chain.upper(),
            TokensMark.contract == contract,
            TokensMark.channel_id == channel_id
        ).order_by(TokensMark.mention_time.desc()).all()
        
        # 转换为字典列表
        mention_data = []
        for mention in mentions:
            mention_data.append({
                'id': mention.id,
                'chain': mention.chain,
                'token_symbol': mention.token_symbol,
                'market_cap': mention.market_cap,
                'market_cap_formatted': format_market_cap(mention.market_cap),
                'mention_time': mention.mention_time,
                'message_id': mention.message_id
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
        return handle_error(str(e))
    finally:
        db_session.close()


def start_web_server(host='0.0.0.0', port=5000, debug=False):
    """
    启动Web服务器
    
    Args:
        host: 主机地址，默认为0.0.0.0
        port: 端口号，默认为5000
        debug: 是否开启调试模式
    
    Returns:
        multiprocessing.Process: Web服务器进程对象
    """
    try:
        # 确保参数合法
        if not host:
            logger.warning("主机地址为空，使用默认值0.0.0.0")
            host = '0.0.0.0'
            
        if not isinstance(port, int) or port <= 0:
            logger.warning(f"端口号无效: {port}，使用默认值5000")
            port = 5000
            
        # 确保必要的目录存在
        os.makedirs('./logs', exist_ok=True)
        os.makedirs('./data', exist_ok=True)
        os.makedirs('./media', exist_ok=True)
        
        # 确保静态资源目录存在
        try:
            static_img_dir = os.path.join(app.static_folder, 'img')
            os.makedirs(static_img_dir, exist_ok=True)
        except Exception as e:
            logger.error(f"创建静态资源目录时出错: {str(e)}")
            # 继续尝试启动服务器
        
        logger.info(f"启动Web服务器 - {host}:{port}")
        
        # 修改为使用线程而不是多进程，解决Windows下的序列化问题
        import threading
        
        def run_flask_app():
            try:
                app.run(host=host, port=port, debug=debug)
            except Exception as e:
                logger.error(f"Flask应用启动失败: {str(e)}")
                # 尝试在其他端口启动
                try:
                    logger.info(f"尝试在备用端口启动: {port+1}")
                    app.run(host=host, port=port+1, debug=debug)
                except Exception as e2:
                    logger.error(f"备用端口启动也失败: {str(e2)}")
        
        # 使用线程而不是多进程
        web_thread = threading.Thread(target=run_flask_app)
        web_thread.daemon = True  # 设置为守护线程，主线程退出时自动结束
        web_thread.start()
        
        # 返回线程对象而不是进程对象
        return web_thread
            
    except Exception as e:
        logger.error(f"启动Web服务器时出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        return None


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
    """获取代币市值历史和频道统计数据的API"""
    try:
        logger.info(f"正在获取代币市值历史数据: {chain}/{contract}")
        session = Session()
        
        # 查询tokens_mark数据
        history_query = session.query(
            TokensMark.id,
            TokensMark.chain,
            TokensMark.token_symbol,
            TokensMark.contract,
            TokensMark.market_cap,
            TokensMark.mention_time,
            TokensMark.channel_id,
            TelegramChannel.channel_name,
            TelegramChannel.member_count
        ).outerjoin(
            TelegramChannel, 
            TokensMark.channel_id == TelegramChannel.channel_id
        ).filter(
            TokensMark.chain == chain.upper(),
            TokensMark.contract == contract
        ).order_by(TokensMark.mention_time.asc())
        
        history_records = history_query.all()
        logger.info(f"查询到 {len(history_records)} 条历史记录")
        
        # 转换为JSON可序列化对象
        history_data = []
        channel_stats = {}  # 用于聚合每个频道的数据
        
        for record in history_records:
            try:
                # 创建历史记录
                history_item = {
                    'id': record.id,
                    'chain': record.chain,
                    'token_symbol': record.token_symbol,
                    'contract': record.contract,
                    'market_cap': record.market_cap,
                    'mention_time': record.mention_time.isoformat() if record.mention_time else None,
                    'channel_id': record.channel_id,
                    'channel_name': record.channel_name,
                    'member_count': record.member_count
                }
                history_data.append(history_item)
                
                # 聚合频道统计数据
                if record.channel_id:
                    if record.channel_id not in channel_stats:
                        channel_stats[record.channel_id] = {
                            'channel_id': record.channel_id,
                            'channel_name': record.channel_name,
                            'mention_count': 1,
                            'first_mention_time': record.mention_time.isoformat() if record.mention_time else None,
                            'first_market_cap': record.market_cap,
                            'member_count': record.member_count
                        }
                    else:
                        # 增加提及次数
                        channel_stats[record.channel_id]['mention_count'] += 1
                        
                        # 检查并更新最早提及时间
                        if record.mention_time and channel_stats[record.channel_id]['first_mention_time']:
                            current_first_time = datetime.fromisoformat(channel_stats[record.channel_id]['first_mention_time'])
                            if record.mention_time < current_first_time:
                                channel_stats[record.channel_id]['first_mention_time'] = record.mention_time.isoformat()
                                channel_stats[record.channel_id]['first_market_cap'] = record.market_cap
            except Exception as record_error:
                logger.error(f"处理记录 {record.id if hasattr(record, 'id') else '未知'} 失败: {str(record_error)}")
                continue
        
        # 将频道统计字典转换为列表
        channel_stats_list = list(channel_stats.values())
        logger.info(f"生成了 {len(channel_stats_list)} 条频道统计数据")
        
        # 检查数据是否为空
        if not history_data:
            logger.warning(f"未找到代币 {chain}/{contract} 的市值历史数据")
        
        if not channel_stats_list:
            logger.warning(f"未找到代币 {chain}/{contract} 的频道统计数据")
            
        return jsonify({
            'success': True,
            'history': history_data,
            'channel_stats': channel_stats_list
        })
        
    except Exception as e:
        logger.error(f"获取代币市值历史数据失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        })
    finally:
        session.close()


@app.route('/message/<chain>/<int:message_id>')
def message_detail(chain, message_id):
    """显示特定消息的详情页面"""
    try:
        db_session = Session()
        
        # 获取当前URL并保存到session
        current_url = request.url
        session['last_message_detail_url'] = current_url
        
        # 查询消息
        message = db_session.query(Message).filter(
            Message.chain == chain.upper(),
            Message.message_id == message_id
        ).first()
        
        if not message:
            return handle_error(f"未找到消息: {chain}/{message_id}")
            
        # 获取与消息相关的频道信息
        channel = None
        # 直接使用message.channel_id获取频道信息
        if message.channel_id:
            channel = db_session.query(TelegramChannel).filter(
                TelegramChannel.channel_id == message.channel_id
            ).first()
        
        # 如果没有找到channel，尝试通过TokensMark查找
        if not channel:
            token_mark = db_session.query(TokensMark).filter(
                TokensMark.chain == chain.upper(),
                TokensMark.message_id == message_id
            ).first()
            
            if token_mark and token_mark.channel_id:
                channel = db_session.query(TelegramChannel).filter(
                    TelegramChannel.channel_id == token_mark.channel_id
                ).first()
        
        # 查找相关代币
        tokens = db_session.query(Token).filter(
            Token.chain == chain.upper(),
            Token.message_id == message_id
        ).all()
        
        tokens_data = []
        for token in tokens:
            tokens_data.append({
                'id': token.id,
                'chain': token.chain,
                'token_symbol': token.token_symbol,
                'contract': token.contract,
                'market_cap': token.market_cap,
                'market_cap_formatted': token.market_cap_formatted,
                'dexscreener_url': token.dexscreener_url or get_dexscreener_url(token.chain, token.contract)
            })
        
        # 渲染模板
        return render_template(
            'message_detail.html',
            message=message,
            channel=channel,
            tokens=tokens_data,
            has_tokens=len(tokens_data) > 0
        )
        
    except Exception as e:
        logger.error(f"获取消息详情失败: {str(e)}")
        return handle_error(str(e))
    finally:
        db_session.close()


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