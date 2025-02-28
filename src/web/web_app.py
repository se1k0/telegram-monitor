import logging
import sqlite3
import json
import time
import os
import multiprocessing
from datetime import datetime, timezone, timedelta
from flask import Flask, render_template, jsonify, request, redirect, url_for, flash, send_from_directory, abort
from flask_cors import CORS
from sqlalchemy import create_engine, func, desc, and_, or_
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# 在开发环境中修改路径
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.database.models import Token, Message, TelegramChannel
from src.database.db_handler import extract_promotion_info
from src.core.channel_manager import ChannelManager
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
            return "0.00"
        if isinstance(value, str):
            try:
                value = float(value.replace(',', ''))
            except:
                return "0.00"
        if value >= 100000000:  # 亿
            return f"{value/100000000:.2f}亿"
        elif value >= 10000:    # 万
            return f"{value/10000:.2f}万"
        return f"{value:.2f}"
    except Exception as e:
        logger.error(f"市值格式化错误: {value}, 错误: {str(e)}")
        return "0.00"


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
        # 获取系统统计数据
        stats = get_system_stats()
        
        # 获取最近的代币
        recent_tokens = session.query(Token).order_by(Token.latest_update.desc()).limit(10).all()
        
        # 处理代币数据
        tokens = []
        for token in recent_tokens:
            token_dict = {
                'id': token.id,
                'chain': token.chain,
                'symbol': token.token_symbol,
                'token_symbol': token.token_symbol,
                'name': token.token_symbol,
                'contract': token.contract,
                'channel_name': token.channel_name,
                'first_seen': token.first_update if token.first_update else '未知',
                'last_seen': token.latest_update if token.latest_update else '未知',
                'mentions': token.promotion_count if token.promotion_count else 0,
                'mentions_percentage': min(token.promotion_count * 5 if token.promotion_count else 0, 100),
                'sentiment_score': token.sentiment_score if token.sentiment_score is not None else 0,
                'hype_score': token.hype_score if token.hype_score is not None else 0,
                'price_change': ((token.price / token.first_price) - 1) * 100 if token.price and token.first_price and token.first_price > 0 else 0,
                'risk_level': token.risk_level,
                'sentiment_class': 'success' if token.sentiment_score and token.sentiment_score > 0.2 else 
                                  ('warning' if token.sentiment_score and token.sentiment_score > -0.2 else 'danger'),
                'is_trending': token.is_trending,
                'dexscreener_url': get_dexscreener_url(token.chain, token.contract),
                'latest_update': token.latest_update,
                'formatted_time': token.latest_update if token.latest_update else '未知',
                'market_cap_formatted': format_market_cap(token.market_cap),
                'first_market_cap_formatted': format_market_cap(token.first_market_cap),
                'image_url': None,  # 添加默认image_url字段
                'trending_score': token.hype_score if token.hype_score is not None else 0,  # 添加trending_score字段，使用hype_score作为替代
                'mentions_count': token.promotion_count if token.promotion_count else 0,  # 添加mentions_count字段
                # 添加price_info嵌套对象
                'price_info': {
                    'current_price': token.price if token.price else None,
                    'current_price_formatted': f"${token.price:.8f}" if token.price else "未知",
                    'price_change_24h': ((token.price / token.first_price) - 1) * 100 if token.price and token.first_price and token.first_price > 0 else None,
                    'price_change_24h_formatted': f"{((token.price / token.first_price) - 1) * 100:.2f}%" if token.price and token.first_price and token.first_price > 0 else "未知"
                },
                # 添加sentiment_info嵌套对象
                'sentiment_info': {
                    'sentiment_score': token.sentiment_score if token.sentiment_score is not None else None,
                    'hype_score': token.hype_score if token.hype_score is not None else None
                },
                # 添加格式化的时间字段
                'first_update_formatted': token.first_update if token.first_update else '未知',
                'last_update_formatted': token.latest_update if token.latest_update else '未知'
            }
            
            # 计算涨跌幅
            if token.first_market_cap and token.first_market_cap > 0:
                change_pct = ((token.market_cap or 0) - token.first_market_cap) / token.first_market_cap * 100
                token_dict['change_percentage'] = f"{change_pct:+.2f}%"
                token_dict['change_pct_value'] = change_pct
                token_dict['is_profit'] = change_pct >= 0
            else:
                token_dict['change_percentage'] = "N/A"
                token_dict['change_pct_value'] = None
                token_dict['is_profit'] = True
                
            # 添加情感分析颜色
            if token.sentiment_score is not None:
                if token.sentiment_score > 0.3:
                    token_dict['sentiment_color'] = 'green'
                elif token.sentiment_score > 0:
                    token_dict['sentiment_color'] = 'lightgreen'
                elif token.sentiment_score < -0.3:
                    token_dict['sentiment_color'] = 'red'
                elif token.sentiment_score < 0:
                    token_dict['sentiment_color'] = 'pink'
                else:
                    token_dict['sentiment_color'] = 'gray'
            else:
                token_dict['sentiment_color'] = 'gray'
                
            # 添加风险等级颜色
            risk_colors = {
                'high': 'red',
                'medium-high': 'orange',
                'medium': 'yellow',
                'low-medium': 'lightgreen',
                'low': 'green',
                'unknown': 'gray'
            }
            token_dict['risk_color'] = risk_colors.get(token.risk_level, 'gray')
            
            tokens.append(token_dict)
            
        # 获取活跃频道数量
        channel_count = session.query(TelegramChannel).filter_by(is_active=True).count()
        
        return render_template('index.html',
                               tokens=tokens,
                               stats=stats,
                               channel_count=channel_count,
                               year=datetime.now().year)
    
    except Exception as e:
        logger.error(f"处理首页请求时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return handle_error(f"处理首页请求时出错: {str(e)}")
    finally:
        session.close()


@app.route('/tokens')
def tokens_page():
    """代币列表页面"""
    try:
        # 获取分页、筛选和搜索参数
        page = request.args.get('page', 1, type=int)
        chain_filter = request.args.get('chain', 'ALL')
        search_query = request.args.get('search', '')
        sort_order = request.args.get('sort', 'recent')
        
        # 每页显示的记录数
        PER_PAGE = 20
        
        session = Session()
        
        # 构建查询
        query = session.query(Token)
        
        # 添加链筛选条件
        if chain_filter != 'ALL':
            query = query.filter(Token.chain == chain_filter)
        
        # 添加搜索条件
        if search_query:
            search_param = f"%{search_query}%"
            query = query.filter((Token.contract.like(search_param)) | 
                                 (Token.token_symbol.like(search_param)))
        
        # 添加排序
        if sort_order == 'profit':
            # 按涨幅排序（降序）
            query = query.order_by(
                func.coalesce(
                    (Token.market_cap - Token.first_market_cap) / 
                    func.nullif(Token.first_market_cap, 0),
                    0
                ).desc()
            )
        elif sort_order == 'loss':
            # 按跌幅排序（升序）
            query = query.order_by(
                func.coalesce(
                    (Token.market_cap - Token.first_market_cap) / 
                    func.nullif(Token.first_market_cap, 0),
                    0
                ).asc()
            )
        else:  # 默认按最近更新排序
            query = query.order_by(Token.latest_update.desc())
        
        # 获取总记录数
        total_count = query.count()
        
        # 分页
        tokens_query = query.limit(PER_PAGE).offset((page - 1) * PER_PAGE)
        
        # 创建一个简单的分页对象
        pagination = {
            'page': page,
            'per_page': PER_PAGE,
            'total': total_count,
            'pages': (total_count + PER_PAGE - 1) // PER_PAGE,
            'has_prev': page > 1,
            'has_next': page < ((total_count + PER_PAGE - 1) // PER_PAGE),
            'prev_num': page - 1,
            'next_num': page + 1,
            'iter_pages': lambda: range(1, ((total_count + PER_PAGE - 1) // PER_PAGE) + 1)
        }
        
        # 处理代币数据
        tokens = []
        for token in tokens_query:
            token_dict = {
                'id': token.id,
                'chain': token.chain,
                'symbol': token.token_symbol,
                'token_symbol': token.token_symbol,
                'name': token.token_symbol,
                'contract': token.contract,
                'channel_name': token.channel_name,
                'first_seen': token.first_update if token.first_update else '未知',
                'last_seen': token.latest_update if token.latest_update else '未知',
                'mentions': token.promotion_count if token.promotion_count else 0,
                'mentions_percentage': min(token.promotion_count * 5 if token.promotion_count else 0, 100),
                'sentiment_score': token.sentiment_score if token.sentiment_score is not None else 0,
                'hype_score': token.hype_score if token.hype_score is not None else 0,
                'price_change': ((token.price / token.first_price) - 1) * 100 if token.price and token.first_price and token.first_price > 0 else 0,
                'risk_level': token.risk_level if token.risk_level else 'unknown',
                'sentiment_class': 'success' if token.sentiment_score and token.sentiment_score > 0.2 else 
                                  ('warning' if token.sentiment_score and token.sentiment_score > -0.2 else 'danger'),
                'is_trending': token.is_trending,
                'dexscreener_url': get_dexscreener_url(token.chain, token.contract),
                'latest_update': token.latest_update,
                'formatted_time': token.latest_update if token.latest_update else '未知',
                'market_cap_formatted': format_market_cap(token.market_cap),
                'first_market_cap_formatted': format_market_cap(token.first_market_cap),
                'image_url': None,  # 添加默认image_url字段
                'trending_score': token.hype_score if token.hype_score is not None else 0,  # 添加trending_score字段，使用hype_score作为替代
                'mentions_count': token.promotion_count if token.promotion_count else 0,  # 添加mentions_count字段
                # 添加price_info嵌套对象
                'price_info': {
                    'current_price': token.price if token.price else None,
                    'current_price_formatted': f"${token.price:.8f}" if token.price else "未知",
                    'price_change_24h': ((token.price / token.first_price) - 1) * 100 if token.price and token.first_price and token.first_price > 0 else None,
                    'price_change_24h_formatted': f"{((token.price / token.first_price) - 1) * 100:.2f}%" if token.price and token.first_price and token.first_price > 0 else "未知"
                },
                # 添加sentiment_info嵌套对象
                'sentiment_info': {
                    'sentiment_score': token.sentiment_score if token.sentiment_score is not None else None,
                    'hype_score': token.hype_score if token.hype_score is not None else None
                },
                # 添加格式化的时间字段
                'first_update_formatted': token.first_update if token.first_update else '未知',
                'last_update_formatted': token.latest_update if token.latest_update else '未知'
            }
            
            # 计算涨跌幅
            if token.first_market_cap and token.first_market_cap > 0:
                change_pct = ((token.market_cap or 0) - token.first_market_cap) / token.first_market_cap * 100
                token_dict['change_percentage'] = f"{change_pct:+.2f}%"
                token_dict['change_pct_value'] = change_pct
                token_dict['is_profit'] = change_pct >= 0
            else:
                token_dict['change_percentage'] = "N/A"
                token_dict['change_pct_value'] = None
                token_dict['is_profit'] = True
            
            tokens.append(token_dict)
        
        return render_template('tokens.html', 
                               tokens=tokens,
                               pagination=pagination,
                               chain_filter=chain_filter,
                               search_query=search_query,
                               sort_order=sort_order,
                               year=datetime.now().year,
                               get_dexscreener_url=get_dexscreener_url)
                               
    except Exception as e:
        logger.error(f"处理代币列表请求时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return handle_error(f"处理代币列表请求时出错: {str(e)}")
    finally:
        session.close()


@app.route('/token/<chain>/<contract>')
def token_detail(chain, contract):
    """代币详情页面"""
    session = Session()
    try:
        # 获取代币信息
        token = session.query(Token).filter_by(chain=chain, contract=contract).first()
        if not token:
            flash(f"未找到代币: {chain}/{contract}", "error")
            return redirect(url_for('tokens_page'))
            
        # 处理代币数据
        token_dict = {
            'id': token.id,
            'chain': token.chain,
            'symbol': token.token_symbol,
            'token_symbol': token.token_symbol,
            'name': token.token_symbol,
            'contract': token.contract,
            'channel_name': token.channel_name,
            'first_seen': token.first_update if token.first_update else '未知',
            'last_seen': token.latest_update if token.latest_update else '未知',
            'mentions': token.promotion_count if token.promotion_count else 0,
            'mentions_percentage': min(token.promotion_count * 5 if token.promotion_count else 0, 100),
            'sentiment_score': token.sentiment_score if token.sentiment_score is not None else 0,
            'hype_score': token.hype_score if token.hype_score is not None else 0,
            'price_change': ((token.price / token.first_price) - 1) * 100 if token.price and token.first_price and token.first_price > 0 else 0,
            'risk_level': token.risk_level,
            'sentiment_class': 'success' if token.sentiment_score and token.sentiment_score > 0.2 else 
                                  ('warning' if token.sentiment_score and token.sentiment_score > -0.2 else 'danger'),
            'is_trending': token.is_trending,
            'dexscreener_url': get_dexscreener_url(token.chain, token.contract),
            'latest_update': token.latest_update,
            'formatted_time': token.latest_update if token.latest_update else '未知',
            'market_cap': token.market_cap,
            'market_cap_formatted': format_market_cap(token.market_cap),
            'first_market_cap': token.first_market_cap,
            'first_market_cap_formatted': format_market_cap(token.first_market_cap),
            'promotion_count': token.promotion_count,
            'likes_count': token.likes_count or 0,
            'telegram_url': token.telegram_url,
            'twitter_url': token.twitter_url,
            'website_url': token.website_url,
            'first_update': token.first_update,
            'first_price': token.first_price,
            'price_change_24h': token.price_change_24h,
            'price_change_7d': token.price_change_7d,
            'volume_24h': token.volume_24h,
            'liquidity': token.liquidity,
            'positive_words': token.positive_words.split(',') if token.positive_words else [],
            'negative_words': token.negative_words.split(',') if token.negative_words else [],
            'hype_text': token.hype_text if token.hype_text else "未知",
            'hype_color': token.hype_color if token.hype_color else "gray",
            'hype_value': token.hype_value if token.hype_value else "N/A",
            'risk_text': token.risk_text if token.risk_text else "未知风险",
            'risk_color': token.risk_color if token.risk_color else "gray",
            'image_url': None,  # 添加默认image_url字段
            'trending_score': token.hype_score if token.hype_score is not None else 0,  # 添加trending_score字段，使用hype_score作为替代
            'mentions_count': token.promotion_count if token.promotion_count else 0,  # 添加mentions_count字段
            # 添加price_info嵌套对象
            'price_info': {
                'current_price': token.price if token.price else None,
                'current_price_formatted': f"${token.price:.8f}" if token.price else "未知",
                'price_change_24h': ((token.price / token.first_price) - 1) * 100 if token.price and token.first_price and token.first_price > 0 else None,
                'price_change_24h_formatted': f"{((token.price / token.first_price) - 1) * 100:.2f}%" if token.price and token.first_price and token.first_price > 0 else "未知"
            },
            # 添加sentiment_info嵌套对象
            'sentiment_info': {
                'sentiment_score': token.sentiment_score if token.sentiment_score is not None else None,
                'hype_score': token.hype_score if token.hype_score is not None else None
            },
            # 添加格式化的时间字段
            'first_update_formatted': token.first_update if token.first_update else '未知',
            'last_update_formatted': token.latest_update if token.latest_update else '未知'
        }
        
        # 计算涨跌幅
        if token.first_market_cap and token.first_market_cap > 0:
            change_pct = ((token.market_cap or 0) - token.first_market_cap) / token.first_market_cap * 100
            token_dict['change_percentage'] = f"{change_pct:+.2f}%"
            token_dict['change_pct_value'] = change_pct
            token_dict['is_profit'] = change_pct >= 0
        else:
            token_dict['change_percentage'] = "N/A"
            token_dict['change_pct_value'] = None
            token_dict['is_profit'] = True
            
        # 价格涨跌幅处理
        if token.price and token.first_price and token.first_price > 0:
            price_change = ((token.price - token.first_price) / token.first_price) * 100
            token_dict['price_change_total'] = f"{price_change:+.2f}%"
            token_dict['price_change_value'] = price_change
            token_dict['is_price_up'] = price_change >= 0
        else:
            token_dict['price_change_total'] = "N/A"
            token_dict['price_change_value'] = None
            token_dict['is_price_up'] = True
            
        # 处理情感分析字段
        if token.sentiment_score is not None:
            if token.sentiment_score > 0.5:
                token_dict['sentiment_text'] = "非常积极"
                token_dict['sentiment_color'] = "green"
            elif token.sentiment_score > 0.1:
                token_dict['sentiment_text'] = "积极"
                token_dict['sentiment_color'] = "lightgreen"
            elif token.sentiment_score < -0.5:
                token_dict['sentiment_text'] = "非常消极"
                token_dict['sentiment_color'] = "red"
            elif token.sentiment_score < -0.1:
                token_dict['sentiment_text'] = "消极"
                token_dict['sentiment_color'] = "pink"
            else:
                token_dict['sentiment_text'] = "中性"
                token_dict['sentiment_color'] = "gray"
            
            # 转换为百分比显示
            token_dict['sentiment_pct'] = f"{token.sentiment_score * 100:.1f}%"
        else:
            token_dict['sentiment_text'] = "未知"
            token_dict['sentiment_color'] = "gray"
            token_dict['sentiment_pct'] = "N/A"
            
        # 处理炒作评分
        if token.hype_score is not None:
            if token.hype_score > 4:
                token_dict['hype_text'] = "极高炒作"
                token_dict['hype_color'] = "red"
            elif token.hype_score > 3:
                token_dict['hype_text'] = "高炒作"
                token_dict['hype_color'] = "orange"
            elif token.hype_score > 2:
                token_dict['hype_text'] = "中等炒作"
                token_dict['hype_color'] = "yellow"
            elif token.hype_score > 1:
                token_dict['hype_text'] = "低炒作"
                token_dict['hype_color'] = "lightgreen"
            else:
                token_dict['hype_text'] = "几乎无炒作"
                token_dict['hype_color'] = "green"
            
            # 格式化显示
            token_dict['hype_value'] = f"{token.hype_score:.1f}/5"
        else:
            token_dict['hype_text'] = "未知"
            token_dict['hype_color'] = "gray"
            token_dict['hype_value'] = "N/A"
            
        # 处理风险等级
        risk_map = {
            'high': {"text": "高风险", "color": "red"},
            'medium-high': {"text": "中高风险", "color": "orange"},
            'medium': {"text": "中风险", "color": "yellow"},
            'low-medium': {"text": "低中风险", "color": "lightgreen"},
            'low': {"text": "低风险", "color": "green"},
            'unknown': {"text": "未知风险", "color": "gray"}
        }
        risk_info = risk_map.get(token.risk_level, risk_map['unknown'])
        token_dict['risk_text'] = risk_info['text']
        token_dict['risk_color'] = risk_info['color']
        
        # 获取原始消息
        original_message = session.query(Message).filter_by(chain=chain, message_id=token.message_id).first()
        
        # 获取相关代币（同一个链的其他代币）
        related_tokens_query = session.query(Token).filter(
            Token.chain == chain,
            Token.contract != contract
        ).order_by(Token.latest_update.desc()).limit(5)
        
        related_tokens = []
        for related in related_tokens_query:
            related_dict = {
                'chain': related.chain,
                'token_symbol': related.token_symbol,
                'contract': related.contract,
                'market_cap': related.market_cap,
                'market_cap_formatted': format_market_cap(related.market_cap),
                'first_market_cap': related.first_market_cap,
                'sentiment_score': related.sentiment_score,
                'risk_level': related.risk_level
            }
            
            # 计算涨跌幅
            if related.first_market_cap and related.first_market_cap > 0:
                change_pct = ((related.market_cap or 0) - related.first_market_cap) / related.first_market_cap * 100
                related_dict['change_percentage'] = f"{change_pct:+.2f}%"
                related_dict['is_profit'] = change_pct >= 0
            else:
                related_dict['change_percentage'] = "N/A"
                related_dict['is_profit'] = True
                
            # 风险等级颜色
            related_dict['risk_color'] = risk_map.get(related.risk_level, risk_map['unknown'])['color']
                
            related_tokens.append(related_dict)
            
        # 获取历史消息，用于分析趋势和情感变化
        history_messages = session.query(Message).filter(
            Message.chain == chain,
            Message.text.like(f"%{token.token_symbol}%")
        ).order_by(Message.date.desc()).limit(10).all()
        
        # 获取价格历史数据用于图表显示
        price_history = []
        if token.price is not None or token.market_cap is not None:
            # 这里可以从其他表或API获取历史数据
            # 示例数据结构
            price_history = [
                {"date": token.first_update, "price": token.first_price, "market_cap": token.first_market_cap},
                {"date": token.latest_update, "price": token.price, "market_cap": token.market_cap}
            ]
            
        return render_template('token_detail.html',
                              token=token_dict,
                              original_message=original_message,
                              related_tokens=related_tokens,
                              history_messages=history_messages,
                              price_history=json.dumps(price_history),
                              positive_words=token.positive_words.split(',') if token.positive_words else [],
                              negative_words=token.negative_words.split(',') if token.negative_words else [],
                              year=datetime.now().year)
                              
    except Exception as e:
        logger.error(f"处理代币详情页面请求时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return handle_error(f"处理代币详情页面请求时出错: {str(e)}")
    finally:
        session.close()


@app.route('/channels')
def channels_page():
    """频道管理页面"""
    try:
        # 获取所有频道
        channel_manager = ChannelManager()
        channels = channel_manager.get_all_channels()
        
        # 计算活跃频道数
        active_channels = [c for c in channels if c.is_active]
        
        # 获取最后更新时间
        last_update = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return render_template('channels.html',
                               channels=channels,
                               active_channels_count=len(active_channels),
                               last_update=last_update,
                               year=datetime.now().year)
    
    except Exception as e:
        logger.error(f"处理频道页面请求时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return handle_error(f"处理频道页面请求时出错: {str(e)}")


@app.route('/channels/add', methods=['POST'])
def add_channel():
    """添加新频道"""
    try:
        channel_username = request.form.get('channel_username')
        chain = request.form.get('chain')
        
        if not channel_username or not chain:
            flash('频道用户名和链类型不能为空', 'danger')
            return redirect(url_for('channels_page'))
        
        # 添加频道
        channel_manager = ChannelManager()
        success = channel_manager.add_channel(
            channel_username=channel_username, 
            channel_name=channel_username, 
            chain=chain,
            channel_id=None,  # Web界面添加时没有ID信息
            is_group=False,   # Web界面添加时默认为普通频道
            is_supergroup=False,  # Web界面添加时默认为非超级群组
            member_count=0    # Web界面添加时不知道成员数
        )
        
        if success:
            flash(f'成功添加频道: {channel_username}', 'success')
        else:
            flash(f'频道已存在: {channel_username}', 'warning')
            
        return redirect(url_for('channels_page'))
    
    except Exception as e:
        logger.error(f"添加频道时出错: {str(e)}")
        flash(f'添加频道时出错: {str(e)}', 'danger')
        return redirect(url_for('channels_page'))


@app.route('/channels/remove/<channel_username>')
def remove_channel(channel_username):
    """移除频道"""
    try:
        # 移除频道
        channel_manager = ChannelManager()
        success = channel_manager.remove_channel(channel_username)
        
        if success:
            flash(f'成功移除频道: {channel_username}', 'success')
        else:
            flash(f'移除频道失败: {channel_username}', 'danger')
            
        return redirect(url_for('channels_page'))
    
    except Exception as e:
        logger.error(f"移除频道时出错: {str(e)}")
        flash(f'移除频道时出错: {str(e)}', 'danger')
        return redirect(url_for('channels_page'))


@app.route('/channels/activate/<channel_username>')
def activate_channel(channel_username):
    """激活频道"""
    try:
        session = Session()
        channel = session.query(TelegramChannel).filter_by(channel_username=channel_username).first()
        
        if channel:
            channel.is_active = True
            session.commit()
            flash(f'成功激活频道: {channel_username}', 'success')
        else:
            flash(f'频道不存在: {channel_username}', 'danger')
            
        return redirect(url_for('channels_page'))
    
    except Exception as e:
        logger.error(f"激活频道时出错: {str(e)}")
        flash(f'激活频道时出错: {str(e)}', 'danger')
        return redirect(url_for('channels_page'))
    finally:
        session.close()


@app.route('/channels/update')
def update_channels():
    """更新所有频道状态"""
    try:
        # 显示详细的指导信息
        script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', 'scripts/channel_manager_cli.py'))
        command = f"python {script_path} update"
        
        flash('频道状态更新需要在命令行执行以下命令:', 'info')
        flash(f'<code>{command}</code>', 'info')
        flash('这是因为更新过程需要与Telegram API交互，需要完整的认证信息', 'info')
        
        return redirect(url_for('channels_page'))
    
    except Exception as e:
        logger.error(f"更新频道时出错: {str(e)}")
        flash(f'更新频道时出错: {str(e)}', 'danger')
        return redirect(url_for('channels_page'))


@app.route('/api/like', methods=['POST'])
def like_token():
    """代币点赞API"""
    try:
        data = request.get_json()
        chain = data.get('chain')
        contract = data.get('contract')
        
        if not chain or not contract:
            return jsonify({'success': False, 'error': '缺少参数'}), 400
        
        session = Session()
        
        # 获取代币
        token = session.query(Token).filter_by(chain=chain, contract=contract).first()
        
        if not token:
            return jsonify({'success': False, 'error': '未找到代币'}), 404
        
        # 更新点赞数
        token.likes_count = (token.likes_count or 0) + 1
        session.commit()
        
        return jsonify({
            'success': True, 
            'likes_count': token.likes_count
        })
            
    except Exception as e:
        logger.error(f"处理点赞时出错: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        session.close()


@app.route('/media/<path:path>')
def send_media(path):
    """提供媒体文件"""
    try:
        media_dir = os.path.abspath('./media')
        file_path = os.path.join(media_dir, path)
        
        # 检查文件是否存在
        if not os.path.exists(file_path):
            logger.warning(f"请求的媒体文件不存在: {file_path}")
            # 返回一个默认的图像或者404
            return send_from_directory(
                os.path.join(app.static_folder, 'img'), 
                'image-not-found.png', 
                as_attachment=False
            )
            
        return send_from_directory(media_dir, path)
    except Exception as e:
        logger.error(f"提供媒体文件时出错: {str(e)}")
        abort(404)


@app.route('/statistics')
def statistics_page():
    """统计页面"""
    session = None
    try:
        # 获取系统统计数据
        stats = get_system_stats()
        
        # 获取每条链的代币数量
        session = Session()
        chain_stats = session.query(Token.chain, func.count(Token.id)).group_by(Token.chain).all()
        chain_data = {chain: count for chain, count in chain_stats}
        
        # 准备图表数据
        chart_data = {
            'chains': list(chain_data.keys()) or [],
            'counts': list(chain_data.values()) or []
        }
        
        return render_template('statistics.html',
                               chart_data=chart_data,
                               year=datetime.now().year,
                               **stats)
    
    except Exception as e:
        logger.error(f"处理统计页面请求时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return handle_error(f"处理统计页面请求时出错: {str(e)}")
    finally:
        if session:
            session.close()


# 添加API端点用于实时数据获取
@app.route('/api/token_trends')
def token_trends():
    """返回代币趋势数据，用于图表显示"""
    session = Session()
    try:
        # 获取最近7天的数据
        days = request.args.get('days', 7, type=int)
        limit = request.args.get('limit', 10, type=int)
        
        # 计算开始日期
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # 获取具有价格变化和市值变化的代币
        tokens = session.query(Token).filter(
            Token.price.isnot(None),
            Token.first_price.isnot(None),
            Token.market_cap.isnot(None)
        ).order_by(Token.latest_update.desc()).limit(limit).all()
        
        result = []
        for token in tokens:
            # 计算价格变化
            price_change = 0
            if token.price and token.first_price and token.first_price > 0:
                price_change = ((token.price - token.first_price) / token.first_price) * 100
                
            # 计算市值变化
            mcap_change = 0
            if token.market_cap and token.first_market_cap and token.first_market_cap > 0:
                mcap_change = ((token.market_cap - token.first_market_cap) / token.first_market_cap) * 100
                
            result.append({
                'chain': token.chain,
                'token_symbol': token.token_symbol,
                'contract': token.contract,
                'price': token.price,
                'first_price': token.first_price,
                'price_change': price_change,
                'market_cap': token.market_cap,
                'first_market_cap': token.first_market_cap,
                'market_cap_change': mcap_change,
                'sentiment_score': token.sentiment_score,
                'risk_level': token.risk_level,
                'first_update': token.first_update,
                'latest_update': token.latest_update
            })
            
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"获取代币趋势数据时出错: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route('/api/sentiment_stats')
def sentiment_stats():
    """返回情感分析统计数据"""
    session = Session()
    try:
        # 获取情感分析分布
        total_tokens = session.query(func.count(Token.id)).scalar()
        
        # 统计情感分布
        very_positive = session.query(func.count(Token.id)).filter(Token.sentiment_score > 0.5).scalar()
        positive = session.query(func.count(Token.id)).filter(and_(Token.sentiment_score <= 0.5, Token.sentiment_score > 0.1)).scalar()
        neutral = session.query(func.count(Token.id)).filter(and_(Token.sentiment_score <= 0.1, Token.sentiment_score >= -0.1)).scalar()
        negative = session.query(func.count(Token.id)).filter(and_(Token.sentiment_score < -0.1, Token.sentiment_score >= -0.5)).scalar()
        very_negative = session.query(func.count(Token.id)).filter(Token.sentiment_score < -0.5).scalar()
        
        # 统计按链的情感平均值
        sentiment_by_chain = session.query(
            Token.chain,
            func.avg(Token.sentiment_score).label('avg_sentiment'),
            func.count(Token.id).label('count')
        ).filter(Token.sentiment_score.isnot(None)).group_by(Token.chain).all()
        
        chain_sentiment = {}
        for chain, avg_score, count in sentiment_by_chain:
            chain_sentiment[chain] = {
                'avg_score': float(avg_score) if avg_score is not None else 0,
                'count': count
            }
            
        # 统计风险分布
        risk_stats = {
            'high': session.query(func.count(Token.id)).filter(Token.risk_level == 'high').scalar(),
            'medium-high': session.query(func.count(Token.id)).filter(Token.risk_level == 'medium-high').scalar(),
            'medium': session.query(func.count(Token.id)).filter(Token.risk_level == 'medium').scalar(),
            'low-medium': session.query(func.count(Token.id)).filter(Token.risk_level == 'low-medium').scalar(),
            'low': session.query(func.count(Token.id)).filter(Token.risk_level == 'low').scalar(),
            'unknown': session.query(func.count(Token.id)).filter(or_(Token.risk_level == 'unknown', Token.risk_level.is_(None))).scalar()
        }
        
        return jsonify({
            'total_tokens': total_tokens,
            'sentiment_distribution': {
                'very_positive': very_positive,
                'positive': positive,
                'neutral': neutral,
                'negative': negative,
                'very_negative': very_negative
            },
            'chain_sentiment': chain_sentiment,
            'risk_distribution': risk_stats
        })
        
    except Exception as e:
        logger.error(f"获取情感统计数据时出错: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


@app.route('/token_advanced')
def token_advanced():
    """代币高级列表页面"""
    try:
        # 获取分页参数
        page = request.args.get('page', 1, type=int)
        PER_PAGE = 20
        
        # 获取所有过滤条件
        filters = {
            'contract': request.args.get('contract', ''),
            'symbol': request.args.get('symbol', ''),
            'channel': request.args.get('channel', ''),
            'trending': request.args.get('trending', ''),
            'date_from': request.args.get('date_from', ''),
            'date_to': request.args.get('date_to', ''),
            'sentiment_min': request.args.get('sentiment_min', ''),
            'sentiment_max': request.args.get('sentiment_max', ''),
            'hype_min': request.args.get('hype_min', ''),
            'hype_max': request.args.get('hype_max', ''),
            'risk_level': request.args.get('risk_level', ''),
            'chain': request.args.get('chain', ''),
            'sort': request.args.get('sort', 'latest_update')
        }
        
        # 创建查询
        session = Session()
        query = session.query(Token)
        
        # 应用过滤条件
        if filters['contract']:
            query = query.filter(Token.contract.like(f"%{filters['contract']}%"))
        
        if filters['symbol']:
            query = query.filter(Token.token_symbol.like(f"%{filters['symbol']}%"))
            
        if filters['channel']:
            query = query.filter(Token.channel_name.like(f"%{filters['channel']}%"))
            
        if filters['trending'] == '1':
            query = query.filter(Token.is_trending == True)
        elif filters['trending'] == '0':
            query = query.filter(Token.is_trending == False)
            
        if filters['date_from']:
            try:
                date_from = datetime.strptime(filters['date_from'], '%Y-%m-%d')
                query = query.filter(func.datetime(Token.first_update) >= date_from)
            except ValueError:
                pass
                
        if filters['date_to']:
            try:
                date_to = datetime.strptime(filters['date_to'], '%Y-%m-%d')
                date_to = date_to.replace(hour=23, minute=59, second=59)
                query = query.filter(func.datetime(Token.latest_update) <= date_to)
            except ValueError:
                pass
                
        if filters['sentiment_min'] and filters['sentiment_min'].replace('.', '', 1).replace('-', '', 1).isdigit():
            sentiment_min = float(filters['sentiment_min'])
            query = query.filter(Token.sentiment_score >= sentiment_min)
            
        if filters['sentiment_max'] and filters['sentiment_max'].replace('.', '', 1).replace('-', '', 1).isdigit():
            sentiment_max = float(filters['sentiment_max'])
            query = query.filter(Token.sentiment_score <= sentiment_max)
            
        if filters['hype_min'] and filters['hype_min'].replace('.', '', 1).isdigit():
            hype_min = float(filters['hype_min'])
            query = query.filter(Token.hype_score >= hype_min)
            
        if filters['hype_max'] and filters['hype_max'].replace('.', '', 1).isdigit():
            hype_max = float(filters['hype_max'])
            query = query.filter(Token.hype_score <= hype_max)
            
        if filters['risk_level']:
            query = query.filter(Token.risk_level == filters['risk_level'])
            
        if filters['chain']:
            query = query.filter(Token.chain == filters['chain'])
            
        # 应用排序
        if filters['sort'] == 'sentiment':
            query = query.order_by(Token.sentiment_score.desc())
        elif filters['sort'] == 'hype':
            query = query.order_by(Token.hype_score.desc())
        elif filters['sort'] == 'risk':
            query = query.order_by(Token.risk_level)
        else:  # 默认按最近更新排序
            query = query.order_by(Token.latest_update.desc())
            
        # 获取总记录数
        total_count = query.count()
        
        # 分页
        tokens_query = query.limit(PER_PAGE).offset((page - 1) * PER_PAGE)
        
        # 创建分页对象
        pagination = {
            'page': page,
            'per_page': PER_PAGE,
            'total': total_count,
            'pages': (total_count + PER_PAGE - 1) // PER_PAGE,
            'has_prev': page > 1,
            'has_next': page < ((total_count + PER_PAGE - 1) // PER_PAGE),
            'prev_num': page - 1,
            'next_num': page + 1,
            'iter_pages': lambda: range(1, ((total_count + PER_PAGE - 1) // PER_PAGE) + 1)
        }
        
        # 处理代币数据
        tokens = []
        for token in tokens_query:
            token_dict = {
                'id': token.id,
                'chain': token.chain,
                'symbol': token.token_symbol,
                'token_symbol': token.token_symbol,
                'name': token.token_symbol,
                'contract': token.contract,
                'channel_name': token.channel_name,
                'first_seen': token.first_update if token.first_update else '未知',
                'last_seen': token.latest_update if token.latest_update else '未知',
                'mentions': token.promotion_count if token.promotion_count else 0,
                'mentions_percentage': min(token.promotion_count * 5 if token.promotion_count else 0, 100),
                'sentiment_score': token.sentiment_score if token.sentiment_score is not None else 0,
                'hype_score': token.hype_score if token.hype_score is not None else 0,
                'price_change': ((token.price / token.first_price) - 1) * 100 if token.price and token.first_price and token.first_price > 0 else 0,
                'risk_level': token.risk_level if token.risk_level else 'unknown',
                'is_trending': token.is_trending,
                'dexscreener_url': get_dexscreener_url(token.chain, token.contract),
                'latest_update': token.latest_update,
                'formatted_time': token.latest_update if token.latest_update else '未知',
                'market_cap_formatted': format_market_cap(token.market_cap),
                'first_market_cap_formatted': format_market_cap(token.first_market_cap),
                'image_url': None,  # 添加默认image_url字段
                'trending_score': token.hype_score if token.hype_score is not None else 0,  # 添加trending_score字段，使用hype_score作为替代
                'mentions_count': token.promotion_count if token.promotion_count else 0,  # 添加mentions_count字段
                # 添加price_info嵌套对象
                'price_info': {
                    'current_price': token.price if token.price else None,
                    'current_price_formatted': f"${token.price:.8f}" if token.price else "未知",
                    'price_change_24h': ((token.price / token.first_price) - 1) * 100 if token.price and token.first_price and token.first_price > 0 else None,
                    'price_change_24h_formatted': f"{((token.price / token.first_price) - 1) * 100:.2f}%" if token.price and token.first_price and token.first_price > 0 else "未知"
                },
                # 添加sentiment_info嵌套对象
                'sentiment_info': {
                    'sentiment_score': token.sentiment_score if token.sentiment_score is not None else None,
                    'hype_score': token.hype_score if token.hype_score is not None else None
                },
                # 添加格式化的时间字段
                'first_update_formatted': token.first_update if token.first_update else '未知',
                'last_update_formatted': token.latest_update if token.latest_update else '未知'
            }
            
            # 处理情绪评分的颜色渐变
            sentiment = token_dict['sentiment_score']
            if sentiment >= 0:
                # 正面情绪，从中性灰色到绿色的渐变
                intensity = min(sentiment * 2, 1)  # 将0-0.5映射到0-1
                token_dict['sentiment_color_start'] = f'rgb(200, 200, 200)'
                token_dict['sentiment_color_end'] = f'rgb({int(200 - 200 * intensity)}, 255, {int(200 - 100 * intensity)})'
            else:
                # 负面情绪，从中性灰色到红色的渐变
                intensity = min(abs(sentiment * 2), 1)  # 将0-0.5映射到0-1
                token_dict['sentiment_color_start'] = f'rgb(200, 200, 200)'
                token_dict['sentiment_color_end'] = f'rgb(255, {int(200 - 200 * intensity)}, {int(200 - 200 * intensity)})'
            
            # 添加情感评分类
            token_dict['sentiment_class'] = 'success' if sentiment > 0.2 else ('warning' if sentiment > -0.2 else 'danger')
            
            # 风险等级样式类
            risk_level = token.risk_level
            if risk_level in ['low', '低']:
                token_dict['risk_class'] = 'risk-low'
            elif risk_level in ['medium', 'medium-high', 'low-medium', '中']:
                token_dict['risk_class'] = 'risk-medium'
            elif risk_level in ['high', '高']:
                token_dict['risk_class'] = 'risk-high'
            else:
                token_dict['risk_class'] = ''
            
            tokens.append(token_dict)
        
        # 为分页URL准备过滤器字典
        filter_params = {k: v for k, v in filters.items() if v}
        
        return render_template('token_advanced.html', 
                               tokens=tokens,
                               pagination=pagination,
                               filters=filters,
                               filter_params=filter_params)
    except Exception as e:
        logger.error(f"加载代币高级列表页面时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return handle_error(f"加载代币高级列表页面时出错: {str(e)}")
    finally:
        if 'session' in locals():
            session.close()


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