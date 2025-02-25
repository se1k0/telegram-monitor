import logging
import sqlite3
import json
import time
import os
from datetime import datetime, timezone, timedelta
from flask import Flask, render_template, jsonify, request, redirect, url_for, flash, send_from_directory, abort
from flask_cors import CORS
from sqlalchemy import create_engine, func
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
        logging.FileHandler("logs/web_app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 创建数据库会话
engine = create_engine(config.DATABASE_URI)
Session = sessionmaker(bind=engine)


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
                'chain': token.chain,
                'token_symbol': token.token_symbol,
                'contract': token.contract,
                'message_id': token.message_id,
                'market_cap': token.market_cap,
                'market_cap_formatted': format_market_cap(token.market_cap),
                'first_market_cap': token.first_market_cap,
                'first_market_cap_formatted': format_market_cap(token.first_market_cap),
                'promotion_count': token.promotion_count,
                'likes_count': token.likes_count or 0,
                'telegram_url': token.telegram_url,
                'twitter_url': token.twitter_url,
                'website_url': token.website_url,
                'latest_update': token.latest_update,
                'first_update': token.first_update,
                'image_url': token.dexscreener_url
            }
            
            # 格式化时间
            if token_dict.get('latest_update'):
                token_dict['latest_update_formatted'] = token_dict['latest_update']
            if token_dict.get('first_update'):
                token_dict['first_update_formatted'] = token_dict['first_update']
            
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
        
        return render_template('index.html', 
                               tokens=tokens, 
                               year=datetime.now().year,
                               **stats)
    except Exception as e:
        logger.error(f"处理首页请求时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return "服务器错误", 500
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
            query = query.order_by((Token.market_cap - Token.first_market_cap) / 
                                  func.case([(Token.first_market_cap == 0, None)], 
                                          else_=Token.first_market_cap).desc())
        elif sort_order == 'loss':
            # 按跌幅排序（升序）
            query = query.order_by((Token.market_cap - Token.first_market_cap) / 
                                  func.case([(Token.first_market_cap == 0, None)], 
                                          else_=Token.first_market_cap).asc())
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
                'chain': token.chain,
                'token_symbol': token.token_symbol,
                'contract': token.contract,
                'message_id': token.message_id,
                'market_cap': token.market_cap,
                'market_cap_formatted': format_market_cap(token.market_cap),
                'first_market_cap': token.first_market_cap,
                'first_market_cap_formatted': format_market_cap(token.first_market_cap),
                'promotion_count': token.promotion_count,
                'likes_count': token.likes_count or 0,
                'telegram_url': token.telegram_url,
                'twitter_url': token.twitter_url,
                'website_url': token.website_url,
                'latest_update': token.latest_update,
                'first_update': token.first_update,
                'image_url': token.dexscreener_url
            }
            
            # 格式化时间
            if token_dict.get('latest_update'):
                token_dict['latest_update_formatted'] = token_dict['latest_update']
            if token_dict.get('first_update'):
                token_dict['first_update_formatted'] = token_dict['first_update']
            
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
        return "服务器错误", 500
    finally:
        session.close()


@app.route('/token/<chain>/<contract>')
def token_detail(chain, contract):
    """代币详情页面"""
    try:
        session = Session()
        
        # 获取代币信息
        token = session.query(Token).filter_by(chain=chain, contract=contract).first()
        
        if not token:
            flash('未找到代币信息', 'warning')
            return redirect(url_for('tokens_page'))
        
        # 处理代币数据
        token_dict = {
            'chain': token.chain,
            'token_symbol': token.token_symbol,
            'contract': token.contract,
            'message_id': token.message_id,
            'market_cap': token.market_cap,
            'market_cap_formatted': format_market_cap(token.market_cap),
            'first_market_cap': token.first_market_cap,
            'first_market_cap_formatted': format_market_cap(token.first_market_cap),
            'promotion_count': token.promotion_count,
            'likes_count': token.likes_count or 0,
            'telegram_url': token.telegram_url,
            'twitter_url': token.twitter_url,
            'website_url': token.website_url,
            'latest_update': token.latest_update,
            'first_update': token.first_update,
            'image_url': token.dexscreener_url,
            'name': '',
            'description': '',
            'holders': 0,
            'top_holders': []
        }
        
        # 格式化时间
        if token_dict.get('latest_update'):
            token_dict['latest_update_formatted'] = token_dict['latest_update']
        if token_dict.get('first_update'):
            token_dict['first_update_formatted'] = token_dict['first_update']
        
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
                'first_market_cap': related.first_market_cap
            }
            
            # 计算涨跌幅
            if related.first_market_cap and related.first_market_cap > 0:
                change_pct = ((related.market_cap or 0) - related.first_market_cap) / related.first_market_cap * 100
                related_dict['change_percentage'] = f"{change_pct:+.2f}%"
                related_dict['is_profit'] = change_pct >= 0
            else:
                related_dict['change_percentage'] = "N/A"
                related_dict['is_profit'] = True
                
            related_tokens.append(related_dict)
        
        # 创建图表数据（这里只是模拟数据，实际应从数据库中获取）
        # 在实际系统中应该有价格历史记录
        chart_data = {
            'labels': ["1天前", "12小时前", "6小时前", "现在"],
            'values': [token.first_market_cap or 0, token.first_market_cap * 1.2 if token.first_market_cap else 0, 
                      token.market_cap * 0.9 if token.market_cap else 0, token.market_cap or 0]
        }
        
        return render_template('token_detail.html',
                               token=token_dict,
                               original_message=original_message,
                               related_tokens=related_tokens,
                               token_history=True,  # 假设有历史数据
                               chart_data=chart_data,
                               year=datetime.now().year,
                               get_dexscreener_url=get_dexscreener_url)
    
    except Exception as e:
        logger.error(f"处理代币详情请求时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return "服务器错误", 500
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
        return "服务器错误", 500


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
        success = channel_manager.add_channel(channel_username, channel_username, chain)
        
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
        return "服务器错误", 500
    finally:
        if session:
            session.close()


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
            logger.error(f"创建默认图像时出错: {str(e)}")
    
    app.run(debug=True, host='0.0.0.0', port=5000) 