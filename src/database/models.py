from datetime import datetime

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, Boolean, Index, Float, ForeignKey, UniqueConstraint

from sqlalchemy.ext.declarative import declarative_base

import config.settings as config

from dataclasses import dataclass

from typing import Optional, List

import os

from sqlalchemy import event

from sqlalchemy.orm import sessionmaker

from sqlalchemy import inspect, text

import logging

import platform

from sqlalchemy.pool import QueuePool



Base = declarative_base()



# 检测操作系统环境

is_windows = platform.system() == 'Windows'



# 设置SQLite连接参数，防止"database is locked"错误

sqlite_connect_args = {

    'check_same_thread': False,

    'timeout': 60  # 增加SQLite的连接超时时间至60秒，提高并发能力

}



# 全局引擎对象

engine = None



# 跳过Supabase URL的引擎创建，因为SQLAlchemy不支持supabase方言

if config.DATABASE_URI.startswith('supabase:'):

    # 使用Supabase适配器，不需要创建SQLAlchemy引擎

    logging.info("检测到Supabase数据库URI，将使用Supabase适配器")

    # engine变量保持为None

elif config.DATABASE_URI.startswith('sqlite:'):

    # SQLite 不支持标准连接池参数，只使用connect_args

    engine = create_engine(

        config.DATABASE_URI, 

        connect_args=sqlite_connect_args,

        # 添加更多针对Windows环境的优化参数

        echo=False,  # 禁用SQL日志，减少开销

        poolclass=QueuePool if not is_windows else None,  # Windows下不使用连接池，避免锁问题

        pool_pre_ping=True,  # 自动检测断开的连接

        pool_recycle=3600    # 一小时后回收连接

    )
    

    # 为SQLite添加优化配置

    @event.listens_for(engine, "connect")

    def set_sqlite_pragma(dbapi_connection, connection_record):

        cursor = dbapi_connection.cursor()

        # 使用WAL模式（Windows中也适用）

        cursor.execute("PRAGMA journal_mode=WAL")

        # Windows环境下使用更保守的设置

        if is_windows:

            cursor.execute("PRAGMA synchronous=NORMAL")  # 平衡安全性和性能

            cursor.execute("PRAGMA cache_size=-32000")   # 32MB缓存，减少内存使用

        else:

            cursor.execute("PRAGMA synchronous=NORMAL")

            cursor.execute("PRAGMA cache_size=-64000")   # 约64MB缓存

        cursor.execute("PRAGMA foreign_keys=ON")

        cursor.execute("PRAGMA busy_timeout=60000")      # 增加至60秒，减少锁错误

        cursor.execute("PRAGMA temp_store=MEMORY")       # 使用内存存储临时表

        cursor.execute("PRAGMA mmap_size=268435456")     # 使用内存映射提高性能(256MB)

        cursor.close()

        

else:

    # 对于其他数据库（如MySQL, PostgreSQL等），可以使用连接池选项

    engine = create_engine(

        config.DATABASE_URI,

        pool_size=10,

        max_overflow=20,

        pool_timeout=30,

        pool_recycle=3600,

        pool_pre_ping=True

    )



class Message(Base):

    """简化的消息表，与 sqlite 实现保持一致"""

    __tablename__ = 'messages'

    

    id = Column(Integer, primary_key=True, autoincrement=True)

    chain = Column(String(10), nullable=False)

    message_id = Column(Integer, nullable=False)

    date = Column(DateTime, nullable=False)

    text = Column(Text)

    media_path = Column(String(255))

    channel_id = Column(Integer)  # 添加channel_id字段，替代is_group和is_supergroup

    

    __table_args__ = (

        UniqueConstraint('chain', 'message_id', name='uq_chain_message_id'),

    )



class Token(Base):

    """代币信息表"""

    __tablename__ = 'tokens'

    

    id = Column(Integer, primary_key=True, autoincrement=True)

    chain = Column(String(10), nullable=False)

    token_symbol = Column(String(50))

    contract = Column(String(255), nullable=False)

    message_id = Column(Integer)

    market_cap = Column(Float)

    market_cap_1h = Column(Float)

    market_cap_formatted = Column(String(50))

    first_market_cap = Column(Float)

    promotion_count = Column(Integer, default=0)

    likes_count = Column(Integer, default=0)

    telegram_url = Column(String(255))

    twitter_url = Column(String(255))

    website_url = Column(String(255))

    latest_update = Column(String(50))

    first_update = Column(String(50))

    dexscreener_url = Column(String(255))

    from_group = Column(Boolean, default=False)  # 是否来自群组

    channel_id = Column(Integer)  # 添加channel_id字段，用于替代channel_name
    

    # 增强字段 - 价格和市值趋势分析

    price = Column(Float)                     # 当前价格

    first_price = Column(Float)               # 首次价格

    price_change_24h = Column(Float)          # 24小时价格变化百分比

    price_change_7d = Column(Float)           # 7天价格变化百分比

    volume_24h = Column(Float)                # 24小时交易量

    volume_1h = Column(Float)                 # 1小时交易量

    liquidity = Column(Float)                 # 流动性

    holders_count = Column(Integer)           # 代币持有者数量

    

    # 新增字段 - 1小时交易数据

    buys_1h = Column(Integer, default=0)      # 1小时买入交易数

    sells_1h = Column(Integer, default=0)     # 1小时卖出交易数

    

    # 新增字段 - 代币传播统计

    spread_count = Column(Integer, default=0)  # 代币传播次数（在电报群中被提及的总次数）

    community_reach = Column(Integer, default=0)  # 代币社群覆盖人数（覆盖的总人数）

    

    # 情感分析字段

    sentiment_score = Column(Float)           # 情感分析得分 (-1到1)

    positive_words = Column(Text)             # 积极词汇列表

    negative_words = Column(Text)             # 消极词汇列表

    is_trending = Column(Boolean, default=False)  # 是否热门

    hype_score = Column(Float, default=0)     # 炒作评分

    risk_level = Column(String(20))           # 风险等级

    

    __table_args__ = (

        UniqueConstraint('chain', 'contract', name='uq_chain_contract'),

    )



class TokensMark(Base):
    """代币标记表，记录每次监听到代币信息的详细数据"""
    __tablename__ = 'tokens_mark'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    chain = Column(String(10), nullable=False)
    token_symbol = Column(String(50))
    contract = Column(String(255), nullable=False)
    message_id = Column(Integer)
    market_cap = Column(Float)
    mention_time = Column(DateTime, nullable=False, default=datetime.now)
    channel_id = Column(Integer)
    
    __table_args__ = (
        Index('idx_tokens_mark_contract', 'chain', 'contract'),
        Index('idx_tokens_mark_time', 'mention_time'),
    )



class PromotionChannel(Base):

    """推广渠道信息表"""

    __tablename__ = 'promotion_channels'

    

    id = Column(Integer, primary_key=True, autoincrement=True)

    chain = Column(String(10), nullable=False)

    message_id = Column(Integer, nullable=False)

    channel_info = Column(String(255))

    

    __table_args__ = (

        UniqueConstraint('chain', 'message_id', 'channel_info', name='uq_promotion_channel'),

    )



class HiddenToken(Base):

    """隐藏的代币表"""

    __tablename__ = 'hidden_tokens'

    

    token_symbol = Column(String(50), primary_key=True)



class TelegramChannel(Base):

    """Telegram频道和群组信息表"""

    __tablename__ = 'telegram_channels'

    

    id = Column(Integer, primary_key=True, autoincrement=True)

    channel_username = Column(String(50), nullable=True)

    channel_id = Column(Integer, nullable=True)

    channel_name = Column(String(255))

    chain = Column(String(10), nullable=False)

    is_active = Column(Boolean, default=True)

    is_group = Column(Boolean, default=False)  # 是否为群组类型

    is_supergroup = Column(Boolean, default=False)  # 是否为超级群组类型

    member_count = Column(Integer)  # 添加成员数量字段

    created_at = Column(DateTime, default=datetime.now)  # 直接使用函数引用，不用lambda

    last_updated = Column(DateTime)  # 不使用onupdate，手动更新

    

    # SQLite不支持添加约束，这些约束只在创建表时生效

    # 在现有表上添加约束需要重建表，这里仅设置模型约束

    __table_args__ = (

        # 创建索引而不是唯一约束，这样更灵活，允许有空值

        Index('idx_channel_username', 'channel_username', unique=True, sqlite_where=text('channel_username IS NOT NULL')),

        Index('idx_channel_id', 'channel_id', unique=True, sqlite_where=text('channel_id IS NOT NULL')),

    )



@dataclass

class PromotionInfo:

    """推广信息的数据类"""

    token_symbol: Optional[str] = None

    contract_address: Optional[str] = None

    market_cap: Optional[str] = None

    promotion_count: int = 1

    telegram_url: Optional[str] = None

    twitter_url: Optional[str] = None

    website_url: Optional[str] = None

    first_trending_time: Optional[datetime] = None

    chain: Optional[str] = None

    

    # 增强字段

    price: Optional[float] = None

    volume_24h: Optional[float] = None

    liquidity: Optional[float] = None

    sentiment_score: Optional[float] = None  # 情感分析得分

    positive_words: List[str] = None          # 积极词汇

    negative_words: List[str] = None          # 消极词汇

    hype_score: Optional[float] = None        # 炒作评分

    risk_level: Optional[str] = None          # 风险等级



def _check_and_add_columns():
    """检查并添加任何缺少的列"""
    logger = logging.getLogger(__name__)
    
    # 如果使用Supabase，跳过列检查
    if config.DATABASE_URI.startswith('supabase:'):
        logger.info("使用Supabase数据库，跳过列检查")
        return
        
    # 确保引擎已创建
    if engine is None:
        logger.error("数据库引擎未创建，无法检查列")
        return
        
    try:
        # 创建连接
        connection = engine.connect()
        transaction = connection.begin()
        
        # 使用inspector检查表和列
        inspector = inspect(engine)
        
        # 检查tokens表中的columns
        tokens_columns_to_check = {
            'price': 'FLOAT',
            'first_price': 'FLOAT',
            'price_change_24h': 'FLOAT',
            'price_change_7d': 'FLOAT',
            'volume_24h': 'FLOAT',
            'volume_1h': 'FLOAT',
            'liquidity': 'FLOAT',
            'holders_count': 'INTEGER',
            'buys_1h': 'INTEGER DEFAULT 0',
            'sells_1h': 'INTEGER DEFAULT 0',
            'spread_count': 'INTEGER DEFAULT 0',
            'community_reach': 'INTEGER DEFAULT 0',
            'sentiment_score': 'FLOAT',
            'positive_words': 'TEXT',
            'negative_words': 'TEXT',
            'is_trending': 'BOOLEAN DEFAULT 0',
            'hype_score': 'FLOAT DEFAULT 0',
            'risk_level': 'VARCHAR(20)',
            'from_group': 'BOOLEAN',  # 添加from_group字段
            'channel_id': 'INTEGER'   # 添加channel_id字段
        }
        
        # 获取tokens表的现有列
        if 'tokens' in inspector.get_table_names():
            existing_columns = {col['name'] for col in inspector.get_columns('tokens')}
            
            # 检查并添加缺失的列
            for col_name, col_type in tokens_columns_to_check.items():
                if col_name not in existing_columns:
                    # 使用原始SQL添加列，因为SQLAlchemy不直接支持添加列
                    alter_stmt = f"ALTER TABLE tokens ADD COLUMN {col_name} {col_type}"
                    connection.execute(text(alter_stmt))
                    logger.info(f"已添加列 {col_name} 到tokens表")
        
        # 检查messages表
        if 'messages' in inspector.get_table_names():
            existing_columns = {col['name'] for col in inspector.get_columns('messages')}
            
            # 定义需要检查的列及其类型 - channel_id 字段已替代 is_group 和 is_supergroup
            messages_columns_to_check = {
                'channel_id': 'INTEGER'  # 确保存在 channel_id 字段
            }
            
            # 检查并添加缺失的列
            for col_name, col_type in messages_columns_to_check.items():
                if col_name not in existing_columns:
                    # 使用原始SQL添加列
                    alter_stmt = f"ALTER TABLE messages ADD COLUMN {col_name} {col_type}"
                    connection.execute(text(alter_stmt))
                    logger.info(f"已添加列 {col_name} 到messages表")
        
        # 检查tokens_mark表
        if 'tokens_mark' in inspector.get_table_names():
            existing_columns = {col['name'] for col in inspector.get_columns('tokens_mark')}
            
            # 定义需要检查的列及其类型
            tokens_mark_columns_to_check = {
                'channel_id': 'INTEGER'  # 确保存在 channel_id 字段
            }
            
            # 检查并添加缺失的列
            for col_name, col_type in tokens_mark_columns_to_check.items():
                if col_name not in existing_columns:
                    # 添加列
                    alter_stmt = f"ALTER TABLE tokens_mark ADD COLUMN {col_name} {col_type}"
                    connection.execute(text(alter_stmt))
                    logger.info(f"已添加列 {col_name} 到tokens_mark表")
        
        # 检查telegram_channels表
        if 'telegram_channels' in inspector.get_table_names():
            existing_columns = {col['name'] for col in inspector.get_columns('telegram_channels')}
            
            # 定义需要检查的列及其类型
            channels_columns_to_check = {
                'channel_id': 'INTEGER',
                'is_group': 'BOOLEAN DEFAULT 0',  # 添加is_group字段，默认为0 (False)
                'is_supergroup': 'BOOLEAN DEFAULT 0',  # 添加is_supergroup字段，默认为0 (False)
                'member_count': 'INTEGER'  # 确保存在成员数量字段
            }
            
            # 检查并添加缺失的列
            for col_name, col_type in channels_columns_to_check.items():
                if col_name not in existing_columns:
                    # 添加列
                    alter_stmt = f"ALTER TABLE telegram_channels ADD COLUMN {col_name} {col_type}"
                    connection.execute(text(alter_stmt))
                    logger.info(f"已添加列 {col_name} 到telegram_channels表")
        
        transaction.commit()
        
    except Exception as e:
        logger.error(f"检查和添加列时出错: {str(e)}")
        if 'transaction' in locals() and transaction:
            transaction.rollback()
        raise
    finally:
        if 'connection' in locals() and connection:
            connection.close()



def init_db():
    """初始化数据库和所需目录"""
    # 确保数据目录存在
    os.makedirs('./data', exist_ok=True)
    
    # 检查是否使用Supabase
    if not config.DATABASE_URI.startswith('supabase://'):
        logging.error("未使用Supabase数据库，请检查配置")
        logging.error(f"当前DATABASE_URI: {config.DATABASE_URI}")
        logging.error("DATABASE_URI应以'supabase://'开头")
        return False
        
    logging.info("使用Supabase数据库，跳过本地表创建和列检查")
    
    # 检查Supabase连接
    try:
        from supabase import create_client
        supabase_url = config.SUPABASE_URL
        supabase_key = config.SUPABASE_KEY
        
        if not supabase_url or not supabase_key:
            logging.error("缺少SUPABASE_URL或SUPABASE_KEY环境变量")
            return False
        
        # 尝试连接Supabase
        logging.info(f"尝试连接到Supabase: {supabase_url}")
        supabase = create_client(supabase_url, supabase_key)
        
        # 检查是否可以访问tokens表
        response = supabase.table('tokens').select('*').limit(1).execute()
        if hasattr(response, 'data'):
            logging.info("成功连接到Supabase数据库")
            return True
        else:
            logging.error("无法从Supabase获取数据")
            return False
    except Exception as e:
        logging.error(f"Supabase连接检查失败: {str(e)}")
        import traceback
        logging.debug(traceback.format_exc())
        return False

