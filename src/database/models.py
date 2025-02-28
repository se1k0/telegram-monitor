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



Base = declarative_base()



# 设置SQLite连接参数，防止"database is locked"错误

sqlite_connect_args = {

    'check_same_thread': False,

    'timeout': 30  # 设置SQLite的连接超时时间为30秒

}



# 创建引擎时添加连接参数

if config.DATABASE_URI.startswith('sqlite:'):

    # SQLite 不支持标准连接池参数，只使用connect_args

    engine = create_engine(

        config.DATABASE_URI, 

        connect_args=sqlite_connect_args

    )

    

    # 为SQLite添加优化配置

    @event.listens_for(engine, "connect")

    def set_sqlite_pragma(dbapi_connection, connection_record):

        cursor = dbapi_connection.cursor()

        cursor.execute("PRAGMA journal_mode=WAL")

        cursor.execute("PRAGMA synchronous=NORMAL")

        cursor.execute("PRAGMA cache_size=-64000")

        cursor.execute("PRAGMA foreign_keys=ON")

        cursor.execute("PRAGMA busy_timeout=30000")

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

    is_group = Column(Boolean, default=False)  # 是否来自群组

    is_supergroup = Column(Boolean, default=False)  # 是否来自超级群组

    

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

    channel_name = Column(String(255))  # 添加channel_name字段
    

    # 增强字段 - 价格和市值趋势分析

    price = Column(Float)                     # 当前价格

    first_price = Column(Float)               # 首次价格

    price_change_24h = Column(Float)          # 24小时价格变化百分比

    price_change_7d = Column(Float)           # 7天价格变化百分比

    volume_24h = Column(Float)                # 24小时交易量

    liquidity = Column(Float)                 # 流动性

    

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

    """检查表中是否存在所需列，如果不存在则添加"""

    logger = logging.getLogger(__name__)

    

    try:

        inspector = inspect(engine)

        connection = engine.connect()

        transaction = connection.begin()

        

        # 获取tokens表的现有列

        if 'tokens' in inspector.get_table_names():

            existing_columns = {col['name'] for col in inspector.get_columns('tokens')}

            

            # 定义需要检查的列及其类型

            columns_to_check = {

                'price': 'FLOAT',

                'first_price': 'FLOAT',

                'price_change_24h': 'FLOAT',

                'price_change_7d': 'FLOAT',

                'volume_24h': 'FLOAT',

                'liquidity': 'FLOAT',

                'sentiment_score': 'FLOAT',

                'positive_words': 'TEXT',

                'negative_words': 'TEXT',

                'is_trending': 'BOOLEAN',

                'hype_score': 'FLOAT',

                'risk_level': 'VARCHAR(20)',

                'from_group': 'BOOLEAN',  # 添加from_group字段

                'channel_name': 'VARCHAR(255)'  # 添加channel_name字段

            }

            

            # 检查并添加缺失的列

            for col_name, col_type in columns_to_check.items():

                if col_name not in existing_columns:

                    # 使用原始SQL添加列，因为SQLAlchemy不直接支持添加列

                    alter_stmt = f"ALTER TABLE tokens ADD COLUMN {col_name} {col_type}"

                    connection.execute(text(alter_stmt))

                    logger.info(f"已添加列 {col_name} 到tokens表")

        

        # 检查messages表

        if 'messages' in inspector.get_table_names():

            existing_columns = {col['name'] for col in inspector.get_columns('messages')}

            

            # 定义需要检查的列及其类型

            messages_columns_to_check = {

                'is_group': 'BOOLEAN',  # 添加is_group字段

                'is_supergroup': 'BOOLEAN'  # 添加is_supergroup字段

            }

            

            # 检查并添加缺失的列

            for col_name, col_type in messages_columns_to_check.items():

                if col_name not in existing_columns:

                    # 使用原始SQL添加列

                    alter_stmt = f"ALTER TABLE messages ADD COLUMN {col_name} {col_type}"

                    connection.execute(text(alter_stmt))

                    logger.info(f"已添加列 {col_name} 到messages表")

        

        # 检查telegram_channels表

        if 'telegram_channels' in inspector.get_table_names():
            existing_columns = {col['name'] for col in inspector.get_columns('telegram_channels')}
            
            # 定义需要检查的列及其类型
            channels_columns_to_check = {
                'channel_id': 'INTEGER',
                'is_group': 'BOOLEAN DEFAULT 0',  # 添加is_group字段，默认为0 (False)
                'is_supergroup': 'BOOLEAN DEFAULT 0'  # 添加is_supergroup字段，默认为0 (False)
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

        if transaction:

            transaction.rollback()

        raise

    finally:

        if connection:

            connection.close()



def init_db():

    """初始化数据库和所需目录"""

    # 确保数据目录存在

    os.makedirs('./data', exist_ok=True)

    

    # 创建数据库URI中的目录结构

    db_path = config.DATABASE_URI

    if db_path.startswith('sqlite:///'):

        file_path = db_path.replace('sqlite:///', '')

        dir_name = os.path.dirname(file_path)

        if dir_name and not os.path.exists(dir_name):

            os.makedirs(dir_name, exist_ok=True)

    

    # 创建所有表

    Base.metadata.create_all(bind=engine)

    print("数据库表创建完成")

    

    # 检查并添加缺失的列

    _check_and_add_columns()

