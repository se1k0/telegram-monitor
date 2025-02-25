from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, Boolean, Index, Float, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
import config.settings as config
from dataclasses import dataclass
from typing import Optional
import os

Base = declarative_base()
engine = create_engine(config.DATABASE_URI)

class TelegramGroup(Base):
    __tablename__ = 'telegram_groups'
    
    id = Column(Integer, primary_key=True)
    group_id = Column(String(50), unique=True, nullable=False)
    group_name = Column(String(255))
    member_count = Column(Integer)
    created_at = Column(DateTime, default=datetime.now)
    last_updated = Column(DateTime, onupdate=datetime.now)

class TelegramMessage(Base):
    __tablename__ = 'telegram_messages'
    
    id = Column(Integer, primary_key=True)
    message_id = Column(Integer, nullable=False)
    group_id = Column(String(50), nullable=False)
    user_id = Column(String(50), nullable=False)
    content = Column(Text)
    timestamp = Column(DateTime, nullable=False)
    raw_data = Column(Text)  # 原始消息的JSON数据
    is_processed = Column(Boolean, default=False)
    is_deleted = Column(Boolean, default=False)
    
    # 创建索引
    __table_args__ = (
        Index('idx_group_timestamp', 'group_id', 'timestamp'),
        Index('idx_user_group', 'user_id', 'group_id'),
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
    """Telegram频道信息表"""
    __tablename__ = 'telegram_channels'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    channel_username = Column(String(50), unique=True, nullable=False)
    channel_name = Column(String(255))
    chain = Column(String(10), nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)
    last_updated = Column(DateTime, onupdate=datetime.now)
    
    __table_args__ = (
        UniqueConstraint('channel_username', name='uq_channel_username'),
    )

@dataclass
class PromotionInfo:
    token_symbol: str
    contract_address: Optional[str]
    market_cap: Optional[float]
    promotion_count: Optional[int]
    telegram_url: Optional[str]
    twitter_url: Optional[str]
    website_url: Optional[str]
    first_trending_time: Optional[datetime]
    chain: Optional[str]

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
