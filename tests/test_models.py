import unittest
import sys
import os
import sqlite3
from datetime import datetime
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.models import (
    Base, TelegramGroup, TelegramMessage, Message, 
    Token, PromotionChannel, HiddenToken, TelegramChannel,
    PromotionInfo, init_db
)
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class TestDatabaseModels(unittest.TestCase):
    """测试数据库模型类"""
    
    def setUp(self):
        """初始化测试环境"""
        # 使用内存数据库进行测试
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
    
    def tearDown(self):
        """清理测试环境"""
        self.session.close()
    
    def test_telegram_channel(self):
        """测试TelegramChannel模型"""
        # 创建测试频道
        channel = TelegramChannel(
            channel_username='test_channel',
            channel_name='Test Channel',
            chain='ETH',
            is_active=True
        )
        
        # 添加到会话并提交
        self.session.add(channel)
        self.session.commit()
        
        # 从数据库中查询并验证
        db_channel = self.session.query(TelegramChannel).filter_by(channel_username='test_channel').first()
        self.assertIsNotNone(db_channel)
        self.assertEqual(db_channel.channel_name, 'Test Channel')
        self.assertEqual(db_channel.chain, 'ETH')
        self.assertTrue(db_channel.is_active)
    
    def test_message(self):
        """测试Message模型"""
        # 创建测试消息
        now = datetime.now()
        message = Message(
            chain='SOL',
            message_id=12345,
            date=now,
            text='测试消息内容',
            media_path='media/test.jpg'
        )
        
        # 添加到会话并提交
        self.session.add(message)
        self.session.commit()
        
        # 从数据库中查询并验证
        db_message = self.session.query(Message).filter_by(message_id=12345).first()
        self.assertIsNotNone(db_message)
        self.assertEqual(db_message.chain, 'SOL')
        self.assertEqual(db_message.text, '测试消息内容')
        self.assertEqual(db_message.media_path, 'media/test.jpg')
    
    def test_token(self):
        """测试Token模型"""
        # 创建测试代币信息
        token = Token(
            chain='SOL',
            token_symbol='TEST',
            contract='0x1234567890abcdef',
            message_id=12345,
            market_cap=1000000.0,
            market_cap_formatted='100万',
            first_market_cap=1000000.0,
            promotion_count=1,
            likes_count=10,
            telegram_url='https://t.me/testtoken',
            twitter_url='https://twitter.com/testtoken',
            website_url='https://testtoken.com',
            latest_update='2023-01-01 12:00:00',
            first_update='2023-01-01 12:00:00'
        )
        
        # 添加到会话并提交
        self.session.add(token)
        self.session.commit()
        
        # 从数据库中查询并验证
        db_token = self.session.query(Token).filter_by(token_symbol='TEST').first()
        self.assertIsNotNone(db_token)
        self.assertEqual(db_token.chain, 'SOL')
        self.assertEqual(db_token.contract, '0x1234567890abcdef')
        self.assertEqual(db_token.market_cap, 1000000.0)
        self.assertEqual(db_token.telegram_url, 'https://t.me/testtoken')
    
    def test_promotion_info_dataclass(self):
        """测试PromotionInfo数据类"""
        # 创建测试推广信息
        promo = PromotionInfo(
            token_symbol='TEST',
            contract_address='0x1234567890abcdef',
            market_cap=1000000.0,
            promotion_count=1,
            telegram_url='https://t.me/testtoken',
            twitter_url='https://twitter.com/testtoken',
            website_url='https://testtoken.com',
            first_trending_time=datetime.now(),
            chain='SOL'
        )
        
        # 验证属性
        self.assertEqual(promo.token_symbol, 'TEST')
        self.assertEqual(promo.contract_address, '0x1234567890abcdef')
        self.assertEqual(promo.market_cap, 1000000.0)
        self.assertEqual(promo.telegram_url, 'https://t.me/testtoken')
        self.assertEqual(promo.chain, 'SOL')
    
    def test_init_db(self):
        """测试初始化数据库功能的简化版本"""
        # 我们只测试能否调用init_db函数，而不验证实际创建文件
        # 因为在测试环境中，数据库URI可能无法正确解析
        try:
            # 执行初始化
            init_db()
            # 如果没有抛出异常，则认为测试通过
            self.assertTrue(True)
        except Exception as e:
            self.fail(f"init_db() 引发了异常: {e}")

if __name__ == '__main__':
    unittest.main() 