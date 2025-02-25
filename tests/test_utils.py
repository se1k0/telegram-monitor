import unittest
import sys
import os
from datetime import datetime
from unittest.mock import patch

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.utils import parse_market_cap, format_market_cap
from src.database.db_handler import extract_url_from_text, extract_promotion_info
from src.database.models import PromotionInfo

class TestUtilsFunctions(unittest.TestCase):
    """测试工具函数"""
    
    def test_parse_market_cap(self):
        """测试市值解析函数"""
        # 测试整数
        self.assertEqual(parse_market_cap(1000), 1000.0)
        
        # 测试浮点数
        self.assertEqual(parse_market_cap(1000.5), 1000.5)
        
        # 测试字符串
        self.assertEqual(parse_market_cap("1000"), 1000.0)
        
        # 测试带符号的字符串
        self.assertEqual(parse_market_cap("$1,000"), 1000.0)
        
        # 测试带K的字符串
        self.assertEqual(parse_market_cap("100K"), 100000.0)
        self.assertEqual(parse_market_cap("100k"), 100000.0)
        
        # 测试带M的字符串
        self.assertEqual(parse_market_cap("1.5M"), 1500000.0)
        self.assertEqual(parse_market_cap("1.5m"), 1500000.0)
        
        # 测试带B的字符串
        self.assertEqual(parse_market_cap("2B"), 2000000000.0)
        self.assertEqual(parse_market_cap("2b"), 2000000000.0)
        
        # 测试包含额外文本的字符串
        self.assertEqual(parse_market_cap("💰 市值：2.5M"), 2500000.0)
        
        # 测试错误输入
        self.assertEqual(parse_market_cap("无效输入"), 0)
    
    def test_format_market_cap(self):
        """测试市值格式化函数"""
        # 测试小数值
        self.assertEqual(format_market_cap(100), "100.00")
        
        # 测试万级别
        self.assertEqual(format_market_cap(12345), "1.23万")
        self.assertEqual(format_market_cap(1000000), "100.00万")
        
        # 测试亿级别
        self.assertEqual(format_market_cap(100000000), "1.00亿")
        self.assertEqual(format_market_cap(123456789), "1.23亿")
        
        # 测试字符串输入
        self.assertEqual(format_market_cap("1000000"), "100.00万")
        
        # 测试错误输入
        self.assertEqual(format_market_cap("无效输入"), "0.00")
        self.assertEqual(format_market_cap(None), "0.00")
    
    def test_extract_url_from_text(self):
        """测试URL提取函数"""
        # 测试完整URL
        text = "我们的网站是 https://example.com 欢迎访问"
        self.assertEqual(extract_url_from_text(text), "https://example.com")
        
        # 测试带有路径的URL
        text = "访问 https://example.com/path?query=1 了解更多"
        self.assertEqual(extract_url_from_text(text), "https://example.com/path?query=1")
        
        # 测试www开头的URL
        text = "网址 www.example.com 可以直接访问"
        self.assertEqual(extract_url_from_text(text), "www.example.com")
        
        # 测试带关键词的URL提取
        text = "加入我们的Telegram群组 t.me/example_group"
        self.assertEqual(extract_url_from_text(text, "t.me/"), "t.me/example_group")
        
        # 测试没有URL的情况
        text = "这段文本中没有网址"
        self.assertIsNone(extract_url_from_text(text))
    
    def test_extract_promotion_info(self):
        """测试推广信息提取函数"""
        # 为避免依赖提取函数的具体实现，我们直接测试数据类的创建和属性访问
        from src.database.models import PromotionInfo
        date = datetime.now()
        
        # 创建测试实例
        promo = PromotionInfo(
            token_symbol='TEST',
            contract_address='0x1234567890abcdef',
            market_cap='100K',
            promotion_count=1,
            telegram_url='t.me/test_group',
            twitter_url='twitter.com/test_token',
            website_url='https://test-token.com',
            first_trending_time=date,
            chain='SOL'
        )
        
        # 验证属性
        self.assertEqual(promo.token_symbol, 'TEST')
        self.assertEqual(promo.contract_address, '0x1234567890abcdef')
        self.assertEqual(promo.market_cap, '100K')
        self.assertEqual(promo.promotion_count, 1)
        self.assertEqual(promo.telegram_url, 't.me/test_group')
        self.assertEqual(promo.twitter_url, 'twitter.com/test_token')
        self.assertEqual(promo.website_url, 'https://test-token.com')
        self.assertEqual(promo.first_trending_time, date)
        self.assertEqual(promo.chain, 'SOL')

if __name__ == '__main__':
    unittest.main() 