import unittest
import sys
import os
from datetime import datetime
from unittest.mock import patch, MagicMock

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.db_handler import extract_promotion_info, validate_token_data, save_token_info
from src.database.models import PromotionInfo, Token

class TestDBHandler(unittest.TestCase):
    """测试数据库处理函数，包括增强的文本解析和数据验证功能"""
    
    def test_extract_promotion_info_enhanced(self):
        """测试增强版的推广信息提取函数"""
        # 测试不同格式的消息
        test_cases = [
            # 案例1: 标准格式
            {
                'text': "🚀 新币推荐 🚀\n\n🪙 代币: TEST\n📝 合约: 0x1234567890abcdef1234567890abcdef12345678\n💰 市值: 100K\nTelegram: t.me/test_group",
                'expected': {
                    'token_symbol': 'TEST',
                    'contract_address': '0x1234567890abcdef1234567890abcdef12345678',
                    'market_cap': '100K',
                    'telegram_url': 't.me/test_group'
                }
            },
            # 案例2: 不同格式 - $符号开头
            {
                'text': "$NEWTOKEN - 下一个100倍币\n合约：0xabcdef1234567890abcdef1234567890abcdef12\n市值：2.5M\n官网: https://example.com",
                'expected': {
                    'token_symbol': 'NEWTOKEN',
                    'contract_address': '0xabcdef1234567890abcdef1234567890abcdef12',
                    'market_cap': '2.5M',
                    'website_url': 'https://example.com'
                }
            },
            # 案例3: 非标准格式 - 从文本中提取
            {
                'text': "看好这个项目 ABC! 目前市值只有50K, 官方Twitter: twitter.com/abc_token",
                'expected': {
                    'token_symbol': 'ABC',
                    'market_cap': '50K',
                    'twitter_url': 'twitter.com/abc_token'
                }
            },
            # 案例4: 中文格式
            {
                'text': "重点关注：DEFI\n链：ETH\n合约地址：0x9876543210abcdef9876543210abcdef98765432\n当前市值：10M\n官方电报：t.me/defi_official",
                'expected': {
                    'token_symbol': 'DEFI',
                    'contract_address': '0x9876543210abcdef9876543210abcdef98765432',
                    'market_cap': '10M',
                    'telegram_url': 't.me/defi_official'
                }
            }
        ]
        
        # 当前日期时间
        now = datetime.now()
        
        for i, case in enumerate(test_cases):
            with self.subTest(f"Case {i+1}"):
                # 调用提取函数
                promo = extract_promotion_info(case['text'], now, 'ETH')
                
                # 验证结果
                self.assertIsNotNone(promo, f"无法从消息提取信息: {case['text']}")
                
                # 验证每个预期字段
                for field, expected_value in case['expected'].items():
                    actual_value = getattr(promo, field)
                    self.assertEqual(actual_value, expected_value, f"字段 {field} 不匹配")
    
    def test_validate_token_data(self):
        """测试代币数据验证功能"""
        # 有效数据
        valid_data = {
            'chain': 'ETH',
            'token_symbol': 'TEST',
            'contract': '0x1234567890abcdef1234567890abcdef12345678',
            'market_cap': 1000000.0,
            'telegram_url': 'https://t.me/test_group'
        }
        is_valid, error_msg = validate_token_data(valid_data)
        self.assertTrue(is_valid)
        self.assertEqual(error_msg, "")
        
        # 无效数据 - 缺少链
        invalid_data1 = {
            'token_symbol': 'TEST',
            'contract': '0x1234567890abcdef1234567890abcdef12345678'
        }
        is_valid, error_msg = validate_token_data(invalid_data1)
        self.assertFalse(is_valid)
        self.assertIn("缺少链信息", error_msg)
        
        # 无效数据 - 合约地址格式错误
        invalid_data2 = {
            'chain': 'ETH',
            'token_symbol': 'TEST',
            'contract': 'invalid_address'
        }
        is_valid, error_msg = validate_token_data(invalid_data2)
        self.assertTrue(is_valid)  # 仍然有效，但有警告
        self.assertIn("警告", error_msg)
        self.assertIn("合约地址格式", error_msg)
        
        # 无效数据 - 负市值
        invalid_data3 = {
            'chain': 'ETH',
            'token_symbol': 'TEST',
            'contract': '0x1234567890abcdef1234567890abcdef12345678',
            'market_cap': -1000.0
        }
        is_valid, error_msg = validate_token_data(invalid_data3)
        self.assertFalse(is_valid)
        self.assertIn("市值不能为负数", error_msg)
    
    @patch('src.database.db_handler.session_scope')
    def test_save_token_info(self, mock_session_scope):
        """测试保存代币信息功能"""
        # 模拟session
        mock_session = MagicMock()
        mock_session_scope.return_value.__enter__.return_value = mock_session
        
        # 模拟查询结果
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.filter_by.return_value = mock_query
        mock_query.first.return_value = None  # 不存在现有代币
        
        # 测试保存新代币
        token_data = {
            'chain': 'ETH',
            'token_symbol': 'TEST',
            'contract': '0x1234567890abcdef1234567890abcdef12345678',
            'market_cap': 1000000.0,
            'telegram_url': 'https://t.me/test_group'
        }
        
        # 调用函数
        result = save_token_info(token_data)
        
        # 验证结果和调用
        self.assertTrue(result)
        mock_session.add.assert_called_once()  # 应该调用add来添加新代币
        
        # 测试无效数据
        invalid_data = {
            'token_symbol': 'TEST',  # 缺少chain
            'contract': '0x1234567890abcdef1234567890abcdef12345678'
        }
        
        # 重置mock
        mock_session.reset_mock()
        
        # 调用函数
        result = save_token_info(invalid_data)
        
        # 验证结果和调用
        self.assertFalse(result)
        mock_session.add.assert_not_called()  # 不应该调用add

if __name__ == '__main__':
    unittest.main() 