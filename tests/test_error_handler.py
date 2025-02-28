import unittest
import sys
import os
import asyncio
import time
from unittest.mock import patch, MagicMock, call
from datetime import datetime

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.error_handler import (
    retry, async_retry, safe_execute, async_safe_execute,
    log_error, ErrorMonitor, get_error_stats, reset_error_stats
)

class TestErrorHandler(unittest.TestCase):
    """测试错误处理模块"""
    
    def setUp(self):
        """设置测试环境"""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        # 每次测试前重置错误统计
        reset_error_stats()
    
    def tearDown(self):
        """清理测试环境"""
        self.loop.close()
    
    def test_retry_decorator(self):
        """测试重试装饰器"""
        mock_func = MagicMock(side_effect=[ValueError("测试错误"), ValueError("测试错误"), "成功"])
        
        @retry(max_retries=2, delay=0.1)
        def test_func():
            return mock_func()
        
        result = test_func()
        self.assertEqual(result, "成功")
        self.assertEqual(mock_func.call_count, 3)  # 初始调用 + 2次重试
    
    def test_retry_max_attempts_reached(self):
        """测试重试达到最大次数后失败"""
        mock_func = MagicMock(side_effect=ValueError("测试错误"))
        
        @retry(max_retries=2, delay=0.1)
        def test_func():
            return mock_func()
        
        with self.assertRaises(ValueError):
            test_func()
        
        self.assertEqual(mock_func.call_count, 3)  # 初始调用 + 2次重试
    
    def test_retry_specific_exceptions(self):
        """测试只对特定异常类型进行重试"""
        mock_func = MagicMock(side_effect=[ValueError("重试"), TypeError("不重试"), "成功"])
        
        @retry(max_retries=2, delay=0.1, exceptions=(ValueError,))
        def test_func():
            return mock_func()
        
        with self.assertRaises(TypeError):
            test_func()
        
        self.assertEqual(mock_func.call_count, 2)  # 仅重试ValueError
    
    def test_safe_execute(self):
        """测试安全执行装饰器"""
        @safe_execute(default_return="默认值")
        def test_func():
            raise ValueError("测试错误")
        
        result = test_func()
        self.assertEqual(result, "默认值")
    
    def test_safe_execute_success(self):
        """测试安全执行装饰器 - 成功场景"""
        @safe_execute(default_return="默认值")
        def test_func():
            return "原始值"
        
        result = test_func()
        self.assertEqual(result, "原始值")
    
    @patch('src.utils.error_handler.logger')
    def test_log_error(self, mock_logger):
        """测试错误日志记录"""
        error = ValueError("测试错误")
        log_error("test_function", error)
        
        # 验证日志调用
        mock_logger.error.assert_called_once()
        mock_logger.debug.assert_called_once()
        
        # 适应实际代码中的错误统计结构
        stats = get_error_stats()
        # 检查函数名是否在function_errors字段中
        self.assertIn("function_errors", stats)
        self.assertIn("test_function", stats["function_errors"])
        # 检查计数是否为1
        self.assertEqual(stats["function_errors"]["test_function"], 1)
    
    def test_error_monitor(self):
        """测试错误监控器"""
        monitor = ErrorMonitor("TestApp")
        
        # 模拟一些错误
        for _ in range(3):
            log_error("test_func1", ValueError("错误1"))
        
        log_error("test_func2", TypeError("错误2"))
        
        # 生成报告
        report = monitor.generate_report()
        
        # 验证报告内容
        self.assertEqual(report["app_name"], "TestApp")
        
        # 适应实际代码中的错误统计结构
        error_stats = report["error_stats"]
        self.assertEqual(error_stats["total_errors"], 4)
        # 检查错误函数是否在function_errors字段中
        self.assertIn("function_errors", error_stats)
        self.assertIn("test_func1", error_stats["function_errors"])
        self.assertIn("test_func2", error_stats["function_errors"])
        # 验证test_func1的错误计数为3
        self.assertEqual(error_stats["function_errors"]["test_func1"], 3)
        # 验证test_func2的错误计数为1
        self.assertEqual(error_stats["function_errors"]["test_func2"], 1)
    
    async def async_test_async_retry(self):
        """测试异步重试装饰器"""
        mock_func = MagicMock(side_effect=[ValueError("测试错误"), "成功"])
        
        @async_retry(max_retries=1, delay=0.1)
        async def test_func():
            return mock_func()
        
        result = await test_func()
        self.assertEqual(result, "成功")
        self.assertEqual(mock_func.call_count, 2)
    
    async def async_test_async_safe_execute(self):
        """测试异步安全执行装饰器"""
        @async_safe_execute(default_return="默认值")
        async def test_func():
            raise ValueError("测试错误")
        
        result = await test_func()
        self.assertEqual(result, "默认值")
    
    def test_async_retry(self):
        """测试异步重试装饰器（包装器）"""
        self.loop.run_until_complete(self.async_test_async_retry())
    
    def test_async_safe_execute(self):
        """测试异步安全执行装饰器（包装器）"""
        self.loop.run_until_complete(self.async_test_async_safe_execute())

if __name__ == '__main__':
    unittest.main() 