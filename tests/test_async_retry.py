import unittest
import sys
import os
import asyncio
from unittest.mock import patch, MagicMock

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.core.telegram_listener import async_retry

class TestAsyncRetry(unittest.TestCase):
    """测试异步重试装饰器"""
    
    def setUp(self):
        """设置测试环境"""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
    
    def tearDown(self):
        """清理测试环境"""
        self.loop.close()
    
    def test_async_retry_success(self):
        """测试异步重试装饰器 - 成功场景"""
        # 创建一个异步函数，第一次调用时成功
        @async_retry(max_retries=3, delay=0.1)
        async def success_func():
            return "success"
        
        # 运行函数并验证结果
        result = self.loop.run_until_complete(success_func())
        self.assertEqual(result, "success")
    
    def test_async_retry_fail_then_succeed(self):
        """测试异步重试装饰器 - 失败后成功的场景"""
        # 创建一个计数器，用于控制函数行为
        fail_count = [0]
        
        # 创建一个异步函数，前两次调用失败，第三次成功
        @async_retry(max_retries=3, delay=0.1)
        async def fail_then_succeed():
            fail_count[0] += 1
            if fail_count[0] < 3:
                raise ConnectionError("模拟连接错误")
            return "third time's a charm"
        
        # 运行函数并验证结果
        result = self.loop.run_until_complete(fail_then_succeed())
        self.assertEqual(result, "third time's a charm")
        self.assertEqual(fail_count[0], 3)  # 应该调用三次
    
    def test_async_retry_always_fail(self):
        """测试异步重试装饰器 - 总是失败的场景"""
        # 创建一个计数器，用于跟踪调用次数
        fail_count = [0]
        
        # 创建一个异步函数，总是失败
        @async_retry(max_retries=2, delay=0.1)
        async def always_fail():
            fail_count[0] += 1
            raise ValueError("模拟错误")
        
        # 运行函数并验证异常
        with self.assertRaises(ValueError):
            self.loop.run_until_complete(always_fail())
        
        self.assertEqual(fail_count[0], 3)  # 初始调用 + 2次重试
    
    def test_async_retry_specific_exceptions(self):
        """测试异步重试装饰器 - 特定异常类型的场景"""
        # 创建一个计数器
        call_count = [0]
        
        # 创建一个异步函数，抛出不同类型的异常
        @async_retry(max_retries=2, delay=0.1, exceptions=(ValueError,))
        async def specific_exceptions():
            call_count[0] += 1
            if call_count[0] == 1:
                raise ValueError("重试这个异常")
            elif call_count[0] == 2:
                raise TypeError("不应该重试这个异常")
            return "success"
        
        # 运行函数并验证异常
        with self.assertRaises(TypeError):
            self.loop.run_until_complete(specific_exceptions())
        
        self.assertEqual(call_count[0], 2)  # 只有一次重试，因为TypeError不在重试列表中

if __name__ == '__main__':
    unittest.main() 