#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
错误处理模块
提供异常处理装饰器和错误监控功能
"""

import os
import sys
import time
import traceback
import functools
import logging
import asyncio
from datetime import datetime
from typing import Callable, Any, Optional, Dict, List, Union, Tuple

# 配置日志
try:
    from src.utils.logger import get_logger
    logger = get_logger(__name__)
except ImportError:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)

# 错误计数器
error_counters: Dict[str, Dict[str, Union[int, float]]] = {}
# 错误历史记录
error_history: List[Dict[str, Any]] = []
# 最大历史记录数
MAX_ERROR_HISTORY = 100
# 错误通知阈值
ERROR_THRESHOLD = 5
# 错误重置时间（秒）
ERROR_RESET_TIME = 3600  # 1小时


def log_error(func_name: str, error: Exception, args: tuple = None, kwargs: dict = None) -> None:
    """记录错误信息到日志和错误历史"""
    error_type = type(error).__name__
    error_msg = str(error)
    stack_trace = traceback.format_exc()
    
    # 记录到日志
    logger.error(f"函数 {func_name} 发生错误: {error_type} - {error_msg}")
    logger.debug(f"堆栈跟踪:\n{stack_trace}")
    
    # 更新错误计数器
    current_time = time.time()
    if func_name not in error_counters:
        error_counters[func_name] = {
            'count': 1,
            'first_error_time': current_time,
            'last_error_time': current_time
        }
    else:
        # 检查是否需要重置计数器
        if current_time - error_counters[func_name]['last_error_time'] > ERROR_RESET_TIME:
            error_counters[func_name] = {
                'count': 1,
                'first_error_time': current_time,
                'last_error_time': current_time
            }
        else:
            error_counters[func_name]['count'] += 1
            error_counters[func_name]['last_error_time'] = current_time
    
    # 添加到错误历史
    error_entry = {
        'timestamp': datetime.now().isoformat(),
        'function': func_name,
        'error_type': error_type,
        'error_message': error_msg,
        'stack_trace': stack_trace,
        'args': str(args) if args else None,
        'kwargs': str(kwargs) if kwargs else None
    }
    
    error_history.append(error_entry)
    
    # 限制历史记录大小
    if len(error_history) > MAX_ERROR_HISTORY:
        error_history.pop(0)
    
    # 检查是否需要发送警报
    if error_counters[func_name]['count'] >= ERROR_THRESHOLD:
        send_error_alert(func_name, error_counters[func_name]['count'], error_type, error_msg)


def send_error_alert(func_name: str, count: int, error_type: str, error_msg: str) -> None:
    """发送错误警报
    
    可以通过多种方式发送警报：
    - 日志
    - 电子邮件
    - 短信
    - Webhook
    - 等等
    """
    alert_msg = f"警报: 函数 {func_name} 在短时间内发生了 {count} 次错误! 最新错误: {error_type} - {error_msg}"
    logger.critical(alert_msg)
    
    # TODO: 实现其他警报方式
    # 例如: 发送电子邮件、短信或调用webhook


def retry(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0, 
          exceptions: tuple = (Exception,), logger: logging.Logger = None) -> Callable:
    """重试装饰器
    
    Args:
        max_retries: 最大重试次数
        delay: 初始延迟时间（秒）
        backoff: 延迟时间的增长因子
        exceptions: 需要捕获的异常类型
        logger: 日志记录器
    
    Returns:
        装饰后的函数
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            local_logger = logger or logging.getLogger(func.__module__)
            retry_count = 0
            current_delay = delay
            
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retry_count += 1
                    if retry_count > max_retries:
                        log_error(func.__name__, e, args, kwargs)
                        raise
                    
                    local_logger.warning(
                        f"函数 {func.__name__} 发生错误: {str(e)}，将在 {current_delay:.2f} 秒后进行第 {retry_count} 次重试"
                    )
                    
                    time.sleep(current_delay)
                    current_delay *= backoff
        
        return wrapper
    
    return decorator


def async_retry(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0, 
                exceptions: tuple = (Exception,), logger: logging.Logger = None) -> Callable:
    """异步重试装饰器
    
    Args:
        max_retries: 最大重试次数
        delay: 初始延迟时间（秒）
        backoff: 延迟时间的增长因子
        exceptions: 需要捕获的异常类型
        logger: 日志记录器
    
    Returns:
        装饰后的异步函数
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            local_logger = logger or logging.getLogger(func.__module__)
            retry_count = 0
            current_delay = delay
            
            while True:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    retry_count += 1
                    if retry_count > max_retries:
                        log_error(func.__name__, e, args, kwargs)
                        raise
                    
                    local_logger.warning(
                        f"异步函数 {func.__name__} 发生错误: {str(e)}，将在 {current_delay:.2f} 秒后进行第 {retry_count} 次重试"
                    )
                    
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
        
        return wrapper
    
    return decorator


def safe_execute(default_return: Any = None, log_exception: bool = True) -> Callable:
    """安全执行装饰器
    
    捕获所有异常并返回默认值
    
    Args:
        default_return: 发生异常时的返回值
        log_exception: 是否记录异常
    
    Returns:
        装饰后的函数
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if log_exception:
                    log_error(func.__name__, e, args, kwargs)
                return default_return
        
        return wrapper
    
    return decorator


def async_safe_execute(default_return: Any = None, log_exception: bool = True) -> Callable:
    """异步安全执行装饰器
    
    捕获所有异常并返回默认值
    
    Args:
        default_return: 发生异常时的返回值
        log_exception: 是否记录异常
    
    Returns:
        装饰后的异步函数
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if log_exception:
                    log_error(func.__name__, e, args, kwargs)
                return default_return
        
        return wrapper
    
    return decorator


def get_error_stats() -> Dict[str, Any]:
    """获取错误统计信息
    
    Returns:
        包含错误统计信息的字典
    """
    return {
        'total_errors': sum(counter['count'] for counter in error_counters.values()),
        'function_errors': {func: data['count'] for func, data in error_counters.items()},
        'error_history': error_history[-10:],  # 最近10条错误记录
    }


def reset_error_stats() -> None:
    """重置错误统计信息"""
    global error_counters, error_history
    error_counters = {}
    error_history = []


class ErrorMonitor:
    """错误监控类
    
    用于监控和管理应用程序中的错误
    """
    
    def __init__(self, app_name: str = "TelegramMonitor"):
        self.app_name = app_name
        self.start_time = datetime.now()
        self.last_report_time = self.start_time
        self.report_interval = 3600  # 1小时
    
    def generate_report(self) -> Dict[str, Any]:
        """生成错误报告
        
        Returns:
            包含错误报告的字典
        """
        now = datetime.now()
        uptime = (now - self.start_time).total_seconds()
        
        report = {
            'app_name': self.app_name,
            'report_time': now.isoformat(),
            'uptime_seconds': uptime,
            'uptime_formatted': self._format_uptime(uptime),
            'error_stats': get_error_stats(),
            'system_info': {
                'python_version': sys.version,
                'platform': sys.platform,
                'pid': os.getpid()
            }
        }
        
        self.last_report_time = now
        return report
    
    def _format_uptime(self, seconds: float) -> str:
        """格式化运行时间
        
        Args:
            seconds: 运行秒数
        
        Returns:
            格式化后的运行时间字符串
        """
        days, remainder = divmod(int(seconds), 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        parts = []
        if days > 0:
            parts.append(f"{days}天")
        if hours > 0 or days > 0:
            parts.append(f"{hours}小时")
        if minutes > 0 or hours > 0 or days > 0:
            parts.append(f"{minutes}分钟")
        parts.append(f"{seconds}秒")
        
        return "".join(parts)
    
    def should_generate_report(self) -> bool:
        """检查是否应该生成报告
        
        Returns:
            如果应该生成报告则返回True
        """
        now = datetime.now()
        seconds_since_last_report = (now - self.last_report_time).total_seconds()
        return seconds_since_last_report >= self.report_interval
    
    def log_report(self) -> None:
        """将报告记录到日志"""
        if not self.should_generate_report():
            return
            
        report = self.generate_report()
        logger.info(f"错误监控报告 - {self.app_name}")
        logger.info(f"运行时间: {report['uptime_formatted']}")
        logger.info(f"总错误数: {report['error_stats']['total_errors']}")
        
        for func, count in report['error_stats']['function_errors'].items():
            logger.info(f"函数 {func}: {count} 个错误")
        
        if report['error_stats']['error_history']:
            logger.info("最近错误:")
            for error in report['error_stats']['error_history']:
                logger.info(f"- {error['timestamp']}: {error['function']} - {error['error_type']} - {error['error_message']}")


# 创建全局错误监控器实例
error_monitor = ErrorMonitor()


def monitor_errors(interval: int = 3600) -> None:
    """定期监控错误并生成报告
    
    Args:
        interval: 报告间隔（秒）
    """
    error_monitor.report_interval = interval
    error_monitor.log_report()


# 示例用法
if __name__ == "__main__":
    # 测试重试装饰器
    @retry(max_retries=3, delay=0.1)
    def test_retry():
        print("尝试执行可能失败的操作...")
        if random.random() < 0.7:  # 70%的概率失败
            raise ValueError("随机错误")
        return "成功"
    
    # 测试安全执行装饰器
    @safe_execute(default_return="默认值")
    def test_safe_execute():
        print("尝试执行可能失败的操作...")
        if random.random() < 0.7:  # 70%的概率失败
            raise ValueError("随机错误")
        return "成功"
    
    # 测试异步重试装饰器
    @async_retry(max_retries=3, delay=0.1)
    async def test_async_retry():
        print("尝试执行可能失败的异步操作...")
        if random.random() < 0.7:  # 70%的概率失败
            raise ValueError("随机错误")
        return "成功"
    
    # 测试异步安全执行装饰器
    @async_safe_execute(default_return="默认值")
    async def test_async_safe_execute():
        print("尝试执行可能失败的异步操作...")
        if random.random() < 0.7:  # 70%的概率失败
            raise ValueError("随机错误")
        return "成功"
    
    # 导入随机模块（仅用于测试）
    import random
    
    # 测试同步函数
    for i in range(5):
        try:
            result = test_retry()
            print(f"重试结果: {result}")
        except Exception as e:
            print(f"重试最终失败: {e}")
    
    for i in range(5):
        result = test_safe_execute()
        print(f"安全执行结果: {result}")
    
    # 测试异步函数
    async def run_async_tests():
        for i in range(5):
            try:
                result = await test_async_retry()
                print(f"异步重试结果: {result}")
            except Exception as e:
                print(f"异步重试最终失败: {e}")
        
        for i in range(5):
            result = await test_async_safe_execute()
            print(f"异步安全执行结果: {result}")
    
    # 运行异步测试
    import asyncio
    asyncio.run(run_async_tests())
    
    # 生成错误报告
    report = error_monitor.generate_report()
    print("错误报告:", report) 