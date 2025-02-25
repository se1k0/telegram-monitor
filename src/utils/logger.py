import logging
import os
import sys
from pathlib import Path
from datetime import datetime
import config.settings as config
import io

def setup_logger(name=None, log_level=None):
    """设置并返回一个配置好的logger
    
    Args:
        name: logger名称，默认为根logger
        log_level: 日志级别，默认从配置中读取
    
    Returns:
        logging.Logger: 配置好的logger实例
    """
    # 确保日志目录存在
    log_dir = config.LOG_DIR
    os.makedirs(log_dir, exist_ok=True)
    
    # 获取logger
    logger = logging.getLogger(name)
    
    # 如果已经配置过，直接返回
    if logger.handlers:
        return logger
    
    # 设置日志级别
    level = log_level or getattr(logging, os.getenv('LOG_LEVEL', 'INFO'), logging.INFO)
    logger.setLevel(level)
    
    # 创建文件处理器
    today = datetime.now().strftime('%Y-%m-%d')
    log_file = log_dir / f"{today}_monitor.log"
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(level)
    
    # 创建控制台处理器
    # 解决Windows控制台编码问题，确保emoji等特殊字符可以正确输出
    if sys.platform == 'win32':
        # 自定义StreamHandler，确保处理编码问题
        class SafeStreamHandler(logging.StreamHandler):
            def emit(self, record):
                try:
                    msg = self.format(record)
                    stream = self.stream
                    # 安全地写入流，避免编码错误
                    stream.write(msg + self.terminator)
                    self.flush()
                except Exception:
                    # 如果发生编码错误，使用ascii替换不可显示的字符
                    try:
                        msg = self.format(record)
                        # 使用errors='replace'替换无法编码的字符
                        safe_msg = msg.encode('ascii', errors='replace').decode('ascii')
                        stream = self.stream
                        stream.write(safe_msg + self.terminator)
                        self.flush()
                    except Exception:
                        self.handleError(record)
        
        # 使用安全的处理器
        console_handler = SafeStreamHandler(stream=sys.stdout)
    else:
        # 非Windows平台直接使用标准StreamHandler
        console_handler = logging.StreamHandler(stream=sys.stdout)
    
    console_handler.setLevel(level)
    
    # 创建格式化器
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # 添加处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # 尝试修复Windows控制台输出
    if sys.platform == 'win32':
        try:
            # 设置控制台编码为utf-8
            os.system('chcp 65001 >nul 2>&1')
            # 尝试重新配置stdout
            sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        except Exception:
            # 如果上述方法失败，不要崩溃
            pass
    
    return logger

def get_logger(name=None):
    """获取一个配置好的logger
    
    Args:
        name: logger名称，默认为根logger
    
    Returns:
        logging.Logger: 配置好的logger实例
    """
    return setup_logger(name) 