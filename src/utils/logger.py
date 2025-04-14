import logging
import os
import sys
from pathlib import Path
from datetime import datetime
import traceback

def setup_logger(name=None, log_level=None):
    """设置并返回一个配置好的logger
    
    Args:
        name: logger名称，默认为根logger
        log_level: 日志级别，默认从配置中读取
    
    Returns:
        logging.Logger: 配置好的logger实例
    """
    try:
        # 导入配置模块
        from config.settings import LOG_LEVEL
        
        # 设置日志级别
        level = log_level or getattr(logging, LOG_LEVEL, logging.INFO)
        
        # 确保日志目录存在并可写
        base_dir = Path(__file__).resolve().parent.parent.parent
        log_dir = base_dir / 'logs'
        os.makedirs(log_dir, exist_ok=True)
        
        # 准备日志文件路径
        today = datetime.now().strftime('%Y-%m-%d')
        log_file = log_dir / f"{today}_monitor.log"
        log_file_str = str(log_file)
        
        # 重置根日志记录器，避免处理器重复添加
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # 重新配置根日志记录器
        root_logger.setLevel(level)
        
        # 使用最基本的Python日志配置
        logging.basicConfig(
            level=level,
            format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file_str, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        # 设置httpx日志级别为WARNING，这样DEBUG和INFO级别的HTTP请求日志就不会显示
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        
        # 获取指定名称的日志记录器
        logger = logging.getLogger(name)
        logger.setLevel(level)
        
        # 尝试写入初始日志消息
        logging.info(f"日志系统初始化完成 - 文件路径: {log_file_str}")
        
        # 直接写入一条测试消息到文件
        try:
            with open(log_file_str, 'a', encoding='utf-8') as f:
                f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - 日志系统测试消息\n")
        except Exception as e:
            print(f"直接写入测试消息失败: {e}")
            
        return logger
            
    except Exception as e:
        print(f"配置日志系统时出错: {e}")
        traceback.print_exc()
        
        # 确保至少有一个可用的logger，即使发生错误
        logger = logging.getLogger(name)
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        
        return logger

def get_logger(name=None):
    """获取一个配置好的logger
    
    Args:
        name: logger名称，默认为根logger
    
    Returns:
        logging.Logger: 配置好的logger实例
    """
    return setup_logger(name)

# 原始的stderr
_orig_stderr = sys.stderr

# 自定义的StreamToLogger类 - 只用于stderr
class StderrToLogger:
    """将stderr重定向到logger"""
    def __init__(self, log_file):
        self.log_file = log_file
        self.encoding = getattr(_orig_stderr, 'encoding', 'utf-8')

    def write(self, buf):
        # 保留对原始stderr的写入
        _orig_stderr.write(buf)
        
        # 同时写入日志文件
        try:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [ERROR] STDERR: {buf}")
        except:
            pass

    def flush(self):
        # 保留对原始stderr的刷新
        _orig_stderr.flush()

def install_stderr_handler(log_file):
    """安装stderr处理器，将错误输出重定向到日志文件"""
    sys.stderr = StderrToLogger(log_file) 