import logging
import logging.config
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
    
    # 设置日志级别
    level = log_level or getattr(logging, config.LOG_LEVEL, logging.INFO)
    logger.setLevel(level)
    
    # 检查根日志记录器是否已配置处理器
    root_logger = logging.getLogger()
    
    # 如果根日志记录器已有处理器，说明已经配置过，直接返回
    if root_logger.handlers:
        return logger
        
    # 使用配置文件中的字典配置日志系统
    try:
        logging.config.dictConfig(config.LOG_CONFIG)
        return logger
    except (AttributeError, ImportError) as e:
        # 如果配置出错，降级到基本配置
        print(f"无法应用日志配置字典: {e}")
        
        # 创建基本的控制台处理器和文件处理器
        # 创建文件处理器
        today = datetime.now().strftime('%Y-%m-%d')
        log_file = log_dir / f"{today}_monitor.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        
        # 创建控制台处理器
        console_handler = logging.StreamHandler(stream=sys.stdout)
        console_handler.setLevel(level)
        
        # 创建格式化器
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # 添加处理器到根日志记录器
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)
        
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