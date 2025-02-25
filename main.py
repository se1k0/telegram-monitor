import asyncio
import os
from pathlib import Path
from src.core.telegram_listener import run_listener
from src.database.models import init_db
import config.settings as config
import logging

def setup():
    """初始化程序运行环境"""
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("logs/main.log"),
            logging.StreamHandler()
        ]
    )
    
    # 初始化数据库
    init_db()
    logging.info("数据库初始化完成")
    
    # 创建必要目录
    os.makedirs('./logs', exist_ok=True)
    os.makedirs('./data', exist_ok=True)
    os.makedirs('./media', exist_ok=True)

if __name__ == '__main__':
    setup()
    logging.info("启动Telegram监听服务...")
    run_listener()
