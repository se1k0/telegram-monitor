#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import sqlite3
import logging
from typing import Optional
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 添加项目根目录到sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# 获取数据库文件路径
DB_PATH = os.path.join(parent_dir, "data", "telegram_data.db")

def check_database_exists() -> bool:
    """
    检查数据库文件是否存在
    
    Returns:
        bool: 数据库文件是否存在
    """
    if not os.path.exists(DB_PATH):
        logger.error(f"数据库文件不存在: {DB_PATH}")
        return False
    return True

def get_connection() -> Optional[sqlite3.Connection]:
    """
    获取数据库连接
    
    Returns:
        Optional[sqlite3.Connection]: 数据库连接对象，如果连接失败则返回None
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        return conn
    except sqlite3.Error as e:
        logger.error(f"连接数据库失败: {e}")
        return None

def check_tokens_mark_table() -> bool:
    """
    检查tokens_mark表是否存在
    
    Returns:
        bool: 表是否存在
    """
    if not check_database_exists():
        return False
        
    conn = get_connection()
    if not conn:
        return False
        
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='tokens_mark'"
        )
        result = bool(cursor.fetchone())
        conn.close()
        return result
    except sqlite3.Error as e:
        logger.error(f"检查tokens_mark表失败: {e}")
        if conn:
            conn.close()
        return False

def create_tokens_mark_table() -> bool:
    """
    创建tokens_mark表 - 严格按照models.py中的定义
    
    Returns:
        bool: 创建是否成功
    """
    if not check_database_exists():
        return False
        
    conn = get_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # 创建tokens_mark表 - 完全按照models.py中的定义
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS tokens_mark (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chain VARCHAR(10) NOT NULL,
            token_symbol VARCHAR(50),
            contract VARCHAR(255) NOT NULL,
            message_id INTEGER,
            market_cap FLOAT,
            mention_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            channel_id INTEGER
        )
        ''')
        
        # 创建索引 - 完全按照models.py中的定义
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_tokens_mark_contract ON tokens_mark (chain, contract)
        ''')
        
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_tokens_mark_time ON tokens_mark (mention_time)
        ''')
        
        conn.commit()
        logger.info("tokens_mark表创建成功")
        conn.close()
        return True
    except sqlite3.Error as e:
        logger.error(f"创建tokens_mark表失败: {e}")
        if conn:
            conn.close()
        return False

def main():
    """
    主函数
    """
    logger.info("开始检查和创建tokens_mark表...")
    
    if check_tokens_mark_table():
        logger.info("tokens_mark表已存在")
    else:
        logger.info("tokens_mark表不存在，尝试创建...")
        result = create_tokens_mark_table()
        if result:
            logger.info("tokens_mark表创建成功")
        else:
            logger.error("tokens_mark表创建失败")
    
    logger.info("处理完成")

if __name__ == "__main__":
    main() 