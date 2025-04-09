#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import sqlite3
import logging
from typing import Optional
from datetime import datetime
import asyncio

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
    # 如果使用Supabase，则不需要SQLite连接
    try:
        import config.settings as config
        if config.DATABASE_URI.startswith('supabase:'):
            logger.info("使用Supabase数据库，不需要直接的SQLite连接")
            return None
    except ImportError:
        pass
        
    try:
        conn = sqlite3.connect(DB_PATH)
        return conn
    except sqlite3.Error as e:
        logger.error(f"连接数据库失败: {e}")
        return None

def check_tokens_mark_table() -> bool:
    """
    检查tokens_mark表是否已存在
    
    Returns:
        bool: tokens_mark表是否存在
    """
    # 如果使用Supabase，则通过Supabase客户端检查
    try:
        import config.settings as config
        if config.DATABASE_URI.startswith('supabase:'):
            from src.database.db_factory import get_db_adapter
            db_adapter = get_db_adapter()
            result = asyncio.run(db_adapter.execute_query(
                'tokens_mark',
                'select',
                limit=1
            ))
            logger.info(f"通过Supabase检查tokens_mark表: {'存在' if result is not None else '不存在'}")
            return result is not None
    except Exception as e:
        logger.error(f"通过Supabase检查tokens_mark表失败: {str(e)}")
    
    # 否则使用SQLite连接检查
    conn = get_connection()
    if conn is None:
        return False
        
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tokens_mark'")
        exists = cursor.fetchone() is not None
        return exists
    except sqlite3.Error as e:
        logger.error(f"检查tokens_mark表是否存在时出错: {e}")
        return False
    finally:
        conn.close()

def create_tokens_mark_table() -> bool:
    """
    创建tokens_mark表，用于记录代币被提及的历史
    
    Returns:
        bool: 表是否创建成功
    """
    # 如果使用Supabase，则通过Supabase客户端创建
    try:
        import config.settings as config
        if config.DATABASE_URI.startswith('supabase:'):
            # 显示手动创建表的指南
            logger.warning("Supabase不支持通过API直接创建表")
            logger.warning("请在Supabase控制台 > SQL Editor中执行以下SQL语句:")
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS tokens_mark (
                id BIGSERIAL PRIMARY KEY,
                chain TEXT NOT NULL,
                token_symbol TEXT,
                contract TEXT,
                message_id BIGINT,
                market_cap FLOAT,
                mention_time TIMESTAMP,
                channel_id BIGINT
            );
            """
            logger.warning(create_table_sql)
            logger.warning("或者使用Supabase控制台的Table Editor界面创建表")
            # 返回False表示未自动创建表
            return False
    except Exception as e:
        logger.error(f"处理Supabase创建tokens_mark表逻辑时出错: {str(e)}")
    
    # 否则使用SQLite连接创建
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS tokens_mark (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chain TEXT NOT NULL,
            token_symbol TEXT,
            contract TEXT,
            message_id INTEGER,
            market_cap REAL,
            mention_time TIMESTAMP,
            channel_id INTEGER
        )
        ''')
        conn.commit()
        logger.info("tokens_mark表创建成功")
        return True
    except sqlite3.Error as e:
        logger.error(f"创建tokens_mark表时出错: {e}")
        return False
    finally:
        conn.close()

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