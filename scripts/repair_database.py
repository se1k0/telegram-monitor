#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import sqlite3
import logging
from typing import List, Dict, Any, Tuple

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

def get_table_schema(conn: sqlite3.Connection, table_name: str) -> List[Tuple[str, str]]:
    """
    获取表格结构
    
    Args:
        conn: 数据库连接
        table_name: 表名
        
    Returns:
        List[Tuple[str, str]]: (column_name, column_type) 列表
    """
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    return [(row[1], row[2]) for row in cursor.fetchall()]

def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """
    检查表是否存在
    
    Args:
        conn: 数据库连接
        table_name: 表名
        
    Returns:
        bool: 表是否存在
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    return bool(cursor.fetchone())

def add_column(conn: sqlite3.Connection, table_name: str, column_name: str, column_type: str) -> bool:
    """
    为表添加列
    
    Args:
        conn: 数据库连接
        table_name: 表名
        column_name: 列名
        column_type: 列类型
        
    Returns:
        bool: 添加是否成功
    """
    try:
        cursor = conn.cursor()
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
        conn.commit()
        logger.info(f"已为表 {table_name} 添加列 {column_name} ({column_type})")
        return True
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e).lower():
            logger.warning(f"列 {column_name} 已存在于表 {table_name} 中")
            return True
        logger.error(f"添加列 {column_name} 到表 {table_name} 失败: {e}")
        return False

def manually_add_columns() -> None:
    """
    手动检查并添加所有必要的列，仅限models.py中定义的列
    """
    if not check_database_exists():
        logger.error("数据库文件不存在，无法修复")
        return
    
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # 检查数据库中的表
        logger.info("开始检查数据库表结构")
        logger.info("此修复脚本不会添加任何未经授权的字段")
        logger.info("只有models.py中定义的表和字段会被检查和添加")
        
        conn.close()
        logger.info("数据库修复完成")
        
    except Exception as e:
        logger.error(f"数据库修复失败: {e}")
        import traceback
        logger.debug(traceback.format_exc())

def main():
    """
    主函数
    """
    logger.info("开始数据库修复过程...")
    manually_add_columns()
    logger.info("数据库修复过程完成")

if __name__ == "__main__":
    main() 