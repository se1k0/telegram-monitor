#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
更新代币社群覆盖人数(community_reach)统计

计算方式：
1. 首先根据token_symbol查询tokens_mark表中的所有条目
2. 统计查询结果中的唯一channel_id
3. 根据channel_id查询telegram_channels表中的member_count字段
4. 将所有member_count相加得到community_reach的数值并存储到tokens表中
"""

import os
import sys
import sqlite3
import logging
from datetime import datetime

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# 导入项目模块
from src.utils.logger import setup_logger, get_logger
import config.settings as config

# 设置日志
setup_logger()
logger = get_logger(__name__)

def update_token_community_reach(token_symbol=None):
    """
    更新特定代币或所有代币的社群覆盖人数
    
    Args:
        token_symbol: 代币符号，如果为None则更新所有代币
        
    Returns:
        bool: 更新是否成功
    """
    logger.info(f"开始更新{'所有代币' if token_symbol is None else token_symbol}的社群覆盖人数")
    
    try:
        # 创建数据库连接
        db_path = config.DATABASE_URI.replace('sqlite:///', '')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 如果未指定token_symbol，则查询所有唯一的代币符号
        if token_symbol is None:
            cursor.execute('''
                SELECT DISTINCT token_symbol
                FROM tokens
                WHERE token_symbol IS NOT NULL
            ''')
            token_symbols = [row[0] for row in cursor.fetchall()]
            logger.info(f"找到 {len(token_symbols)} 个唯一的代币符号")
        else:
            token_symbols = [token_symbol]
        
        # 统计更新的代币数
        updated_count = 0
        
        # 对每个代币进行处理
        for symbol in token_symbols:
            # 步骤一：查询tokens_mark表中与token_symbol相关的所有条目
            cursor.execute('''
                SELECT DISTINCT channel_id
                FROM tokens_mark
                WHERE token_symbol = ? AND channel_id IS NOT NULL
            ''', (symbol,))
            
            channels = cursor.fetchall()
            total_reach = 0
            
            # 记录找到的唯一channel_id数量
            logger.info(f"代币 {symbol} 找到 {len(channels)} 个唯一channel_id")
            
            # 步骤二和三：根据channel_id查询member_count并累加
            for (channel_id,) in channels:
                if channel_id:
                    cursor.execute('''
                        SELECT member_count
                        FROM telegram_channels
                        WHERE channel_id = ? AND is_active = 1
                    ''', (channel_id,))
                    
                    result = cursor.fetchone()
                    if result and result[0]:
                        total_reach += result[0]
                        logger.debug(f"频道/群组 {channel_id} 的成员数: {result[0]}")
            
            # 获取当前的community_reach值
            cursor.execute('''
                SELECT community_reach
                FROM tokens
                WHERE token_symbol = ?
            ''', (symbol,))
            
            current_reach = cursor.fetchone()
            current_reach = current_reach[0] if current_reach and current_reach[0] is not None else 0
            
            # 步骤四：更新代币的社群覆盖人数
            cursor.execute('''
                UPDATE tokens 
                SET community_reach = ? 
                WHERE token_symbol = ?
            ''', (total_reach, symbol))
            
            updated_count += 1
            
            # 记录更新结果
            if current_reach != total_reach:
                logger.info(f"更新代币 {symbol} 的社群覆盖人数: {current_reach} -> {total_reach}")
            else:
                logger.debug(f"代币 {symbol} 的社群覆盖人数无需更新: {current_reach}")
            
            # 每100个代币提交一次事务
            if updated_count % 100 == 0:
                conn.commit()
                logger.info(f"已更新 {updated_count}/{len(token_symbols)} 个代币的社群覆盖人数")
        
        # 提交事务
        conn.commit()
        logger.info(f"社群覆盖人数更新完成，共更新 {updated_count} 个代币")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"更新社群覆盖人数时出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        
        return False

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='更新代币社群覆盖人数')
    parser.add_argument('--token', '-t', help='指定要更新的代币符号')
    args = parser.parse_args()
    
    if args.token:
        success = update_token_community_reach(args.token)
    else:
        success = update_token_community_reach()
        
    if success:
        print("社群覆盖人数更新成功")
        return 0
    else:
        print("社群覆盖人数更新失败")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 