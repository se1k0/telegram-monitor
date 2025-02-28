#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库修复工具
用于修复数据库结构和数据问题
"""

import os
import sys
import logging
from sqlalchemy import text, inspect

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

# 导入项目模块
from src.database.models import engine
from src.utils.logger import setup_logger, get_logger

# 设置日志
setup_logger()
logger = get_logger(__name__)

def fix_token_from_group():
    """修复token表中的from_group字段，确保与消息的is_group/is_supergroup字段一致"""
    from src.database.models import Token, Message, engine
    from sqlalchemy.orm import sessionmaker
    
    logger.info("开始修复token表中的from_group字段...")
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 查询所有token记录
        tokens = session.query(Token).all()
        updated_count = 0
        
        for token in tokens:
            # 如果token记录有关联的消息ID
            if token.message_id:
                # 查找关联的消息
                message = session.query(Message).filter_by(
                    chain=token.chain,
                    message_id=token.message_id
                ).first()
                
                if message:
                    # 检查消息的is_group/is_supergroup字段
                    is_from_group = message.is_group or message.is_supergroup
                    
                    # 如果token的from_group字段与消息的is_group/is_supergroup不一致，则更新
                    if token.from_group != is_from_group:
                        token.from_group = is_from_group
                        updated_count += 1
                        
        # 如果有更新，提交到数据库
        if updated_count > 0:
            session.commit()
            logger.info(f"成功更新了 {updated_count} 条token记录的from_group字段")
        else:
            logger.info("没有token记录需要更新from_group字段")
        
        return True
    except Exception as e:
        logger.error(f"修复token表from_group字段时出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        session.rollback()
        return False
    finally:
        session.close()

def manually_add_columns():
    """手动添加可能缺少的列和索引"""
    logger.info("开始手动修复数据库结构...")
    
    try:
        inspector = inspect(engine)
        connection = engine.connect()
        transaction = connection.begin()
        
        # 获取tokens表的现有列
        if 'tokens' in inspector.get_table_names():
            existing_columns = {col['name'] for col in inspector.get_columns('tokens')}
            
            # 定义需要检查的列及其类型
            columns_to_check = {
                'price': 'FLOAT',
                'first_price': 'FLOAT',
                'price_change_24h': 'FLOAT',
                'price_change_7d': 'FLOAT',
                'volume_24h': 'FLOAT',
                'liquidity': 'FLOAT',
                'sentiment_score': 'FLOAT',
                'positive_words': 'TEXT',
                'negative_words': 'TEXT',
                'is_trending': 'BOOLEAN',
                'hype_score': 'FLOAT',
                'risk_level': 'VARCHAR(20)',
                'from_group': 'BOOLEAN'  # 添加from_group字段
            }
            
            # 检查并添加缺失的列
            for col_name, col_type in columns_to_check.items():
                if col_name not in existing_columns:
                    # 使用原始SQL添加列
                    alter_stmt = f"ALTER TABLE tokens ADD COLUMN {col_name} {col_type}"
                    connection.execute(text(alter_stmt))
                    logger.info(f"已添加列 {col_name} 到tokens表")
        
        # 检查messages表
        if 'messages' in inspector.get_table_names():
            existing_columns = {col['name'] for col in inspector.get_columns('messages')}
            
            # 定义需要检查的列及其类型
            messages_columns_to_check = {
                'is_group': 'BOOLEAN',  # 添加is_group字段
                'is_supergroup': 'BOOLEAN'  # 添加is_supergroup字段
            }
            
            # 检查并添加缺失的列
            for col_name, col_type in messages_columns_to_check.items():
                if col_name not in existing_columns:
                    # 使用原始SQL添加列
                    alter_stmt = f"ALTER TABLE messages ADD COLUMN {col_name} {col_type}"
                    connection.execute(text(alter_stmt))
                    logger.info(f"已添加列 {col_name} 到messages表")
        
        # 检查telegram_channels表
        if 'telegram_channels' in inspector.get_table_names():
            existing_columns = {col['name'] for col in inspector.get_columns('telegram_channels')}
            
            # 定义需要检查的列及其类型
            channels_columns_to_check = {
                'channel_id': 'INTEGER',
                'is_group': 'BOOLEAN DEFAULT 0',  # 添加is_group字段，默认为0 (False)
                'is_supergroup': 'BOOLEAN DEFAULT 0'  # 添加is_supergroup字段，默认为0 (False)
            }
            
            # 检查并添加缺失的列
            for col_name, col_type in channels_columns_to_check.items():
                if col_name not in existing_columns:
                    # 添加列
                    alter_stmt = f"ALTER TABLE telegram_channels ADD COLUMN {col_name} {col_type}"
                    connection.execute(text(alter_stmt))
                    logger.info(f"已添加列 {col_name} 到telegram_channels表")
        
        transaction.commit()
        logger.info("数据库结构修复完成")
        
        # 修复token表中的from_group字段
        logger.info("开始修复token表中的from_group字段...")
        fix_token_from_group()
        
        return True
    except Exception as e:
        logger.error(f"手动修复数据库结构时出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        if 'transaction' in locals() and transaction:
            transaction.rollback()
        return False
    finally:
        if 'connection' in locals() and connection:
            connection.close()

if __name__ == "__main__":
    print("开始修复数据库...")
    
    if manually_add_columns():
        print("数据库修复成功！")
    else:
        print("数据库修复失败！请检查日志了解详情。") 