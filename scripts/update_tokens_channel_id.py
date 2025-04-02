#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
tokens表channel_name字段迁移工具

将tokens表中的channel_name字段替换为channel_id字段：
1. 在tokens表中添加channel_id字段
2. 从channel_name提取channel_id并更新到channel_id字段
3. 删除channel_name字段
"""

import os
import sys
import logging
import argparse
import sqlite3
from sqlalchemy import text, inspect, Column, Integer
from sqlalchemy.orm import sessionmaker

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# 导入项目模块
from src.utils.logger import setup_logger, get_logger
from src.database.models import engine, Base, Token, TelegramChannel
import config.settings as config

# 设置日志
setup_logger()
logger = get_logger(__name__)

def add_channel_id_column():
    """
    在tokens表中添加channel_id字段
    """
    try:
        logger.info("开始添加channel_id字段到tokens表...")
        
        # 获取tokens表的现有列
        inspector = inspect(engine)
        if 'tokens' in inspector.get_table_names():
            existing_columns = {col['name'] for col in inspector.get_columns('tokens')}
            
            # 检查channel_id列是否已存在
            if 'channel_id' not in existing_columns:
                connection = engine.connect()
                transaction = connection.begin()
                
                try:
                    # 使用原始SQL添加列
                    alter_stmt = "ALTER TABLE tokens ADD COLUMN channel_id INTEGER"
                    connection.execute(text(alter_stmt))
                    transaction.commit()
                    logger.info("成功添加channel_id列到tokens表")
                except Exception as e:
                    transaction.rollback()
                    logger.error(f"添加channel_id列时出错: {str(e)}")
                    raise
                finally:
                    connection.close()
            else:
                logger.info("channel_id列已存在，无需添加")
        else:
            logger.error("tokens表不存在")
            return False
            
        return True
    except Exception as e:
        logger.error(f"添加channel_id列时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def update_channel_id_from_channel_name():
    """
    从channel_name提取channel_id并更新到channel_id字段
    """
    try:
        logger.info("开始从channel_name提取并更新channel_id...")
        
        # 创建数据库会话
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            # 获取所有包含channel_name的tokens记录
            tokens_with_channel_name = session.query(Token).filter(Token.channel_name.isnot(None)).all()
            
            if not tokens_with_channel_name:
                logger.info("没有找到包含channel_name的tokens记录")
                return True
                
            logger.info(f"找到 {len(tokens_with_channel_name)} 条包含channel_name的tokens记录")
            
            # 创建channel_username到channel_id的映射
            channels = session.query(TelegramChannel).all()
            username_to_id = {}
            id_to_id = {}
            
            for channel in channels:
                if channel.channel_username:
                    username_to_id[channel.channel_username] = channel.channel_id
                if channel.channel_id:
                    id_to_id[f"id_{channel.channel_id}"] = channel.channel_id
            
            # 更新tokens记录的channel_id
            updated_count = 0
            for token in tokens_with_channel_name:
                channel_name = token.channel_name
                channel_id = None
                
                # 检查channel_name是否是id_格式
                if channel_name and channel_name.startswith("id_"):
                    try:
                        # 直接从id_格式提取数字
                        channel_id = int(channel_name.replace("id_", ""))
                    except (ValueError, TypeError):
                        logger.warning(f"无法从'{channel_name}'提取channel_id")
                
                # 如果不是id_格式，则从映射中查找
                elif channel_name and channel_name in username_to_id:
                    channel_id = username_to_id[channel_name]
                
                # 更新channel_id
                if channel_id:
                    token.channel_id = channel_id
                    updated_count += 1
                else:
                    logger.warning(f"无法为token {token.id} 找到对应的channel_id，channel_name: {channel_name}")
            
            # 提交更改
            session.commit()
            logger.info(f"成功更新 {updated_count} 条tokens记录的channel_id")
            
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"更新channel_id时出错: {str(e)}")
            raise
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"更新channel_id时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def drop_channel_name_column():
    """
    删除tokens表中的channel_name字段
    
    注意：SQLite不直接支持删除列，需要创建新表再复制数据
    """
    try:
        logger.info("开始删除tokens表中的channel_name字段...")
        
        # 获取tokens表的现有列
        inspector = inspect(engine)
        if 'tokens' in inspector.get_table_names():
            existing_columns = {col['name'] for col in inspector.get_columns('tokens')}
            
            # 检查channel_name列是否存在
            if 'channel_name' in existing_columns:
                # 创建SQLite连接
                db_path = config.DATABASE_URI
                if db_path.startswith('sqlite:///'):
                    db_path = db_path.replace('sqlite:///', '')
                else:
                    logger.error("不支持非SQLite数据库")
                    return False
                
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                try:
                    # SQLite不能直接删除列，需要创建一个新表，然后复制数据，最后重命名
                    
                    # 1. 获取tokens表的所有列（除了channel_name）
                    cursor.execute("PRAGMA table_info(tokens)")
                    columns_info = cursor.fetchall()
                    columns = [col[1] for col in columns_info if col[1] != 'channel_name']
                    columns_str = ', '.join(columns)
                    
                    # 2. 创建临时表，不包含channel_name列
                    cursor.execute(f"CREATE TABLE tokens_temp ({', '.join([f'{col[1]} {col[2]}' for col in columns_info if col[1] != 'channel_name'])})")
                    
                    # 3. 复制数据到临时表
                    cursor.execute(f"INSERT INTO tokens_temp SELECT {columns_str} FROM tokens")
                    
                    # 4. 删除原表
                    cursor.execute("DROP TABLE tokens")
                    
                    # 5. 重命名临时表为原表名
                    cursor.execute("ALTER TABLE tokens_temp RENAME TO tokens")
                    
                    # 6. 重新创建索引和约束
                    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_chain_contract ON tokens (chain, contract)")
                    
                    # 提交更改
                    conn.commit()
                    logger.info("成功删除tokens表中的channel_name字段")
                    
                except Exception as e:
                    conn.rollback()
                    logger.error(f"删除channel_name字段时出错: {str(e)}")
                    raise
                finally:
                    conn.close()
            else:
                logger.info("channel_name列不存在，无需删除")
        else:
            logger.error("tokens表不存在")
            return False
            
        return True
    except Exception as e:
        logger.error(f"删除channel_name字段时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def update_model_definition():
    """
    更新Token模型定义，添加channel_id字段并移除channel_name字段
    
    注意：这在运行时不会更改模型的行为，因为SQLAlchemy已经创建了表结构
    但会在代码中反映新的结构，以便将来使用
    """
    try:
        logger.info("开始更新Token模型定义...")
        
        # 添加channel_id字段到Token模型
        # 这不会改变数据库结构，仅更新模型定义
        if not hasattr(Token, 'channel_id'):
            Token.channel_id = Column(Integer)
            logger.info("成功添加channel_id字段到Token模型")
        
        # 注意：无法在运行时从模型中移除channel_name字段
        # 但可以通过设置_check_and_add_columns函数中的逻辑来避免自动重新添加
        
        logger.info("Token模型定义更新完成")
        return True
    except Exception as e:
        logger.error(f"更新Token模型定义时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def main():
    """主程序"""
    parser = argparse.ArgumentParser(description='tokens表channel_name迁移工具 - 将channel_name替换为channel_id')
    
    parser.add_argument('--add-column', action='store_true', help='只添加channel_id列')
    parser.add_argument('--update', action='store_true', help='只更新channel_id')
    parser.add_argument('--drop-column', action='store_true', help='只删除channel_name列')
    parser.add_argument('--force', action='store_true', help='强制执行所有步骤，即使发生错误')
    parser.add_argument('--no-drop', action='store_true', help='不删除channel_name列，仅添加并更新channel_id')
    
    args = parser.parse_args()
    
    # 执行单独的步骤
    if args.add_column:
        success = add_channel_id_column()
        if success:
            print("成功添加channel_id列")
        else:
            print("添加channel_id列失败")
        return success
    
    if args.update:
        success = update_channel_id_from_channel_name()
        if success:
            print("成功更新channel_id")
        else:
            print("更新channel_id失败")
        return success
    
    if args.drop_column:
        success = drop_channel_name_column()
        if success:
            print("成功删除channel_name列")
        else:
            print("删除channel_name列失败")
        return success
    
    # 执行完整迁移流程
    print("开始执行tokens表channel_name字段迁移...")
    
    # 1. 添加channel_id列
    success1 = add_channel_id_column()
    if success1:
        print("成功添加channel_id列")
    else:
        print("添加channel_id列失败")
        if not args.force:
            return False
    
    # 2. 更新channel_id值
    success2 = update_channel_id_from_channel_name()
    if success2:
        print("成功更新channel_id")
    else:
        print("更新channel_id失败")
        if not args.force:
            return False
    
    # 3. 更新模型定义
    success3 = update_model_definition()
    if success3:
        print("成功更新Token模型定义")
    else:
        print("更新Token模型定义失败")
        if not args.force:
            return False
    
    # 4. 删除channel_name列（可选）
    if not args.no_drop:
        success4 = drop_channel_name_column()
        if success4:
            print("成功删除channel_name列")
        else:
            print("删除channel_name列失败")
            if not args.force:
                return False
    else:
        print("按照要求，跳过删除channel_name列")
    
    print("tokens表channel_name字段迁移完成")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 