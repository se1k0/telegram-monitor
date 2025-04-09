#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
检查tokens_mark表是否存在并正常工作
"""

import os
import sys
import logging
import asyncio
from datetime import datetime

# 添加项目根目录到sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def check_tokens_mark_table():
    """
    检查tokens_mark表是否存在并正常工作
    """
    try:
        # 导入数据库适配器
        from src.database.db_factory import get_db_adapter
        
        logger.info("获取数据库适配器...")
        db_adapter = get_db_adapter()
        
        # 检查表是否存在
        logger.info("检查tokens_mark表...")
        result = await db_adapter.check_tokens_mark_table()
        
        if result.get('status'):
            logger.info("tokens_mark表检查成功!")
            logger.info(f"消息: {result.get('message')}")
            
            # 显示查询结果
            select_result = result.get('select_result')
            if select_result and isinstance(select_result, list):
                logger.info(f"表中现有数据: {len(select_result)} 条")
                if len(select_result) > 0:
                    logger.info(f"示例数据: {select_result[0]}")
            else:
                logger.info("表中暂无数据")
                
            # 显示插入测试结果
            insert_result = result.get('insert_result')
            if insert_result and isinstance(insert_result, list) and len(insert_result) > 0:
                logger.info(f"测试数据插入成功: ID = {insert_result[0].get('id')}")
            
            # 尝试创建真实的测试记录
            logger.info("尝试创建一条真实的测试记录...")
            test_mark_data = {
                'chain': 'ETH',
                'token_symbol': 'TEST_REAL',
                'contract': '0xtest' + datetime.now().strftime('%Y%m%d%H%M%S'),
                'message_id': 12345,
                'market_cap': 1000000,
                'channel_id': 9876543
            }
            
            mark_result = await db_adapter.save_token_mark(test_mark_data)
            if mark_result:
                logger.info("测试记录创建成功!")
            else:
                logger.error("测试记录创建失败")
                
        else:
            logger.error("tokens_mark表检查失败!")
            logger.error(f"错误: {result.get('error')}")
            
            # 如果表不存在，尝试创建
            if "不存在" in str(result.get('error', '')).lower():
                logger.info("尝试创建tokens_mark表...")
                # 导入表创建脚本
                from scripts.create_tokens_mark_table import create_tokens_mark_table
                
                # 执行创建
                create_result = create_tokens_mark_table()
                if create_result:
                    logger.info("tokens_mark表创建成功，请重新运行此检查脚本")
                else:
                    logger.error("tokens_mark表创建失败，请在Supabase控制台手动创建")
    
    except Exception as e:
        logger.error(f"检查过程中出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

def main():
    """主函数"""
    logger.info("开始检查tokens_mark表...")
    asyncio.run(check_tokens_mark_table())
    logger.info("检查完成")

if __name__ == "__main__":
    main() 