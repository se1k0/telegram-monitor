#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
代币数据更新命令行工具
根据配置的参数动态更新代币数据
支持限制更新数量、调整延迟等参数
"""

import os
import sys
import argparse
import logging
import time
import asyncio
import random
from typing import Dict, Any
from datetime import datetime

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# 导入项目模块
from src.utils.logger import setup_logger, get_logger
from src.api.token_updater import token_update
from config.settings import env_config

# 设置日志
setup_logger()
logger = get_logger(__name__)

async def main_async():
    """异步主函数"""
    parser = argparse.ArgumentParser(description='代币数据更新工具')
    
    parser.add_argument('--limit', type=int, 
                      help=f'最大更新代币数量，默认为配置文件中的值 ({env_config.TOKEN_UPDATE_LIMIT})')
    
    parser.add_argument('--test', action='store_true', 
                      help='测试模式，只打印不实际执行更新')
    
    parser.add_argument('--loop', action='store_true',
                      help=f'循环模式，每隔指定的间隔时间（分钟）更新一次，默认值为 {env_config.TOKEN_UPDATE_INTERVAL} 分钟')
    
    parser.add_argument('--interval', type=int, 
                      help=f'循环模式下的更新间隔（分钟），默认为配置文件中的值 ({env_config.TOKEN_UPDATE_INTERVAL})')
    
    args = parser.parse_args()
    
    # 使用参数值或默认值
    limit = args.limit
    interval_minutes = args.interval or env_config.TOKEN_UPDATE_INTERVAL
    
    if args.test:
        logger.info("="*60)
        logger.info("测试模式：只显示将要更新的内容，不实际执行")
        logger.info(f"更新参数：最大更新数量 = {limit or env_config.TOKEN_UPDATE_LIMIT}")
        logger.info(f"批次大小 = {env_config.TOKEN_UPDATE_BATCH_SIZE}")
        logger.info(f"延迟范围 = {env_config.TOKEN_UPDATE_MIN_DELAY} - {env_config.TOKEN_UPDATE_MAX_DELAY} 秒")
        if args.loop:
            logger.info(f"循环模式：每 {interval_minutes} 分钟更新一次")
        logger.info("="*60)
        return 0
    
    # 循环模式
    if args.loop:
        logger.info(f"启动循环模式，每 {interval_minutes} 分钟更新一次，限制数量: {limit or env_config.TOKEN_UPDATE_LIMIT}")
        
        while True:
            try:
                start_time = datetime.now()
                logger.info(f"开始更新周期: {start_time}")
                
                # 调用token更新函数
                result = token_update(limit=limit)
                
                if result.get("error"):
                    logger.error(f"更新失败: {result['error']}")
                else:
                    success_count = result.get("success", 0)
                    total_count = result.get("total", 0)
                    duration = result.get("duration", 0)
                    logger.info(f"更新完成: 总数={total_count}, 成功={success_count}, " 
                              f"用时={duration:.2f}秒")
                
                # 计算下次执行的等待时间
                end_time = datetime.now()
                elapsed_seconds = (end_time - start_time).total_seconds()
                wait_seconds = max(1, interval_minutes * 60 - elapsed_seconds)
                
                logger.info(f"等待 {wait_seconds:.2f} 秒后开始下一次更新...")
                await asyncio.sleep(wait_seconds)
                
            except KeyboardInterrupt:
                logger.info("收到中断信号，退出循环...")
                break
                
            except Exception as e:
                logger.error(f"发生错误: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                
                # 等待一段时间后重试
                logger.info("5分钟后重试...")
                await asyncio.sleep(300)
    
    # 单次执行模式
    else:
        try:
            logger.info(f"开始更新代币数据，限制数量: {limit or env_config.TOKEN_UPDATE_LIMIT}...")
            
            # 调用token更新函数
            result = token_update(limit=limit)
            
            if result.get("error"):
                logger.error(f"更新失败: {result['error']}")
                return 1
            else:
                success_count = result.get("success", 0)
                total_count = result.get("total", 0)
                duration = result.get("duration", 0)
                logger.info(f"更新完成: 总数={total_count}, 成功={success_count}, " 
                          f"用时={duration:.2f}秒")
                return 0
                
        except Exception as e:
            logger.error(f"发生错误: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return 1

def main():
    """同步主函数入口点"""
    exit_code = asyncio.run(main_async())
    sys.exit(exit_code)

if __name__ == "__main__":
    main() 