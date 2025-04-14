#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
代币社区覆盖数据更新工具 - 命令行接口

这是一个向后兼容的命令行工具，实际功能已经移至 src/utils/update_reach 包
本脚本只是一个简单的入口点，调用对应的功能
"""

import os
import sys
import argparse
import asyncio
import logging

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入实际的功能模块
from src.utils.update_reach import (
    update_token_community_reach_async,
    update_all_tokens_community_reach,
    update_all_tokens_community_reach_async,
    aggregate_member_counts,
    update_community_reach_and_spread
)
from src.utils.logger import setup_logger, get_logger

# 设置日志
setup_logger()
logger = get_logger(__name__)

async def main_async():
    """异步主函数"""
    parser = argparse.ArgumentParser(description='代币社区覆盖数据更新工具')
    
    # 创建子命令
    subparsers = parser.add_subparsers(dest='command', help='命令')
    
    # 单个代币更新命令
    token_parser = subparsers.add_parser('token', help='更新单个代币的社区覆盖数据')
    token_parser.add_argument('token_symbol', type=str, help='代币symbol')
    
    # 所有代币更新命令
    all_parser = subparsers.add_parser('all', help='更新所有代币的社区覆盖数据')
    all_parser.add_argument('--limit', type=int, help='最大更新数量')
    all_parser.add_argument('--continue', dest='continue_from_last', action='store_true', 
                         help='从上次中断的位置继续更新')
    all_parser.add_argument('--async', dest='use_async', action='store_true', 
                         help='使用异步并发更新')
    all_parser.add_argument('--concurrency', type=int, default=5, 
                         help='并发数量 (默认: 5)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    if args.command == 'token':
        # 更新单个代币
        aggregated_data = aggregate_member_counts(args.token_symbol)
        if not aggregated_data["success"]:
            logger.error(f"获取代币(symbol: {args.token_symbol})的聚合数据失败: {aggregated_data['error']}")
            return 1
            
        result = update_community_reach_and_spread(args.token_symbol, aggregated_data)
        return 0 if result["success"] else 1
        
    elif args.command == 'all':
        # 更新所有代币
        if hasattr(args, 'use_async') and args.use_async:
            # 使用异步并发更新
            result = await update_all_tokens_community_reach_async(
                args.limit,
                args.continue_from_last,
                args.concurrency
            )
        else:
            # 使用同步更新
            result = update_all_tokens_community_reach(
                args.limit,
                args.continue_from_last
            )
            
        return 0 if result.get("success", 0) > 0 else 1
    
    return 0

def main():
    """命令行工具主函数"""
    try:
        # 使用asyncio运行异步主函数
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("收到用户中断，正在退出...")
        return 1
    except Exception as e:
        logger.critical(f"程序崩溃: {str(e)}")
        import traceback
        logger.critical(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main()) 