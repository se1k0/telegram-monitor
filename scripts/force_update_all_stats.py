#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
强制更新所有代币统计数据的脚本
包括：
- 社群覆盖人数和传播次数
- 1h交易数据(buys_1h, sells_1h, volume_1h)
- 持有者数量

此脚本会绕过常规的检查和限制，强制更新所有数据
"""

import os
import sys
import asyncio
import logging
import argparse
import random
import time
from typing import Dict, List, Any, Optional

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# 导入项目模块
from src.utils.logger import setup_logger, get_logger
from src.database.db_factory import get_db_adapter

# 设置日志
setup_logger()
logger = get_logger(__name__)

async def update_community_data(limit: int = None) -> bool:
    """
    更新所有代币的社群覆盖人数和传播次数
    
    Args:
        limit: 最大更新数量，None表示不限制
        
    Returns:
        bool: 更新是否成功
    """
    logger.info("开始更新社区覆盖人数和传播次数...")
    
    try:
        # 使用异步版本的社区覆盖更新功能
        from scripts.update_community_reach import update_all_tokens_community_reach_async
        result = await update_all_tokens_community_reach_async(
            limit=limit, 
            continue_from_last=True,  # 启用断点续传
            concurrency=5  # 并发数量
        )
        
        success = result.get("success", 0) > 0
        
        if success:
            logger.info(f"社区覆盖人数和传播次数更新成功: 成功更新 {result.get('success', 0)} 个代币")
            if result.get('failed', 0) > 0:
                logger.warning(f"有 {result.get('failed', 0)} 个代币更新失败")
        else:
            logger.error("社区覆盖人数和传播次数更新失败")
            
        return success
    except Exception as e:
        logger.error(f"更新社区覆盖人数和传播次数时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def update_transaction_data(limit: int = None) -> bool:
    """
    更新所有代币的1h交易数据
    
    Args:
        limit: 最大更新数量，None表示不限制
        
    Returns:
        bool: 更新是否成功
    """
    logger.info("开始更新1h交易数据...")
    
    try:
        # 获取数据库适配器
        db_adapter = get_db_adapter()
        
        # 获取所有代币
        tokens = await db_adapter.execute_query('tokens', 'select', limit=limit)
        
        if not tokens:
            logger.warning("未找到需要更新的代币")
            return False
        
        logger.info(f"找到 {len(tokens)} 个代币需要更新")
        
        # 导入需要的模块
        from src.api.dex_screener_api import get_token_pools
        from src.api.token_market_updater import _normalize_chain_id
        
        success_count = 0
        fail_count = 0
        
        # 随机打乱代币列表顺序，避免总是相同顺序请求
        import random
        random.shuffle(tokens)
        
        # 对每个代币进行更新
        for token in tokens:
            try:
                chain = token.get('chain')
                contract = token.get('contract')
                symbol = token.get('token_symbol')
                
                if not chain or not contract:
                    continue
                
                logger.info(f"更新代币 {symbol} ({chain}/{contract}) 的1h交易数据")
                
                # 标准化链ID
                chain_id = _normalize_chain_id(chain)
                if not chain_id:
                    logger.warning(f"不支持的链: {chain}")
                    continue
                
                # 获取代币池数据
                pools_data = await asyncio.to_thread(get_token_pools, chain_id, contract)
                
                # 检查API响应
                if isinstance(pools_data, dict) and "error" in pools_data:
                    logger.error(f"获取代币池数据失败: {pools_data['error']}")
                    fail_count += 1
                    continue
                
                # 处理API返回的数据结构 - 根据API文档，token-pairs/v1返回的是数组
                if not pools_data or not isinstance(pools_data, list) or len(pools_data) == 0:
                    logger.warning(f"未找到代币 {symbol} 的交易对")
                    fail_count += 1
                    continue
                
                # 汇总所有交易对的交易数据
                buys_1h = 0
                sells_1h = 0
                volume_1h = 0
                
                for pair in pools_data:
                    # 获取交易数据
                    if "txns" in pair and "h1" in pair["txns"]:
                        txns_1h = pair["txns"]["h1"]
                        buys = txns_1h.get("buys", 0)
                        sells = txns_1h.get("sells", 0)
                        
                        # 累加交易数据
                        buys_1h += buys
                        sells_1h += sells
                    
                    # 计算1小时交易量
                    if 'volume' in pair and 'h1' in pair['volume']:
                        volume_h1_data = pair['volume']['h1']
                        if 'USD' in volume_h1_data:
                            volume_1h += float(volume_h1_data['USD'])
                
                # 更新数据库
                update_result = await db_adapter.execute_query(
                    'tokens',
                    'update',
                    data={
                        'buys_1h': buys_1h,
                        'sells_1h': sells_1h,
                        'volume_1h': volume_1h
                    },
                    filters={
                        'chain': chain,
                        'contract': contract
                    }
                )
                
                logger.info(f"成功更新代币 {symbol} 的1h交易数据: 买入={buys_1h}, 卖出={sells_1h}, 交易量=${volume_1h}")
                success_count += 1
                
                # 随机延迟，避免API限制
                delay = 0.5 + random.random() * 1.5
                await asyncio.sleep(delay)
                
            except Exception as e:
                logger.error(f"更新代币 {symbol} 的1h交易数据时出错: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                fail_count += 1
        
        logger.info(f"1h交易数据更新完成: 成功 {success_count} 个, 失败 {fail_count} 个")
        return success_count > 0
        
    except Exception as e:
        logger.error(f"更新1h交易数据时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def update_holders_data(limit: int = None) -> bool:
    """
    更新所有代币的持有者数据
    
    Args:
        limit: 最大更新数量，None表示不限制
        
    Returns:
        bool: 更新是否成功
    """
    logger.info("开始更新持有者数据...")
    
    try:
        # 获取数据库适配器
        db_adapter = get_db_adapter()
        
        # 获取所有SOL链代币(目前只支持SOL链的持有者数据获取)
        tokens = await db_adapter.execute_query(
            'tokens', 
            'select', 
            filters={'chain': 'SOL'},
            limit=limit
        )
        
        if not tokens:
            logger.warning("未找到需要更新的SOL代币")
            return False
        
        logger.info(f"找到 {len(tokens)} 个SOL代币需要更新持有者数据")
        
        # 导入DAS API
        from src.api.das_api import get_token_holders_count
        
        success_count = 0
        fail_count = 0
        
        # 对每个SOL代币进行更新
        for token in tokens:
            try:
                contract = token.get('contract')
                symbol = token.get('token_symbol')
                
                if not contract:
                    continue
                
                logger.info(f"更新SOL代币 {symbol} ({contract}) 的持有者数据")
                
                # 获取持有者数量
                holders_count = await asyncio.to_thread(get_token_holders_count, contract)
                
                if holders_count is not None:
                    # 更新数据库
                    update_result = await db_adapter.execute_query(
                        'tokens',
                        'update',
                        data={'holders_count': holders_count},
                        filters={'contract': contract, 'chain': 'SOL'}
                    )
                    
                    logger.info(f"成功更新代币 {symbol} 的持有者数量: {holders_count}")
                    success_count += 1
                else:
                    logger.warning(f"无法获取代币 {symbol} 的持有者数量")
                    fail_count += 1
                
                # 随机延迟，避免API限制
                delay = 0.5 + random.random() * 1.5
                await asyncio.sleep(delay)
                
            except Exception as e:
                logger.error(f"更新代币 {symbol} 的持有者数据时出错: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                fail_count += 1
        
        logger.info(f"持有者数据更新完成: 成功 {success_count} 个, 失败 {fail_count} 个")
        return success_count > 0
        
    except Exception as e:
        logger.error(f"更新持有者数据时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='强制更新所有代币统计数据')
    parser.add_argument('--limit', type=int, help='最大更新数量')
    parser.add_argument('--skip-community', action='store_true', help='跳过社群数据更新')
    parser.add_argument('--skip-transaction', action='store_true', help='跳过交易数据更新')
    parser.add_argument('--skip-holders', action='store_true', help='跳过持有者数据更新')
    args = parser.parse_args()
    
    logger.info("===== 开始强制更新所有代币统计数据 =====")
    
    tasks = []
    
    # 添加需要执行的任务
    if not args.skip_community:
        tasks.append(update_community_data(args.limit))
    
    if not args.skip_transaction:
        tasks.append(update_transaction_data(args.limit))
    
    if not args.skip_holders:
        tasks.append(update_holders_data(args.limit))
    
    # 顺序执行所有任务
    for task in tasks:
        await task
    
    logger.info("===== 所有数据更新任务已完成 =====")

if __name__ == "__main__":
    asyncio.run(main()) 