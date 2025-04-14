#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
统一的代币数据更新工具
提供以下功能：
1. 市场数据更新（market_cap, liquidity, price等）
2. 交易数据更新（buys_1h, sells_1h等）
3. 交易量数据更新（volume_1h等）
4. 全量数据综合更新

支持单个代币更新、批量更新和全量更新
支持并发处理提高效率
"""

import os
import sys
import logging
import time
import asyncio
import random
from typing import List, Dict, Any, Optional
from datetime import datetime
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager

# 导入项目模块
from src.utils.logger import setup_logger, get_logger
from src.database.models import engine, init_db
from src.api.token_market_updater import (
    update_token_market_data,
    update_all_tokens_market_data,
    update_tokens_by_symbols,
    update_token_txn_data,
    update_all_tokens_txn_data,
    update_token_market_and_txn_data,
    update_all_tokens_market_and_txn_data
)

# 设置日志
logger = get_logger(__name__)

# 创建会话工厂，使用连接池
# 使用QueuePool连接池，提高连接效率
engine_with_pool = create_engine(
    str(engine.url),
    poolclass=QueuePool,
    pool_size=10,  # 连接池大小
    pool_timeout=30,  # 连接池超时时间
    pool_recycle=1800,  # 连接回收时间（30分钟）
    max_overflow=20  # 最大溢出连接数
)
SessionPool = sessionmaker(bind=engine_with_pool)

# 更新类型常量
UPDATE_MARKET = 'market'     # 仅更新市场数据
UPDATE_TXN = 'txn'           # 仅更新交易数据
UPDATE_ALL = 'all'           # 更新所有数据

# Session上下文管理，自动处理会话创建和异常
@contextmanager
def get_session():
    """创建数据库会话的上下文管理器，自动处理事务和异常"""
    session = SessionPool()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()

def ensure_database_structure() -> bool:
    """
    确保数据库结构正确，添加缺失的列
    
    Returns:
        bool: 更新是否成功
    """
    try:
        logger.info("检查数据库结构...")
        
        # 调用数据库模块的初始化函数，它会自动创建表和添加缺失的列
        logger.info("初始化数据库表结构...")
        init_db()
        
        logger.info("数据库结构检查完成")
        return True
            
    except Exception as e:
        logger.error(f"检查数据库结构时发生错误: {str(e)}")
        return False

def update_token(chain: str, contract: str, update_type: str = UPDATE_ALL) -> Dict[str, Any]:
    """
    更新单个代币的数据
    
    Args:
        chain: 区块链名称
        contract: 代币合约地址
        update_type: 更新类型，'market'仅更新市场数据，'txn'仅更新交易数据，'all'更新全部数据
        
    Returns:
        Dict[str, Any]: 包含更新结果的字典
    """
    result = {"success": False, "error": None, "data": None}
    
    # 使用上下文管理器处理会话
    with get_session() as session:
        try:
            # 根据更新类型选择不同的更新函数
            if update_type == UPDATE_MARKET:
                logger.info(f"开始更新代币 {chain}/{contract} 的市场数据")
                update_result = update_token_market_data(session, chain, contract)
            elif update_type == UPDATE_TXN:
                logger.info(f"开始更新代币 {chain}/{contract} 的交易数据")
                update_result = update_token_txn_data(session, chain, contract)
            else:  # UPDATE_ALL
                logger.info(f"开始全量更新代币 {chain}/{contract} 的数据")
                update_result = update_token_market_and_txn_data(session, chain, contract)
            
            if "error" in update_result:
                logger.error(f"更新失败: {update_result['error']}")
                result["error"] = update_result["error"]
                return result
            
            logger.info(f"更新成功!")
            result["success"] = True
            result["data"] = update_result
            
            # 根据更新类型输出不同的结果信息
            if update_type in [UPDATE_MARKET, UPDATE_ALL]:
                if 'marketCap' in update_result:
                    logger.info(f"市值: {update_result.get('marketCap', 'N/A')}")
                if 'liquidity' in update_result:
                    logger.info(f"流动性: {update_result.get('liquidity', 'N/A')}")
                if 'price' in update_result and update_result['price']:
                    logger.info(f"价格: {update_result.get('price', 'N/A')}")
                    
            if update_type in [UPDATE_TXN, UPDATE_ALL]:
                logger.info(f"1小时买入交易数: {update_result.get('buys_1h', 'N/A')}")
                logger.info(f"1小时卖出交易数: {update_result.get('sells_1h', 'N/A')}")
            
            return result
        except Exception as e:
            logger.error(f"更新代币时发生错误: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
            result["error"] = str(e)
            return result

async def update_token_async(chain: str, contract: str, symbol: str, update_type: str = UPDATE_ALL) -> Dict[str, Any]:
    """
    异步更新单个代币的数据
    
    Args:
        chain: 区块链名称
        contract: 代币合约地址
        symbol: 代币符号，用于日志记录
        update_type: 更新类型，'market'仅更新市场数据，'txn'仅更新交易数据，'all'更新全部数据
        
    Returns:
        Dict[str, Any]: 包含更新结果的字典
    """
    result = {"chain": chain, "contract": contract, "symbol": symbol, "success": False, "error": None}
    
    try:
        # 使用线程池执行同步数据库操作
        loop = asyncio.get_event_loop()
        # 在线程池中执行更新操作
        update_result = await loop.run_in_executor(
            None, 
            lambda: update_token(chain, contract, update_type)
        )
        
        result.update(update_result)
        return result
    except Exception as e:
        logger.error(f"异步更新代币 {symbol} ({chain}/{contract}) 时发生错误: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        result["error"] = str(e)
        return result

def update_by_symbols(symbols: List[str], update_type: str = UPDATE_ALL) -> Dict[str, Any]:
    """
    根据代币符号批量更新代币数据
    
    Args:
        symbols: 代币符号列表
        update_type: 更新类型，'market'仅更新市场数据，'txn'仅更新交易数据，'all'更新全部数据
        
    Returns:
        Dict[str, Any]: 包含更新结果的字典
    """
    result = {
        "success": False,
        "total": len(symbols),
        "updated": 0,
        "failed": 0,
        "skipped": 0,
        "errors": []
    }
    
    if not symbols:
        logger.warning("未提供代币符号列表，无法更新")
        result["error"] = "未提供代币符号列表"
        return result
    
    # 使用上下文管理器处理会话
    with get_session() as session:
        try:
            # 根据更新类型选择不同的更新函数
            if update_type == UPDATE_MARKET:
                logger.info(f"开始批量更新 {len(symbols)} 个代币的市场数据")
                update_result = update_tokens_by_symbols(session, symbols, update_market=True, update_txn=False)
            elif update_type == UPDATE_TXN:
                logger.info(f"开始批量更新 {len(symbols)} 个代币的交易数据")
                update_result = update_tokens_by_symbols(session, symbols, update_market=False, update_txn=True)
            else:  # UPDATE_ALL
                logger.info(f"开始全量批量更新 {len(symbols)} 个代币的数据")
                update_result = update_tokens_by_symbols(session, symbols, update_market=True, update_txn=True)
            
            # 处理结果
            result["success"] = True
            result["updated"] = update_result.get("updated", 0)
            result["failed"] = update_result.get("failed", 0)
            result["skipped"] = update_result.get("skipped", 0)
            result["errors"] = update_result.get("errors", [])
            
            logger.info(f"批量更新完成: 成功 {result['updated']}，失败 {result['failed']}，跳过 {result['skipped']}")
            
            return result
        except Exception as e:
            logger.error(f"批量更新代币时发生错误: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
            result["error"] = str(e)
            return result

async def update_by_symbols_async(symbols: List[str], update_type: str = UPDATE_ALL) -> Dict[str, Any]:
    """
    异步批量更新代币数据
    
    Args:
        symbols: 代币符号列表
        update_type: 更新类型，'market'仅更新市场数据，'txn'仅更新交易数据，'all'更新全部数据
        
    Returns:
        Dict[str, Any]: 包含更新结果的字典
    """
    try:
        # 使用线程池执行同步数据库操作
        loop = asyncio.get_event_loop()
        # 在线程池中执行更新操作
        update_result = await loop.run_in_executor(
            None, 
            lambda: update_by_symbols(symbols, update_type)
        )
        
        return update_result
    except Exception as e:
        logger.error(f"异步批量更新代币时发生错误: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        return {
            "success": False,
            "total": len(symbols),
            "updated": 0,
            "failed": len(symbols),
            "skipped": 0,
            "errors": [str(e)],
            "error": str(e)
        }

async def update_tokens_batch_async(tokens: List[Dict[str, str]], update_type: str = UPDATE_ALL, 
                               concurrency: int = 3, delay: float = 1.0) -> Dict[str, Any]:
    """
    异步批量更新代币数据，支持并发控制
    
    Args:
        tokens: 代币信息列表，每个字典包含chain、contract和symbol
        update_type: 更新类型，'market'仅更新市场数据，'txn'仅更新交易数据，'all'更新全部数据
        concurrency: 并发数量
        delay: 每个任务之间的延迟时间(秒)
        
    Returns:
        Dict[str, Any]: 包含更新结果的字典
    """
    result = {
        "success": True,
        "total": len(tokens),
        "updated": 0,
        "failed": 0,
        "skipped": 0,
        "results": [],
        "errors": []
    }
    
    if not tokens:
        logger.warning("未提供代币列表，无法更新")
        result["success"] = False
        result["error"] = "未提供代币列表"
        return result
    
    async def process_token(token: Dict[str, str]) -> Dict[str, Any]:
        """处理单个代币的更新"""
        if not all(k in token for k in ["chain", "contract", "symbol"]):
            logger.warning(f"代币信息不完整: {token}")
            return {"success": False, "error": "代币信息不完整", "token": token}
        
        # 添加随机延迟，避免请求过于集中
        rand_delay = delay * (0.5 + random.random())
        await asyncio.sleep(rand_delay)
        
        chain = token["chain"]
        contract = token["contract"]
        symbol = token["symbol"]
        
        try:
            logger.info(f"更新代币 {symbol} ({chain}/{contract})")
            return await update_token_async(chain, contract, symbol, update_type)
        except Exception as e:
            logger.error(f"更新代币 {symbol} 时发生错误: {str(e)}")
            return {
                "chain": chain,
                "contract": contract,
                "symbol": symbol,
                "success": False,
                "error": str(e)
            }
    
    # 使用信号量控制并发数量
    semaphore = asyncio.Semaphore(concurrency)
    
    async def process_with_semaphore(token):
        async with semaphore:
            return await process_token(token)
    
    # 创建所有任务
    tasks = [process_with_semaphore(token) for token in tokens]
    
    # 等待所有任务完成
    token_results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 处理结果
    for res in token_results:
        if isinstance(res, Exception):
            result["failed"] += 1
            result["errors"].append(str(res))
            logger.error(f"任务执行异常: {str(res)}")
        elif res.get("success"):
            result["updated"] += 1
            result["results"].append(res)
        else:
            result["failed"] += 1
            result["errors"].append(res.get("error", "未知错误"))
            result["results"].append(res)
    
    logger.info(f"批量更新完成: 成功 {result['updated']}，失败 {result['failed']}，跳过 {result['skipped']}")
    
    return result

async def update_all_async(limit: int = 100, update_type: str = UPDATE_ALL, 
                      concurrency: int = 3, delay: float = 1.0) -> Dict[str, Any]:
    """
    异步更新所有代币数据
    
    Args:
        limit: 最大更新数量，0表示不限制
        update_type: 更新类型，'market'仅更新市场数据，'txn'仅更新交易数据，'all'更新全部数据
        concurrency: 并发数量
        delay: 每个任务之间的延迟时间(秒)
        
    Returns:
        Dict[str, Any]: 包含更新结果的字典
    """
    # 查询所有代币
    from src.database.models import Token
    
    with get_session() as session:
        query = session.query(Token)
        if limit > 0:
            query = query.limit(limit)
        tokens = query.all()
        
        token_list = [
            {"chain": token.chain, "contract": token.contract, "symbol": token.token_symbol}
            for token in tokens
        ]
    
    if not token_list:
        logger.warning("数据库中没有代币数据")
        return {
            "success": False,
            "error": "数据库中没有代币数据",
            "total": 0,
            "updated": 0,
            "failed": 0,
            "skipped": 0
        }
    
    logger.info(f"开始更新 {len(token_list)} 个代币的数据")
    return await update_tokens_batch_async(
        token_list, 
        update_type=update_type,
        concurrency=concurrency,
        delay=delay
    )

def update_all(limit: int = 100, update_type: str = UPDATE_ALL, delay: float = 0.2) -> Dict[str, Any]:
    """
    同步更新所有代币数据
    
    Args:
        limit: 最大更新数量，0表示不限制
        update_type: 更新类型，'market'仅更新市场数据，'txn'仅更新交易数据，'all'更新全部数据
        delay: 每个代币更新之间的延迟(秒)
        
    Returns:
        Dict[str, Any]: 包含更新结果的字典
    """
    result = {
        "success": True,
        "total": 0,
        "updated": 0,
        "failed": 0,
        "skipped": 0,
        "errors": []
    }
    
    # 使用上下文管理器处理会话
    with get_session() as session:
        try:
            if update_type == UPDATE_MARKET:
                logger.info(f"开始同步更新所有代币的市场数据")
                update_result = update_all_tokens_market_data(session, limit=limit)
            elif update_type == UPDATE_TXN:
                logger.info(f"开始同步更新所有代币的交易数据")
                update_result = update_all_tokens_txn_data(session, limit=limit)
            else:  # UPDATE_ALL
                logger.info(f"开始同步全量更新所有代币的数据")
                update_result = update_all_tokens_market_and_txn_data(session, limit=limit)
            
            # 处理结果
            result["total"] = update_result.get("total", 0)
            result["updated"] = update_result.get("updated", 0)
            result["failed"] = update_result.get("failed", 0)
            result["skipped"] = update_result.get("skipped", 0)
            result["errors"] = update_result.get("errors", [])
            
            logger.info(f"全量更新完成: 成功 {result['updated']}，失败 {result['failed']}，跳过 {result['skipped']}")
            
            return result
        except Exception as e:
            logger.error(f"全量更新代币时发生错误: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
            result["success"] = False
            result["error"] = str(e)
            return result

async def main_async():
    """
    异步主函数，用于命令行运行
    """
    import argparse
    
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='代币数据更新工具')
    
    # 更新类型
    parser.add_argument('--type', choices=['market', 'txn', 'all'], default='all',
                        help='更新类型: market=市场数据, txn=交易数据, all=全部数据')
    
    # 更新方式
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--chain', help='指定区块链名称')
    group.add_argument('--symbol', help='指定代币符号')
    group.add_argument('--symbols', help='指定多个代币符号，以逗号分隔')
    group.add_argument('--all', action='store_true', help='更新所有代币')
    
    # 合约地址，只有在指定chain时需要
    parser.add_argument('--contract', help='指定代币合约地址 (与--chain一起使用)')
    
    # 其他选项
    parser.add_argument('--limit', type=int, default=100, help='最大更新数量 (与--all一起使用)')
    parser.add_argument('--concurrency', type=int, default=3, help='并发数量')
    parser.add_argument('--delay', type=float, default=1.0, help='请求间隔时间(秒)')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 检查数据库结构
    if not ensure_database_structure():
        logger.error("数据库结构检查失败，退出程序")
        return 1
    
    # 根据选择的更新方式执行不同的操作
    try:
        if args.chain:
            # 检查是否提供了合约地址
            if not args.contract:
                logger.error("使用--chain时必须提供--contract参数")
                return 1
                
            # 更新单个代币
            logger.info(f"开始更新 {args.chain}/{args.contract} 的数据")
            result = await update_token_async(
                args.chain, 
                args.contract,
                "未知代币",  # 这里没有代币符号信息
                args.type
            )
            
            if result.get("success"):
                logger.info(f"更新成功!")
            else:
                logger.error(f"更新失败: {result.get('error', '未知错误')}")
                return 1
                
        elif args.symbol:
            # 更新单个代币符号
            logger.info(f"开始更新代币 {args.symbol} 的数据")
            result = await update_by_symbols_async([args.symbol], args.type)
            
            if result.get("success") and result.get("updated") > 0:
                logger.info(f"更新成功!")
            else:
                logger.error(f"更新失败: {result.get('error', '未知错误')}")
                if result.get("errors"):
                    logger.error(f"错误详情: {result['errors']}")
                return 1
                
        elif args.symbols:
            # 更新多个代币符号
            symbols = [s.strip() for s in args.symbols.split(',')]
            logger.info(f"开始更新代币列表 {symbols} 的数据")
            result = await update_by_symbols_async(symbols, args.type)
            
            if result.get("success"):
                logger.info(f"批量更新完成: 成功 {result.get('updated')}，失败 {result.get('failed')}，跳过 {result.get('skipped')}")
                if result.get("failed") > 0 and result.get("errors"):
                    logger.warning(f"失败错误详情: {result['errors']}")
            else:
                logger.error(f"批量更新失败: {result.get('error', '未知错误')}")
                return 1
                
        elif args.all:
            # 更新所有代币
            logger.info(f"开始更新所有代币数据，限制为 {args.limit} 个")
            result = await update_all_async(
                limit=args.limit, 
                update_type=args.type,
                concurrency=args.concurrency,
                delay=args.delay
            )
            
            if result.get("success"):
                logger.info(f"全量更新完成: 成功 {result.get('updated')}，失败 {result.get('failed')}，跳过 {result.get('skipped')}")
                if result.get("failed") > 0 and result.get("errors"):
                    logger.warning(f"部分更新失败，错误样例: {result['errors'][:3]}")
            else:
                logger.error(f"全量更新失败: {result.get('error', '未知错误')}")
                return 1
        
        logger.info("更新任务执行完成")
        return 0
        
    except Exception as e:
        logger.error(f"执行过程中发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

def main():
    """
    同步主函数，用于命令行运行
    """
    import sys
    
    # 设置日志
    setup_logger()
    
    # 运行异步主函数
    exit_code = asyncio.run(main_async())
    sys.exit(exit_code)

if __name__ == "__main__":
    main() 