#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
统一的代币数据更新命令行工具
集成了以下功能：
1. 市场数据更新（market_cap, liquidity, price等）
2. 交易数据更新（buys_1h, sells_1h等）
3. 交易量数据更新（volume_1h等）
4. 全量数据综合更新

支持单个代币更新、批量更新和全量更新
支持定时任务和循环执行
使用连接池和并发处理提高效率
"""

import os
import sys
import argparse
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

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
setup_logger()
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
        "total": 0,
        "success": 0,
        "failed": 0,
        "details": []
    }
    
    logger.info(f"开始更新代币符号为 {', '.join(symbols)} 的数据 (类型: {update_type})")
    
    # 使用上下文管理器处理会话
    with get_session() as session:
        try:
            from src.database.models import Token
            tokens = session.query(Token).filter(Token.token_symbol.in_(symbols)).all()
            
            result["total"] = len(tokens)
            
            for token in tokens:
                try:
                    if update_type == UPDATE_MARKET:
                        update_result = update_token_market_data(session, token.chain, token.contract)
                    elif update_type == UPDATE_TXN:
                        update_result = update_token_txn_data(session, token.chain, token.contract)
                    else:  # UPDATE_ALL
                        update_result = update_token_market_and_txn_data(session, token.chain, token.contract)
                        
                    if "error" not in update_result:
                        result["success"] += 1
                    else:
                        result["failed"] += 1
                        
                    details = {
                        "chain": token.chain,
                        "symbol": token.token_symbol,
                        "contract": token.contract,
                        "result": "success" if "error" not in update_result else "failed",
                        "error": update_result.get("error")
                    }
                    
                    result["details"].append(details)
                    
                except Exception as e:
                    result["failed"] += 1
                    details = {
                        "chain": token.chain,
                        "symbol": token.token_symbol,
                        "contract": token.contract,
                        "result": "failed",
                        "error": str(e)
                    }
                    result["details"].append(details)
                    logger.error(f"更新代币 {token.chain}/{token.contract} 时发生错误: {str(e)}")
            
            logger.info(f"更新完成: 总共 {result['total']} 个代币")
            logger.info(f"成功: {result['success']}, 失败: {result['failed']}")
            
            # 如果有失败的更新，输出详细信息
            if result['failed'] > 0:
                logger.warning("以下代币更新失败:")
                for detail in result['details']:
                    if detail['result'] == 'failed':
                        logger.warning(f"  - {detail['chain']}/{detail['symbol']}: {detail['error']}")
            
            return result
        except Exception as e:
            logger.error(f"批量更新代币时发生错误: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
            result["error"] = str(e)
            return result

async def update_by_symbols_async(symbols: List[str], update_type: str = UPDATE_ALL) -> Dict[str, Any]:
    """
    异步根据代币符号批量更新代币数据
    
    Args:
        symbols: 代币符号列表
        update_type: 更新类型，'market'仅更新市场数据，'txn'仅更新交易数据，'all'更新全部数据
        
    Returns:
        Dict[str, Any]: 包含更新结果的字典
    """
    # 首先获取所有符号对应的代币信息
    with get_session() as session:
        from src.database.models import Token
        tokens = session.query(Token).filter(Token.token_symbol.in_(symbols)).all()
        token_infos = [
            {"chain": token.chain, "contract": token.contract, "symbol": token.token_symbol}
            for token in tokens
        ]
    
    return await update_tokens_batch_async(token_infos, update_type)

async def update_tokens_batch_async(tokens: List[Dict[str, str]], update_type: str = UPDATE_ALL, 
                                   concurrency: int = 3, delay: float = 1.0) -> Dict[str, Any]:
    """
    异步批量更新代币数据
    
    Args:
        tokens: 代币信息列表，每项包含chain、contract和symbol
        update_type: 更新类型，'market'仅更新市场数据，'txn'仅更新交易数据，'all'更新全部数据
        concurrency: 并发数量
        delay: 请求间延迟时间(秒)
        
    Returns:
        Dict[str, Any]: 包含更新结果的字典
    """
    result = {
        "total": len(tokens),
        "success": 0,
        "failed": 0,
        "details": [],
        "start_time": datetime.now(),
        "end_time": None,
        "duration": None
    }
    
    if not tokens:
        logger.warning("没有找到需要更新的代币")
        result["end_time"] = datetime.now()
        result["duration"] = result["end_time"] - result["start_time"]
        return result
    
    logger.info(f"开始异步批量更新 {len(tokens)} 个代币 (类型: {update_type}, 并发数: {concurrency})")
    
    # 使用信号量控制并发数
    semaphore = asyncio.Semaphore(concurrency)
    
    # 优先级处理：根据代币重要性对列表进行排序
    # 例如，可以把交易量大的代币放在前面
    # 这里简单随机打乱，避免总是按相同顺序请求
    random.shuffle(tokens)
    
    async def process_token(token: Dict[str, str]) -> Dict[str, Any]:
        """处理单个代币的异步函数"""
        async with semaphore:  # 使用信号量控制并发
            # 随机延迟，避免API限制
            actual_delay = delay * (0.5 + random.random())
            await asyncio.sleep(actual_delay)
            
            return await update_token_async(
                token["chain"], 
                token["contract"], 
                token["symbol"],
                update_type
            )
    
    # 创建所有任务
    tasks = [process_token(token) for token in tokens]
    
    # 等待所有任务完成
    update_results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 处理结果
    for res in update_results:
        if isinstance(res, Exception):
            # 处理异常情况
            result["failed"] += 1
            logger.error(f"更新代币时发生异常: {str(res)}")
            continue
            
        # 添加到详情列表
        result["details"].append(res)
        
        # 统计成功失败
        if res.get("success", False):
            result["success"] += 1
        else:
            result["failed"] += 1
    
    # 计算结束时间和持续时间
    result["end_time"] = datetime.now()
    result["duration"] = result["end_time"] - result["start_time"]
    
    # 打印结果摘要
    logger.info(f"异步批量更新完成: 总共 {result['total']} 个代币")
    logger.info(f"成功: {result['success']}, 失败: {result['failed']}")
    logger.info(f"总用时: {result['duration']}")
    
    return result

async def update_all_async(limit: int = 100, update_type: str = UPDATE_ALL, 
                          concurrency: int = 3, delay: float = 1.0) -> Dict[str, Any]:
    """
    异步更新所有代币的数据
    
    Args:
        limit: 最大更新数量
        update_type: 更新类型，'market'仅更新市场数据，'txn'仅更新交易数据，'all'更新全部数据
        concurrency: 并发数量
        delay: 请求间延迟时间(秒)
        
    Returns:
        Dict[str, Any]: 包含更新结果的字典
    """
    logger.info(f"开始异步更新所有代币数据 (类型: {update_type}, 限制: {limit}, 并发数: {concurrency})")
    
    # 获取需要更新的代币
    with get_session() as session:
        from src.database.models import Token
        
        # 添加优先级排序
        # 例如，按照更新时间排序，先更新最久未更新的
        query = session.query(Token)
        
        # 可以根据需要添加其他排序条件
        # 例如交易量、价格等
        
        if limit:
            query = query.limit(limit)
            
        tokens = query.all()
        
        # 转换为字典列表
        token_infos = [
            {"chain": token.chain, "contract": token.contract, "symbol": token.token_symbol}
            for token in tokens
        ]
    
    # 使用批量异步更新
    return await update_tokens_batch_async(
        token_infos, 
        update_type=update_type,
        concurrency=concurrency,
        delay=delay
    )

def update_all(limit: int = 100, update_type: str = UPDATE_ALL, delay: float = 0.2) -> Dict[str, Any]:
    """
    更新所有代币的数据（同步版本，为了兼容性保留）
    
    Args:
        limit: 最大更新数量
        update_type: 更新类型，'market'仅更新市场数据，'txn'仅更新交易数据，'all'更新全部数据
        delay: API请求间隔时间(秒)
        
    Returns:
        Dict[str, Any]: 包含更新结果的字典
    """
    with get_session() as session:
        try:
            logger.info(f"开始更新所有代币数据 (类型: {update_type}, 限制: {limit}, 延迟: {delay}秒)")
            
            # 根据更新类型选择不同的更新函数
            if update_type == UPDATE_MARKET:
                result = update_all_tokens_market_data(session, limit)
            elif update_type == UPDATE_TXN:
                result = update_all_tokens_txn_data(session, limit)
            else:  # UPDATE_ALL
                result = update_all_tokens_market_and_txn_data(session, limit)
            
            logger.info(f"更新完成: 总共 {result['total']} 个代币")
            logger.info(f"成功: {result['success']}, 失败: {result['failed']}")
            
            # 如果有失败的更新，输出详细信息
            if result['failed'] > 0:
                logger.warning("以下代币更新失败:")
                for detail in result['details']:
                    if detail['result'] == 'failed':
                        logger.warning(f"  - {detail['chain']}/{detail['symbol']}: {detail['error']}")
            
            return result
        except Exception as e:
            logger.error(f"更新所有代币时发生错误: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
            return {
                "success": 0, 
                "failed": 0, 
                "total": 0, 
                "error": str(e),
                "details": []
            }

async def main_async():
    """异步主函数"""
    # 确保数据库结构正确
    if not ensure_database_structure():
        logger.error("数据库结构检查失败，无法继续执行")
        return 1
        
    parser = argparse.ArgumentParser(description='统一的代币数据更新工具')
    
    # 创建子命令
    subparsers = parser.add_subparsers(dest='command', help='命令')
    
    # 单个代币更新命令
    token_parser = subparsers.add_parser('token', help='更新单个代币')
    token_parser.add_argument('chain', help='区块链名称 (如 SOL, ETH, BSC 等)')
    token_parser.add_argument('contract', help='代币合约地址')
    token_parser.add_argument('--type', choices=['market', 'txn', 'all'], default='all', 
                           help='更新类型: market=市场数据, txn=交易数据, all=全部数据')
    
    # 符号批量更新命令
    symbols_parser = subparsers.add_parser('symbols', help='根据代币符号批量更新')
    symbols_parser.add_argument('symbols', nargs='+', help='代币符号列表')
    symbols_parser.add_argument('--type', choices=['market', 'txn', 'all'], default='all', 
                             help='更新类型: market=市场数据, txn=交易数据, all=全部数据')
    symbols_parser.add_argument('--async', dest='use_async', action='store_true', 
                             help='使用异步并发更新（推荐）')
    symbols_parser.add_argument('--concurrency', type=int, default=3, 
                             help='并发更新数（仅异步模式有效）')
    
    # 全部更新命令
    all_parser = subparsers.add_parser('all', help='更新所有代币')
    all_parser.add_argument('--limit', type=int, default=100, help='最大更新数量 (默认: 100)')
    all_parser.add_argument('--type', choices=['market', 'txn', 'all'], default='all', 
                         help='更新类型: market=市场数据, txn=交易数据, all=全部数据')
    all_parser.add_argument('--delay', type=float, default=0.2, help='API请求间隔时间(秒) (默认: 0.2)')
    all_parser.add_argument('--async', dest='use_async', action='store_true', 
                         help='使用异步并发更新（推荐）')
    all_parser.add_argument('--concurrency', type=int, default=3, 
                         help='并发更新数（仅异步模式有效）')
    
    # 循环执行相关参数
    parser.add_argument('--repeat', type=int, default=1, help='重复执行次数 (默认: 1)')
    parser.add_argument('--interval', type=int, default=60, help='重复执行间隔时间(分钟) (默认: 60)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # 循环执行
    for i in range(args.repeat):
        if i > 0:
            # 计算等待时间
            wait_minutes = args.interval
            logger.info(f"等待 {wait_minutes} 分钟后执行第 {i+1}/{args.repeat} 次更新...")
            
            # 分钟转秒
            wait_seconds = wait_minutes * 60
            
            # 分段等待，便于中断
            chunk_size = 10  # 每次等待10秒
            for j in range(0, wait_seconds, chunk_size):
                remaining = min(chunk_size, wait_seconds - j)
                await asyncio.sleep(remaining)
                
                # 定期输出剩余时间
                if (j + chunk_size) % 60 == 0:
                    remaining_minutes = (wait_seconds - j - chunk_size) // 60
                    logger.info(f"还需等待 {remaining_minutes} 分钟...")
                
                # 检测中断请求
                try:
                    if asyncio.current_task().cancelled():
                        logger.info("任务被取消，提前退出等待")
                        return 1
                except Exception:
                    pass
        
        # 显示当前执行次数
        if args.repeat > 1:
            logger.info(f"开始执行第 {i+1}/{args.repeat} 次更新")
            
        # 基于命令执行相应操作
        if args.command == 'token':
            # 单个代币更新
            result = update_token(args.chain, args.contract, args.type)
            return 0 if result.get("success", False) else 1
            
        elif args.command == 'symbols':
            # 符号批量更新
            if hasattr(args, 'use_async') and args.use_async:
                # 使用异步并发更新
                result = await update_by_symbols_async(
                    args.symbols, 
                    args.type,
                )
            else:
                # 使用同步更新
                result = update_by_symbols(args.symbols, args.type)
                
            success = result.get("success", 0) > 0
            return 0 if success else 1
            
        elif args.command == 'all':
            # 全部更新
            if hasattr(args, 'use_async') and args.use_async:
                # 使用异步并发更新
                result = await update_all_async(
                    args.limit, 
                    args.type,
                    args.concurrency,
                    args.delay
                )
            else:
                # 使用同步更新
                result = update_all(args.limit, args.type, args.delay)
                
            success = result.get("success", 0) > 0
            return 0 if success else 1
    
    return 0  # 所有循环执行完成，返回成功

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