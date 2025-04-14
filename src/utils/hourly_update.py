#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
自动每小时更新代币数据的工具
- 使用数据库连接池提高性能
- 智能并发控制和批量处理
- 处理速率限制和请求优先级排序
- 随机化请求序列避免被识别为机器人
- 错误处理与重试机制
- 记录详细的日志
"""

import os
import sys
import time
import random
import logging
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import traceback
import asyncio
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager

# 导入项目模块
from src.utils.logger import setup_logger, get_logger
from src.database.models import engine, init_db, Token
from src.api.token_market_updater import (
    update_token_market_and_txn_data,
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

def ensure_database_ready() -> bool:
    """
    确保数据库结构正确
    """
    try:
        logger.info("检查数据库结构...")
        init_db()
        logger.info("数据库结构检查完成")
        return True
    except Exception as e:
        logger.error(f"检查数据库结构时发生错误: {str(e)}")
        return False

def get_tokens_to_update(limit: Optional[int] = None, 
                         prioritize: bool = True) -> List[Dict[str, str]]:
    """
    获取需要更新的代币列表
    
    Args:
        limit: 最大返回数量，None表示不限制
        prioritize: 是否对代币列表进行优先级排序
        
    Returns:
        List[Dict]: 代币信息列表，每个字典包含chain、contract和symbol
    """
    with get_session() as session:
        try:
            query = session.query(Token)
            
            # 如果需要优先级排序
            if prioritize:
                # 这里可以实现多种优先级排序策略
                # 例如：先更新交易量较大的代币、社区覆盖较广的代币、或最近更新时间较早的代币
                # 目前简单按照交易量和成员数排序
                query = query.order_by(Token.volume_24h.desc(), Token.community_reach.desc())
            
            if limit:
                query = query.limit(limit)
                
            tokens = query.all()
            
            return [{"chain": token.chain, "contract": token.contract, 
                    "symbol": token.token_symbol, "id": token.id} for token in tokens]
        except Exception as e:
            logger.error(f"获取代币列表时发生错误: {str(e)}")
            return []

async def update_token_with_retry(chain: str, contract: str, symbol: str, token_id: int = None,
                            max_retries: int = 3, base_delay: float = 1.0, 
                            max_delay: float = 10.0) -> Dict[str, Any]:
    """
    更新单个代币的数据，带有重试机制
    
    Args:
        chain: 区块链名称
        contract: 代币合约地址
        symbol: 代币符号，用于日志
        token_id: 代币ID，用于更新社区覆盖
        max_retries: 最大重试次数
        base_delay: 基础延迟时间(秒)
        max_delay: 最大延迟时间(秒)
        
    Returns:
        Dict[str, Any]: 包含更新结果的字典
    """
    result = {
        "success": False,
        "chain": chain,
        "contract": contract,
        "symbol": symbol,
        "retries": 0,
        "error": None,
        "details": {}
    }
    
    session = SessionPool()
    retries = 0
    
    # 获取Supabase适配器 - 优化错误处理和重试逻辑
    db_adapter = None
    adapter_retry = 0
    max_adapter_retry = 2
    
    while adapter_retry <= max_adapter_retry and not db_adapter:
        try:
            from src.database.db_factory import get_db_adapter
            db_adapter = get_db_adapter()
            logger.info(f"已获取Supabase适配器，准备更新代币 {symbol}")
        except Exception as e:
            adapter_retry += 1
            logger.error(f"获取Supabase适配器时出错 (尝试 {adapter_retry}/{max_adapter_retry}): {str(e)}")
            if adapter_retry <= max_adapter_retry:
                retry_delay = base_delay * (2 ** (adapter_retry - 1))
                logger.info(f"等待 {retry_delay:.2f} 秒后重试获取适配器...")
                await asyncio.sleep(retry_delay)
            else:
                logger.error("无法获取Supabase适配器，放弃更新")
                result["error"] = "无法获取Supabase适配器"
                return result
    
    while retries <= max_retries:
        try:
            if retries > 0:
                # 使用指数退避策略计算延迟
                delay = min(base_delay * (2 ** (retries - 1)), max_delay)
                # 添加随机抖动，避免多个请求同时重试
                delay = delay * (0.75 + 0.5 * random.random())
                logger.info(f"第 {retries} 次重试，等待 {delay:.2f} 秒...")
                await asyncio.sleep(delay)
            
            logger.info(f"更新代币 {symbol} ({chain}/{contract})")
            
            # 直接使用异步API更新市场和交易数据
            try:
                from src.api.token_market_updater import update_token_market_and_txn_data_async
                api_result = await update_token_market_and_txn_data_async(chain, contract)
                logger.info(f"调用异步API更新代币 {symbol} 的结果: {api_result}")
                
                if "error" in api_result:
                    error_msg = str(api_result['error']).lower()
                    logger.warning(f"更新代币 {symbol} 失败: {api_result['error']}")
                    result["retries"] += 1
                    retries += 1
                    
                    # 改进API速率限制处理
                    if "rate limit" in error_msg or "too many requests" in error_msg:
                        # 解析等待时间（如果有）
                        wait_time = None
                        import re
                        time_match = re.search(r'(\d+) seconds', error_msg)
                        if time_match:
                            wait_time = int(time_match.group(1))
                        else:
                            # 默认等待时间递增
                            wait_time = min(60 * retries, 600)  # 最多等待10分钟
                            
                        logger.info(f"检测到API速率限制，等待 {wait_time} 秒...")
                        await asyncio.sleep(wait_time)
                        
                        # 不计入重试次数，因为这是可恢复的错误
                        retries -= 1
                        continue
                        
                    # 如果达到最大重试次数，记录错误并返回
                    if retries > max_retries:
                        result["error"] = api_result["error"]
                        return result
                        
                    # 其他错误继续重试
                    continue
                
                # 更新成功
                result["success"] = True
                result["details"] = api_result
                
                # 如果提供了token_id，尝试更新社区覆盖数据
                if token_id and db_adapter:
                    if symbol:
                        await update_community_reach(db_adapter, symbol)
                
                # 记录市场数据
                if 'marketCap' in api_result:
                    logger.info(f"市值: {api_result.get('marketCap', 'N/A')}")
                if 'liquidity' in api_result:
                    logger.info(f"流动性: {api_result.get('liquidity', 'N/A')}")
                if 'price' in api_result:
                    logger.info(f"价格: {api_result.get('price', 'N/A')}")
                
                # 记录交易数据
                logger.info(f"1小时买入交易数: {api_result.get('buys_1h', 'N/A')}")
                logger.info(f"1小时卖出交易数: {api_result.get('sells_1h', 'N/A')}")
                
                # 更新成功，返回结果
                return result
                
            except Exception as e:
                logger.error(f"调用异步API更新代币 {symbol} 时出错: {str(e)}")
                logger.error(traceback.format_exc())
                result["retries"] += 1
                retries += 1
                
                # 如果达到最大重试次数，记录错误并返回
                if retries > max_retries:
                    result["error"] = str(e)
                    return result
            
        except Exception as e:
            logger.error(f"更新代币 {symbol} 时发生未处理错误: {str(e)}")
            logger.error(traceback.format_exc())
            result["retries"] += 1
            retries += 1
            
            # 如果达到最大重试次数，记录错误并返回
            if retries > max_retries:
                result["error"] = str(e)
                return result
                
    # 如果达到这里，说明所有重试都失败了
    result["error"] = "达到最大重试次数"
    return result

async def update_community_reach(db_adapter, token_symbol: str) -> bool:
    """
    更新代币的社区覆盖数据
    
    Args:
        db_adapter: 数据库适配器
        token_symbol: 代币符号
        
    Returns:
        bool: 是否更新成功
    """
    try:
        # 查询该代币在tokens_mark表中的提及次数
        query_result = await db_adapter.execute_query(
            'tokens_mark',
            'select',
            columns=['count(*) as mention_count', 'count(distinct channel_id) as channel_count'],
            conditions={'token_symbol': token_symbol}
        )
        
        if not query_result or not query_result.data or not len(query_result.data) > 0:
            logger.warning(f"未找到代币 {token_symbol} 的提及记录")
            return False
            
        mention_count = int(query_result.data[0].get('mention_count', 0))
        channel_count = int(query_result.data[0].get('channel_count', 0))
        
        # 计算社区覆盖指数（简单算法）
        community_reach = mention_count * channel_count
        if channel_count > 5:
            community_reach = community_reach * 1.5  # 如果在多个频道被提及，给予额外权重
            
        # 更新代币表中的社区覆盖字段
        if community_reach > 0:
            update_result = await db_adapter.execute_query(
                'tokens',
                'update',
                data={'community_reach': community_reach},
                conditions={'token_symbol': token_symbol}
            )
            
            if update_result:
                logger.info(f"已更新代币 {token_symbol} 的社区覆盖: {community_reach} (提及次数: {mention_count}, 频道数: {channel_count})")
                return True
                
        return False
        
    except Exception as e:
        logger.error(f"更新代币 {token_symbol} 的社区覆盖时出错: {str(e)}")
        logger.error(traceback.format_exc())
        return False

async def batch_update_tokens_async(tokens: List[Dict[str, str]], 
                        batch_size: int = 50,
                        concurrency: int = 3,
                        min_delay: float = 0.5, 
                        max_delay: float = 2.0) -> Dict[str, Any]:
    """
    异步批量更新代币数据
    
    Args:
        tokens: 代币列表
        batch_size: 每批处理的代币数量
        concurrency: 并发数量
        min_delay: 最小请求间隔时间(秒)
        max_delay: 最大请求间隔时间(秒)
        
    Returns:
        Dict[str, Any]: 包含更新结果的字典
    """
    result = {
        "total": len(tokens),
        "updated": 0,
        "failed": 0,
        "skipped": 0,
        "errors": []
    }
    
    if not tokens:
        logger.warning("没有需要更新的代币")
        return result
    
    # 随机打乱顺序，避免总是按同一顺序处理
    random.shuffle(tokens)
    
    # 使用信号量控制并发数量
    semaphore = asyncio.Semaphore(concurrency)
    
    async def process_token(token_data, delay_time):
        # 应用延迟
        await asyncio.sleep(delay_time)
        
        # 限制并发
        async with semaphore:
            chain = token_data.get("chain")
            contract = token_data.get("contract")
            symbol = token_data.get("symbol")
            token_id = token_data.get("id")
            
            if not chain or not contract:
                logger.warning(f"跳过更新: 缺少必要信息 chain={chain}, contract={contract}")
                return {"success": False, "skipped": True, "error": "缺少必要信息"}
            
            symbol_display = symbol or f"{chain}/{contract[:8]}..."
            logger.info(f"开始更新代币 {symbol_display}")
            
            try:
                update_result = await update_token_with_retry(
                    chain=chain,
                    contract=contract,
                    symbol=symbol,
                    token_id=token_id
                )
                
                if update_result.get("success"):
                    return {"success": True, "token": token_data, "details": update_result.get("details", {})}
                else:
                    return {"success": False, "token": token_data, "error": update_result.get("error", "未知错误")}
                    
            except Exception as e:
                logger.error(f"更新代币 {symbol_display} 时发生错误: {str(e)}")
                return {"success": False, "token": token_data, "error": str(e)}
    
    # 将代币列表分批处理
    all_tasks = []
    
    for i, token in enumerate(tokens):
        # 计算随机延迟，避免请求过度集中
        random_delay = min_delay + random.random() * (max_delay - min_delay)
        
        # 创建任务
        task = asyncio.create_task(process_token(token, random_delay))
        all_tasks.append(task)
        
        # 分批等待完成，避免创建太多任务
        if len(all_tasks) >= batch_size or i == len(tokens) - 1:
            for task_result in asyncio.as_completed(all_tasks):
                token_result = await task_result
                
                if token_result.get("skipped", False):
                    result["skipped"] += 1
                elif token_result.get("success"):
                    result["updated"] += 1
                else:
                    result["failed"] += 1
                    error_msg = token_result.get("error", "未知错误")
                    token_info = token_result.get("token", {})
                    symbol = token_info.get("symbol", "未知代币")
                    
                    result["errors"].append({
                        "symbol": symbol,
                        "error": error_msg
                    })
            
            # 清空任务列表，准备下一批
            all_tasks = []
    
    return result

async def hourly_update(limit: Optional[int] = None, test_mode: bool = False, 
                   concurrency: int = 3, prioritize: bool = True):
    """
    执行每小时更新任务
    
    Args:
        limit: 每次更新的代币数量限制，None表示不限制
        test_mode: 是否为测试模式，测试模式下只更新少量代币
        concurrency: 并发数量
        prioritize: 是否按优先级排序
    """
    start_time = time.time()
    
    # 测试模式下设置较小的限制
    if test_mode and (limit is None or limit > 10):
        limit = 10
        logger.info("测试模式: 限制为10个代币")
    
    # 确保数据库结构正确
    if not ensure_database_ready():
        logger.error("数据库结构检查失败，无法执行更新")
        return
    
    # 获取需要更新的代币列表
    logger.info(f"获取需要更新的代币列表 (限制: {limit if limit else '无'}, 优先级排序: {prioritize})")
    tokens = get_tokens_to_update(limit=limit, prioritize=prioritize)
    
    if not tokens:
        logger.warning("没有找到需要更新的代币")
        return
    
    logger.info(f"找到 {len(tokens)} 个需要更新的代币")
    
    # 分批异步更新
    logger.info(f"开始批量更新代币数据 (并发数: {concurrency})")
    update_result = await batch_update_tokens_async(
        tokens=tokens,
        concurrency=concurrency
    )
    
    # 记录结果
    total_time = time.time() - start_time
    minutes = int(total_time // 60)
    seconds = int(total_time % 60)
    
    logger.info(f"代币数据更新完成!")
    logger.info(f"总计: {update_result['total']} 个代币")
    logger.info(f"成功: {update_result['updated']} 个")
    logger.info(f"失败: {update_result['failed']} 个")
    logger.info(f"跳过: {update_result['skipped']} 个")
    logger.info(f"耗时: {minutes}分{seconds}秒")
    
    # 记录错误详情
    if update_result['failed'] > 0:
        logger.error(f"以下 {len(update_result['errors'])} 个代币更新失败:")
        for i, error in enumerate(update_result['errors']):
            logger.error(f"  {i+1}. {error['symbol']}: {error['error']}")
            
            # 限制错误日志数量，避免日志过长
            if i >= 9 and len(update_result['errors']) > 10:
                logger.error(f"  ...以及其他 {len(update_result['errors']) - 10} 个错误")
                break

def main():
    """
    主函数，用于命令行运行
    """
    parser = argparse.ArgumentParser(description="每小时更新代币数据")
    parser.add_argument("--limit", type=int, help="限制更新的代币数量")
    parser.add_argument("--test", action="store_true", help="测试模式，只更新少量代币")
    parser.add_argument("--concurrency", type=int, default=3, help="并发数量")
    parser.add_argument("--no-prioritize", action="store_true", help="不按优先级排序")
    
    args = parser.parse_args()
    
    # 设置日志
    setup_logger()
    
    # 运行异步更新
    asyncio.run(hourly_update(
        limit=args.limit,
        test_mode=args.test,
        concurrency=args.concurrency,
        prioritize=not args.no_prioritize
    ))
    
    logger.info("程序执行完毕")

if __name__ == "__main__":
    main() 