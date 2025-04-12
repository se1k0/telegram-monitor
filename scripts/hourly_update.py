#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
自动每小时更新代币数据脚本
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

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入项目模块
from src.utils.logger import setup_logger, get_logger
from src.database.models import engine, init_db, Token
from src.api.token_market_updater import (
    update_token_market_and_txn_data,
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
                        
                        # 使用智能的等待策略
                        if wait_time:
                            # 如果API明确告诉我们等待时间，就使用它
                            wait_time = min(wait_time + 5, 300)  # 最多等待5分钟
                            logger.warning(f"检测到API速率限制，API要求等待 {wait_time} 秒")
                        else:
                            # 否则使用指数退避
                            wait_time = min(max_delay * (2 ** retries), 300)
                            logger.warning(f"检测到API速率限制，使用指数退避等待 {wait_time:.2f} 秒")
                        
                        logger.info(f"等待 {wait_time:.2f} 秒后继续...")
                        await asyncio.sleep(wait_time)
                    continue
                
                # 记录API更新结果
                result["details"]["api_update"] = True
                for key in ["marketCap", "price", "liquidity"]:
                    if key in api_result:
                        result["details"][key] = api_result[key]
                
                # 单独查询DEX Screener API获取交易数据
                try:
                    from src.api.dex_screener_api import get_token_pools
                    from src.api.token_market_updater import _normalize_chain_id
                    
                    chain_id = _normalize_chain_id(chain)
                    if not chain_id:
                        logger.warning(f"不支持的链: {chain}")
                        result["details"]["dex_update"] = False
                        result["details"]["dex_error"] = "不支持的链"
                        continue
                    
                    logger.info(f"从DEX Screener获取代币 {symbol} ({chain_id}/{contract}) 的交易数据")
                    pools_data = await asyncio.to_thread(get_token_pools, chain_id, contract)
                    
                    # 检查API响应
                    if isinstance(pools_data, dict) and "error" in pools_data:
                        error_msg = str(pools_data['error']).lower()
                        logger.error(f"获取代币池数据失败: {pools_data['error']}")
                        
                        result["details"]["dex_update"] = False
                        result["details"]["dex_error"] = pools_data['error']
                        
                        # 如果是速率限制错误，添加智能等待
                        if "rate limit" in error_msg or "too many requests" in error_msg:
                            wait_time = max_delay * (2 ** retries)
                            logger.warning(f"DEX API速率限制，等待 {wait_time:.2f} 秒...")
                            await asyncio.sleep(wait_time)
                        continue
                    
                    # 处理API返回的数据结构
                    pairs = None
                    if isinstance(pools_data, dict) and "pairs" in pools_data:
                        pairs = pools_data.get("pairs", [])
                    else:
                        pairs = pools_data
                        
                    if not pairs:
                        logger.warning(f"未找到代币 {symbol} 的交易对")
                        result["details"]["dex_update"] = False
                        result["details"]["dex_error"] = "未找到交易对"
                        continue
                    
                    buys_1h = 0
                    sells_1h = 0
                    volume_1h = 0
                    
                    # 从所有交易对中收集1小时交易数据
                    for pair in pairs:
                        if 'txns' in pair and 'h1' in pair['txns']:
                            h1_data = pair['txns']['h1']
                            current_buys = h1_data.get('buys', 0)
                            current_sells = h1_data.get('sells', 0)
                            
                            buys_1h += current_buys
                            sells_1h += current_sells
                            
                            # 如果交易对有volume数据，累加1小时交易量
                            if 'volume' in pair and 'h1' in pair['volume']:
                                volume_h1_data = pair['volume']['h1']
                                if 'USD' in volume_h1_data:
                                    volume_1h += float(volume_h1_data['USD'])
                    
                    # 使用Supabase适配器更新1小时交易数据
                    txn_data = {
                        'buys_1h': buys_1h,
                        'sells_1h': sells_1h,
                        'volume_1h': volume_1h
                    }
                    
                    logger.info(f"更新代币 {symbol} 的1小时交易数据: buys={buys_1h}, sells={sells_1h}, volume=${volume_1h}")
                    
                    # 保存交易数据到结果
                    result["details"]["buys_1h"] = buys_1h
                    result["details"]["sells_1h"] = sells_1h
                    result["details"]["volume_1h"] = volume_1h
                    
                    # 更新数据库，带有重试机制
                    db_update_success = False
                    db_retry = 0
                    max_db_retry = 2
                    
                    while not db_update_success and db_retry <= max_db_retry:
                        try:
                            update_result = await db_adapter.execute_query(
                                'tokens',
                                'update',
                                data=txn_data,
                                filters={'chain': chain, 'contract': contract}
                            )
                            db_update_success = True
                            result["details"]["dex_update"] = True
                            logger.info(f"成功更新代币 {symbol} 的1小时交易数据")
                        except Exception as db_error:
                            db_retry += 1
                            logger.error(f"更新数据库时出错 (尝试 {db_retry}/{max_db_retry}): {str(db_error)}")
                            if db_retry <= max_db_retry:
                                await asyncio.sleep(1)  # 短暂等待后重试
                            else:
                                logger.error(f"更新数据库失败，达到最大重试次数")
                                result["details"]["dex_update"] = False
                                result["details"]["dex_error"] = str(db_error)
                                raise  # 重新抛出异常，让外层处理
                except Exception as txn_error:
                    logger.error(f"获取交易数据时出错: {str(txn_error)}")
                    import traceback
                    logger.debug(traceback.format_exc())
                    result["details"]["dex_update"] = False
                    result["details"]["dex_error"] = str(txn_error)
            except Exception as async_error:
                logger.error(f"异步更新代币数据时出错: {str(async_error)}")
                import traceback
                logger.debug(traceback.format_exc())
                
                # 使用原始方法作为备份
                logger.info(f"尝试使用原始方法更新代币 {symbol}")
                try:
                    fallback_result = update_token_market_and_txn_data(session, chain, contract)
                    result["details"]["fallback_update"] = True
                    result["details"]["fallback_result"] = fallback_result
                except Exception as fallback_error:
                    logger.error(f"备用更新方法也失败: {str(fallback_error)}")
                    result["details"]["fallback_update"] = False
                    result["details"]["fallback_error"] = str(fallback_error)
            
            # 改进的持有者数据获取 - 为支持多链做准备
            try:
                # 为不同链选择适当的API
                holders_count = None
                
                if chain == "SOL":
                    # 对于SOL链使用DAS API
                    from src.api.das_api import get_token_holders_count
                    logger.info(f"获取SOL代币 {symbol} ({contract}) 的持有者数量")
                    holders_count = await asyncio.to_thread(get_token_holders_count, contract)
                elif chain == "ETH":
                    # 为ETH链预留API调用位置
                    logger.debug(f"ETH链持有者数据获取功能尚未实现")
                    # 未来可以添加ETH链的API
                    # holders_count = await get_eth_token_holders_count(contract)
                elif chain == "BSC":
                    # 为BSC链预留API调用位置
                    logger.debug(f"BSC链持有者数据获取功能尚未实现")
                    # 未来可以添加BSC链的API
                    # holders_count = await get_bsc_token_holders_count(contract)
                else:
                    # 其他链暂不支持
                    logger.debug(f"目前不支持获取 {chain} 链上的持有者数量")
                
                # 如果成功获取到持有者数量，则更新数据库
                if holders_count is not None:
                    logger.info(f"代币 {symbol} 的持有者数量: {holders_count}")
                    # 记录到结果
                    result["details"]["holders_count"] = holders_count
                    
                    # 使用Supabase适配器更新持有者数量
                    token_data = {
                        'holders_count': holders_count
                    }
                    
                    update_result = await db_adapter.execute_query(
                        'tokens',
                        'update',
                        data=token_data,
                        filters={
                            'chain': chain,
                            'contract': contract
                        }
                    )
                    
                    result["details"]["holders_update"] = True
                    logger.info(f"成功更新代币 {symbol} 持有者数量: {holders_count}")
                else:
                    if chain == "SOL":  # 只有SOL链应该警告，其他链暂不支持
                        logger.warning(f"无法获取代币 {symbol} 的持有者数量")
                    result["details"]["holders_update"] = False
            except Exception as e:
                logger.error(f"更新代币 {symbol} 持有者数量时发生错误: {str(e)}")
                import traceback
                logger.debug(traceback.format_exc())
                result["details"]["holders_update"] = False
                result["details"]["holders_error"] = str(e)
                # 持有者数量更新失败不影响整体结果
            
            # 更新社群覆盖人数和传播次数
            try:
                logger.info(f"更新代币 {symbol} 的社群覆盖人数和传播次数")
                # 使用新优化的社区覆盖更新功能
                if token_id:
                    # 使用ID直接更新
                    from scripts.update_community_reach import update_token_community_reach_async
                    community_result = await update_token_community_reach_async(token_id, symbol)
                    result["details"]["community_update"] = community_result.get("success", False)
                    if not community_result.get("success", False):
                        result["details"]["community_error"] = community_result.get("error", "未知错误")
                else:
                    # 使用旧方法
                    from scripts.update_community_reach import update_token_community_reach
                    await update_token_community_reach(symbol, contract)
                    result["details"]["community_update"] = True
                
                logger.info(f"成功更新代币 {symbol} 的社区覆盖数据")
            except Exception as e:
                logger.error(f"更新代币 {symbol} 社区覆盖数据时发生错误: {str(e)}")
                import traceback
                logger.debug(traceback.format_exc())
                result["details"]["community_update"] = False
                result["details"]["community_error"] = str(e)
            
            # 整体更新成功
            result["success"] = True
            return result
            
        except Exception as e:
            logger.error(f"更新代币 {symbol} 时发生异常: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
            retries += 1
            result["retries"] = retries
            result["error"] = str(e)
        
    logger.error(f"更新代币 {symbol} 已达最大重试次数，放弃")
    return result

async def batch_update_tokens_async(tokens: List[Dict[str, str]], 
                        batch_size: int = 50,
                        concurrency: int = 3,
                        min_delay: float = 0.5, 
                        max_delay: float = 2.0) -> Dict[str, Any]:
    """
    批量异步更新代币数据，带有速率控制和并发限制
    
    Args:
        tokens: 代币信息列表
        batch_size: 每批处理的代币数量
        concurrency: 最大并发数量
        min_delay: 请求间最小延迟时间(秒)
        max_delay: 请求间最大延迟时间(秒)
        
    Returns:
        Dict: 包含更新结果的字典
    """
    results = {
        "total": len(tokens),
        "success": 0,
        "failed": 0,
        "skipped": 0,
        "start_time": datetime.now(),
        "end_time": None,
        "duration": None,
        "details": []
    }
    
    # 随机打乱代币列表顺序，避免按相同顺序请求
    random.shuffle(tokens)
    
    # 使用信号量控制并发
    semaphore = asyncio.Semaphore(concurrency)
    
    # 分批处理，每批创建一组任务并等待完成
    for i in range(0, len(tokens), batch_size):
        batch = tokens[i:i+batch_size]
        logger.info(f"处理第 {i//batch_size + 1}/{(len(tokens)-1)//batch_size + 1} 批，共 {len(batch)} 个代币")
        
        # 当前批次的任务
        batch_tasks = []
        batch_delays = []
        
        # 为每个代币创建任务和延迟
        for token in batch:
            # 为每个任务计算随机延迟，避免同时发送大量请求
            delay = min_delay + random.random() * (max_delay - min_delay)
            batch_delays.append(delay)
            
            # 创建异步任务
            async def process_token(token_data, delay_time):
                # 应用延迟
                await asyncio.sleep(delay_time)
                
                # 使用信号量限制并发
                async with semaphore:
                    return await update_token_with_retry(
                        token_data["chain"], 
                        token_data["contract"],
                        token_data["symbol"],
                        token_data.get("id")
                    )
            
            # 添加到当前批次任务列表
            task = asyncio.create_task(process_token(token, delay))
            batch_tasks.append(task)
        
        # 等待当前批次所有任务完成
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        
        # 处理批次结果
        for result in batch_results:
            if isinstance(result, Exception):
                logger.error(f"更新代币时出现异常: {str(result)}")
                results["failed"] += 1
                results["details"].append({
                    "success": False,
                    "error": str(result)
                })
            else:
                # 正常结果处理
                if result.get("success", False):
                    results["success"] += 1
                else:
                    results["failed"] += 1
                
                # 添加到详细结果列表
                results["details"].append(result)
        
        # 批次之间的延迟更长一些
        if i + batch_size < len(tokens):  # 如果不是最后一批
            batch_delay = max_delay * 1.5 + random.random() * max_delay  # 加入随机因素
            logger.info(f"批次完成，等待 {batch_delay:.2f} 秒后继续...")
            await asyncio.sleep(batch_delay)
    
    # 记录结束时间和持续时间
    results["end_time"] = datetime.now()
    results["duration"] = results["end_time"] - results["start_time"]
    
    return results

async def hourly_update(limit: Optional[int] = None, test_mode: bool = False, 
                   concurrency: int = 3, prioritize: bool = True):
    """
    执行每小时更新任务
    
    Args:
        limit: 最大更新代币数量，None表示不限制
        test_mode: 是否为测试模式
        concurrency: 并发数量
        prioritize: 是否对代币列表进行优先级排序
    """
    logger.info("="*50)
    start_time = datetime.now()
    logger.info(f"开始每小时自动更新任务: {start_time}")
    
    # 确保数据库结构正确
    if not ensure_database_ready():
        logger.error("数据库结构检查失败，无法继续执行")
        return {
            "success": False,
            "error": "数据库结构检查失败",
            "start_time": start_time,
            "end_time": datetime.now(),
            "duration": datetime.now() - start_time
        }
    
    try:
        # 获取需要更新的代币列表
        tokens = get_tokens_to_update(limit, prioritize)
        
        if not tokens:
            logger.warning("没有找到需要更新的代币")
            return {
                "success": True,
                "total": 0,
                "message": "没有找到需要更新的代币",
                "start_time": start_time,
                "end_time": datetime.now(),
                "duration": datetime.now() - start_time
            }
        
        logger.info(f"找到 {len(tokens)} 个代币需要更新")
        
        # 计算最佳批次大小和并发数
        # 根据代币数量和1小时时间限制计算
        total_tokens = len(tokens)
        time_limit_seconds = 3000  # 50分钟 (给其他任务留出余量)
        
        # 保守估计：平均每个代币更新需要3-6秒
        avg_token_time = 4.5
        max_possible_tokens = time_limit_seconds / avg_token_time
        
        if total_tokens > max_possible_tokens:
            logger.warning(f"代币数量({total_tokens})超过1小时内可更新的最大数量({max_possible_tokens:.0f})")
            # 如果不是测试模式，则限制数量
            if not test_mode:
                limit = int(max_possible_tokens * 0.9)  # 留出10%的安全余量
                logger.info(f"限制本次更新的代币数量为: {limit}")
                tokens = tokens[:limit]
        
        # 计算理想批次大小和并发度
        batch_size = min(50, max(10, len(tokens) // 4))
        actual_concurrency = min(concurrency, max(1, len(tokens) // 10))
        
        # 计算理想请求延迟
        ideal_delay_min = 0.5
        ideal_delay_max = 2.0
        
        # 在测试模式下调整参数
        if test_mode:
            logger.info("测试模式: 使用较小的批次和较长的延迟")
            batch_size = min(5, batch_size)
            actual_concurrency = min(2, actual_concurrency)
            ideal_delay_min = 1.0
            ideal_delay_max = 3.0
        
        logger.info(f"更新参数: 批次大小={batch_size}, 并发数={actual_concurrency}, 延迟={ideal_delay_min}-{ideal_delay_max}秒")
        
        # 执行批量更新
        results = await batch_update_tokens_async(
            tokens, 
            batch_size=batch_size,
            concurrency=actual_concurrency,
            min_delay=ideal_delay_min, 
            max_delay=ideal_delay_max
        )
        
        # 打印结果摘要
        end_time = datetime.now()
        duration = end_time - start_time
        success_rate = results["success"] / results["total"] * 100 if results["total"] > 0 else 0
        
        logger.info("="*30)
        logger.info(f"更新完成: {end_time}")
        logger.info(f"总代币数: {results['total']}")
        logger.info(f"成功: {results['success']} ({success_rate:.1f}%)")
        logger.info(f"失败: {results['failed']}")
        logger.info(f"总用时: {duration}")
        logger.info("="*50)
        
        # 返回完整的结果数据
        return {
            "success": results["failed"] < results["total"] * 0.3,  # 如果失败率低于30%，视为整体成功
            "total": results["total"],
            "success_count": results["success"],
            "failed_count": results["failed"],
            "success_rate": success_rate,
            "start_time": start_time,
            "end_time": end_time,
            "duration": duration,
            "details": results.get("details", [])
        }
        
    except Exception as e:
        logger.error(f"执行每小时更新任务时发生错误: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "start_time": start_time,
            "end_time": datetime.now(),
            "duration": datetime.now() - start_time
        }

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='每小时自动更新代币数据')
    parser.add_argument('--limit', type=int, help='最大更新代币数量')
    parser.add_argument('--test', action='store_true', help='测试模式，使用较小的批次和较长的延迟')
    parser.add_argument('--concurrency', type=int, default=3, help='并发数量')
    parser.add_argument('--no-priority', dest='prioritize', action='store_false', 
                      help='禁用优先级排序')
    parser.add_argument('--output', help='将结果输出到指定文件')
    
    args = parser.parse_args()
    
    try:
        # 执行更新
        result = asyncio.run(hourly_update(
            limit=args.limit, 
            test_mode=args.test,
            concurrency=args.concurrency,
            prioritize=args.prioritize
        ))
        
        # 如果需要输出到文件
        if args.output:
            try:
                import json
                # 转换datetime对象为字符串
                serializable_result = result.copy()
                serializable_result["start_time"] = serializable_result["start_time"].isoformat()
                serializable_result["end_time"] = serializable_result["end_time"].isoformat()
                serializable_result["duration"] = str(serializable_result["duration"])
                
                with open(args.output, 'w') as f:
                    json.dump(serializable_result, f, indent=2)
                logger.info(f"结果已保存到: {args.output}")
            except Exception as e:
                logger.error(f"保存结果到文件时出错: {str(e)}")
        
        # 退出码
        sys.exit(0 if result.get("success", False) else 1)
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在退出...")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"发生未处理的异常: {str(e)}")
        logger.critical(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main() 