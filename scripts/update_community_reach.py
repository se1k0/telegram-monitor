#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
代币社区覆盖数据更新工具

用于更新代币的社区覆盖数据，包括：
1. 统计代币在所有频道中的出现次数
2. 计算代币的社区覆盖度（根据相关频道的成员数总和）
3. 更新数据库中的community_reach和spread_count字段

可单独运行或集成到其他脚本中
支持断点续传和并行处理
"""

import os
import sys
import argparse
import logging
import time
import json
import asyncio
import random
from typing import List, Dict, Any, Optional, Tuple, Set
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import text, create_engine, func, or_, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入项目模块
from src.utils.logger import setup_logger, get_logger
from src.database.models import engine, Channel, Token, TokenMention

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

# 状态文件路径，用于断点续传
STATUS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "community_reach_status.json")

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

def load_status() -> Dict[str, Any]:
    """加载上次更新状态，用于断点续传"""
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"无法加载状态文件: {e}")
    return {"last_updated": None, "completed_tokens": []}

def save_status(status: Dict[str, Any]) -> None:
    """保存当前更新状态，用于断点续传"""
    try:
        with open(STATUS_FILE, 'w') as f:
            json.dump(status, f)
    except Exception as e:
        logger.warning(f"无法保存状态文件: {e}")

def get_token_community_data(token_id: int) -> Dict[str, Any]:
    """
    获取单个代币的社区数据
    
    Args:
        token_id: 代币ID
        
    Returns:
        Dict[str, Any]: 包含社区数据的字典
    """
    result = {
        "token_id": token_id,
        "channels": [],
        "spread_count": 0,
        "total_members": 0,
        "active_channels": 0,
        "success": False,
        "error": None
    }
    
    try:
        with get_session() as session:
            # 使用一次查询获取所有数据，减少数据库请求
            query = (
                session.query(
                    TokenMention.channel_id,
                    Channel.channel_name,
                    Channel.members_count,
                    Channel.username,
                    Channel.active
                )
                .join(Channel, TokenMention.channel_id == Channel.id)
                .filter(TokenMention.token_id == token_id)
                .filter(Channel.active == True)  # 只考虑活跃频道
            )
            
            channels = query.all()
            
            # 统计
            active_channels = set()
            total_members = 0
            
            for channel_id, name, members, username, active in channels:
                if channel_id in active_channels:
                    continue  # 防止重复计算
                    
                active_channels.add(channel_id)
                
                if members and members > 0:
                    total_members += members
                
                result["channels"].append({
                    "id": channel_id,
                    "name": name,
                    "username": username,
                    "members": members
                })
            
            # 更新结果
            result["spread_count"] = len(active_channels)
            result["total_members"] = total_members
            result["active_channels"] = len(active_channels)
            result["success"] = True
            
            return result
            
    except Exception as e:
        logger.error(f"获取代币(ID: {token_id})社区数据时出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        result["error"] = str(e)
        return result
    
def update_token_community_reach(token_id: int, community_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    更新代币的社区覆盖数据
    
    Args:
        token_id: 代币ID
        community_data: 预先获取的社区数据，如果为None则自动获取
        
    Returns:
        Dict[str, Any]: 包含更新结果的字典
    """
    result = {
        "token_id": token_id,
        "spread_count": 0,
        "community_reach": 0,
        "previous_reach": 0,
        "change": 0,
        "success": False,
        "error": None
    }
    
    try:
        # 如果没有提供社区数据，则获取
        if not community_data:
            community_data = get_token_community_data(token_id)
            
        if not community_data["success"]:
            result["error"] = community_data.get("error", "获取社区数据失败")
            return result
            
        with get_session() as session:
            # 获取当前的community_reach值
            token = session.query(Token).filter(Token.id == token_id).first()
            
            if not token:
                result["error"] = f"找不到ID为{token_id}的代币"
                return result
                
            previous_reach = token.community_reach or 0
            result["previous_reach"] = previous_reach
            
            # 更新社区覆盖和传播计数
            token.community_reach = community_data["total_members"]
            token.spread_count = community_data["spread_count"]
            session.commit()
            
            # 更新结果
            result["spread_count"] = community_data["spread_count"]
            result["community_reach"] = community_data["total_members"]
            result["change"] = result["community_reach"] - previous_reach
            result["success"] = True
            
            logger.info(f"已更新代币(ID: {token_id}, 符号: {token.token_symbol})的社区覆盖数据: "
                       f"覆盖: {result['community_reach']}, 传播: {result['spread_count']}, "
                       f"变化: {result['change']:+}")
            
            return result
            
    except Exception as e:
        logger.error(f"更新代币(ID: {token_id})社区覆盖数据时出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        result["error"] = str(e)
        return result
        
async def update_token_community_reach_async(token_id: int, token_symbol: str) -> Dict[str, Any]:
    """
    异步更新代币的社区覆盖数据
    
    Args:
        token_id: 代币ID
        token_symbol: 代币符号，用于日志
        
    Returns:
        Dict[str, Any]: 包含更新结果的字典
    """
    result = {
        "token_id": token_id,
        "token_symbol": token_symbol,
        "success": False
    }
    
    try:
        # 使用线程池执行同步数据库操作
        loop = asyncio.get_event_loop()
        
        # 获取社区数据
        community_data = await loop.run_in_executor(
            None, 
            lambda: get_token_community_data(token_id)
        )
        
        if not community_data["success"]:
            result["error"] = community_data.get("error", "获取社区数据失败")
            return result
            
        # 更新社区覆盖数据
        update_result = await loop.run_in_executor(
            None,
            lambda: update_token_community_reach(token_id, community_data)
        )
        
        result.update(update_result)
        return result
        
    except Exception as e:
        logger.error(f"异步更新代币(ID: {token_id}, 符号: {token_symbol})社区覆盖数据时出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        result["error"] = str(e)
        return result

def update_all_tokens_community_reach(limit: Optional[int] = None, 
                                     continue_from_last: bool = False) -> Dict[str, Any]:
    """
    更新所有代币的社区覆盖数据
    
    Args:
        limit: 最大更新数量，如果为None则更新所有代币
        continue_from_last: 是否从上次中断的位置继续更新
        
    Returns:
        Dict[str, Any]: 包含更新结果的字典
    """
    result = {
        "total": 0,
        "success": 0,
        "failed": 0,
        "details": [],
        "start_time": datetime.now(),
        "end_time": None,
        "duration": None
    }
    
    status = {}
    completed_tokens = set()
    
    # 如果需要从上次中断的位置继续
    if continue_from_last:
        status = load_status()
        completed_tokens = set(status.get("completed_tokens", []))
        
    try:
        with get_session() as session:
            # 获取所有需要更新的代币
            query = session.query(Token.id, Token.token_symbol)
            
            # 如果从上次中断的位置继续，排除已完成的代币
            if continue_from_last and completed_tokens:
                query = query.filter(~Token.id.in_(completed_tokens))
                
            # 限制数量
            if limit:
                query = query.limit(limit)
                
            tokens = query.all()
            result["total"] = len(tokens)
            
            logger.info(f"开始更新{result['total']}个代币的社区覆盖数据")
            
            for i, (token_id, token_symbol) in enumerate(tokens):
                try:
                    # 获取社区数据
                    community_data = get_token_community_data(token_id)
                    
                    if not community_data["success"]:
                        result["failed"] += 1
                        result["details"].append({
                            "token_id": token_id,
                            "token_symbol": token_symbol,
                            "result": "failed",
                            "error": community_data.get("error", "获取社区数据失败")
                        })
                        continue
                        
                    # 更新社区覆盖数据
                    update_result = update_token_community_reach(token_id, community_data)
                    
                    if update_result["success"]:
                        result["success"] += 1
                        completed_tokens.add(token_id)
                    else:
                        result["failed"] += 1
                        
                    result["details"].append({
                        "token_id": token_id,
                        "token_symbol": token_symbol,
                        "result": "success" if update_result["success"] else "failed",
                        "spread_count": update_result.get("spread_count", 0),
                        "community_reach": update_result.get("community_reach", 0),
                        "change": update_result.get("change", 0),
                        "error": update_result.get("error")
                    })
                    
                    # 每更新10个代币保存一次状态
                    if i % 10 == 0:
                        status["last_updated"] = datetime.now().isoformat()
                        status["completed_tokens"] = list(completed_tokens)
                        save_status(status)
                        
                        # 输出进度
                        progress = (i + 1) / result["total"] * 100
                        logger.info(f"进度: {progress:.1f}% ({i+1}/{result['total']})")
                    
                except Exception as e:
                    result["failed"] += 1
                    result["details"].append({
                        "token_id": token_id,
                        "token_symbol": token_symbol,
                        "result": "failed",
                        "error": str(e)
                    })
                    logger.error(f"更新代币(ID: {token_id}, 符号: {token_symbol})时出错: {str(e)}")
            
            # 更新最终状态
            result["end_time"] = datetime.now()
            result["duration"] = result["end_time"] - result["start_time"]
            
            # 保存最终状态
            status["last_updated"] = result["end_time"].isoformat()
            status["completed_tokens"] = list(completed_tokens)
            save_status(status)
            
            logger.info(f"更新完成: 总共 {result['total']} 个代币")
            logger.info(f"成功: {result['success']}, 失败: {result['failed']}")
            logger.info(f"总用时: {result['duration']}")
            
            return result
                
    except Exception as e:
        logger.error(f"更新所有代币社区覆盖数据时出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        result["error"] = str(e)
        
        # 出错也保存状态
        if continue_from_last:
            status["last_updated"] = datetime.now().isoformat()
            status["completed_tokens"] = list(completed_tokens)
            save_status(status)
            
        return result
    
async def update_all_tokens_community_reach_async(limit: Optional[int] = None, 
                                                continue_from_last: bool = False,
                                                concurrency: int = 5) -> Dict[str, Any]:
    """
    异步更新所有代币的社区覆盖数据
    
    Args:
        limit: 最大更新数量，如果为None则更新所有代币
        continue_from_last: 是否从上次中断的位置继续更新
        concurrency: 并发数量
        
    Returns:
        Dict[str, Any]: 包含更新结果的字典
    """
    result = {
        "total": 0,
        "success": 0,
        "failed": 0,
        "details": [],
        "start_time": datetime.now(),
        "end_time": None,
        "duration": None
    }
    
    status = {}
    completed_tokens = set()
    
    # 如果需要从上次中断的位置继续
    if continue_from_last:
        status = load_status()
        completed_tokens = set(status.get("completed_tokens", []))
    
    # 获取所有需要更新的代币
    with get_session() as session:
        query = session.query(Token.id, Token.token_symbol)
        
        # 如果从上次中断的位置继续，排除已完成的代币
        if continue_from_last and completed_tokens:
            query = query.filter(~Token.id.in_(completed_tokens))
            
        # 限制数量
        if limit:
            query = query.limit(limit)
            
        tokens = query.all()
    
    result["total"] = len(tokens)
    
    if not tokens:
        logger.warning("没有找到需要更新的代币")
        result["end_time"] = datetime.now()
        result["duration"] = result["end_time"] - result["start_time"]
        return result
    
    logger.info(f"开始异步更新{result['total']}个代币的社区覆盖数据 (并发数: {concurrency})")
    
    # 使用信号量控制并发数
    semaphore = asyncio.Semaphore(concurrency)
    
    async def process_token(token_id: int, token_symbol: str) -> Dict[str, Any]:
        """处理单个代币的异步函数"""
        async with semaphore:  # 使用信号量控制并发
            return await update_token_community_reach_async(token_id, token_symbol)
    
    # 创建所有任务
    tasks = [process_token(token_id, token_symbol) for token_id, token_symbol in tokens]
    
    # 分批处理任务，便于断点续传
    batch_size = min(50, len(tasks))  # 每批最多50个任务
    completed_results = []
    
    for i in range(0, len(tasks), batch_size):
        batch_tasks = tasks[i:i+batch_size]
        logger.info(f"处理批次 {i//batch_size + 1}/{(len(tasks) + batch_size - 1)//batch_size}")
        
        # 等待当前批次完成
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        
        # 处理批次结果
        for res in batch_results:
            if isinstance(res, Exception):
                # 处理异常情况
                result["failed"] += 1
                logger.error(f"更新代币时发生异常: {str(res)}")
                continue
                
            # 添加到详情列表
            completed_results.append(res)
            
            # 更新完成的代币ID列表
            if res.get("success", False):
                result["success"] += 1
                completed_tokens.add(res["token_id"])
            else:
                result["failed"] += 1
        
        # 保存当前状态
        status["last_updated"] = datetime.now().isoformat()
        status["completed_tokens"] = list(completed_tokens)
        save_status(status)
        
        # 输出进度
        progress = min(100, (i + len(batch_tasks)) / len(tasks) * 100)
        logger.info(f"进度: {progress:.1f}% ({i + len(batch_tasks)}/{len(tasks)})")
    
    # 更新详情
    result["details"] = completed_results
    
    # 计算结束时间和持续时间
    result["end_time"] = datetime.now()
    result["duration"] = result["end_time"] - result["start_time"]
    
    # 保存最终状态
    status["last_updated"] = result["end_time"].isoformat()
    status["completed_tokens"] = list(completed_tokens)
    save_status(status)
    
    # 打印结果摘要
    logger.info(f"异步更新完成: 总共 {result['total']} 个代币")
    logger.info(f"成功: {result['success']}, 失败: {result['failed']}")
    logger.info(f"总用时: {result['duration']}")
    
    return result

def aggregate_member_counts(token_id: int) -> Dict[str, Any]:
    """
    聚合特定代币相关频道的成员数
    
    Args:
        token_id: 代币ID
        
    Returns:
        Dict[str, Any]: 包含聚合结果的字典
    """
    result = {
        "token_id": token_id,
        "total_members": 0,
        "channel_count": 0,
        "channels": [],
        "success": False,
        "error": None
    }
    
    try:
        logger.info(f"开始聚合代币(ID: {token_id})相关频道的成员数...")
        
        # 使用单一查询获取所有数据
        with get_session() as session:
            token = session.query(Token).filter(Token.id == token_id).first()
            
            if not token:
                result["error"] = f"找不到ID为{token_id}的代币"
                return result
            
            # 获取所有提及该代币的活跃频道
            channels_query = (
                session.query(
                    Channel.id,
                    Channel.channel_name,
                    Channel.username,
                    Channel.members_count
                )
                .join(TokenMention, TokenMention.channel_id == Channel.id)
                .filter(TokenMention.token_id == token_id)
                .filter(Channel.active == True)
                .filter(Channel.members_count > 0)
                .distinct()
            )
            
            channels = channels_query.all()
            
            # 计算总成员数和频道数
            unique_channels = set()
            total_members = 0
            channel_details = []
            
            for channel_id, name, username, members in channels:
                if channel_id in unique_channels:
                    continue
                
                unique_channels.add(channel_id)
                
                if members and members > 0:
                    total_members += members
                    
                channel_details.append({
                    "id": channel_id,
                    "name": name,
                    "username": username,
                    "members": members
                })
            
            # 更新结果
            result["total_members"] = total_members
            result["channel_count"] = len(unique_channels)
            result["channels"] = channel_details
            result["success"] = True
            
            logger.info(f"代币 {token.token_symbol} (ID: {token_id}) 聚合结果:")
            logger.info(f"频道数: {result['channel_count']}")
            logger.info(f"总成员数: {result['total_members']}")
            
            return result
            
    except Exception as e:
        logger.error(f"聚合代币(ID: {token_id})频道成员数时出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        result["error"] = str(e)
        return result

def update_community_reach_and_spread(token_id: int, aggregated_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    更新代币的社区覆盖和传播计数
    
    Args:
        token_id: 代币ID
        aggregated_data: 预先聚合的数据，如果为None则自动聚合
        
    Returns:
        Dict[str, Any]: 包含更新结果的字典
    """
    result = {
        "token_id": token_id,
        "previous_reach": 0,
        "new_reach": 0,
        "change": 0,
        "spread_count": 0,
        "success": False,
        "error": None
    }
    
    try:
        # 如果没有提供聚合数据，则获取
        if not aggregated_data:
            aggregated_data = aggregate_member_counts(token_id)
            
        if not aggregated_data["success"]:
            result["error"] = aggregated_data.get("error", "聚合数据失败")
            return result
            
        with get_session() as session:
            token = session.query(Token).filter(Token.id == token_id).first()
            
            if not token:
                result["error"] = f"找不到ID为{token_id}的代币"
                return result
                
            # 记录之前的覆盖度
            previous_reach = token.community_reach or 0
            result["previous_reach"] = previous_reach
            
            # 更新覆盖度和传播计数
            token.community_reach = aggregated_data["total_members"]
            token.spread_count = aggregated_data["channel_count"]
            
            # 提交更改
            session.commit()
            
            # 更新结果
            result["new_reach"] = token.community_reach
            result["change"] = token.community_reach - previous_reach
            result["spread_count"] = token.spread_count
            result["success"] = True
            
            logger.info(f"已更新代币 {token.token_symbol} (ID: {token_id})的社区覆盖数据:")
            logger.info(f"社区覆盖: {token.community_reach} (变化: {result['change']:+})")
            logger.info(f"传播计数: {token.spread_count}")
            
            return result
            
    except Exception as e:
        logger.error(f"更新代币(ID: {token_id})社区覆盖和传播计数时出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        result["error"] = str(e)
        return result

async def main_async():
    """异步主函数"""
    parser = argparse.ArgumentParser(description='代币社区覆盖数据更新工具')
    
    # 创建子命令
    subparsers = parser.add_subparsers(dest='command', help='命令')
    
    # 单个代币更新命令
    token_parser = subparsers.add_parser('token', help='更新单个代币的社区覆盖数据')
    token_parser.add_argument('token_id', type=int, help='代币ID')
    
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
        aggregated_data = aggregate_member_counts(args.token_id)
        if not aggregated_data["success"]:
            logger.error(f"获取代币(ID: {args.token_id})的聚合数据失败: {aggregated_data['error']}")
            return 1
            
        result = update_community_reach_and_spread(args.token_id, aggregated_data)
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