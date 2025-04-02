#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
自动每小时更新代币数据脚本
- 处理速率限制
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
from typing import List, Dict, Any
import traceback

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入项目模块
from src.utils.logger import setup_logger, get_logger
from src.database.models import engine, init_db, Token
from sqlalchemy.orm import sessionmaker
from src.api.token_market_updater import (
    update_token_market_and_txn_data,
)

# 设置日志
setup_logger()
logger = get_logger(__name__)

# 创建会话工厂
Session = sessionmaker(bind=engine)

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

def get_tokens_to_update(limit: int = None) -> List[Dict[str, str]]:
    """
    获取需要更新的代币列表
    
    Args:
        limit: 最大返回数量，None表示不限制
        
    Returns:
        List[Dict]: 代币信息列表，每个字典包含chain和contract
    """
    session = Session()
    try:
        query = session.query(Token)
        
        if limit:
            query = query.limit(limit)
            
        tokens = query.all()
        
        return [{"chain": token.chain, "contract": token.contract, 
                 "symbol": token.token_symbol} for token in tokens]
    except Exception as e:
        logger.error(f"获取代币列表时发生错误: {str(e)}")
        return []
    finally:
        session.close()

def update_token_with_retry(chain: str, contract: str, symbol: str, max_retries: int = 3, 
                            base_delay: float = 1.0, max_delay: float = 10.0) -> bool:
    """
    更新单个代币的数据，带有重试机制
    
    Args:
        chain: 区块链名称
        contract: 代币合约地址
        symbol: 代币符号，用于日志
        max_retries: 最大重试次数
        base_delay: 基础延迟时间(秒)
        max_delay: 最大延迟时间(秒)
        
    Returns:
        bool: 更新是否成功
    """
    session = Session()
    retries = 0
    
    while retries <= max_retries:
        try:
            if retries > 0:
                # 使用指数退避策略计算延迟
                delay = min(base_delay * (2 ** (retries - 1)), max_delay)
                # 添加随机抖动，避免多个请求同时重试
                delay = delay * (0.75 + 0.5 * random.random())
                logger.info(f"第 {retries} 次重试，等待 {delay:.2f} 秒...")
                time.sleep(delay)
            
            logger.info(f"更新代币 {symbol} ({chain}/{contract})")
            result = update_token_market_and_txn_data(session, chain, contract)
            
            # 如果是Solana链的代币，更新持有者数量
            if chain == "SOL":
                try:
                    from src.api.das_api import get_token_holders_count
                    # 获取代币持有者数量
                    holders_count = get_token_holders_count(contract)
                    if holders_count is not None:
                        # 更新数据库中的持有者数量
                        token = session.query(Token).filter(
                            Token.chain == chain,
                            Token.contract == contract
                        ).first()
                        if token:
                            token.holders_count = holders_count
                            session.commit()
                            logger.info(f"成功更新代币 {symbol} 持有者数量: {holders_count}")
                except Exception as e:
                    logger.error(f"更新代币 {symbol} 持有者数量时发生错误: {str(e)}")
                    # 持有者数量更新失败不影响整体结果
            
            if "error" in result:
                logger.warning(f"更新代币 {symbol} 失败: {result['error']}")
                retries += 1
                # 检查是否达到API限制
                if "rate limit" in str(result['error']).lower() or "too many requests" in str(result['error']).lower():
                    logger.warning("检测到API速率限制，增加等待时间...")
                    time.sleep(max_delay * 2)  # 遇到速率限制，等待更长时间
            else:
                # 更新成功
                logger.info(f"成功更新代币 {symbol}: 市值={result.get('marketCap', 'N/A')}, "
                           f"1小时买入={result.get('buys_1h', 'N/A')}, "
                           f"1小时卖出={result.get('sells_1h', 'N/A')}")
                return True
        except Exception as e:
            logger.error(f"更新代币 {symbol} 时发生异常: {str(e)}")
            retries += 1
        
    logger.error(f"更新代币 {symbol} 已达最大重试次数，放弃")
    return False

def batch_update_tokens(tokens: List[Dict[str, str]], 
                        batch_size: int = 50,
                        min_delay: float = 0.5, 
                        max_delay: float = 2.0) -> Dict[str, Any]:
    """
    批量更新代币数据，带有速率控制
    
    Args:
        tokens: 代币信息列表
        batch_size: 每批处理的代币数量
        min_delay: 请求间最小延迟时间(秒)
        max_delay: 请求间最大延迟时间(秒)
        
    Returns:
        Dict: 包含更新结果的字典
    """
    results = {
        "total": len(tokens),
        "success": 0,
        "failed": 0,
        "start_time": datetime.now(),
        "end_time": None,
        "duration": None
    }
    
    # 随机打乱代币列表顺序，避免按相同顺序请求
    random.shuffle(tokens)
    
    # 分批处理
    for i in range(0, len(tokens), batch_size):
        batch = tokens[i:i+batch_size]
        logger.info(f"处理第 {i//batch_size + 1} 批，共 {len(batch)} 个代币")
        
        # 处理每个批次中的代币
        for token in batch:
            # 随机延迟，模拟人工操作
            delay = min_delay + random.random() * (max_delay - min_delay)
            time.sleep(delay)
            
            # 更新代币数据
            success = update_token_with_retry(
                token["chain"], 
                token["contract"],
                token["symbol"]
            )
            
            if success:
                results["success"] += 1
            else:
                results["failed"] += 1
        
        # 批次之间的延迟更长一些
        batch_delay = max_delay * 2
        logger.info(f"批次完成，等待 {batch_delay:.2f} 秒后继续...")
        time.sleep(batch_delay)
    
    # 记录结束时间和持续时间
    results["end_time"] = datetime.now()
    results["duration"] = results["end_time"] - results["start_time"]
    
    return results

def hourly_update(limit: int = None, test_mode: bool = False):
    """
    执行每小时更新任务
    
    Args:
        limit: 最大更新代币数量，None表示不限制
        test_mode: 是否为测试模式
    """
    logger.info("="*50)
    logger.info(f"开始每小时自动更新任务: {datetime.now()}")
    
    # 确保数据库结构正确
    if not ensure_database_ready():
        logger.error("数据库结构检查失败，无法继续执行")
        return False
    
    try:
        # 获取需要更新的代币列表
        tokens = get_tokens_to_update(limit)
        
        if not tokens:
            logger.warning("没有找到需要更新的代币")
            return True
        
        logger.info(f"找到 {len(tokens)} 个代币需要更新")
        
        # 计算最佳批次大小和请求延迟
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
        
        # 计算理想批次大小
        batch_size = min(50, max(10, len(tokens) // 4))
        
        # 计算理想请求延迟
        ideal_delay_min = 0.5
        ideal_delay_max = 2.0
        
        # 在测试模式下调整参数
        if test_mode:
            logger.info("测试模式: 使用较小的批次和较长的延迟")
            batch_size = min(5, batch_size)
            ideal_delay_min = 1.0
            ideal_delay_max = 3.0
        
        # 执行批量更新
        results = batch_update_tokens(
            tokens, 
            batch_size=batch_size,
            min_delay=ideal_delay_min, 
            max_delay=ideal_delay_max
        )
        
        # 打印结果摘要
        logger.info("="*30)
        logger.info(f"更新完成: {datetime.now()}")
        logger.info(f"总代币数: {results['total']}")
        logger.info(f"成功: {results['success']}")
        logger.info(f"失败: {results['failed']}")
        logger.info(f"总用时: {results['duration']}")
        logger.info("="*50)
        
        return results['failed'] == 0
        
    except Exception as e:
        logger.error(f"执行每小时更新任务时发生错误: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='每小时自动更新代币数据')
    parser.add_argument('--limit', type=int, help='最大更新代币数量')
    parser.add_argument('--test', action='store_true', help='测试模式，使用较小的批次和较长的延迟')
    
    args = parser.parse_args()
    
    try:
        success = hourly_update(limit=args.limit, test_mode=args.test)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在退出...")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"发生未处理的异常: {str(e)}")
        logger.critical(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main() 