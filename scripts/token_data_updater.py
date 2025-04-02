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
"""

import os
import sys
import argparse
import logging
import time
from typing import List, Dict, Any
from datetime import datetime
from sqlalchemy import text

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入项目模块
from src.utils.logger import setup_logger, get_logger
from src.database.models import engine, init_db
from sqlalchemy.orm import sessionmaker
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

# 创建会话工厂
Session = sessionmaker(bind=engine)

# 更新类型常量
UPDATE_MARKET = 'market'     # 仅更新市场数据
UPDATE_TXN = 'txn'           # 仅更新交易数据
UPDATE_ALL = 'all'           # 更新所有数据

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

def update_token(chain: str, contract: str, update_type: str = UPDATE_ALL) -> bool:
    """
    更新单个代币的数据
    
    Args:
        chain: 区块链名称
        contract: 代币合约地址
        update_type: 更新类型，'market'仅更新市场数据，'txn'仅更新交易数据，'all'更新全部数据
        
    Returns:
        bool: 更新是否成功
    """
    session = Session()
    try:
        # 根据更新类型选择不同的更新函数
        if update_type == UPDATE_MARKET:
            logger.info(f"开始更新代币 {chain}/{contract} 的市场数据")
            result = update_token_market_data(session, chain, contract)
        elif update_type == UPDATE_TXN:
            logger.info(f"开始更新代币 {chain}/{contract} 的交易数据")
            result = update_token_txn_data(session, chain, contract)
        else:  # UPDATE_ALL
            logger.info(f"开始全量更新代币 {chain}/{contract} 的数据")
            result = update_token_market_and_txn_data(session, chain, contract)
        
        if "error" in result:
            logger.error(f"更新失败: {result['error']}")
            return False
        
        logger.info(f"更新成功!")
        
        # 根据更新类型输出不同的结果信息
        if update_type in [UPDATE_MARKET, UPDATE_ALL]:
            if 'marketCap' in result:
                logger.info(f"市值: {result.get('marketCap', 'N/A')}")
            if 'liquidity' in result:
                logger.info(f"流动性: {result.get('liquidity', 'N/A')}")
            if 'price' in result and result['price']:
                logger.info(f"价格: {result.get('price', 'N/A')}")
                
        if update_type in [UPDATE_TXN, UPDATE_ALL]:
            logger.info(f"1小时买入交易数: {result.get('buys_1h', 'N/A')}")
            logger.info(f"1小时卖出交易数: {result.get('sells_1h', 'N/A')}")
        
        return True
    except Exception as e:
        logger.error(f"更新代币时发生错误: {str(e)}")
        return False
    finally:
        session.close()

def update_by_symbols(symbols: List[str], update_type: str = UPDATE_ALL) -> bool:
    """
    根据代币符号批量更新代币数据
    
    Args:
        symbols: 代币符号列表
        update_type: 更新类型，'market'仅更新市场数据，'txn'仅更新交易数据，'all'更新全部数据
        
    Returns:
        bool: 更新是否成功
    """
    session = Session()
    try:
        logger.info(f"开始更新代币符号为 {', '.join(symbols)} 的数据 (类型: {update_type})")
        
        from src.database.models import Token
        tokens = session.query(Token).filter(Token.token_symbol.in_(symbols)).all()
        
        success_count = 0
        failed_count = 0
        details = []
        
        for token in tokens:
            try:
                if update_type == UPDATE_MARKET:
                    result = update_token_market_data(session, token.chain, token.contract)
                elif update_type == UPDATE_TXN:
                    result = update_token_txn_data(session, token.chain, token.contract)
                else:  # UPDATE_ALL
                    result = update_token_market_and_txn_data(session, token.chain, token.contract)
                    
                if "error" not in result:
                    success_count += 1
                else:
                    failed_count += 1
                    
                details.append({
                    "chain": token.chain,
                    "symbol": token.token_symbol,
                    "contract": token.contract,
                    "result": "success" if "error" not in result else "failed",
                    "error": result.get("error")
                })
                
            except Exception as e:
                failed_count += 1
                details.append({
                    "chain": token.chain,
                    "symbol": token.token_symbol,
                    "contract": token.contract,
                    "result": "failed",
                    "error": str(e)
                })
                logger.error(f"更新代币 {token.chain}/{token.contract} 时发生错误: {str(e)}")
        
        result = {
            "total": len(tokens),
            "success": success_count,
            "failed": failed_count,
            "details": details
        }
        
        logger.info(f"更新完成: 总共 {result['total']} 个代币")
        logger.info(f"成功: {result['success']}, 失败: {result['failed']}")
        
        # 如果有失败的更新，输出详细信息
        if result['failed'] > 0:
            logger.warning("以下代币更新失败:")
            for detail in result['details']:
                if detail['result'] == 'failed':
                    logger.warning(f"  - {detail['chain']}/{detail['symbol']}: {detail['error']}")
        
        return result['failed'] == 0
    except Exception as e:
        logger.error(f"批量更新代币时发生错误: {str(e)}")
        return False
    finally:
        session.close()

def update_all(limit: int = 100, update_type: str = UPDATE_ALL, delay: float = 0.2) -> bool:
    """
    更新所有代币的数据
    
    Args:
        limit: 最大更新数量
        update_type: 更新类型，'market'仅更新市场数据，'txn'仅更新交易数据，'all'更新全部数据
        delay: API请求间隔时间(秒)
        
    Returns:
        bool: 更新是否成功
    """
    session = Session()
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
        
        return result['failed'] == 0
    except Exception as e:
        logger.error(f"更新所有代币时发生错误: {str(e)}")
        return False
    finally:
        session.close()

def main():
    """命令行工具主函数"""
    # 确保数据库结构正确
    if not ensure_database_structure():
        logger.error("数据库结构检查失败，无法继续执行")
        sys.exit(1)
        
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
    
    # 全部更新命令
    all_parser = subparsers.add_parser('all', help='更新所有代币')
    all_parser.add_argument('--limit', type=int, default=100, help='最大更新数量 (默认: 100)')
    all_parser.add_argument('--type', choices=['market', 'txn', 'all'], default='all', 
                           help='更新类型: market=市场数据, txn=交易数据, all=全部数据')
    all_parser.add_argument('--delay', type=float, default=0.2, help='API请求间隔时间(秒) (默认: 0.2)')
    
    # 循环执行相关参数
    parser.add_argument('--repeat', type=int, default=1, help='重复执行次数 (默认: 1)')
    parser.add_argument('--interval', type=int, default=0, help='重复执行间隔(分钟) (默认: 0)')
    
    # 解析参数
    args = parser.parse_args()
    
    # 执行更新
    start_time = datetime.now()
    logger.info(f"开始执行更新任务: {start_time}")
    
    for i in range(args.repeat):
        if i > 0:
            # 等待指定的间隔时间
            if args.interval > 0:
                logger.info(f"等待 {args.interval} 分钟后执行下一次更新...")
                time.sleep(args.interval * 60)
            
            logger.info(f"执行第 {i+1}/{args.repeat} 次更新")
        
        # 执行具体的更新命令
        if args.command == 'token':
            success = update_token(args.chain, args.contract, args.type)
        elif args.command == 'symbols':
            success = update_by_symbols(args.symbols, args.type)
        elif args.command == 'all':
            success = update_all(args.limit, args.type, args.delay)
        else:
            parser.print_help()
            sys.exit(1)
    
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"任务完成: {end_time}, 总用时: {duration}")
    
    # 返回退出代码
    if 'success' in locals():
        sys.exit(0 if success else 1)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main() 