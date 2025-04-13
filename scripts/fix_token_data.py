#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
修复代币数据中的零值和空值

此脚本用于修复token表中的数据问题，特别是处理以下情况：
1. 修复错误设置为0的市值和传播统计数据
2. 修复community_reach和spread_count字段的错误值
3. 从备份恢复或重新计算缺失的值

使用方法：
    python fix_token_data.py [--chain <chain>] [--symbol <symbol>]
    
参数：
    --chain: 限制为特定链的代币
    --symbol: 限制为特定符号的代币
    --dry-run: 仅显示将要修改的内容，不实际修改
    --force: 强制更新所有token，即使不存在问题
"""

import os
import sys
import argparse
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

# 添加项目根目录到Python路径
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

# 导入项目模块
from src.utils.logger import setup_logger, get_logger
from src.database.db_factory import get_db_adapter
from src.database.models import Token

# 设置日志
logger = get_logger(__name__)

# 处理参数
def parse_args():
    parser = argparse.ArgumentParser(description='修复代币数据中的零值和空值')
    parser.add_argument('--chain', help='限制为特定链的代币')
    parser.add_argument('--symbol', help='限制为特定符号的代币')
    parser.add_argument('--dry-run', action='store_true', help='仅显示将要修改的内容，不实际修改')
    parser.add_argument('--force', action='store_true', help='强制更新所有token，即使不存在问题')
    return parser.parse_args()

async def check_tokens_for_zero_values(db_adapter, chain=None, symbol=None):
    """检查代币数据中的零值和空值"""
    
    # 构建过滤条件
    filters = {}
    if chain:
        filters['chain'] = chain
    if symbol:
        filters['token_symbol'] = symbol
    
    # 获取所有代币数据
    tokens = await db_adapter.execute_query('tokens', 'select', filters=filters)
    
    if not tokens or not isinstance(tokens, list):
        logger.error("获取代币列表失败或结果为空")
        return []
    
    logger.info(f"获取到 {len(tokens)} 个代币，开始检查数据问题")
    
    # 筛选出需要修复的代币
    tokens_to_fix = []
    
    for token in tokens:
        needs_fix = False
        fix_reason = []
        fixed_data = {}
        
        # 检查字段是否存在
        token_id = token.get('id')
        chain = token.get('chain')
        contract = token.get('contract')
        symbol = token.get('token_symbol')
        
        if not all([token_id, chain, contract]):
            logger.warning(f"跳过ID={token_id}的代币：缺少关键字段")
            continue
        
        # 1. 检查market_cap是否为0，但first_market_cap不为0
        if (token.get('market_cap', 0) == 0 or token.get('market_cap') is None) and token.get('first_market_cap', 0) > 0:
            needs_fix = True
            fix_reason.append("市值为0但初始市值不为0")
            fixed_data['market_cap'] = token.get('first_market_cap')
        
        # 2. 检查community_reach是否为0，但可以从tokens_mark计算得到
        if token.get('community_reach', 0) == 0 or token.get('community_reach') is None:
            # 在修复脚本中会计算
            needs_fix = True
            fix_reason.append("社群覆盖人数为0或空")
        
        # 3. 检查spread_count是否为0，但可以从tokens_mark计算得到
        if token.get('spread_count', 0) == 0 or token.get('spread_count') is None:
            # 在修复脚本中会计算
            needs_fix = True
            fix_reason.append("传播次数为0或空")
        
        # 将需要修复的代币添加到列表
        if needs_fix:
            tokens_to_fix.append({
                'id': token_id,
                'chain': chain,
                'contract': contract,
                'symbol': symbol,
                'current': token,
                'reasons': fix_reason,
                'fixed_data': fixed_data
            })
    
    logger.info(f"检测到 {len(tokens_to_fix)} 个代币需要修复")
    return tokens_to_fix

async def calculate_community_stats(db_adapter, chain, contract):
    """计算代币的社群统计数据"""
    spread_count = 0
    community_reach = 0
    
    # 计算传播次数
    spread_count_result = await db_adapter.execute_query(
        'tokens_mark', 
        'select',
        filters={'chain': chain, 'contract': contract},
        count='exact'
    )
    
    if isinstance(spread_count_result, int):
        spread_count = spread_count_result
    
    # 获取所有提到该代币的频道
    channel_ids_result = await db_adapter.execute_query(
        'tokens_mark',
        'select',
        filters={'chain': chain, 'contract': contract},
        fields=['channel_id']
    )
    
    if isinstance(channel_ids_result, list) and channel_ids_result:
        unique_channel_ids = set()
        for item in channel_ids_result:
            if item and 'channel_id' in item and item['channel_id']:
                unique_channel_ids.add(item['channel_id'])
        
        # 计算社区覆盖人数
        for channel_id in unique_channel_ids:
            channel_result = await db_adapter.execute_query(
                'telegram_channels',
                'select',
                filters={'channel_id': channel_id},
                fields=['member_count'],
                limit=1
            )
            
            if isinstance(channel_result, list) and channel_result and 'member_count' in channel_result[0]:
                member_count = channel_result[0]['member_count']
                if member_count:
                    community_reach += member_count
    
    return {
        'spread_count': spread_count,
        'community_reach': community_reach
    }

async def fix_token_data(tokens_to_fix, dry_run=False, force=False):
    """修复代币数据问题"""
    db_adapter = get_db_adapter()
    
    success_count = 0
    error_count = 0
    
    for token_info in tokens_to_fix:
        token_id = token_info['id']
        chain = token_info['chain']
        contract = token_info['contract']
        symbol = token_info['symbol'] or f"{chain}/{contract}"
        fixed_data = dict(token_info['fixed_data'])
        
        try:
            # 计算社群统计数据
            community_stats = await calculate_community_stats(db_adapter, chain, contract)
            
            # 只有当社群数据大于0时才更新
            if community_stats['spread_count'] > 0:
                fixed_data['spread_count'] = community_stats['spread_count']
            
            if community_stats['community_reach'] > 0:
                fixed_data['community_reach'] = community_stats['community_reach']
            
            # 设置最后更新时间
            fixed_data['latest_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 显示修复信息
            logger.info(f"代币 {symbol} (ID={token_id}) 将被修复:")
            for reason in token_info['reasons']:
                logger.info(f" - 原因: {reason}")
            
            for key, value in fixed_data.items():
                current_value = token_info['current'].get(key)
                logger.info(f" - {key}: {current_value} -> {value}")
            
            # 执行修复
            if not dry_run:
                update_result = await db_adapter.execute_query(
                    'tokens',
                    'update',
                    data=fixed_data,
                    filters={'id': token_id}
                )
                
                if isinstance(update_result, dict) and update_result.get('error'):
                    logger.error(f"修复代币 {symbol} 失败: {update_result.get('error')}")
                    error_count += 1
                else:
                    logger.info(f"✅ 代币 {symbol} 已成功修复")
                    success_count += 1
            else:
                logger.info("模拟模式: 未执行实际修复")
                success_count += 1
                
        except Exception as e:
            logger.error(f"修复代币 {symbol} 时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            error_count += 1
    
    return success_count, error_count

async def main_async():
    # 解析命令行参数
    args = parse_args()
    
    logger.info("="*50)
    logger.info("开始检查和修复代币数据问题")
    if args.dry_run:
        logger.info("运行模式: 模拟 (不会实际修改数据)")
    else:
        logger.info("运行模式: 实际修复")
    
    if args.chain:
        logger.info(f"限制链: {args.chain}")
    if args.symbol:
        logger.info(f"限制符号: {args.symbol}")
    if args.force:
        logger.info("强制模式: 将更新所有代币")
    
    try:
        # 获取数据库适配器
        db_adapter = get_db_adapter()
        
        # 检查代币数据问题
        tokens_to_fix = await check_tokens_for_zero_values(db_adapter, args.chain, args.symbol)
        
        if args.force and not tokens_to_fix:
            # 强制模式下，如果没有找到需要修复的代币，则获取所有代币
            logger.info("强制模式: 尝试获取所有代币并更新")
            
            filters = {}
            if args.chain:
                filters['chain'] = args.chain
            if args.symbol:
                filters['token_symbol'] = args.symbol
                
            tokens = await db_adapter.execute_query('tokens', 'select', filters=filters)
            
            if tokens and isinstance(tokens, list):
                tokens_to_fix = []
                for token in tokens:
                    tokens_to_fix.append({
                        'id': token.get('id'),
                        'chain': token.get('chain'),
                        'contract': token.get('contract'),
                        'symbol': token.get('token_symbol'),
                        'current': token,
                        'reasons': ["强制更新"],
                        'fixed_data': {}
                    })
                
                logger.info(f"强制模式: 将更新 {len(tokens_to_fix)} 个代币")
        
        if not tokens_to_fix:
            logger.info("没有发现需要修复的代币数据，任务完成")
            return
        
        # 修复代币数据
        success_count, error_count = await fix_token_data(tokens_to_fix, args.dry_run, args.force)
        
        # 显示结果统计
        logger.info("="*50)
        logger.info("代币数据修复完成")
        logger.info(f"总共处理: {len(tokens_to_fix)} 个代币")
        logger.info(f"成功修复: {success_count} 个代币")
        logger.info(f"修复失败: {error_count} 个代币")
        
    except Exception as e:
        logger.error(f"修复过程中发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
    
    return 0

def main():
    # 设置日志
    log_dir = Path(project_root) / 'logs'
    os.makedirs(log_dir, exist_ok=True)
    setup_logger(__name__, log_file=log_dir / f"{datetime.now().strftime('%Y-%m-%d')}_token_fix.log")
    
    # 运行异步主函数
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    return asyncio.run(main_async())

if __name__ == "__main__":
    sys.exit(main()) 