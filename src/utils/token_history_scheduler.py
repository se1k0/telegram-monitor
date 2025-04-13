#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
代币历史数据定时任务模块
负责定期(每天0点和12点)将tokens表中的数据记录到token_history表中
"""

import asyncio
import logging
from datetime import datetime, timedelta

# 设置日志
logger = logging.getLogger(__name__)

async def record_daily_token_history():
    """
    每天0点和12点记录token历史数据到历史表中
    将所有token表中的数据记录到token_history表中，作为历史快照
    """
    try:
        logger.info("开始记录每日token历史数据快照")
        
        # 获取数据库适配器
        from src.database.db_factory import get_db_adapter
        db_adapter = get_db_adapter()
        
        # 获取所有token数据
        tokens = await db_adapter.execute_query('tokens', 'select')
        
        if not tokens or not isinstance(tokens, list):
            logger.warning("获取代币列表失败或结果为空，无法记录历史数据")
            return
        
        # 记录成功和失败的计数
        success_count = 0
        fail_count = 0
        current_time = datetime.now()  # 直接使用datetime对象，而非格式化字符串
        
        # 记录每个token的历史数据
        for token in tokens:
            try:
                # 提取需要记录的数据
                history_data = {
                    'chain': token.get('chain'),
                    'contract': token.get('contract'),
                    'token_symbol': token.get('token_symbol'),
                    'timestamp': current_time,  # 使用datetime对象
                    'market_cap': token.get('market_cap'),
                    'price': token.get('price'),
                    'liquidity': token.get('liquidity'),
                    'volume_24h': token.get('volume_24h'),
                    'volume_1h': token.get('volume_1h'),
                    'holders_count': token.get('holders_count'),
                    'buys_1h': token.get('buys_1h', 0),
                    'sells_1h': token.get('sells_1h', 0),
                    'community_reach': token.get('community_reach', 0),
                    'spread_count': token.get('spread_count', 0),
                    'market_cap_change_pct': token.get('last_calculated_change_pct'),
                    'price_change_pct': token.get('price_change_24h')
                }
                
                # 确保存在必要的字段
                if not history_data['chain'] or not history_data['contract']:
                    logger.warning(f"跳过记录历史数据: 代币 {token.get('token_symbol')} 缺少必要字段")
                    fail_count += 1
                    continue
                
                # 插入历史记录
                insert_result = await db_adapter.execute_query(
                    'token_history',
                    'insert',
                    data=history_data
                )
                
                if isinstance(insert_result, dict) and insert_result.get('error'):
                    logger.error(f"记录代币 {token.get('token_symbol')} 历史数据失败: {insert_result.get('error')}")
                    fail_count += 1
                else:
                    success_count += 1
                    
            except Exception as e:
                logger.error(f"处理代币 {token.get('token_symbol', '未知')} 的历史数据时出错: {str(e)}")
                fail_count += 1
                import traceback
                logger.error(traceback.format_exc())
        
        logger.info(f"历史数据记录完成: 成功 {success_count} 个, 失败 {fail_count} 个")
        
    except Exception as e:
        logger.error(f"记录每日token历史数据时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

def register_token_history_tasks():
    """
    注册token历史数据记录任务
    - 每天0点和12点记录所有token数据到历史表
    """
    try:
        # 导入调度器
        from src.utils.scheduler import scheduler
        
        # 计算下一个0点和12点的时间
        now = datetime.now()
        
        # 下一个0点
        next_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if next_midnight <= now:
            next_midnight += timedelta(days=1)
            
        # 下一个12点
        next_noon = now.replace(hour=12, minute=0, second=0, microsecond=0)
        if next_noon <= now:
            next_noon += timedelta(days=1)
        
        # 注册0点任务
        scheduler.schedule_task(
            task_id='daily_token_history_midnight',
            func=record_daily_token_history,
            run_at=next_midnight,
            interval=24*60*60  # 每24小时执行一次
        )
        logger.info(f"已注册每天0点token历史数据记录任务，下次执行时间: {next_midnight}")
        
        # 注册12点任务
        scheduler.schedule_task(
            task_id='daily_token_history_noon',
            func=record_daily_token_history,
            run_at=next_noon,
            interval=24*60*60  # 每24小时执行一次
        )
        logger.info(f"已注册每天12点token历史数据记录任务，下次执行时间: {next_noon}")
        
        return True
        
    except Exception as e:
        logger.error(f"注册token历史数据任务时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False 