#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
代币数据更新模块
用于在项目运行期间自动更新代币数据
设计为每小时整点自动执行
"""

import logging
import random
import time
import traceback
import asyncio
from datetime import datetime
from typing import Dict, List, Any, Optional

# 导入数据库适配器
from src.database.db_factory import get_db_adapter

# 设置日志
try:
    from src.utils.logger import get_logger
    logger = get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

class TokenDataUpdater:
    """代币数据更新器"""
    
    def __init__(self):
        """初始化更新器"""
        self.running = False
        self.default_batch_size = 50
        self.default_min_delay = 0.5
        self.default_max_delay = 2.0
    
    async def get_tokens_to_update(self, limit: int = None) -> List[Dict[str, str]]:
        """
        获取需要更新的代币列表
        
        Args:
            limit: 最大返回数量，None表示不限制
            
        Returns:
            List[Dict]: 代币信息列表，每个字典包含chain和contract
        """
        try:
            # 获取数据库适配器
            db_adapter = get_db_adapter()
            
            # 构建查询参数
            query_params = {'limit': limit} if limit else {}
            
            # 查询所有代币
            tokens = await db_adapter.execute_query('tokens', 'select', limit=limit)
            
            if not tokens or not isinstance(tokens, list):
                logger.warning("获取代币列表失败或结果为空")
                return []
                
            # 提取需要的字段
            return [{"chain": token.get('chain'), 
                     "contract": token.get('contract'), 
                     "symbol": token.get('token_symbol')} 
                    for token in tokens 
                    if token.get('chain') and token.get('contract')]
                
        except Exception as e:
            logger.error(f"获取代币列表时发生错误: {str(e)}")
            logger.error(traceback.format_exc())
            return []
    
    async def update_token(self, chain: str, contract: str, symbol: str) -> Dict[str, Any]:
        """
        更新单个代币数据
        
        Args:
            chain: 区块链名称
            contract: 代币合约地址
            symbol: 代币符号，用于日志
            
        Returns:
            Dict: 更新结果字典
        """
        try:
            logger.info(f"更新代币 {symbol} ({chain}/{contract})")
            
            # 获取数据库适配器
            db_adapter = get_db_adapter()
            
            # 查询现有代币
            token = await db_adapter.get_token_by_contract(chain, contract)
            
            if not token:
                logger.warning(f"找不到代币 {symbol} ({chain}/{contract})")
                return {"success": False, "error": "代币不存在"}
            
            # 更新市场数据，此处需修改为使用db_adapter实现
            # 这里是一个简化版实现，实际应根据token_market_updater模块的设计调整
            updated_data = {}
            
            # 尝试从DEX Screener获取市场数据
            try:
                from src.api.dex_screener_api import get_pair_info
                pair_info = await get_pair_info(chain, contract)
                
                if pair_info and isinstance(pair_info, dict):
                    # 提取市场数据
                    updated_data = {
                        'price': pair_info.get('priceUsd'),
                        'liquidity': pair_info.get('liquidity', {}).get('usd'),
                        'volume_24h': pair_info.get('volume', {}).get('h24', {}).get('USD'),
                        'market_cap': pair_info.get('fdv')
                    }
                    
                    # 提取交易数据
                    if 'txns' in pair_info:
                        txns = pair_info['txns'].get('h1', {})
                        updated_data['buys_1h'] = txns.get('buys')
                        updated_data['sells_1h'] = txns.get('sells')
                    
                    # 更新数据库
                    update_result = await db_adapter.execute_query(
                        'tokens',
                        'update',
                        data=updated_data,
                        filters={'chain': chain, 'contract': contract}
                    )
                    
                    logger.info(f"成功更新代币 {symbol}: 市值={updated_data.get('market_cap', 'N/A')}, "
                               f"1小时买入={updated_data.get('buys_1h', 'N/A')}, "
                               f"1小时卖出={updated_data.get('sells_1h', 'N/A')}")
                    
                    return {"success": True, "result": updated_data}
                else:
                    logger.warning(f"无法从DEX Screener获取代币 {symbol} 的数据")
                    return {"success": False, "error": "无法获取市场数据"}
                    
            except Exception as api_error:
                logger.error(f"从DEX Screener获取数据时出错: {str(api_error)}")
                return {"success": False, "error": str(api_error)}
            
        except Exception as e:
            logger.error(f"更新代币 {symbol} 时发生异常: {str(e)}")
            logger.error(traceback.format_exc())
            return {"success": False, "error": str(e)}
    
    async def hourly_update_async(self, limit: int = 500, 
                      batch_size: int = None, 
                      min_delay: float = None, 
                      max_delay: float = None) -> Dict[str, Any]:
        """
        执行每小时更新任务的异步实现
        
        Args:
            limit: 最大更新代币数量
            batch_size: 每批处理的代币数量
            min_delay: 请求间最小延迟(秒)
            max_delay: 请求间最大延迟(秒)
            
        Returns:
            Dict: 包含更新结果的字典
        """
        self.running = True
        
        # 使用默认值
        if batch_size is None:
            batch_size = self.default_batch_size
        if min_delay is None:
            min_delay = self.default_min_delay
        if max_delay is None:
            max_delay = self.default_max_delay
        
        logger.info("="*50)
        logger.info(f"开始每小时代币数据更新: {datetime.now()}")
        
        results = {
            "start_time": datetime.now(),
            "end_time": None,
            "total": 0,
            "success": 0,
            "failed": 0,
            "details": []
        }
        
        try:
            # 获取需要更新的代币列表
            tokens = await self.get_tokens_to_update(limit)
            
            if not tokens:
                logger.warning("没有找到需要更新的代币")
                results["end_time"] = datetime.now()
                results["duration"] = (results["end_time"] - results["start_time"]).total_seconds()
                return results
            
            results["total"] = len(tokens)
            logger.info(f"找到 {len(tokens)} 个代币需要更新")
            
            # 随机打乱代币列表顺序，避免按相同顺序请求
            random.shuffle(tokens)
            
            # 分批处理
            for i in range(0, len(tokens), batch_size):
                if not self.running:
                    logger.info("更新过程被中断")
                    break
                    
                batch = tokens[i:i+batch_size]
                logger.info(f"处理第 {i//batch_size + 1} 批，共 {len(batch)} 个代币")
                
                # 处理每个批次中的代币
                for token in batch:
                    if not self.running:
                        logger.info("更新过程被中断")
                        break
                        
                    # 随机延迟，模拟人工操作
                    delay = min_delay + random.random() * (max_delay - min_delay)
                    await asyncio.sleep(delay)
                    
                    # 更新代币数据
                    result = await self.update_token(
                        token["chain"], 
                        token["contract"],
                        token["symbol"]
                    )
                    
                    # 记录结果
                    if result["success"]:
                        results["success"] += 1
                    else:
                        results["failed"] += 1
                        # 检查是否达到API限制
                        error = result.get("error", "")
                        if isinstance(error, str) and ("rate limit" in error.lower() or "too many requests" in error.lower()):
                            logger.warning("检测到API速率限制，增加等待时间...")
                            await asyncio.sleep(max_delay * 3)  # 遇到速率限制，等待更长时间
                    
                    results["details"].append({
                        "chain": token["chain"],
                        "symbol": token["symbol"],
                        "contract": token["contract"],
                        "success": result["success"],
                        "error": result.get("error")
                    })
                
                # 批次之间的延迟更长一些，避免触发API限制
                batch_delay = max_delay * 2
                logger.info(f"批次完成，等待 {batch_delay:.2f} 秒后继续...")
                await asyncio.sleep(batch_delay)
            
        except Exception as e:
            logger.error(f"执行每小时更新任务时发生错误: {str(e)}")
            logger.error(traceback.format_exc())
            
        finally:
            # 记录结束时间和持续时间
            results["end_time"] = datetime.now()
            results["duration"] = (results["end_time"] - results["start_time"]).total_seconds()
            
            # 打印结果摘要
            logger.info("="*30)
            logger.info(f"更新完成: {datetime.now()}")
            logger.info(f"总代币数: {results['total']}")
            logger.info(f"成功: {results['success']}")
            logger.info(f"失败: {results['failed']}")
            logger.info(f"总用时: {results['duration']}秒")
            logger.info("="*50)
            
            self.running = False
            return results
    
    def hourly_update(self, limit: int = 500, 
                     batch_size: int = None, 
                     min_delay: float = None, 
                     max_delay: float = None) -> Dict[str, Any]:
        """
        执行每小时更新任务（同步包装异步函数）
        
        Args:
            limit: 最大更新代币数量
            batch_size: 每批处理的代币数量
            min_delay: 请求间最小延迟(秒)
            max_delay: 请求间最大延迟(秒)
            
        Returns:
            Dict: 包含更新结果的字典
        """
        # 创建新的事件循环来运行异步函数
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(
                self.hourly_update_async(limit, batch_size, min_delay, max_delay)
            )
            return result
        finally:
            loop.close()
    
    def stop(self):
        """停止正在进行的更新任务"""
        if self.running:
            logger.info("停止更新任务...")
            self.running = False
        else:
            logger.info("没有正在运行的更新任务")

# 创建全局更新器实例
token_updater = TokenDataUpdater()

# 直接调用的便捷函数
def hourly_update(limit: int = 500) -> Dict[str, Any]:
    """
    执行每小时更新任务的便捷函数
    
    Args:
        limit: 最大更新代币数量
        
    Returns:
        Dict: 包含更新结果的字典
    """
    return token_updater.hourly_update(limit=limit) 