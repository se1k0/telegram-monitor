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
        更新单个代币的数据
        
        Args:
            chain: 链名称
            contract: 合约地址
            symbol: 代币符号
            
        Returns:
            Dict: 包含更新结果的字典
        """
        logger.info(f"更新代币 {symbol} ({chain}/{contract})")
        
        result = {
            "success": False,
            "error": None,
            "data": {}
        }
        
        try:
            # 获取数据库适配器
            db_adapter = get_db_adapter()
            
            # 获取代币当前数据
            token_data = await db_adapter.execute_query(
                'tokens',
                'select',
                filters={
                    'chain': chain,
                    'contract': contract
                },
                limit=1
            )
            
            if not token_data or not isinstance(token_data, list) or len(token_data) == 0:
                logger.warning(f"未找到代币 {symbol} ({chain}/{contract})")
                result["error"] = "未找到代币"
                return result
                
            token = token_data[0]
            
            # 保存当前市值作为1小时前市值
            previous_market_cap = token.get('market_cap')
            
            # 尝试从DEX Screener获取市场数据
            try:
                from src.api.dex_screener_api import get_token_pools
                pools = get_token_pools(chain.lower(), contract)
                
                if pools and 'pairs' in pools and pools['pairs']:
                    pairs = pools['pairs']
                    
                    # 提取市场数据
                    max_market_cap = 0  # 市值
                    max_liquidity = 0   # 流动性
                    buys_1h = 0         # 1小时买入交易数
                    sells_1h = 0        # 1小时卖出交易数
                    volume_1h = 0       # 1小时交易量
                    price = None        # 价格
                    image_url = None    # 代币图像URL
                    
                    for pair in pairs:
                        # 提取市值数据
                        if 'fdv' in pair and pair['fdv']:
                            current_market_cap = float(pair['fdv'])
                            if current_market_cap > max_market_cap:
                                max_market_cap = current_market_cap
                        
                        # 提取流动性数据
                        if 'liquidity' in pair and 'usd' in pair['liquidity']:
                            current_liquidity = float(pair['liquidity']['usd'] or 0)
                            max_liquidity += current_liquidity
                        
                        # 提取交易数据
                        if 'txns' in pair and 'h1' in pair['txns']:
                            h1_data = pair['txns']['h1']
                            buys_1h += h1_data.get('buys', 0)
                            sells_1h += h1_data.get('sells', 0)
                        
                        # 提取交易量数据
                        if 'volume' in pair and 'h1' in pair['volume']:
                            volume_1h += float(pair['volume']['h1'] or 0)
                        
                        # 提取价格数据
                        if not price and 'priceUsd' in pair:
                            price = float(pair['priceUsd'])
                        
                        # 提取图像URL
                        if not image_url and 'info' in pair and 'imageUrl' in pair['info']:
                            image_url = pair['info']['imageUrl']
                    
                    # 计算涨跌幅
                    change_pct = 0
                    if previous_market_cap and max_market_cap > 0 and previous_market_cap > 0:
                        change_pct = (max_market_cap - previous_market_cap) / previous_market_cap * 100
                    
                    # 准备更新数据 - 只包含有实际值的字段
                    updated_data = {
                        'last_calculation_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 计算时间始终更新
                    }
                    
                    # 只有当市值大于0时才更新相关字段
                    if max_market_cap > 0:
                        # 添加详细的市值更新日志
                        logger.info(f"市值更新: {previous_market_cap} -> {max_market_cap}")
                        
                        # 始终将当前市值保存到market_cap_1h，然后更新最新市值
                        updated_data['market_cap_1h'] = previous_market_cap  # 当前值变为1小时前值
                        updated_data['market_cap'] = max_market_cap          # 新的市值
                        
                        # 同时更新市值的格式化显示
                        if max_market_cap >= 1000000000:  # 十亿 (B)
                            formatted = f"${max_market_cap/1000000000:.2f}B"
                        elif max_market_cap >= 1000000:   # 百万 (M)
                            formatted = f"${max_market_cap/1000000:.2f}M"
                        elif max_market_cap >= 1000:      # 千 (K)
                            formatted = f"${max_market_cap/1000:.2f}K"
                        else:
                            formatted = f"${max_market_cap:.2f}"
                        
                        updated_data['market_cap_formatted'] = formatted
                        logger.info(f"更新格式化市值: {formatted}")
                    
                    # 只有当流动性大于0时才更新
                    if max_liquidity > 0:
                        updated_data['liquidity'] = max_liquidity            # 流动性
                    
                    # 只有当计算出涨跌幅时才更新
                    if change_pct != 0:
                        updated_data['last_calculated_change_pct'] = change_pct  # 涨跌幅
                    
                    # 只有当交易数据大于0时才更新
                    if buys_1h > 0:
                        updated_data['buys_1h'] = buys_1h                    # 1小时买入交易数
                    
                    if sells_1h > 0:
                        updated_data['sells_1h'] = sells_1h                  # 1小时卖出交易数
                    
                    if volume_1h > 0:
                        updated_data['volume_1h'] = volume_1h                # 1小时交易量
                    
                    # 只有当有值时才更新价格
                    if price is not None and price > 0:
                        updated_data['price'] = price
                    
                    # 只有当有值时才更新图像URL
                    if image_url:
                        updated_data['image_url'] = image_url
                    
                    # 添加日志记录实际更新的字段
                    logger.info(f"将为代币 {symbol} 更新以下字段: {list(updated_data.keys())}")
                    
                    # 更新数据库
                    update_result = await db_adapter.execute_query(
                        'tokens',
                        'update',
                        data=updated_data,
                        filters={'chain': chain, 'contract': contract}
                    )
                    
                    if isinstance(update_result, dict) and update_result.get('error'):
                        logger.error(f"更新代币 {symbol} 失败: {update_result.get('error')}")
                        result["error"] = update_result.get('error')
                    else:
                        logger.info(f"成功更新代币 {symbol}: 市值=${max_market_cap:.2f}, "
                                   f"涨跌幅={change_pct:.2f}%, "
                                   f"1小时买入={buys_1h}, "
                                   f"1小时卖出={sells_1h}, "
                                   f"1小时交易量=${volume_1h:.2f}")
                        
                        result["success"] = True
                        result["data"] = updated_data
                else:
                    logger.warning(f"无法从DEX Screener获取代币 {symbol} 的数据")
                    result["error"] = "无法获取DEX Screener数据"
            except Exception as e:
                logger.error(f"获取DEX Screener数据失败: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                result["error"] = f"获取市场数据出错: {str(e)}"
            
            # 更新社区覆盖数据
            try:
                from src.database.supabase_adapter import SupabaseAdapter
                
                spread_count = 0
                community_reach = 0
                
                # 计算传播次数
                spread_count_result = await db_adapter.execute_query(
                    'tokens_mark',
                    'select',
                    filters={'chain': chain, 'contract': contract}
                )
                
                if isinstance(spread_count_result, list):
                    spread_count = len(spread_count_result)
                    logger.info(f"代币 {symbol} 的传播次数: {spread_count}")
                
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
                
                # 更新社区数据
                if spread_count > 0 or community_reach > 0:
                    community_data = {}
                    
                    # 检查当前值，只有当新值更大时才更新
                    current_token = await db_adapter.get_token_by_contract(chain, contract)
                    
                    if current_token:
                        current_spread_count = current_token.get('spread_count', 0) or 0
                        current_community_reach = current_token.get('community_reach', 0) or 0
                        
                        # 只有当新的传播次数大于当前值时才更新
                        if spread_count > current_spread_count:
                            community_data['spread_count'] = spread_count
                            logger.info(f"更新 {symbol} 传播次数: {current_spread_count} -> {spread_count}")
                        
                        # 只有当新的社群覆盖人数大于当前值时才更新
                        if community_reach > current_community_reach:
                            community_data['community_reach'] = community_reach
                            logger.info(f"更新 {symbol} 覆盖人数: {current_community_reach} -> {community_reach}")
                    else:
                        # 如果找不到当前代币记录，则直接更新
                        if spread_count > 0:
                            community_data['spread_count'] = spread_count
                        
                        if community_reach > 0:
                            community_data['community_reach'] = community_reach
                    
                    # 只有当有实际字段需要更新时才执行
                    if community_data:
                        logger.info(f"将为代币 {symbol} 更新社区数据: {community_data}")
                        
                        update_result = await db_adapter.execute_query(
                            'tokens',
                            'update',
                            data=community_data,
                            filters={'chain': chain, 'contract': contract}
                        )
                        
                        if not (isinstance(update_result, dict) and update_result.get('error')):
                            logger.info(f"成功更新代币 {symbol} 的社区数据")
                            
                            # 更新结果
                            result["data"].update(community_data)
                            result["success"] = True
                    else:
                        logger.info(f"代币 {symbol} 的社区数据未发生变化，无需更新")
            except Exception as e:
                logger.error(f"更新社区覆盖数据出错: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                # 不影响整体成功状态
                
            return result
        except Exception as e:
            logger.error(f"更新代币 {symbol} 时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            result["error"] = str(e)
            return result
    
    async def hourly_update_async(self, limit: int = 500, 
                              batch_size: int = None, 
                              min_delay: float = None, 
                              max_delay: float = None) -> Dict[str, Any]:
        """
        执行每小时更新任务，获取最新的代币数据
        
        Args:
            limit: 最大更新代币数量
            batch_size: 每批处理的代币数量
            min_delay: 请求间最小延迟(秒)
            max_delay: 请求间最大延迟(秒)
            
        Returns:
            Dict: 包含更新结果的字典
        """
        if self.running:
            logger.warning("更新任务已在运行中，请等待当前任务完成")
            return {"error": "更新任务已在运行中"}
            
        # 设置标志为运行中
        self.running = True
        
        # 如果未提供参数，使用默认值
        if batch_size is None:
            batch_size = self.default_batch_size
        if min_delay is None:
            min_delay = self.default_min_delay
        if max_delay is None:
            max_delay = self.default_max_delay
        
        # 记录启动信息
        logger.info("="*50)
        logger.info(f"开始每小时代币数据更新: {datetime.now()}")
        logger.info(f"最大更新数量: {limit}")
        logger.info(f"批次大小: {batch_size}")
        logger.info(f"请求延迟范围: {min_delay}-{max_delay}秒")
        
        # 结果汇总
        results = {
            "start_time": datetime.now(),
            "end_time": None,
            "duration": None,
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
                
                # 分批处理每个代币
                batch_results = await self.process_batch(batch, min_delay, max_delay)
                
                # 更新统计信息
                results["success"] += batch_results["success"]
                results["failed"] += batch_results["failed"]
                results["details"].extend(batch_results["details"])
                
                logger.info(f"当前批次更新完成: 成功={batch_results['success']}, 失败={batch_results['failed']}")
                
                # 批次之间的延迟，防止过于频繁请求
                batch_delay = random.uniform(batch_size * min_delay * 0.2, batch_size * max_delay * 0.2)
                logger.info(f"等待 {batch_delay:.2f} 秒后继续下一批...")
                
                await asyncio.sleep(batch_delay)
            
            # 更新社区覆盖人数和传播次数
            logger.info("开始更新代币社区覆盖人数和传播次数...")
            try:
                # 尝试导入社区覆盖数据更新函数
                try:
                    from scripts.update_community_reach import update_all_tokens_community_reach_async
                    community_result = await update_all_tokens_community_reach_async(limit=limit, continue_from_last=True, concurrency=5)
                    
                    logger.info(f"社区覆盖数据更新完成: 成功={community_result.get('success', 0)}, 失败={community_result.get('failed', 0)}")
                except ImportError:
                    logger.error("导入社区数据更新函数失败，请检查scripts/update_community_reach.py是否存在")
            except Exception as e:
                logger.error(f"更新社区覆盖数据时出错: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
            
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

    async def process_batch(self, batch, min_delay, max_delay):
        """
        处理一批代币的更新
        
        Args:
            batch: 代币列表
            min_delay: 最小延迟时间
            max_delay: 最大延迟时间
            
        Returns:
            Dict: 批处理结果统计
        """
        results = {
            "success": 0,
            "failed": 0,
            "details": []
        }
        
        for token in batch:
            if not self.running:
                logger.info("批处理被中断")
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
            if result.get("success", False):
                results["success"] += 1
            else:
                results["failed"] += 1
                # 检查是否达到API限制
                error = result.get("error", "")
                if isinstance(error, str) and ("rate limit" in error.lower() or "too many requests" in error.lower()):
                    logger.warning("检测到API速率限制，增加等待时间...")
                    await asyncio.sleep(max_delay * 3)  # 遇到速率限制，等待更长时间
            
            # 记录详细结果
            results["details"].append({
                "chain": token["chain"],
                "symbol": token["symbol"],
                "contract": token["contract"],
                "success": result.get("success", False),
                "error": result.get("error", "")
            })
            
        return results

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