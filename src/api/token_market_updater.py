#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
代币市值和流动性池价值更新模块
使用DexScreenerAPI获取代币市值和流动性信息并更新数据库
"""

import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text
import time
from datetime import datetime, timedelta
import sys

from src.api.dex_screener_api import get_token_pools, DexScreenerAPI
from src.database.models import Token
from src.utils.error_handler import retry, safe_execute
from src.database.db_factory import get_db_adapter
from src.api.das_api import DASAPI

# 设置日志记录
logger = logging.getLogger(__name__)

class TokenMarketUpdater:
    """代币市值和流动性更新类"""
    
    def __init__(self, session: Session):
        """
        初始化代币市值更新器
        
        Args:
            session: 数据库会话
        """
        self.session = session
        self.dex_screener = DexScreenerAPI()
    
    @retry(max_retries=3, delay=1.0, exceptions=(Exception,))
    def update_token_market_data(self, chain: str, contract: str) -> Dict[str, Any]:
        """
        更新单个代币的市值和流动性数据
        
        Args:
            chain: 区块链名称
            contract: 代币合约地址
            
        Returns:
            Dict: 包含更新结果的字典，包括marketCap和liquidity
        """
        logger.info(f"开始更新代币 {chain}/{contract} 的市值和流动性数据")
        
        # 标准化链ID
        chain_id = self._normalize_chain_id(chain)
        if not chain_id:
            logger.warning(f"不支持的链: {chain}")
            return {"error": f"不支持的链: {chain}"}
        
        # 获取代币池数据
        pools_data = get_token_pools(chain_id, contract)
        
        # 检查API响应
        if isinstance(pools_data, dict) and "error" in pools_data:
            logger.error(f"获取代币池数据失败: {pools_data['error']}")
            return {"error": pools_data["error"]}
        
        # 处理API返回的数据结构
        # API文档显示token-pairs/v1 返回直接就是数组，而不是包含pairs字段的对象
        pairs = pools_data  # 直接使用返回的数据，它应该是一个数组
            
        if not pairs or not isinstance(pairs, list) or len(pairs) == 0:
            logger.warning(f"未找到代币 {chain}/{contract} 的交易对")
            return {"error": "未找到代币交易对"}
            
        # 添加API返回数据的详细日志
        # logger.info(f"DEX API原始返回数据: {pools_data}")
        
        # 查找市值最高的池
        max_market_cap = 0
        max_liquidity = 0
        dex_screener_url = None
        price = None
        first_price = None
        symbol = None
        telegram_url = None  # 新增：初始化telegram_url
        twitter_url = None   # 新增：初始化twitter_url
        website_url = None   # 新增：初始化website_url
        
        for pair in pairs:
            # 添加每个交易对的详细日志
            # logger.info(f"处理交易对数据: {pair}")

            if not symbol and pair.get("baseToken") and "symbol" in pair.get("baseToken", {}):
                symbol = pair["baseToken"]["symbol"]
            
            # 获取市值数据
            market_cap = pair.get("marketCap", 0)
            if market_cap and float(market_cap) > max_market_cap:
                max_market_cap = float(market_cap)
                logger.info(f"更新最大市值: {max_market_cap}")
                
            # 获取流动性数据
            liquidity = pair.get("liquidity", {}).get("usd", 0)
            if liquidity and float(liquidity) > max_liquidity:
                max_liquidity = float(liquidity)
                logger.info(f"更新最大流动性: {max_liquidity}")
                
            # 获取价格
            if not price and "priceUsd" in pair:
                price = float(pair["priceUsd"])

            # 获取首次价格
            if not first_price and "priceNative" in pair:
                first_price = float(pair["priceNative"])
                
            # 获取DEX Screener URL
            if not dex_screener_url:
                chain_path = pair.get("chainId", "").lower()
                pair_address = pair.get("pairAddress", "")
                if chain_path and pair_address:
                    dex_screener_url = f"https://dexscreener.com/{chain_path}/{pair_address}"
            
            # 获取代币图像URL
            if not dex_screener_url and pair.get("info") and pair.get("info").get("imageUrl"):
                dex_screener_url = pair.get("info").get("imageUrl")
                logger.info(f"从DEX API获取到代币图像URL: {dex_screener_url}")
            
            # 获取交易数据
            if "txns" in pair and "h1" in pair["txns"]:
                txns_1h = pair["txns"]["h1"]
                buys = txns_1h.get("buys", 0)
                sells = txns_1h.get("sells", 0)
                
                # 累加交易数据
                total_buys_1h += buys
                total_sells_1h += sells
                
                # 计算1小时交易量
                if 'volume' in pair and 'h1' in pair['volume']:
                    volume_h1_data = pair['volume']['h1']
                    if 'USD' in volume_h1_data:
                        volume_1h = float(volume_h1_data['USD'])
                        total_volume_1h += volume_1h
            
            # 新增：提取社交链接
            if pair.get("info") and pair["info"].get("websites"):
                for w in pair["info"]["websites"]:
                    url = w.get("url", "")
                    if not telegram_url and ("t.me/" in url or "telegram" in url):
                        telegram_url = url
                    elif not twitter_url and ("twitter.com/" in url or "x.com/" in url):
                        twitter_url = url
                    elif not website_url and (url.startswith("http") and not any(x in url for x in ["t.me/", "telegram", "twitter.com/", "x.com/"])):
                        website_url = url
        
        # 在判断之前添加汇总日志
        logger.info(f"DEX API数据解析汇总:")
        logger.info(f"- 最大市值: {max_market_cap}")
        logger.info(f"- 价格: {price}")
        logger.info(f"- 代币符号: {symbol}")
        logger.info(f"- 最大流动性: {max_liquidity}")
        logger.info(f"- 1小时交易量: {volume_1h}")
        logger.info(f"- 1小时买入次数: {total_buys_1h}")
        logger.info(f"- 1小时卖出次数: {total_sells_1h}")
        
        # 获取目标代币
        token = self.session.query(Token).filter(
            Token.chain == chain,
            Token.contract == contract
        ).first()
        
        if token:
            # 更新代币数据
            try:
                # 保存当前市值到market_cap_1h字段
                token.market_cap_1h = token.market_cap
                token.market_cap = max_market_cap
                token.market_cap_formatted = self._format_market_cap(max_market_cap)
                token.liquidity = max_liquidity
                
                token.price = price
                # 如果是首次设置价格，同时设置first_price
                if token.first_price is None:
                    token.first_price = first_price
                
                if dex_screener_url:
                    token.dexscreener_url = dex_screener_url
                
                if telegram_url:
                    token.telegram_url = telegram_url
                if twitter_url:
                    token.twitter_url = twitter_url
                if website_url:
                    token.website_url = website_url
                
                self.session.commit()
                logger.info(f"成功更新代币 {chain}/{contract} 的市值和流动性数据")
                logger.info(f"市值: {max_market_cap}, 上一小时市值: {token.market_cap_1h}, 流动性: {max_liquidity}")
                
                return {
                    "success": True,
                    "marketCap": max_market_cap,
                    "marketCap1h": token.market_cap_1h,
                    "liquidity": max_liquidity,
                    "price": price,
                    "dexScreenerUrl": dex_screener_url,
                    "telegram_url": telegram_url,
                    "twitter_url": twitter_url,
                    "website_url": website_url
                }
                
            except Exception as e:
                self.session.rollback()
                logger.error(f"更新代币数据时发生错误: {str(e)}")
                return {"error": str(e)}
        else:
            logger.warning(f"数据库中未找到代币 {chain}/{contract}")
            return {"error": "数据库中未找到该代币"}
    
    @safe_execute(default_return={"total": 0, "success": 0, "failed": 0, "details": []})
    def update_all_tokens(self, limit: int = 100, delay: float = 0.2) -> Dict[str, Any]:
        """
        批量更新所有代币的市值和流动性数据
        
        Args:
            limit: 最大更新数量
            delay: 每次API请求之间的延迟（秒）
            
        Returns:
            Dict: 包含更新结果的字典
        """
        tokens = self.session.query(Token).limit(limit).all()
        
        results = {
            "total": len(tokens),
            "success": 0,
            "failed": 0,
            "details": []
        }
        
        for token in tokens:
            try:
                result = self.update_token_market_data(token.chain, token.contract)
                
                if "error" not in result:
                    results["success"] += 1
                else:
                    results["failed"] += 1
                    
                results["details"].append({
                    "chain": token.chain,
                    "symbol": token.token_symbol,
                    "contract": token.contract,
                    "result": "success" if "error" not in result else "failed",
                    "error": result.get("error")
                })
                
                # 添加延迟避免API限制
                time.sleep(delay)
                
            except Exception as e:
                results["failed"] += 1
                results["details"].append({
                    "chain": token.chain,
                    "symbol": token.token_symbol,
                    "contract": token.contract,
                    "result": "failed",
                    "error": str(e)
                })
                logger.error(f"更新代币 {token.chain}/{token.contract} 时发生错误: {str(e)}")
                
        return results
    
    @safe_execute(default_return={"total": 0, "success": 0, "failed": 0, "details": []})
    def update_tokens_by_symbols(self, symbols: List[str]) -> Dict[str, Any]:
        """
        根据代币符号批量更新代币的市值和流动性数据
        
        Args:
            symbols: 代币符号列表
            
        Returns:
            Dict: 包含更新结果的字典
        """
        tokens = self.session.query(Token).filter(Token.token_symbol.in_(symbols)).all()
        
        results = {
            "total": len(tokens),
            "success": 0,
            "failed": 0,
            "details": []
        }
        
        for token in tokens:
            try:
                result = self.update_token_market_data(token.chain, token.contract)
                
                if "error" not in result:
                    results["success"] += 1
                else:
                    results["failed"] += 1
                    
                results["details"].append({
                    "chain": token.chain,
                    "symbol": token.token_symbol,
                    "contract": token.contract,
                    "result": "success" if "error" not in result else "failed",
                    "error": result.get("error")
                })
                
                # 添加延迟避免API限制
                time.sleep(0.2)
                
            except Exception as e:
                results["failed"] += 1
                results["details"].append({
                    "chain": token.chain,
                    "symbol": token.token_symbol,
                    "contract": token.contract,
                    "result": "failed",
                    "error": str(e)
                })
                logger.error(f"更新代币 {token.chain}/{token.contract} 时发生错误: {str(e)}")
                
        return results
    
    @safe_execute(default_return={"total": 0, "success": 0, "failed": 0, "details": []})
    def update_all_tokens_txn_data(self, limit: int = 100, delay: float = 0.2) -> Dict[str, Any]:
        """
        批量更新所有代币的1小时交易数据
        
        Args:
            limit: 最大更新数量
            delay: 每次API请求之间的延迟（秒）
            
        Returns:
            Dict: 包含更新结果的字典
        """
        tokens = self.session.query(Token).limit(limit).all()
        
        results = {
            "total": len(tokens),
            "success": 0,
            "failed": 0,
            "details": []
        }
        
        for token in tokens:
            try:
                result = self.update_token_txn_data(token.chain, token.contract)
                
                if "error" not in result:
                    results["success"] += 1
                else:
                    results["failed"] += 1
                    
                results["details"].append({
                    "chain": token.chain,
                    "symbol": token.token_symbol,
                    "contract": token.contract,
                    "result": "success" if "error" not in result else "failed",
                    "error": result.get("error"),
                    "buys_1h": result.get("buys_1h"),
                    "sells_1h": result.get("sells_1h")
                })
                
                # 添加延迟避免API限制
                time.sleep(delay)
                
            except Exception as e:
                results["failed"] += 1
                results["details"].append({
                    "chain": token.chain,
                    "symbol": token.token_symbol,
                    "contract": token.contract,
                    "result": "failed",
                    "error": str(e)
                })
                logger.error(f"更新代币 {token.chain}/{token.contract} 的交易数据时发生错误: {str(e)}")
                
        return results
    
    @retry(max_retries=3, delay=1.0, exceptions=(Exception,))
    def update_token_txn_data(self, chain: str, contract: str) -> Dict[str, Any]:
        """
        更新单个代币的1小时交易数据
        
        Args:
            chain: 区块链名称
            contract: 代币合约地址
            
        Returns:
            Dict: 包含更新结果的字典，包括buys_1h和sells_1h
        """
        logger.info(f"开始更新代币 {chain}/{contract} 的1小时交易数据")
        
        # 标准化链ID
        chain_id = self._normalize_chain_id(chain)
        if not chain_id:
            logger.warning(f"不支持的链: {chain}")
            return {"error": f"不支持的链: {chain}"}
        
        # 获取代币池数据
        pools_data = get_token_pools(chain_id, contract)
        
        # 检查API响应
        if isinstance(pools_data, dict) and "error" in pools_data:
            logger.error(f"获取代币池数据失败: {pools_data['error']}")
            return {"error": pools_data["error"]}
        
        # 处理API返回的数据结构
        # API文档显示token-pairs/v1 返回直接就是数组，而不是包含pairs字段的对象
        pairs = pools_data  # 直接使用返回的数据，它应该是一个数组
            
        if not pairs or not isinstance(pairs, list) or len(pairs) == 0:
            logger.warning(f"未找到代币 {chain}/{contract} 的交易对")
            return {"error": "未找到代币交易对"}
            
        # 汇总所有交易对的交易数据
        total_buys_1h = 0
        total_sells_1h = 0
        total_volume_1h = 0
        
        for pair in pairs:
            # 获取交易数据
            if "txns" in pair and "h1" in pair["txns"]:
                txns_1h = pair["txns"]["h1"]
                buys = txns_1h.get("buys", 0)
                sells = txns_1h.get("sells", 0)
                
                # 累加交易数据
                total_buys_1h += buys
                total_sells_1h += sells
                
                # 计算1小时交易量
                if 'volume' in pair and 'h1' in pair['volume']:
                    volume_h1_data = pair['volume']['h1']
                    if 'USD' in volume_h1_data:
                        volume_1h = float(volume_h1_data['USD'])
                        total_volume_1h += volume_1h
        
        try:
            # 获取数据库适配器
            db_adapter = get_db_adapter()
            
            # 使用异步运行
            import asyncio
            
            # 更新数据库
            async def update_token_data():
                # 使用Supabase适配器更新数据
                token_data = {
                    'buys_1h': total_buys_1h,
                    'sells_1h': total_sells_1h,
                    'volume_1h': total_volume_1h
                }
                
                # 更新数据库
                await db_adapter.execute_query(
                    'tokens',
                    'update',
                    data=token_data,
                    filters={
                        'chain': chain,
                        'contract': contract
                    }
                )
            
            # 执行异步任务
            asyncio.run(update_token_data())
            
            logger.info(f"成功更新代币 {chain}/{contract} 的1小时交易数据")
            logger.info(f"1小时买入: {total_buys_1h}, 1小时卖出: {total_sells_1h}, 1小时交易量: {total_volume_1h}")
            
            return {
                "success": True,
                "buys_1h": total_buys_1h,
                "sells_1h": total_sells_1h,
                "volume_1h": total_volume_1h
            }
            
        except Exception as e:
            logger.error(f"更新代币1小时交易数据时发生错误: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {"error": str(e)}
    
    def _format_market_cap(self, market_cap: float) -> str:
        """
        格式化市值数据为易读形式
        
        Args:
            market_cap: 代币市值
            
        Returns:
            str: 格式化后的市值字符串
        """
        if market_cap is None:
            return "N/A"
            
        if market_cap >= 1_000_000_000:
            return f"${market_cap / 1_000_000_000:.2f}B"
        elif market_cap >= 1_000_000:
            return f"${market_cap / 1_000_000:.2f}M"
        elif market_cap >= 1_000:
            return f"${market_cap / 1_000:.2f}K"
        else:
            return f"${market_cap:.2f}"
    
    def _normalize_chain_id(self, chain: str) -> Optional[str]:
        """标准化链ID到DexScreener API支持的格式
        
        Args:
            chain: 链ID，支持大写简写(如ETH)或小写全称(如ethereum)
            
        Returns:
            Optional[str]: 标准化后的链ID，如果不支持则返回None
        """
        chain_map = {
            "SOL": "solana",
            "ETH": "ethereum",
            "BSC": "bsc",
            "AVAX": "avalanche",
            "MATIC": "polygon",
            "ARB": "arbitrum",
            "OP": "optimism",
            "BASE": "base",
            "ZK": "zksync",
            "TON": "ton"
        }
        
        # 支持的小写全称列表
        valid_chain_ids = {
            "solana", "ethereum", "bsc", "avalanche", "polygon", 
            "arbitrum", "optimism", "base", "zksync", "ton"
        }
        
        # 检查是否已经是小写全称格式
        if chain.lower() in valid_chain_ids:
            return chain.lower()
        
        # 尝试将大写简写转换为小写全称
        return chain_map.get(chain.upper())
    
    @retry(max_retries=3, delay=1.0, exceptions=(Exception,))
    async def update_token_market_and_txn_data(self, chain: str, contract: str) -> Dict[str, Any]:
        """
        综合更新单个代币的市值和1小时交易数据
        
        Args:
            chain: 区块链名称
            contract: 代币合约地址
            
        Returns:
            Dict: 包含更新结果的字典
        """
        logger.info(f"开始综合更新代币 {chain}/{contract} 的市值和交易数据")
        
        # 标准化链ID
        chain_id = self._normalize_chain_id(chain)
        if not chain_id:
            logger.warning(f"不支持的链: {chain}")
            return {"error": f"不支持的链: {chain}"}
        
        # 获取代币池数据
        pools_data = get_token_pools(chain_id, contract)
        
        # 检查API响应
        if isinstance(pools_data, dict) and "error" in pools_data:
            logger.error(f"获取代币池数据失败: {pools_data['error']}")
            return {"error": pools_data["error"]}
        
        # 处理API返回的数据结构
        # API文档显示token-pairs/v1 返回直接就是数组，而不是包含pairs字段的对象
        pairs = pools_data  # 直接使用返回的数据，它应该是一个数组
            
        if not pairs or not isinstance(pairs, list) or len(pairs) == 0:
            logger.warning(f"未找到代币 {chain}/{contract} 的交易对")
            
            # 检查代币是否存在于数据库中
            from src.database.db_factory import get_db_adapter
            db_adapter = get_db_adapter()
            token_result = await db_adapter.execute_query('tokens', 'select', filters={'chain': chain, 'contract': contract})
            
            if isinstance(token_result, list) and len(token_result) > 0:
                token = token_result[0]
                token_symbol = token.get('token_symbol', '未知')
                last_update = token.get('latest_update', '未知')
                
                # 如果代币存在但DEX API没有数据，则删除该代币及其相关数据
                logger.info(f"代币 {token_symbol} ({chain}/{contract}) 在DEX上不存在，将尝试从数据库中删除")
                logger.info(f"代币详情 - 符号: {token_symbol}, 最后更新: {last_update}")
                
                delete_result = await delete_token_data(chain, contract, double_check=True)
                
                if delete_result['success']:
                    deleted_info = delete_result.get('deleted_token_data', {})
                    logger.info(f"成功删除无效代币 {token_symbol} ({chain}/{contract})")
                    logger.info(f"已删除代币信息 - 首次记录: {deleted_info.get('first_update')}, 最后更新: {deleted_info.get('latest_update')}")
                    
                    return {
                        "success": False, 
                        "deleted": True, 
                        "token_symbol": token_symbol,
                        "message": "代币在DEX上不存在，已从数据库中删除", 
                        "deleted_info": deleted_info,
                        "error": "未找到交易对"
                    }
                else:
                    logger.error(f"删除代币 {token_symbol} ({chain}/{contract}) 失败: {delete_result.get('error', '未知错误')}")
                    return {
                        "success": False, 
                        "error": "未找到交易对，尝试删除代币失败: " + delete_result.get('error', '未知错误')
                    }
            
            return {"error": "未找到交易对"}
        
        # 查找市值最高的池
        max_market_cap = 0
        max_liquidity = 0
        total_buys_1h = 0
        total_sells_1h = 0
        total_volume_1h = 0
        dex_screener_url = None
        price = None
        first_price = None
        symbol = None  # 初始化symbol变量
        holders_count = 0  # 初始化holders_count变量
        telegram_url = None  # 新增：初始化telegram_url
        twitter_url = None   # 新增：初始化twitter_url
        website_url = None   # 新增：初始化website_url
        
        for pair in pairs:
            # 获取代币符号（如果尚未获取）
            if not symbol and pair.get("baseToken") and "symbol" in pair.get("baseToken", {}):
                symbol = pair["baseToken"]["symbol"]
                
            # 获取市值数据
            market_cap = pair.get("marketCap", 0)
            if market_cap and float(market_cap) > max_market_cap:
                max_market_cap = float(market_cap)
                
            # 获取流动性数据
            liquidity = pair.get("liquidity", {}).get("usd", 0)
            if liquidity and float(liquidity) > max_liquidity:
                max_liquidity = float(liquidity)
                
            # 获取价格
            if not price and "priceUsd" in pair:
                price = float(pair["priceUsd"])

            # 获取首次价格
            if not first_price and "priceNative" in pair:
                first_price = float(pair["priceNative"])
                
            # 获取DEX Screener URL
            if not dex_screener_url:
                chain_path = pair.get("chainId", "").lower()
                pair_address = pair.get("pairAddress", "")
                if chain_path and pair_address:
                    dex_screener_url = f"https://dexscreener.com/{chain_path}/{pair_address}"
                
            # 获取交易数据
            if "txns" in pair and "h1" in pair["txns"]:
                txns_1h = pair["txns"]["h1"]
                buys = txns_1h.get("buys", 0)
                sells = txns_1h.get("sells", 0)
                
                # 累加交易数据
                total_buys_1h += buys
                total_sells_1h += sells
                
                # 计算1小时交易量
                if 'volume' in pair and 'h1' in pair['volume']:
                    volume_h1_data = pair['volume']['h1']
                    if 'USD' in volume_h1_data:
                        volume_1h = float(volume_h1_data['USD'])
                        total_volume_1h += volume_1h
            
            # 获取代币图像URL
            if not dex_screener_url and pair.get("info") and pair.get("info").get("imageUrl"):
                dex_screener_url = pair.get("info").get("imageUrl")
                logger.info(f"从DEX API获取到代币图像URL: {dex_screener_url}")
            
            # 新增：提取社交链接
            if pair.get("info") and pair["info"].get("websites"):
                for w in pair["info"]["websites"]:
                    url = w.get("url", "")
                    if not telegram_url and ("t.me/" in url or "telegram" in url):
                        telegram_url = url
                    elif not twitter_url and ("twitter.com/" in url or "x.com/" in url):
                        twitter_url = url
                    elif not website_url and (url.startswith("http") and not any(x in url for x in ["t.me/", "telegram", "twitter.com/", "x.com/"])):
                        website_url = url
        
        try:
            # 获取数据库适配器
            db_adapter = get_db_adapter()
            
            # 获取之前的市值以计算market_cap_1h
            prev_market_cap = None
            token = None  # 初始化token变量
            
            # 获取token信息
            token_result = await db_adapter.execute_query(
                'tokens',
                'select',
                filters={
                    'chain': chain,
                    'contract': contract
                }
            )
            if isinstance(token_result, list) and len(token_result) > 0:
                token = token_result[0]
                prev_market_cap = token.get('market_cap')
            
            # 获取当前时间
            current_time = datetime.now().isoformat()
            
            # 准备更新数据
            token_data = {
                'market_cap_1h': prev_market_cap,  # 当前值变为1小时前值
                'market_cap': max_market_cap,
                'market_cap_formatted': self._format_market_cap(max_market_cap),
                'liquidity': max_liquidity,
                'buys_1h': total_buys_1h,
                'sells_1h': total_sells_1h,
                'volume_1h': total_volume_1h,
                'price': price,
                'latest_update': current_time,
                'from_api': 'DEX'  # 标记数据来源
            }
            
            # 计算并添加涨跌幅数据
            if prev_market_cap is not None and max_market_cap is not None and prev_market_cap > 0 and max_market_cap > 0:
                change_pct = ((max_market_cap - prev_market_cap) / prev_market_cap) * 100
                token_data['change_pct_value'] = change_pct
                token_data['change_percentage'] = f"{'+' if change_pct > 0 else ''}{change_pct:.2f}%"
                logger.info(f"计算涨跌幅: {token_data['change_percentage']} (现在: {max_market_cap}, 1小时前: {prev_market_cap})")
            else:
                logger.info(f"无法计算涨跌幅: prev_market_cap={prev_market_cap}, max_market_cap={max_market_cap}")
            
            # 格式化交易量数据
            if total_volume_1h is not None and total_volume_1h > 0:
                if total_volume_1h >= 1000000:
                    volume_formatted = f"${total_volume_1h / 1000000:.2f}M"
                elif total_volume_1h >= 1000:
                    volume_formatted = f"${total_volume_1h / 1000:.2f}K"
                else:
                    volume_formatted = f"${total_volume_1h:.2f}"
                token_data['volume_1h_formatted'] = volume_formatted
            else:
                logger.info(f"交易量为空或为零: total_volume_1h={total_volume_1h}")
                token_data['volume_1h_formatted'] = '$0.00'
            
            # 如果有其他数据，也添加到更新数据中
            if dex_screener_url:
                token_data['dexscreener_url'] = dex_screener_url
                
            # 如果是首次设置价格，同时设置first_price
            if first_price:
                # 检查是否已有first_price
                if prev_market_cap is None:
                    token_data['first_price'] = first_price
            
            # 新增：写入社交链接
            if telegram_url:
                token_data['telegram_url'] = telegram_url
            if twitter_url:
                token_data['twitter_url'] = twitter_url
            if website_url:
                token_data['website_url'] = website_url
            
            # 更新数据库
            await db_adapter.execute_query(
                'tokens',
                'update',
                data=token_data,
                filters={
                    'chain': chain,
                    'contract': contract
                }
            )
            
            logger.info(f"成功更新代币 {chain}/{contract} 的综合数据")
            logger.info(f"市值: {max_market_cap}, 上一小时市值: {prev_market_cap}, 流动性: {max_liquidity}, 1小时买入: {total_buys_1h}, 1小时卖出: {total_sells_1h}, 1小时交易量: {total_volume_1h}")
            
            # 在返回结果中包含所有字段
            return {
                "success": True,
                "marketCap": max_market_cap,
                "marketCap1h": prev_market_cap,
                "liquidity": max_liquidity,
                "buys_1h": total_buys_1h,
                "sells_1h": total_sells_1h,
                "volume_1h": total_volume_1h,
                "price": price,
                "dexScreenerUrl": dex_screener_url,
                "telegram_url": telegram_url,
                "twitter_url": twitter_url,
                "website_url": website_url
            }
            
        except Exception as e:
            logger.error(f"更新代币综合数据时发生错误: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {"error": str(e)}


# 创建便捷函数，供其他模块使用
def update_token_market_data(session: Session, chain: str, contract: str) -> Dict[str, Any]:
    """
    更新单个代币的市值和流动性数据的便捷函数
    
    Args:
        session: 数据库会话
        chain: 区块链名称
        contract: 代币合约地址
        
    Returns:
        Dict: 包含更新结果的字典
    """
    updater = TokenMarketUpdater(session)
    return updater.update_token_market_data(chain, contract)

def update_all_tokens_market_data(session: Session, limit: int = 100) -> Dict[str, Any]:
    """全局函数：综合更新所有代币的市值、流动性和交易数据
    
    此函数在所有平台(Windows/Ubuntu/Linux)上均可正常运行
    
    Args:
        session: 数据库会话
        limit: 最大更新数量
        
    Returns:
        Dict: 包含更新结果的字典
    """
    updater = TokenMarketUpdater(session)
    
    results = {
        "total": 0,
        "success": 0,
        "failed": 0,
        "details": []
    }
    
    tokens = session.query(Token).limit(limit).all()
    results["total"] = len(tokens)
    
    # 使用异步运行
    import asyncio
    
    # 定义异步更新函数
    async def update_all_tokens_async():
        for token in tokens:
            try:
                # 调用异步更新函数
                result = await updater.update_token_market_and_txn_data(token.chain, token.contract)
                
                if "error" not in result:
                    results["success"] += 1
                else:
                    results["failed"] += 1
                    
                results["details"].append({
                    "chain": token.chain,
                    "symbol": token.token_symbol,
                    "contract": token.contract,
                    "result": "success" if "error" not in result else "failed",
                    "error": result.get("error"),
                    "marketCap": result.get("marketCap"),
                    "buys_1h": result.get("buys_1h"),
                    "sells_1h": result.get("sells_1h")
                })
                
                # 添加延迟避免API限制
                await asyncio.sleep(0.2)
                
            except Exception as e:
                results["failed"] += 1
                results["details"].append({
                    "chain": token.chain,
                    "symbol": token.token_symbol,
                    "contract": token.contract,
                    "result": "failed",
                    "error": str(e)
                })
                logger.error(f"综合更新代币 {token.chain}/{token.contract} 时发生错误: {str(e)}")
        
        return results
    
    # 平台兼容性处理
    # 只在Windows平台上设置特定的事件循环策略，Ubuntu/Linux平台使用默认策略
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # 执行异步任务
    return asyncio.run(update_all_tokens_async())

async def update_token_market_data_async(chain: str, contract: str, message_id: int = None, channel_id: int = None, risk_level: str = None, promotion_count: int = 1) -> Dict[str, Any]:
    """
    异步更新代币市值和流动性数据
    
    Args:
        chain: 链标识
        contract: 代币合约地址
        message_id: 消息ID
        channel_id: 频道ID
        risk_level: 风险等级
        promotion_count: 推广次数
        
    Returns:
        Dict[str, Any]: 包含更新结果的字典
    """
    # 只在函数开头生成一次当前时间
    current_time = datetime.now().isoformat()
    
    # 初始化结果
    result = {
        "success": False,
        "marketCap": 0,
        "marketCap1h": 0,
        "liquidity": 0,
        "price": 0,
        "dexScreenerUrl": None,
    }
    
    try:
        from src.api.dex_screener_api import get_token_pools
        from src.api.das_api import DASAPI
        from src.database.db_factory import get_db_adapter
        
        # 标准化链ID
        chain_id = _normalize_chain_id(chain)
        if not chain_id:
            logger.error(f"不支持的链ID: {chain}")
            return {"error": f"不支持的链ID: {chain}"}
        
        # 获取代币流动池
        pools_data = get_token_pools(chain_id, contract)
        
        # 如果API返回错误或返回空列表，尝试使用 DAS API
        if (isinstance(pools_data, dict) and "error" in pools_data) or (isinstance(pools_data, list) and len(pools_data) == 0):
            error_reason = pools_data.get("error", "未找到交易对") if isinstance(pools_data, dict) else "未找到交易对"
            logger.warning(f"DEX Screener API {error_reason}，尝试使用 DAS API")
            
            # 初始化 DAS API
            das_api = DASAPI()
            asset_data = das_api.get_asset(contract)
            
            if asset_data:
                # 转换 DAS API 数据为代币数据格式
                token_data = das_api.convert_to_token_data(asset_data, chain, contract, message_id)
                if token_data:
                    token_data['from_api'] = 'HELIUS'  # 标记数据来源
                    # 保存到数据库
                    db_adapter = get_db_adapter()
                    save_result = await db_adapter.save_token(token_data)
                    if save_result:
                        logger.info(f"使用 DAS API 数据成功保存代币: {chain}/{contract}")
                        return {
                            "success": True,
                            "marketCap": token_data.get('market_cap', 0),
                            "liquidity": token_data.get('liquidity', 0),
                            "price": token_data.get('price', 0),
                            "from_api": "HELIUS"
                        }
            
            # 如果 DAS API 也失败，判断 token 是否存在于 tokens 表
            logger.warning(f"无法从 DEX Screener 和 DAS API 获取数据，检查是否需要删除 tokens 表中的记录: {chain}/{contract}")
            db_adapter = get_db_adapter()
            token_result = await db_adapter.execute_query('tokens', 'select', filters={'chain': chain, 'contract': contract}, limit=1)
            if isinstance(token_result, list) and len(token_result) > 0:
                # token 存在，执行删除
                logger.info(f"token 已存在于 tokens 表，执行删除: {chain}/{contract}")
                from src.api.token_market_updater import delete_token_data
                delete_result = await delete_token_data(chain, contract, double_check=False)
                if delete_result.get('success'):
                    logger.info(f"成功删除无效代币 {chain}/{contract}，并记录到 hidden_tokens")
                else:
                    logger.error(f"删除代币 {chain}/{contract} 失败: {delete_result.get('error', '未知错误')}")
            # 无论是否存在，都插入 hidden_tokens
            hidden_token_data = {
                'chain': chain,
                'contract': contract,
                'message_id': message_id,
                'timestamp': current_time
            }
            await db_adapter.execute_query('hidden_tokens', 'insert', data=hidden_token_data)
            return {"error": f"无法获取代币数据，{error_reason}，已处理 hidden_tokens", "chain": chain, "contract": contract}
        
        # 处理API返回的数据
        # 根据API文档，token-pairs/v1/{chainId}/{tokenAddress}返回的是交易对数组
        pairs = pools_data
        
        # 添加API返回数据的详细日志
        # logger.info(f"DEX API原始返回数据: {pools_data}")
        
        # 初始化变量
        max_market_cap = 0
        max_liquidity = 0
        price = None
        first_price = None
        dex_screener_url = None
        symbol = None
        image_url = None
        buys_1h = 0
        sells_1h = 0
        volume_1h = 0
        holders_count = 0
        telegram_url = None  # 新增：初始化telegram_url
        twitter_url = None   # 新增：初始化twitter_url
        website_url = None   # 新增：初始化website_url
        
        # 从交易对中提取数据
        for pair in pairs:
            # 添加每个交易对的详细日志
            # logger.info(f"处理交易对数据: {pair}")
            
            # 尝试提取代币符号
            if not symbol and pair.get("baseToken"):
                baseToken = pair.get("baseToken", {})
                if baseToken and "symbol" in baseToken:
                    symbol = baseToken.get("symbol")
                    logger.info(f"从DEX API获取到代币符号: {symbol}")
            
            # 获取市值数据
            market_cap = pair.get("marketCap", 0)
            if market_cap and float(market_cap) > max_market_cap:
                max_market_cap = float(market_cap)
                logger.info(f"更新最大市值: {max_market_cap}")
                
            # 获取流动性数据
            liquidity = pair.get("liquidity", {}).get("usd", 0)
            if liquidity and float(liquidity) > max_liquidity:
                max_liquidity = float(liquidity)
                logger.info(f"更新最大流动性: {max_liquidity}")
                
            # 获取价格
            if not price and "priceUsd" in pair:
                price = float(pair["priceUsd"])
                logger.info(f"获取到价格: {price}")

            # 获取首次价格
            if not first_price and "priceNative" in pair:
                first_price = float(pair["priceNative"])
                
            # 获取DEX Screener URL
            if not dex_screener_url:
                chain_path = pair.get("chainId", "").lower()
                pair_address = pair.get("pairAddress", "")
                if chain_path and pair_address:
                    dex_screener_url = f"https://dexscreener.com/{chain_path}/{pair_address}"
            
            # 获取代币图像URL
            if not image_url and pair.get("info") and pair.get("info").get("imageUrl"):
                image_url = pair.get("info").get("imageUrl")
                logger.info(f"从DEX API获取到代币图像URL: {image_url}")
            
            # 获取交易数据
            if 'txns' in pair and 'h1' in pair['txns']:
                h1_data = pair['txns']['h1']
                buys_1h += h1_data.get('buys', 0)
                sells_1h += h1_data.get('sells', 0)
            
            # 获取交易量数据
            if 'volume' in pair and 'h1' in pair['volume']:
                volume_1h += float(pair['volume']['h1'] or 0)
            
            # 新增：提取社交链接
            if pair.get("info") and pair["info"].get("websites"):
                for w in pair["info"]["websites"]:
                    url = w.get("url", "")
                    if not telegram_url and ("t.me/" in url or "telegram" in url):
                        telegram_url = url
                    elif not twitter_url and ("twitter.com/" in url or "x.com/" in url):
                        twitter_url = url
                    elif not website_url and (url.startswith("http") and not any(x in url for x in ["t.me/", "telegram", "twitter.com/", "x.com/"])):
                        website_url = url
        
        # 在判断之前添加汇总日志
        logger.info(f"DEX API数据解析汇总:")
        logger.info(f"- 最大市值: {max_market_cap}")
        logger.info(f"- 价格: {price}")
        logger.info(f"- 代币符号: {symbol}")
        logger.info(f"- 最大流动性: {max_liquidity}")
        logger.info(f"- 1小时交易量: {volume_1h}")
        logger.info(f"- 1小时买入次数: {buys_1h}")
        logger.info(f"- 1小时卖出次数: {sells_1h}")
        
        # 获取数据库适配器
        db_adapter = get_db_adapter()
        token_result = await db_adapter.execute_query('tokens', 'select', filters={'chain': chain, 'contract': contract})
        
        # 初始化token变量
        token = None
        if isinstance(token_result, list) and len(token_result) > 0:
            token = token_result[0]
        
        # 获取市值和社区覆盖数量
        result["marketCap"] = max_market_cap
        result["liquidity"] = max_liquidity
        result["price"] = price
        result["dexScreenerUrl"] = dex_screener_url
        
        if token:
            # 现有代币，准备更新数据
            logger.info(f"更新现有代币: {chain}/{contract}")
            
            # 添加市值更新日志
            if max_market_cap > 0:
                logger.info(f"更新市值: {token.get('market_cap')} -> {max_market_cap}")
                # 计算市值变化百分比
                if token.get('market_cap') and token.get('market_cap') > 0:
                    change_pct = (max_market_cap - token.get('market_cap')) / token.get('market_cap') * 100
                    logger.info(f"市值变化: {change_pct:+.2f}%")
            
            # 准备更新数据
            token_data = {
                'chain': chain,
                'contract': contract,
                'market_cap_1h': token.get('market_cap'),  # 将当前市值设为1小时前市值
                'market_cap': max_market_cap,
                'market_cap_formatted': _format_market_cap(max_market_cap),
                'liquidity': max_liquidity,
                'price': price,
                'latest_update': current_time,
                'from_api': 'DEX'  # 标记数据来源
            }
            
            # 如果从DEX API获取到了代币符号，则更新数据库中的代币符号
            if symbol:
                token_data['token_symbol'] = symbol
                result['symbol'] = symbol
                logger.info(f"将使用DEX API获取的代币符号: {symbol} 更新数据库")
            else:
                # 如果没有获取到新符号，使用现有符号
                result['symbol'] = token.get('token_symbol')
            
            # 如果获取到了代币图像URL，则更新数据库
            if image_url:
                token_data['image_url'] = image_url
                result['image_url'] = image_url
                logger.info(f"将使用DEX API获取的代币图像URL更新数据库")
            else:
                # 如果没有获取到新图像URL，使用现有图像URL
                result['image_url'] = token.get('image_url')
                
            # 更新交易数据
            if buys_1h > 0:
                token_data['buys_1h'] = buys_1h
                result['buys_1h'] = buys_1h
            
            if sells_1h > 0:
                token_data['sells_1h'] = sells_1h
                result['sells_1h'] = sells_1h
            
            if volume_1h > 0:
                token_data['volume_1h'] = volume_1h
                result['volume_1h'] = volume_1h
            
            # 更新持有者数量
            if holders_count > 0:
                token_data['holders_count'] = holders_count
                result['holders_count'] = holders_count
            
            # 如果是首次设置价格，同时设置first_price
            if token.get('first_price') is None and first_price:
                token_data['first_price'] = first_price
                logger.info(f"首次设置first_price: {first_price}")
            
            # 更新DEX Screener URL
            if dex_screener_url:
                token_data['dexscreener_url'] = dex_screener_url
            
            # 新增：写入社交链接
            if telegram_url:
                token_data['telegram_url'] = telegram_url
                result['telegram_url'] = telegram_url
            if twitter_url:
                token_data['twitter_url'] = twitter_url
                result['twitter_url'] = twitter_url
            if website_url:
                token_data['website_url'] = website_url
                result['website_url'] = website_url
            
            # 更新数据库
            update_result = await db_adapter.execute_query('tokens', 'update', data=token_data, filters={'chain': chain, 'contract': contract})
            
            if isinstance(update_result, dict) and update_result.get('error'):
                logger.error(f"更新代币数据失败: {update_result.get('error')}")
                return {"error": f"更新代币数据失败: {update_result.get('error')}"}
            
            result["success"] = True
            return result
            
        else:
            # 数据库中未找到该代币，但已成功从API获取到数据
            if max_market_cap > 0 or price is not None or symbol:
                logger.info(f"数据库中未找到代币，但已从API获取到数据: {chain}/{contract}")
                
                # 尝试从DAS API获取持有者数量（仅对SOL链代币）
                holders_count = 0
                if chain == 'SOL':
                    try:
                        from src.api.das_api import get_token_holders_count
                        # 使用线程池执行同步操作，避免阻塞
                        import concurrent.futures
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            future = executor.submit(get_token_holders_count, contract)
                            api_holders_count = future.result()
                            if api_holders_count is not None:
                                holders_count = api_holders_count
                                logger.info(f"从DAS API获取到持有者数量: {holders_count}")
                    except Exception as e:
                        logger.warning(f"尝试获取持有者数量时出错: {str(e)}")
                
                # 返回明确的错误消息，告知数据库中未找到该代币但API数据有效
                result["error"] = f"数据库中未找到该代币 {chain}/{contract}，但已成功获取API数据"
                # 添加必要的字段用于创建新代币
                result["chain"] = chain
                result["contract"] = contract
                result["symbol"] = symbol or ''
                result["marketCap"] = max_market_cap
                result["price"] = price
                result["liquidity"] = max_liquidity
                result["dexScreenerUrl"] = dex_screener_url
                result["image_url"] = image_url
                result["first_price"] = first_price
                result["volume_1h"] = volume_1h
                result["buys_1h"] = buys_1h
                result["sells_1h"] = sells_1h
                result["holders_count"] = holders_count  # 添加持有者数量
                if telegram_url:
                    result["telegram_url"] = telegram_url
                if twitter_url:
                    result["twitter_url"] = twitter_url
                if website_url:
                    result["website_url"] = website_url
                return result
            else:
                # 数据库中未找到该代币，且API返回的数据也不足以创建新代币
                logger.warning(f"数据库中未找到代币且API数据不足: {chain}/{contract}")
                # 插入到hidden_tokens表
                hidden_token_data = {
                    'chain': chain,
                    'contract': contract,
                    'message_id': message_id,
                    'timestamp': current_time
                }
                await db_adapter.execute_query('hidden_tokens', 'insert', data=hidden_token_data)
                return {"error": f"数据库中未找到代币 {chain}/{contract}，且API数据不足以创建新token", "chain": chain, "contract": contract}
            
    except Exception as e:
        logger.error(f"更新代币数据时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {"error": f"更新代币数据时出错: {str(e)}"}

def _format_market_cap(market_cap: float) -> str:
    """格式化市值显示
    
    Args:
        market_cap: 市值数字
        
    Returns:
        str: 格式化后的市值字符串
    """
    if market_cap >= 1000000000:  # 十亿 (B)
        return f"${market_cap/1000000000:.2f}B"
    elif market_cap >= 1000000:   # 百万 (M)
        return f"${market_cap/1000000:.2f}M"
    elif market_cap >= 1000:      # 千 (K)
        return f"${market_cap/1000:.2f}K"
    return f"${market_cap:.2f}"

def _normalize_chain_id(chain: str) -> Optional[str]:
    """标准化链ID到DexScreener API支持的格式
    
    Args:
        chain: 链ID，支持大写简写(如ETH)或小写全称(如ethereum)
        
    Returns:
        Optional[str]: 标准化后的链ID，如果不支持则返回None
    """
    chain_map = {
        "SOL": "solana",
        "ETH": "ethereum",
        "BSC": "bsc",
        "AVAX": "avalanche",
        "MATIC": "polygon",
        "ARB": "arbitrum",
        "OP": "optimism",
        "BASE": "base",
        "ZK": "zksync",
        "TON": "ton"
    }
    
    # 支持的小写全称列表
    valid_chain_ids = {
        "solana", "ethereum", "bsc", "avalanche", "polygon", 
        "arbitrum", "optimism", "base", "zksync", "ton"
    }
    
    # 检查是否已经是小写全称格式
    if chain.lower() in valid_chain_ids:
        return chain.lower()
    
    # 尝试将大写简写转换为小写全称
    return chain_map.get(chain.upper())

# 添加被删除的函数

def update_tokens_by_symbols(session: Session, symbols: List[str]) -> Dict[str, Any]:
    """全局函数：根据代币符号批量更新代币市值
    
    此函数在所有平台(Windows/Ubuntu/Linux)上均可正常运行
    
    Args:
        session: 数据库会话
        symbols: 代币符号列表
    
    Returns:
        Dict: 包含更新结果的字典
    """
    updater = TokenMarketUpdater(session)
    return updater.update_tokens_by_symbols(symbols)

def update_token_txn_data(session: Session, chain: str, contract: str) -> Dict[str, Any]:
    """全局函数：更新单个代币的1小时交易数据
    
    此函数在所有平台(Windows/Ubuntu/Linux)上均可正常运行
    
    Args:
        session: 数据库会话
        chain: 区块链名称
        contract: 代币合约地址
        
    Returns:
        Dict: 包含更新结果的字典
    """
    updater = TokenMarketUpdater(session)
    return updater.update_token_txn_data(chain, contract)

def update_all_tokens_txn_data(session: Session, limit: int = 100) -> Dict[str, Any]:
    """全局函数：批量更新所有代币的1小时交易数据
    
    此函数在所有平台(Windows/Ubuntu/Linux)上均可正常运行
    
    Args:
        session: 数据库会话
        limit: 最大更新数量
        
    Returns:
        Dict: 包含更新结果的字典
    """
    updater = TokenMarketUpdater(session)
    return updater.update_all_tokens_txn_data(limit=limit)

# 添加同步版本，用于兼容现有代码
def update_token_market_and_txn_data(session: Session, chain: str, contract: str) -> Dict[str, Any]:
    """全局函数：综合更新单个代币的市值、流动性和交易数据（同步版本）
    
    这是一个同步包装器，内部通过asyncio.run调用异步方法
    可在Windows和Ubuntu/Linux平台上正常运行
    
    Args:
        session: 数据库会话
        chain: 区块链名称
        contract: 代币合约地址
        
    Returns:
        Dict: 包含更新结果的字典
    """
    import asyncio
    updater = TokenMarketUpdater(session)
    
    # 平台兼容性处理
    # 只在Windows平台上设置特定的事件循环策略
    # Ubuntu/Linux平台使用默认策略即可
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # 通过asyncio.run调用异步方法
    return asyncio.run(updater.update_token_market_and_txn_data(chain, contract))

# 添加异步版本，用于新代码
async def update_token_market_and_txn_data_async(session: Session, chain: str, contract: str) -> Dict[str, Any]:
    """全局函数：综合更新单个代币的市值、流动性和交易数据（异步版本）
    
    异步函数可直接在异步上下文中使用，无需考虑平台差异
    
    Args:
        session: 数据库会话
        chain: 区块链名称
        contract: 代币合约地址
        
    Returns:
        Dict: 包含更新结果的字典
    """
    updater = TokenMarketUpdater(session)
    return await updater.update_token_market_and_txn_data(chain, contract)

async def delete_token_data(chain: str, contract: str, double_check: bool = True) -> Dict[str, Any]:
    """
    删除代币及其相关数据
    
    Args:
        chain: 链标识
        contract: 代币合约地址
        double_check: 是否在删除前再次检查DEX API，确保代币确实不存在
        
    Returns:
        Dict[str, Any]: 包含删除结果的字典
    """
    result = {"success": False, "error": None}
    
    try:
        logger.info(f"开始处理代币 {chain}/{contract} 的删除请求")
        
        # 如果需要再次确认，先检查DEX API
        if double_check:
            try:
                from src.api.dex_screener_api import get_token_pools
                # 标准化链ID
                chain_id = _normalize_chain_id(chain)
                if not chain_id:
                    logger.error(f"不支持的链ID: {chain}，无法进行二次验证")
                else:
                    logger.info(f"二次验证: 从DEX API检查代币 {chain}/{contract} 是否存在")
                    pools_data = get_token_pools(chain_id, contract)
                    
                    # 验证API返回结果
                    if isinstance(pools_data, dict) and "error" in pools_data:
                        # API返回了错误，检查错误类型
                        error_msg = str(pools_data.get("error", "")).lower()
                        if "not found" in error_msg or "no pools found" in error_msg:
                            logger.info(f"二次验证确认: 代币 {chain}/{contract} 在DEX上不存在")
                        else:
                            # 其他类型的错误，可能是API限制等
                            logger.warning(f"二次验证时遇到API错误: {error_msg}，谨慎处理")
                            # 如果是API限制或其他类型的错误，我们仍然继续删除流程
                    elif isinstance(pools_data, list):
                        # 处理列表类型的返回结果
                        if len(pools_data) > 0:
                            # 如果返回非空列表，说明代币存在
                            logger.warning(f"代币 {chain}/{contract} 在DEX上仍然存在，取消删除操作")
                            result["error"] = "代币在DEX上仍然存在，取消删除操作"
                            result["pools_data"] = pools_data
                            return result
                        else:
                            # 明确处理空列表情况，确认代币不存在
                            logger.info(f"二次验证确认: 代币 {chain}/{contract} 在DEX上不存在（API返回空列表）")
                    else:
                        # 处理其他未预期的返回类型
                        logger.warning(f"DEX API返回了未预期的数据类型: {type(pools_data).__name__}, 谨慎处理")
            except Exception as e:
                logger.error(f"二次验证时出错: {str(e)}")
                # 发生错误时，我们继续删除流程，但记录警告
                logger.warning("由于验证错误，将继续删除操作，但请注意可能存在风险")
        
        logger.info(f"开始删除代币 {chain}/{contract} 及其相关数据")
        
        # 获取数据库适配器
        from src.database.db_factory import get_db_adapter
        db_adapter = get_db_adapter()
        
        # 获取代币数据，记录删除前的状态以便日志记录
        token_data = None
        try:
            token_result = await db_adapter.execute_query(
                'tokens',
                'select',
                filters={'chain': chain, 'contract': contract},
                limit=1
            )
            if isinstance(token_result, list) and len(token_result) > 0:
                token_data = token_result[0]
                logger.info(f"删除前记录代币数据: 符号={token_data.get('token_symbol')}, "
                           f"首次更新={token_data.get('first_update')}, "
                           f"最近更新={token_data.get('latest_update')}")
        except Exception as e:
            logger.error(f"获取代币数据时出错: {str(e)}")
            # 继续执行，不中断流程
        
        # 删除代币标记数据
        try:
            mark_result = await db_adapter.execute_query(
                'tokens_mark',
                'delete',
                filters={'chain': chain, 'contract': contract}
            )
            logger.info(f"已删除代币 {chain}/{contract} 的标记数据")
        except Exception as e:
            logger.error(f"删除代币标记数据时出错: {str(e)}")
            # 继续执行，不中断流程
        
        # 最后删除代币主记录
        try:
            token_delete_result = await db_adapter.execute_query(
                'tokens',
                'delete',
                filters={'chain': chain, 'contract': contract}
            )
            logger.info(f"已删除代币 {chain}/{contract} 的主记录")
            result["success"] = True
            
            # 添加额外的删除信息
            if token_data:
                result["token_symbol"] = token_data.get("token_symbol")
                result["deleted_token_data"] = {
                    "symbol": token_data.get("token_symbol"),
                    "first_update": token_data.get("first_update"),
                    "latest_update": token_data.get("latest_update"),
                    "market_cap": token_data.get("market_cap"),
                    "price": token_data.get("price")
                }
        except Exception as e:
            logger.error(f"删除代币主记录时出错: {str(e)}")
            result["error"] = f"删除代币主记录失败: {str(e)}"
            return result
        
        # 所有删除操作完成
        logger.info(f"成功删除代币 {chain}/{contract} 及其所有相关数据")
        result["success"] = True
        return result
        
    except Exception as e:
        logger.error(f"删除代币数据时出错: {str(e)}")
        result["error"] = str(e)
        return result 