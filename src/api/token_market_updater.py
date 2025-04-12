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
from datetime import datetime

from src.api.dex_screener_api import get_token_pools, DexScreenerAPI
from src.database.models import Token
from src.utils.error_handler import retry, safe_execute
from src.database.db_factory import get_db_adapter

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
        if isinstance(pools_data, dict) and "pairs" in pools_data:
            pairs = pools_data.get("pairs", [])
        else:
            pairs = pools_data
            
        if not pairs:
            logger.warning(f"未找到代币 {chain}/{contract} 的交易对")
            return {"error": "未找到代币交易对"}
            
        # 查找市值最高的池
        max_market_cap = 0
        max_liquidity = 0
        dex_screener_url = None
        price = None
        first_price = None
        
        for pair in pairs:
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
                
                self.session.commit()
                logger.info(f"成功更新代币 {chain}/{contract} 的市值和流动性数据")
                logger.info(f"市值: {max_market_cap}, 上一小时市值: {token.market_cap_1h}, 流动性: {max_liquidity}")
                
                return {
                    "success": True,
                    "marketCap": max_market_cap,
                    "marketCap1h": token.market_cap_1h,
                    "liquidity": max_liquidity,
                    "price": price,
                    "dexScreenerUrl": dex_screener_url
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
        if isinstance(pools_data, dict) and "pairs" in pools_data:
            pairs = pools_data.get("pairs", [])
        else:
            pairs = pools_data
            
        if not pairs:
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
        """
        将内部链名称标准化为DexScreener API中使用的链ID
        
        Args:
            chain: 内部链名称
            
        Returns:
            Optional[str]: DexScreener API链ID或None（如果不支持）
        """
        chain_mapping = {
            "SOL": "solana",
            "ETH": "ethereum",
            "BSC": "bsc",
            "MATIC": "polygon",
            "AVAX": "avalanche",
            "ARB": "arbitrum",
            "OP": "optimism",
            "FTM": "fantom",
            "BASE": "base",
            "ZK": "zksync",
            "CELO": "celo",
            "TRX": "tron",
            "TON": "ton"
        }
        
        return chain_mapping.get(chain.upper())
    
    @retry(max_retries=3, delay=1.0, exceptions=(Exception,))
    def update_token_market_and_txn_data(self, chain: str, contract: str) -> Dict[str, Any]:
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
        if isinstance(pools_data, dict) and "pairs" in pools_data:
            pairs = pools_data.get("pairs", [])
        else:
            pairs = pools_data
            
        if not pairs:
            logger.warning(f"未找到代币 {chain}/{contract} 的交易对")
            return {"error": "未找到代币交易对"}
            
        # 查找市值最高的池
        max_market_cap = 0
        max_liquidity = 0
        total_buys_1h = 0
        total_sells_1h = 0
        total_volume_1h = 0
        dex_screener_url = None
        price = None
        first_price = None
        
        for pair in pairs:
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
        
        try:
            # 获取数据库适配器
            db_adapter = get_db_adapter()
            
            # 使用异步运行
            import asyncio
            
            # 获取之前的市值以计算market_cap_1h
            prev_market_cap = None
            
            async def get_prev_market_cap():
                token_result = await db_adapter.execute_query(
                    'tokens',
                    'select',
                    filters={
                        'chain': chain,
                        'contract': contract
                    }
                )
                if isinstance(token_result, list) and len(token_result) > 0:
                    return token_result[0].get('market_cap')
                return None
            
            prev_market_cap = asyncio.run(get_prev_market_cap())
            
            # 准备更新数据
            token_data = {
                'market_cap_1h': prev_market_cap,  # 当前值变为1小时前值
                'market_cap': max_market_cap,
                'market_cap_formatted': self._format_market_cap(max_market_cap),
                'liquidity': max_liquidity,
                'buys_1h': total_buys_1h,
                'sells_1h': total_sells_1h,
                'volume_1h': total_volume_1h,
                'price': price
            }
            
            # 如果有其他数据，也添加到更新数据中
            if dex_screener_url:
                token_data['dexscreener_url'] = dex_screener_url
                
            # 如果是首次设置价格，同时设置first_price
            if first_price:
                # 检查是否已有first_price
                if prev_market_cap is None:
                    token_data['first_price'] = first_price
            
            # 更新数据库
            async def update_token_data():
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
            
            logger.info(f"成功更新代币 {chain}/{contract} 的综合数据")
            logger.info(f"市值: {max_market_cap}, 上一小时市值: {prev_market_cap}, 流动性: {max_liquidity}, 1小时买入: {total_buys_1h}, 1小时卖出: {total_sells_1h}, 1小时交易量: {total_volume_1h}")
            
            return {
                "success": True,
                "marketCap": max_market_cap,
                "marketCap1h": prev_market_cap,
                "liquidity": max_liquidity,
                "buys_1h": total_buys_1h,
                "sells_1h": total_sells_1h,
                "volume_1h": total_volume_1h,
                "price": price,
                "dexScreenerUrl": dex_screener_url
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
    """
    批量更新所有代币的市值和流动性数据的便捷函数
    
    Args:
        session: 数据库会话
        limit: 最大更新数量
        
    Returns:
        Dict: 包含更新结果的字典
    """
    updater = TokenMarketUpdater(session)
    return updater.update_all_tokens(limit=limit)

def update_tokens_by_symbols(session: Session, symbols: List[str]) -> Dict[str, Any]:
    """全局函数：根据代币符号批量更新代币市值"""
    updater = TokenMarketUpdater(session)
    return updater.update_tokens_by_symbols(symbols)

def update_token_txn_data(session: Session, chain: str, contract: str) -> Dict[str, Any]:
    """全局函数：更新单个代币的1小时交易数据"""
    updater = TokenMarketUpdater(session)
    return updater.update_token_txn_data(chain, contract)

def update_all_tokens_txn_data(session: Session, limit: int = 100) -> Dict[str, Any]:
    """全局函数：批量更新所有代币的1小时交易数据"""
    updater = TokenMarketUpdater(session)
    return updater.update_all_tokens_txn_data(limit=limit)

def update_token_market_and_txn_data(session: Session, chain: str, contract: str) -> Dict[str, Any]:
    """全局函数：综合更新单个代币的市值、流动性和交易数据"""
    updater = TokenMarketUpdater(session)
    return updater.update_token_market_and_txn_data(chain, contract)

def update_all_tokens_market_and_txn_data(session: Session, limit: int = 100) -> Dict[str, Any]:
    """全局函数：综合更新所有代币的市值、流动性和交易数据"""
    updater = TokenMarketUpdater(session)
    
    results = {
        "total": 0,
        "success": 0,
        "failed": 0,
        "details": []
    }
    
    tokens = session.query(Token).limit(limit).all()
    results["total"] = len(tokens)
    
    for token in tokens:
        try:
            result = updater.update_token_market_and_txn_data(token.chain, token.contract)
            
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
            logger.error(f"综合更新代币 {token.chain}/{token.contract} 时发生错误: {str(e)}")
    
    return results

async def update_token_market_data_async(chain: str, contract: str, message_id: int = None, channel_id: int = None, risk_level: str = None, promotion_count: int = 1) -> Dict[str, Any]:
    """
    异步更新单个代币的市值和流动性数据，使用Supabase适配器
    
    Args:
        chain: 区块链名称
        contract: 代币合约地址
        message_id: 消息ID，可选
        channel_id: 频道ID，可选
        risk_level: 风险等级，可选
        promotion_count: 推广计数，默认为1
        
    Returns:
        Dict: 包含更新结果的字典，包括marketCap和liquidity
              注意：返回值中的is_new字段仅用于API响应，不存储在数据库中
    """
    logger.info(f"开始异步更新代币 {chain}/{contract} 的市值和流动性数据")
    
    # 获取数据库适配器
    db_adapter = get_db_adapter()
    
    # 标准化链ID
    chain_id = _normalize_chain_id(chain)
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
    if isinstance(pools_data, dict) and "pairs" in pools_data:
        pairs = pools_data.get("pairs", [])
    else:
        pairs = pools_data
        
    if not pairs:
        logger.warning(f"未找到代币 {chain}/{contract} 的交易对")
        return {"error": "未找到代币交易对"}
        
    # 查找市值最高的池
    max_market_cap = 0
    max_liquidity = 0
    dex_screener_url = None
    price = None
    first_price = None
    symbol = None  # 添加代币符号变量
    image_url = None  # 添加代币图像URL变量
    
    for pair in pairs:
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
        
        # 获取代币图像URL
        if not image_url and pair.get("info") and pair.get("info").get("imageUrl"):
            image_url = pair.get("info").get("imageUrl")
            logger.info(f"从DEX API获取到代币图像URL: {image_url}")
    
    # 获取目标代币
    token = await db_adapter.get_token_by_contract(chain, contract)
    
    if token:
        # 更新代币数据
        try:
            # 准备更新数据
            update_data = {
                'market_cap_1h': token.get('market_cap'),
                'market_cap': max_market_cap,
                'market_cap_formatted': _format_market_cap(max_market_cap),
                'liquidity': max_liquidity,
                'price': price
            }
            
            # 如果从DEX API获取到了代币符号，则更新数据库中的代币符号
            if symbol:
                update_data['token_symbol'] = symbol
                logger.info(f"将使用DEX API获取的代币符号: {symbol} 更新数据库")
            
            # 如果获取到了代币图像URL，则更新数据库
            if image_url:
                update_data['image_url'] = image_url
                logger.info(f"将使用DEX API获取的代币图像URL更新数据库")
            
            # 如果是首次设置价格，同时设置first_price
            if token.get('first_price') is None:
                update_data['first_price'] = first_price
            
            if dex_screener_url:
                update_data['dexscreener_url'] = dex_screener_url
                
            # 添加新增字段
            # 仅当字段为空时才设置message_id
            if message_id and not token.get('message_id'):
                update_data['message_id'] = message_id
                
            # 仅当字段为空时才设置first_market_cap
            if max_market_cap > 0 and not token.get('first_market_cap'):
                update_data['first_market_cap'] = max_market_cap
                
            # 累加promotion_count
            if promotion_count > 0:
                current_promotion_count = token.get('promotion_count', 0) or 0
                update_data['promotion_count'] = current_promotion_count + promotion_count
                
            # 仅当字段为空时才设置channel_id
            if channel_id and not token.get('channel_id'):
                update_data['channel_id'] = channel_id
                
            # 仅当字段为空时才设置risk_level
            if risk_level and not token.get('risk_level'):
                update_data['risk_level'] = risk_level
            
            # 执行更新
            filters = {'chain': chain, 'contract': contract}
            await db_adapter.execute_query('tokens', 'update', update_data, filters)
            
            logger.info(f"成功更新代币 {chain}/{contract} 的市值和流动性数据")
            logger.info(f"市值: {max_market_cap}, 上一小时市值: {token.get('market_cap')}, 流动性: {max_liquidity}")
            
            # 在返回结果中包含代币符号和图像URL
            return {
                "success": True,
                "marketCap": max_market_cap,
                "marketCap1h": token.get('market_cap'),
                "liquidity": max_liquidity,
                "price": price,
                "dexScreenerUrl": dex_screener_url,
                "symbol": symbol,  # 添加代币符号到返回结果中
                "image_url": image_url,  # 添加代币图像URL到返回结果中
                "message_id": update_data.get('message_id') or token.get('message_id'),
                "first_market_cap": update_data.get('first_market_cap') or token.get('first_market_cap'),
                "promotion_count": update_data.get('promotion_count') or token.get('promotion_count', 0),
                "channel_id": update_data.get('channel_id') or token.get('channel_id'),
                "risk_level": update_data.get('risk_level') or token.get('risk_level')
            }
            
        except Exception as e:
            logger.error(f"更新代币数据时发生错误: {str(e)}")
            return {"error": str(e)}
    else:
        # 如果数据库中未找到代币，创建新的代币记录
        try:
            # 基本检查，确保我们有必要的信息来创建新记录
            if not symbol:
                logger.warning(f"从DEX API未能获取到代币符号，将使用合约地址前8位作为临时符号")
                # 使用合约地址的前8位作为临时符号
                symbol = contract[:8] if len(contract) > 8 else contract
            
            # 获取当前时间格式化为字符串
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 准备新的代币数据 - 注意：不包含is_new字段，因为它不应该存储在数据库中
            new_token_data = {
                'chain': chain,
                'contract': contract,
                'token_symbol': symbol,
                'market_cap': max_market_cap,
                'market_cap_formatted': _format_market_cap(max_market_cap),
                'liquidity': max_liquidity,
                'price': price,
                'first_price': first_price,
                'latest_update': current_time,
                'first_update': current_time,
                'dexscreener_url': dex_screener_url,
                # 添加新增字段
                'message_id': message_id,
                'first_market_cap': max_market_cap,  # 首次发现的市值就是first_market_cap
                'promotion_count': promotion_count,
                'channel_id': channel_id,
                'risk_level': risk_level
            }
            
            # 保存新代币记录
            result = await db_adapter.execute_query('tokens', 'insert', new_token_data)
            
            if not isinstance(result, dict) or not result.get('error'):
                logger.info(f"成功创建新代币记录 {chain}/{contract}")
                
                # 返回成功结果，is_new字段仅用于API响应，不存储在数据库中
                return {
                    "success": True,
                    "marketCap": max_market_cap,
                    "marketCap1h": 0,  # 新代币没有历史市值
                    "liquidity": max_liquidity,
                    "price": price,
                    "dexScreenerUrl": dex_screener_url,
                    "symbol": symbol,
                    "is_new": True,  # 标记为新代币，仅用于API响应，不存储在数据库中
                    "message_id": message_id,
                    "first_market_cap": max_market_cap,
                    "promotion_count": promotion_count,
                    "channel_id": channel_id,
                    "risk_level": risk_level
                }
            else:
                logger.error(f"创建新代币记录失败: {result.get('error')}")
                return {"error": f"创建新代币记录失败: {result.get('error')}"}
            
        except Exception as e:
            logger.error(f"创建新代币记录时发生错误: {str(e)}")
            return {"error": f"创建新代币记录时发生错误: {str(e)}"}

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
        chain: 链ID
        
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
    
    return chain_map.get(chain.upper())

async def update_token_market_and_txn_data_async(chain: str, contract: str) -> Dict[str, Any]:
    """
    异步更新代币的市场和交易数据
    
    Args:
        chain: 区块链名称
        contract: 代币合约地址
        
    Returns:
        Dict: 包含更新结果的字典
    """
    logger.info(f"开始异步综合更新代币 {chain}/{contract} 的市场和交易数据")
    
    try:
        # 获取数据库适配器
        db_adapter = get_db_adapter()
        
        # 标准化链ID
        chain_id = _normalize_chain_id(chain)
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
        if isinstance(pools_data, dict) and "pairs" in pools_data:
            pairs = pools_data.get("pairs", [])
        else:
            pairs = pools_data
            
        if not pairs:
            logger.warning(f"未找到代币 {chain}/{contract} 的交易对")
            return {"error": "未找到代币交易对"}
            
        # 查找市值最高的池
        max_market_cap = 0
        max_liquidity = 0
        dex_screener_url = None
        price = None
        first_price = None
        symbol = None
        buys_1h = 0
        sells_1h = 0
        volume_1h = 0
        image_url = None  # 添加代币图像URL变量
        
        # 记录找到的交易对数量
        logger.info(f"找到 {len(pairs)} 个交易对")
        
        for pair in pairs:
            logger.debug(f"处理交易对: {pair.get('pairAddress')}")
            
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
            
            # 获取代币图像URL
            if not image_url and pair.get("info") and pair.get("info").get("imageUrl"):
                image_url = pair.get("info").get("imageUrl")
                logger.info(f"从DEX API获取到代币图像URL: {image_url}")
            
            # 累计1小时交易数据
            if 'txns' in pair and 'h1' in pair['txns']:
                h1_data = pair['txns']['h1']
                buys_1h += h1_data.get('buys', 0)
                sells_1h += h1_data.get('sells', 0)
                
            # 累计1小时交易量
            if 'volume' in pair and 'h1' in pair['volume']:
                volume_h1_data = pair['volume']['h1']
                if 'USD' in volume_h1_data:
                    volume_1h += float(volume_h1_data['USD'])
        
        # 记录提取的数据
        logger.info(f"市值: {max_market_cap}, 流动性: {max_liquidity}, 价格: {price}")
        logger.info(f"1小时交易: 买入 {buys_1h}, 卖出 {sells_1h}, 交易量 ${volume_1h}")
        
        # 获取目标代币
        token = await db_adapter.get_token_by_contract(chain, contract)
        
        if token:
            # 更新代币数据
            try:
                # 准备更新数据
                update_data = {
                    'market_cap_1h': token.get('market_cap', 0),
                    'market_cap': max_market_cap,
                    'market_cap_formatted': _format_market_cap(max_market_cap),
                    'liquidity': max_liquidity,
                    'price': price,
                    'buys_1h': buys_1h,
                    'sells_1h': sells_1h,
                    'volume_1h': volume_1h
                }
                
                # 如果从DEX API获取到了代币符号，则更新数据库中的代币符号
                if symbol:
                    update_data['token_symbol'] = symbol
                
                # 如果获取到了代币图像URL，则更新数据库
                if image_url:
                    update_data['image_url'] = image_url
                    logger.info(f"将使用DEX API获取的代币图像URL更新数据库")
                
                # 如果是首次设置价格，同时设置first_price
                if token.get('first_price') is None:
                    update_data['first_price'] = first_price
                
                if dex_screener_url:
                    update_data['dexscreener_url'] = dex_screener_url
                
                # 执行更新
                filters = {'chain': chain, 'contract': contract}
                await db_adapter.execute_query('tokens', 'update', update_data, filters)
                
                logger.info(f"成功更新代币 {chain}/{contract} 的市场和交易数据")
                
                # 返回结果
                return {
                    "success": True,
                    "marketCap": max_market_cap,
                    "marketCap1h": token.get('market_cap', 0),
                    "liquidity": max_liquidity,
                    "price": price,
                    "dexScreenerUrl": dex_screener_url,
                    "symbol": symbol,
                    "image_url": image_url,  # 添加代币图像URL到返回结果中
                    "buys_1h": buys_1h,
                    "sells_1h": sells_1h,
                    "volume_1h": volume_1h
                }
                
            except Exception as e:
                logger.error(f"更新代币数据时发生错误: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                return {"error": str(e)}
                
        else:
            # 如果数据库中未找到代币，创建新的代币记录
            try:
                # 基本检查，确保我们有必要的信息来创建新记录
                if not symbol:
                    logger.warning(f"从DEX API未能获取到代币符号，将使用合约地址前8位作为临时符号")
                    # 使用合约地址的前8位作为临时符号
                    symbol = contract[:8] if len(contract) > 8 else contract
                
                # 获取当前时间格式化为字符串
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 准备新的代币数据
                new_token_data = {
                    'chain': chain,
                    'contract': contract,
                    'token_symbol': symbol,
                    'market_cap': max_market_cap,
                    'market_cap_formatted': _format_market_cap(max_market_cap),
                    'liquidity': max_liquidity,
                    'price': price,
                    'first_price': first_price,
                    'latest_update': current_time,
                    'first_update': current_time,
                    'dexscreener_url': dex_screener_url,
                    'buys_1h': buys_1h,
                    'sells_1h': sells_1h,
                    'volume_1h': volume_1h,
                    'market_cap_1h': 0,  # 首次创建没有历史市值
                    'first_market_cap': max_market_cap,  # 首次发现的市值就是first_market_cap
                    'image_url': image_url  # 添加代币图像URL
                }
                
                # 保存新代币记录
                result = await db_adapter.execute_query('tokens', 'insert', new_token_data)
                
                if not isinstance(result, dict) or not result.get('error'):
                    logger.info(f"成功创建新代币记录 {chain}/{contract}")
                    
                    # 返回成功结果
                    return {
                        "success": True,
                        "is_new": True,  # 标记为新代币
                        "marketCap": max_market_cap,
                        "marketCap1h": 0,  # 新代币没有历史市值
                        "liquidity": max_liquidity,
                        "price": price,
                        "dexScreenerUrl": dex_screener_url,
                        "symbol": symbol,
                        "image_url": image_url,  # 添加代币图像URL到返回结果中
                        "buys_1h": buys_1h,
                        "sells_1h": sells_1h,
                        "volume_1h": volume_1h
                    }
                else:
                    logger.error(f"创建新代币记录失败: {result.get('error')}")
                    return {"error": f"创建新代币记录失败: {result.get('error')}"}
                
            except Exception as e:
                logger.error(f"创建新代币记录时发生错误: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                return {"error": f"创建新代币记录时发生错误: {str(e)}"}
                
    except Exception as e:
        logger.error(f"综合更新代币数据时发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {"error": str(e)} 