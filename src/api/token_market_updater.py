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

from src.api.dex_screener_api import get_token_pools, DexScreenerAPI
from src.database.models import Token
from src.utils.error_handler import retry, safe_execute

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
        更新单个代币的1小时买入卖出交易数据
        
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
        
        # 汇总所有池的1小时交易数据
        total_buys_1h = 0
        total_sells_1h = 0
        
        for pair in pairs:
            # 提取txns数据
            txns = pair.get("txns", {})
            
            # 提取h1数据（1小时交易数据）
            h1_data = txns.get("h1", {})
            if h1_data:
                buys = h1_data.get("buys", 0)
                sells = h1_data.get("sells", 0)
                
                # 累加交易数据
                total_buys_1h += buys
                total_sells_1h += sells
        
        # 获取目标代币
        token = self.session.query(Token).filter(
            Token.chain == chain,
            Token.contract == contract
        ).first()
        
        if token:
            # 更新代币数据
            try:
                token.buys_1h = total_buys_1h
                token.sells_1h = total_sells_1h
                
                self.session.commit()
                logger.info(f"成功更新代币 {chain}/{contract} 的1小时交易数据")
                logger.info(f"1小时买入: {total_buys_1h}, 1小时卖出: {total_sells_1h}")
                
                return {
                    "success": True,
                    "buys_1h": total_buys_1h,
                    "sells_1h": total_sells_1h
                }
                
            except Exception as e:
                self.session.rollback()
                logger.error(f"更新代币交易数据时发生错误: {str(e)}")
                return {"error": str(e)}
        else:
            logger.warning(f"数据库中未找到代币 {chain}/{contract}")
            return {"error": "数据库中未找到该代币"}
    
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
        综合更新单个代币的市值、流动性和交易数据
        
        Args:
            chain: 区块链名称
            contract: 代币合约地址
            
        Returns:
            Dict: 包含更新结果的字典
        """
        logger.info(f"开始综合更新代币 {chain}/{contract} 的市场和交易数据")
        
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
            
        # 初始化数据
        max_market_cap = 0
        max_liquidity = 0
        dex_screener_url = None
        price = None
        first_price = None
        total_buys_1h = 0
        total_sells_1h = 0
        total_volume_1h = 0  # 初始化1小时总交易量
        
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
            
            # 提取交易数据
            txns = pair.get("txns", {})
            
            # 提取h1数据（1小时交易数据）
            h1_data = txns.get("h1", {})
            if h1_data:
                buys = h1_data.get("buys", 0)
                sells = h1_data.get("sells", 0)
                
                # 累加交易数据
                total_buys_1h += buys
                total_sells_1h += sells
            
            # 提取1小时交易量数据
            volume_data = pair.get("volume", {})
            if volume_data and "h1" in volume_data:
                volume_1h = volume_data.get("h1", 0)
                if volume_1h:
                    total_volume_1h += float(volume_1h)
        
        # 获取目标代币
        token = self.session.query(Token).filter(
            Token.chain == chain,
            Token.contract == contract
        ).first()
        
        if token:
            # 更新代币数据
            try:
                # 将当前market_cap值保存到market_cap_1h字段
                token.market_cap_1h = token.market_cap
                logger.info(f"代币 {chain}/{contract} 的市值 {token.market_cap} 已保存到 market_cap_1h")
                
                # 更新市值和流动性
                token.market_cap = max_market_cap
                token.market_cap_formatted = self._format_market_cap(max_market_cap)
                token.liquidity = max_liquidity
                
                token.price = price

                # 如果是首次设置价格，同时设置first_price
                if token.first_price is None:
                    token.first_price = first_price
                
                if dex_screener_url:
                    token.dexscreener_url = dex_screener_url
                
                # 更新交易数据
                token.buys_1h = total_buys_1h
                token.sells_1h = total_sells_1h
                
                # 更新1小时交易量数据
                token.volume_1h = total_volume_1h
                
                self.session.commit()
                logger.info(f"成功综合更新代币 {chain}/{contract} 的市场和交易数据")
                logger.info(f"市值: {max_market_cap}, 上一小时市值: {token.market_cap_1h}, 流动性: {max_liquidity}, 1小时买入: {total_buys_1h}, 1小时卖出: {total_sells_1h}, 1小时交易量: {total_volume_1h}")
                
                return {
                    "success": True,
                    "marketCap": max_market_cap,
                    "marketCap1h": token.market_cap_1h,
                    "liquidity": max_liquidity,
                    "price": price,
                    "dexScreenerUrl": dex_screener_url,
                    "buys_1h": total_buys_1h,
                    "sells_1h": total_sells_1h,
                    "volume_1h": total_volume_1h
                }
                
            except Exception as e:
                self.session.rollback()
                logger.error(f"综合更新代币数据时发生错误: {str(e)}")
                return {"error": str(e)}
        else:
            logger.warning(f"数据库中未找到代币 {chain}/{contract}")
            return {"error": "数据库中未找到该代币"}


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