#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
DEX Screener API 接口模块
实现DEX Screener提供的所有API接口
"""

import requests
from typing import Dict, List, Any, Optional, Union
import logging

# 设置日志记录
logger = logging.getLogger(__name__)

class DexScreenerAPI:
    """DEX Screener API 客户端类"""
    
    # API 基础URL
    BASE_URL = "https://api.dexscreener.com"
    
    # API 速率限制（每分钟请求次数）
    RATE_LIMITS = {
        "token_profiles": 60,   # token profiles API
        "token_boosts": 60,     # token boosts API
        "orders": 60,           # orders API
        "pairs": 300,           # pairs API
        "search": 300,          # search API
        "tokens": 300           # tokens API
    }
    
    def __init__(self):
        """初始化DEX Screener API客户端"""
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "Telegram-Monitor/1.0"
        })
    
    def _handle_request(self, url: str, method: str = "GET", params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        处理API请求并返回响应
        
        Args:
            url: API请求的完整URL
            method: HTTP方法，默认为GET
            params: 请求参数，用于GET请求的查询参数
            
        Returns:
            Dict: API响应的JSON数据
        """
        try:
            if method == "GET":
                response = self.session.get(url, params=params)
            else:
                response = self.session.post(url, json=params)
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API请求错误: {str(e)}")
            return {"error": str(e)}
        except ValueError as e:
            logger.error(f"JSON解析错误: {str(e)}")
            return {"error": f"JSON解析错误: {str(e)}"}
        except Exception as e:
            logger.error(f"未知错误: {str(e)}")
            return {"error": f"未知错误: {str(e)}"}
    
    def get_latest_token_profiles(self) -> Dict[str, Any]:
        """
        获取最新的代币档案
        速率限制: 每分钟60次请求
        
        Returns:
            Dict: 包含最新代币档案信息的字典
        """
        url = f"{self.BASE_URL}/token-profiles/latest/v1"
        return self._handle_request(url)
    
    def get_latest_boosted_tokens(self) -> Dict[str, Any]:
        """
        获取最新的推广代币
        速率限制: 每分钟60次请求
        
        Returns:
            Dict: 包含最新推广代币信息的字典
        """
        url = f"{self.BASE_URL}/token-boosts/latest/v1"
        return self._handle_request(url)
    
    def get_top_boosted_tokens(self) -> Dict[str, Any]:
        """
        获取最活跃推广的代币
        速率限制: 每分钟60次请求
        
        Returns:
            Dict: 包含最活跃推广代币信息的字典
        """
        url = f"{self.BASE_URL}/token-boosts/top/v1"
        return self._handle_request(url)
    
    def check_token_orders(self, chain_id: str, token_address: str) -> List[Dict[str, Any]]:
        """
        检查代币的已支付订单
        速率限制: 每分钟60次请求
        
        Args:
            chain_id: 区块链ID，例如 "solana"
            token_address: 代币地址
            
        Returns:
            List[Dict]: 包含订单信息的列表
        """
        url = f"{self.BASE_URL}/orders/v1/{chain_id}/{token_address}"
        return self._handle_request(url)
    
    def get_pairs_by_chain_and_address(self, chain_id: str, pair_id: str) -> Dict[str, Any]:
        """
        通过区块链ID和交易对地址获取交易对信息
        速率限制: 每分钟300次请求
        
        Args:
            chain_id: 区块链ID，例如 "solana"
            pair_id: 交易对地址
            
        Returns:
            Dict: 包含交易对信息的字典
        """
        url = f"{self.BASE_URL}/latest/dex/pairs/{chain_id}/{pair_id}"
        return self._handle_request(url)
    
    def search_pairs(self, query: str) -> Dict[str, Any]:
        """
        搜索匹配查询的交易对
        速率限制: 每分钟300次请求
        
        Args:
            query: 搜索查询，例如 "SOL/USDC"
            
        Returns:
            Dict: 包含搜索结果的字典
        """
        url = f"{self.BASE_URL}/latest/dex/search"
        params = {"q": query}
        return self._handle_request(url, params=params)
    
    def get_token_pools(self, chain_id: str, token_address: str) -> List[Dict[str, Any]]:
        """
        获取指定代币地址的流动池
        速率限制: 每分钟300次请求
        
        Args:
            chain_id: 区块链ID，例如 "solana"
            token_address: 代币地址
            
        Returns:
            List[Dict]: 包含代币流动池信息的列表
        """
        url = f"{self.BASE_URL}/token-pairs/v1/{chain_id}/{token_address}"
        return self._handle_request(url)
    
    def get_pairs_by_token_address(self, chain_id: str, token_addresses: Union[str, List[str]]) -> List[Dict[str, Any]]:
        """
        通过代币地址获取交易对
        速率限制: 每分钟300次请求
        
        Args:
            chain_id: 区块链ID，例如 "solana"
            token_addresses: 代币地址或地址列表（最多30个地址，用逗号分隔）
            
        Returns:
            List[Dict]: 包含交易对信息的列表
        """
        # 确保token_addresses是字符串
        if isinstance(token_addresses, list):
            token_addresses = ",".join(token_addresses)
            
        url = f"{self.BASE_URL}/tokens/v1/{chain_id}/{token_addresses}"
        return self._handle_request(url)


# 创建单例实例供其他模块使用
dex_screener = DexScreenerAPI()


# 便捷函数，直接调用单例实例的方法
def get_latest_token_profiles() -> Dict[str, Any]:
    """获取最新的代币档案"""
    return dex_screener.get_latest_token_profiles()

def get_latest_boosted_tokens() -> Dict[str, Any]:
    """获取最新的推广代币"""
    return dex_screener.get_latest_boosted_tokens()

def get_top_boosted_tokens() -> Dict[str, Any]:
    """获取最活跃推广的代币"""
    return dex_screener.get_top_boosted_tokens()

def check_token_orders(chain_id: str, token_address: str) -> List[Dict[str, Any]]:
    """检查代币的已支付订单"""
    return dex_screener.check_token_orders(chain_id, token_address)

def get_pairs_by_chain_and_address(chain_id: str, pair_id: str) -> Dict[str, Any]:
    """通过区块链ID和交易对地址获取交易对信息"""
    return dex_screener.get_pairs_by_chain_and_address(chain_id, pair_id)

def search_pairs(query: str) -> Dict[str, Any]:
    """搜索匹配查询的交易对"""
    return dex_screener.search_pairs(query)

def get_token_pools(chain_id: str, token_address: str) -> List[Dict[str, Any]]:
    """获取指定代币地址的流动池"""
    return dex_screener.get_token_pools(chain_id, token_address)

def get_pairs_by_token_address(chain_id: str, token_addresses: Union[str, List[str]]) -> List[Dict[str, Any]]:
    """通过代币地址获取交易对"""
    return dex_screener.get_pairs_by_token_address(chain_id, token_addresses)

# 添加缺失的get_pair_info函数
async def get_pair_info(chain_id: str, token_address: str) -> Dict[str, Any]:
    """
    获取代币配对信息
    
    Args:
        chain_id: 区块链ID，例如 "ethereum"
        token_address: 代币合约地址
        
    Returns:
        Dict: 包含代币配对信息的字典，包含价格、流动性、交易量等数据
    """
    try:
        # 获取代币池数据
        pools_data = get_token_pools(chain_id, token_address)
        
        # 如果发生错误，返回错误信息
        if isinstance(pools_data, dict) and "error" in pools_data:
            logger.error(f"获取代币池数据失败: {pools_data['error']}")
            return {"error": pools_data["error"]}
        
        # 处理API返回的数据结构
        # 根据API文档，token-pairs/v1/{chainId}/{tokenAddress}返回的直接是交易对数组
        pairs = pools_data  # 直接使用返回的数据
            
        if not pairs or not isinstance(pairs, list) or len(pairs) == 0:
            logger.warning(f"未找到代币 {chain_id}/{token_address} 的交易对")
            return {"error": "未找到代币交易对"}
            
        # 查找最合适的交易对（通常是流动性最高的）
        best_pair = None
        max_liquidity = 0
        
        for pair in pairs:
            liquidity = pair.get("liquidity", {}).get("usd", 0)
            if liquidity and float(liquidity) > max_liquidity:
                max_liquidity = float(liquidity)
                best_pair = pair
        
        # 如果没有找到合适的交易对，返回第一个
        if not best_pair and pairs:
            best_pair = pairs[0]
            
        return best_pair or {"error": "无法确定最佳交易对"}
        
    except Exception as e:
        logger.error(f"获取代币配对信息时出错: {str(e)}")
        return {"error": str(e)} 