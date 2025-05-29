#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Digital Asset Standard (DAS) API 接口模块
实现DAS提供的API接口，用于获取代币账户信息
"""

import requests
import json
import logging
import os
import time
from collections import deque
import threading
from typing import Dict, List, Any, Optional, Union, Tuple
from dotenv import load_dotenv
from decimal import Decimal
import asyncio
import aiohttp
from datetime import datetime

# 加载环境变量
load_dotenv()

# 设置日志记录
logger = logging.getLogger(__name__)

class DASAPI:
    """Digital Asset Standard API 客户端类"""
    
    # API 基础URL
    BASE_URL = "https://mainnet.helius-rpc.com/"
    
    # API 密钥
    API_KEY = os.getenv("DAS_API_KEY", "")
    
    # API 速率限制（每秒请求次数）
    RATE_LIMIT = 5  # 每秒5次请求
    
    def __init__(self, api_key: Optional[str] = None):
        """
        初始化DAS API客户端
        
        Args:
            api_key: 可选的API密钥，如不提供则使用环境变量中的DAS_API_KEY
        """
        self.api_key = api_key or self.API_KEY
        if not self.api_key:
            logger.warning("未提供DAS API密钥，API调用可能会失败")
            
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "Telegram-Monitor/1.0"
        })
        
        # 速率限制相关变量
        self.request_timestamps = deque(maxlen=self.RATE_LIMIT)  # 存储最近的请求时间戳
        self.request_lock = threading.Lock()  # 用于线程安全
    
    def _handle_request(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理API请求并返回响应，包含速率限制控制
        
        Args:
            method: API方法名称
            params: API请求参数
            
        Returns:
            Dict: API响应的JSON数据
        """
        # 执行速率限制检查和等待
        with self.request_lock:
            current_time = time.time()
            
            # 如果请求队列已满
            if len(self.request_timestamps) >= self.RATE_LIMIT:
                # 计算最早请求到现在的时间差
                elapsed = current_time - self.request_timestamps[0]
                # 如果时间差小于1秒，需要等待
                if elapsed < 1.0:
                    wait_time = 1.0 - elapsed
                    logger.debug(f"速率限制: 等待 {wait_time:.2f} 秒后发送请求")
                    time.sleep(wait_time)
                    current_time = time.time()  # 更新当前时间
            
            # 添加当前请求时间戳到队列
            self.request_timestamps.append(current_time)
        
        url = f"{self.BASE_URL}?api-key={self.api_key}"
        
        payload = {
            "jsonrpc": "2.0",
            "id": "telegram-monitor",
            "method": method,
            "params": params
        }
        
        try:
            response = self.session.post(url, json=payload)
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
    
    def get_token_accounts(self, 
                          mint: Optional[str] = None, 
                          owner: Optional[str] = None, 
                          page: int = 1, 
                          limit: int = 100, 
                          cursor: Optional[str] = None,
                          before: Optional[str] = None,
                          after: Optional[str] = None,
                          show_zero_balance: bool = False) -> Dict[str, Any]:
        """
        获取特定铸币或所有者的所有代币账户信息
        
        Args:
            mint: 铸币地址键
            owner: 所有者地址键
            page: 返回结果的页码，默认为1
            limit: 返回结果的最大数量，默认为100，最大值为1000
            cursor: 用于分页的游标
            before: 返回指定游标之前的结果
            after: 返回指定游标之后的结果
            show_zero_balance: 如果为True，显示余额为零的账户
            
        Returns:
            Dict: 包含代币账户信息的字典
        """
        if not mint and not owner:
            logger.error("mint或owner参数必须至少提供一个")
            return {"error": "mint或owner参数必须至少提供一个"}
        
        params = {}
        
        if mint:
            params["mint"] = mint
        
        if owner:
            params["owner"] = owner
        
        params["page"] = page
        # 确保limit不超过API限制
        params["limit"] = min(limit, 1000)
        
        if cursor:
            params["cursor"] = cursor
        
        if before:
            params["before"] = before
            
        if after:
            params["after"] = after
        
        if show_zero_balance:
            params["options"] = {"showZeroBalance": True}
        
        response = self._handle_request("getTokenAccounts", params)
        
        # 增加更好的错误处理和日志
        if "error" in response:
            logger.error(f"获取代币账户信息出错: {response['error']}")
            return response
            
        # 确保返回结果正确，优化响应结构处理
        if "result" not in response:
            logger.warning(f"API响应缺少result字段: {response}")
            return {"error": "API响应格式不正确", "response": response}
            
        return response["result"]
        
    def get_token_holders_count(self, mint: str) -> Optional[int]:
        """
        获取代币持有者数量
        
        Args:
            mint: 代币铸币地址
            
        Returns:
            int: 持有者数量，如果出错则返回None
        """
        if not mint:
            logger.error("mint参数不能为空")
            return None
            
        try:
            # 调用get_token_accounts方法获取数据
            response = self.get_token_accounts(mint=mint, page=1, limit=1000)
            
            if "error" in response:
                logger.error(f"获取代币持有者数量出错: {response['error']}")
                return None
                
            # 从结果中提取持有者总数，优化路径判断
            if "total" in response:
                # 检查API返回的total值是否可靠
                # 如果持有者数量超过1000，需要手动翻页计算真实数量
                reported_total = response["total"]
                
                # 如果返回的total恰好等于1000，有可能是API限制导致的，需要继续翻页确认
                if reported_total >= 1000:
                    logger.debug(f"代币 {mint} 报告的持有者数量为 {reported_total}，开始翻页验证真实数量")
                    
                    # 手动计算真实持有者数量
                    actual_total = 0
                    current_page = 1
                    max_pages = 50  # 设置最大页数限制，避免无限循环
                    
                    while current_page <= max_pages:
                        page_response = self.get_token_accounts(mint=mint, page=current_page, limit=1000)
                        
                        if "error" in page_response:
                            logger.error(f"获取第 {current_page} 页时发生错误: {page_response['error']}")
                            break
                            
                        page_accounts = page_response.get("token_accounts", [])
                        page_count = len(page_accounts)
                        actual_total += page_count
                        
                        # 如果当前页记录数小于1000，说明已经获取完所有数据
                        if page_count < 1000:
                            break
                        
                        current_page += 1
                    
                    logger.debug(f"代币 {mint} 真实持有者数量: {actual_total}")
                    return actual_total
                else:
                    # API返回的总数小于1000，可以认为是准确的
                    logger.debug(f"代币 {mint} 持有者数量: {reported_total}")
                    return reported_total
            else:
                logger.warning(f"无法从响应中获取持有者数量: {response}")
                return None
        except Exception as e:
            logger.error(f"获取代币持有者数量时发生异常: {str(e)}")
            return None
            
    def get_token_holders_info(self, mint: str, max_pages: int = 10) -> Tuple[Optional[int], Optional[List[Dict]]]:
        """
        获取代币持有者数量和前10大持有者信息
        
        Args:
            mint (str): 代币的mint地址
            max_pages (int): 获取的最大页数，默认为10
        
        Returns:
            Tuple[Optional[int], Optional[List[Dict]]]: 
                - 持有者总数
                - 前10大持有者列表，每个持有者包含地址、数量和占比
        """
        try:
            # 存储所有持有者信息
            all_holders = []
            total_supply = Decimal(0)
            
            # 获取第一页数据 - 限制为1000条记录
            response = self.get_token_accounts(mint=mint, page=1, limit=1000)
            
            if "error" in response:
                logger.error(f"API 错误: {response['error']}")
                return None, None
                
            total = response.get("total", 0)
            
            # 处理第一页数据
            token_accounts = response.get("token_accounts", [])
            for account in token_accounts:
                amount = Decimal(account.get("amount", 0))
                total_supply += amount
                all_holders.append({
                    "address": account.get("owner"),
                    "amount": amount
                })
            
            # 如果需要翻页且总数大于1000
            if total > 1000 and max_pages > 1:
                current_page = 1
                
                while current_page < max_pages:
                    current_page += 1
                    
                    try:
                        page_response = self.get_token_accounts(mint=mint, page=current_page, limit=1000)
                        
                        if "error" in page_response:
                            logger.error(f"获取第 {current_page} 页时发生错误: {page_response['error']}")
                            break
                            
                        page_accounts = page_response.get("token_accounts", [])
                        
                        # 处理当前页数据
                        for account in page_accounts:
                            amount = Decimal(account.get("amount", 0))
                            total_supply += amount
                            all_holders.append({
                                "address": account.get("owner"),
                                "amount": amount
                            })
                    except Exception as e:
                        logger.error(f"处理第 {current_page} 页时出错: {str(e)}")
                        break
            
            # 计算前10大持有者
            all_holders.sort(key=lambda x: x["amount"], reverse=True)
            top_holders = all_holders[:10]
            
            # 计算每个持有者的占比
            for holder in top_holders:
                if total_supply > 0:
                    holder["percentage"] = (holder["amount"] / total_supply * 100).quantize(Decimal("0.01"))
                else:
                    holder["percentage"] = Decimal("0.00")
            
            return total, top_holders

        except Exception as e:
            logger.error(f"获取代币持有者信息时发生异常: {str(e)}")
            return None, None

    def get_asset(self, asset_id: str) -> Optional[Dict[str, Any]]:
        """
        获取资产信息
        
        Args:
            asset_id: 资产ID
            
        Returns:
            Dict[str, Any]: 资产信息，如果获取失败则返回 None
        """
        try:
            url = f"{self.BASE_URL}?api-key={self.api_key}"
            payload = {
                "jsonrpc": "2.0",
                "id": "1",
                "method": "getAsset",
                "params": {"id": asset_id}
            }
            headers = {"Content-Type": "application/json"}
            
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            if 'result' in data:
                return data['result']
            return None
            
        except Exception as e:
            logger.error(f"获取资产信息失败: {str(e)}")
            return None
            
    def convert_to_token_data(self, asset_data: Dict[str, Any], chain: str, contract: str, message_id: int) -> Dict[str, Any]:
        """
        将 DAS API 返回的数据转换为代币数据格式
        
        Args:
            asset_data: DAS API 返回的资产数据
            chain: 链标识
            contract: 合约地址
            message_id: 消息ID
            
        Returns:
            Dict[str, Any]: 转换后的代币数据
        """
        try:
            # 基础数据
            token_data = {
                'chain': chain,
                'contract': contract,
                'message_id': message_id,
                'latest_update': datetime.now().isoformat(),
                'first_update': datetime.now().isoformat(),
                'market_cap': 0,
                'market_cap_formatted': '0',
                'liquidity': 0,
                'price': 0,
                'volume_1h': 0,
                'buys_1h': 0,
                'sells_1h': 0,
                'holders_count': 0,
                'promotion_count': 1,
                'risk_level': 'unknown'
            }
            
            # 从 DAS API 数据中提取信息
            if 'content' in asset_data and 'metadata' in asset_data['content']:
                metadata = asset_data['content']['metadata']
                token_data.update({
                    'token_symbol': metadata.get('symbol', ''),
                    'name': metadata.get('name', '')
                })
                
            # 从 token_info 中提取信息
            if 'token_info' in asset_data:
                token_info = asset_data['token_info']
                if 'price_info' in token_info:
                    price_info = token_info['price_info']
                    token_data.update({
                        'price': float(price_info.get('price_per_token', 0)),
                        'market_cap': float(token_info.get('supply', 0)) * float(price_info.get('price_per_token', 0)),
                        'market_cap_formatted': f"{float(token_info.get('supply', 0)) * float(price_info.get('price_per_token', 0)):,.2f}"
                    })
                
            # 从 content 中提取图像 URL
            if 'content' in asset_data and 'files' in asset_data['content']:
                for file in asset_data['content']['files']:
                    if file.get('mime', '').startswith('image/'):
                        token_data['image_url'] = file.get('uri', '')
                        break
            
            # 对SOL链代币获取持有者数量
            if chain == 'SOL':
                try:
                    holders_count = self.get_token_holders_count(contract)
                    if holders_count is not None:
                        token_data['holders_count'] = holders_count
                        logger.info(f"从DAS API获取到代币 {contract} 的持有者数量: {holders_count}")
                except Exception as e:
                    logger.warning(f"获取代币 {contract} 的持有者数量时出错: {str(e)}")
                        
            return token_data
            
        except Exception as e:
            logger.error(f"转换代币数据失败: {str(e)}")
            return None


# 创建单例实例供其他模块使用
das_api = DASAPI()


# 便捷函数，直接调用单例实例的方法
def get_token_accounts(mint: Optional[str] = None, 
                      owner: Optional[str] = None, 
                      page: int = 1, 
                      limit: int = 100, 
                      cursor: Optional[str] = None,
                      before: Optional[str] = None,
                      after: Optional[str] = None,
                      show_zero_balance: bool = False) -> Dict[str, Any]:
    """
    获取特定铸币或所有者的所有代币账户信息
    
    Args:
        mint: 铸币地址键
        owner: 所有者地址键
        page: 返回结果的页码，默认为1
        limit: 返回结果的最大数量，默认为100，最大值为1000
        cursor: 用于分页的游标
        before: 返回指定游标之前的结果
        after: 返回指定游标之后的结果
        show_zero_balance: 如果为True，显示余额为零的账户
        
    Returns:
        Dict: 包含代币账户信息的字典
    """
    # 确保limit不超过API限制
    limit = min(limit, 1000)
    
    # 获取DAS API客户端实例
    api = DASAPI()
    
    # 调用实例方法
    return api.get_token_accounts(mint, owner, page, limit, cursor, before, after, show_zero_balance)

def get_token_holders_count(mint: str) -> Optional[int]:
    """
    获取代币持有者数量
    
    Args:
        mint: 代币铸币地址
        
    Returns:
        int: 持有者数量，如果出错则返回None
    """
    return das_api.get_token_holders_count(mint) 

def get_token_holders_info(mint: str, max_pages: int = 10) -> Tuple[Optional[int], Optional[List[Dict]]]:
    """
    获取代币持有者数量和持有者信息
    
    Args:
        mint: 代币mint地址
        max_pages: 最大获取页数，默认为10
        
    Returns:
        元组: (持有者数量, 持有者列表)
    """
    # 获取DAS API客户端实例
    api = DASAPI()
    
    # 调用实例方法
    return api.get_token_holders_info(mint, max_pages) 

async def async_get_token_holders_info(mint: str, max_pages: int = 10) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    异步获取代币持有者数量和前10大持有者信息
    
    Args:
        mint (str): 代币的mint地址
        max_pages (int): 获取的最大页数，默认为10
    
    Returns:
        Tuple[Dict[str, Any], List[Dict[str, Any]]]: 
            - 持有者统计信息，包含总数、总供应量等
            - 前10大持有者列表，每个持有者包含地址、数量和占比
    """
    try:
        # 创建一个事件循环中执行同步函数
        loop = asyncio.get_event_loop()
        # 在事件循环中调用同步版本的函数
        holders_count, top_holders = await loop.run_in_executor(
            None, get_token_holders_info, mint, max_pages
        )
        
        # 构造返回结果
        holders_info = {
            "count": holders_count,
            "mint": mint
        }
        
        return holders_info, top_holders
    except Exception as e:
        logger.error(f"异步获取代币持有者信息时出错: {str(e)}")
        # 返回空数据
        return {"count": 0, "mint": mint}, [] 