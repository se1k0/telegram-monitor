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
            limit: 返回结果的最大数量，默认为100
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
        params["limit"] = limit
        
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
            # 现在response直接是result部分，不再需要从response["result"]获取
            if "total" in response:
                logger.debug(f"代币 {mint} 持有者数量: {response['total']}")
                return response["total"]
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
            
            # 获取第一页数据
            response = self.get_token_accounts(mint=mint, page=1, limit=1000000)
            
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
            
            # 如果需要翻页且总数大于100
            if total > 1000000 and max_pages > 1:
                current_page = 1
                
                while current_page < max_pages:
                    current_page += 1
                    
                    try:
                        page_response = self.get_token_accounts(mint=mint, page=current_page, limit=1000000)
                        
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
        limit: 返回结果的最大数量，默认为100
        cursor: 用于分页的游标
        before: 返回指定游标之前的结果
        after: 返回指定游标之后的结果
        show_zero_balance: 如果为True，显示余额为零的账户
        
    Returns:
        Dict: 包含代币账户信息的字典
    """
    return das_api.get_token_accounts(
        mint=mint, 
        owner=owner, 
        page=page, 
        limit=limit, 
        cursor=cursor,
        before=before,
        after=after,
        show_zero_balance=show_zero_balance
    ) 

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
    获取代币持有者数量和前10大持有者信息
    
    Args:
        mint (str): 代币的mint地址
        max_pages (int): 获取的最大页数，默认为10
    
    Returns:
        Tuple[Optional[int], Optional[List[Dict]]]: 
            - 持有者总数
            - 前10大持有者列表，每个持有者包含地址、数量和占比
    """
    return das_api.get_token_holders_info(mint, max_pages) 