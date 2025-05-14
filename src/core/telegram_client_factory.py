#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Telegram客户端工厂模块
用于集中管理Telegram客户端实例，确保系统中只有一个活跃的Telegram连接
"""

import os
import logging
import asyncio
from typing import Optional
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError, 
    PhoneCodeInvalidError, 
    PhoneCodeExpiredError, 
    PasswordHashInvalidError,
    SessionPasswordNeededError
)

# 导入日志工具
from src.utils.logger import get_logger
logger = get_logger(__name__)

class TelegramClientFactory:
    """Telegram客户端工厂类，确保系统中只有一个活跃的Telegram客户端连接"""
    
    _instance = None
    _lock = asyncio.Lock()
    _connection_count = 0
    _max_reconnect_attempts = 3
    
    @classmethod
    async def get_client(cls, session_path: str, api_id: int, api_hash: str, **kwargs) -> Optional[TelegramClient]:
        """
        获取Telegram客户端实例
        如果实例不存在或未连接，则创建新实例
        
        Args:
            session_path: 会话文件路径
            api_id: Telegram API ID
            api_hash: Telegram API Hash
            **kwargs: 其他传递给TelegramClient的参数
            
        Returns:
            TelegramClient: Telegram客户端实例
        """
        async with cls._lock:
            # 检查现有客户端
            if cls._instance is not None:
                try:
                    # 检查客户端是否已连接
                    if cls._instance.is_connected():
                        logger.debug("使用现有的Telegram客户端连接")
                        return cls._instance
                    else:
                        logger.info("现有客户端未连接，尝试重新连接")
                        try:
                            await cls._instance.connect()
                            if await cls._instance.is_user_authorized():
                                logger.info("重连成功，使用现有客户端")
                                return cls._instance
                        except Exception as e:
                            logger.warning(f"重连失败，将创建新客户端: {str(e)}")
                except Exception as e:
                    logger.warning(f"检查客户端连接状态时出错: {str(e)}")
            
            # 先确保旧的连接已完全关闭
            await cls.disconnect_client()
                
            # 创建新的客户端实例
            try:
                logger.info(f"创建新的Telegram客户端实例，会话路径: {session_path}")
                cls._instance = TelegramClient(
                    session_path,
                    api_id,
                    api_hash,
                    **kwargs
                )
                cls._connection_count += 1
                logger.info(f"已创建第 {cls._connection_count} 个Telegram客户端实例")
                
                return cls._instance
            except Exception as e:
                logger.error(f"创建Telegram客户端实例时出错: {str(e)}")
                return None
    
    @classmethod
    async def disconnect_client(cls):
        """断开当前客户端连接"""
        if cls._instance is not None:
            try:
                if cls._instance.is_connected():
                    logger.info("断开现有Telegram客户端连接")
                    await cls._instance.disconnect()
                    # 等待连接完全关闭
                    await asyncio.sleep(1)
            except Exception as e:
                logger.warning(f"断开客户端连接时出错: {str(e)}")
            
            # 设置为None以便垃圾回收
            cls._instance = None
    
    @classmethod
    async def reconnect_client(cls):
        """重新连接当前客户端"""
        if cls._instance is None:
            logger.warning("没有现有客户端，无法重连")
            return False
            
        for attempt in range(cls._max_reconnect_attempts):
            try:
                logger.info(f"尝试重新连接客户端 (尝试 {attempt+1}/{cls._max_reconnect_attempts})")
                
                # 确保客户端已断开连接
                if cls._instance.is_connected():
                    await cls._instance.disconnect()
                    await asyncio.sleep(1)  # 等待断开完成
                
                # 重新连接
                await cls._instance.connect()
                
                # 检查连接和授权状态
                if cls._instance.is_connected() and await cls._instance.is_user_authorized():
                    logger.info("重新连接成功")
                    return True
                else:
                    logger.warning("重新连接后客户端未授权")
                    
            except FloodWaitError as e:
                # 处理API限制错误，等待指定时间
                wait_time = e.seconds
                logger.warning(f"触发API限制，需要等待 {wait_time} 秒")
                await asyncio.sleep(min(wait_time, 300))  # 最多等待5分钟
                
            except Exception as e:
                logger.error(f"重新连接时出错: {str(e)}")
                await asyncio.sleep(5)  # 等待5秒后重试
                
        logger.error("多次重连尝试后仍然失败")
        return False
    
    @classmethod
    def get_connection_count(cls):
        """获取连接计数"""
        return cls._connection_count 