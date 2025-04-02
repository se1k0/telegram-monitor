#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Telegram监听器模块
处理Telegram消息监听和处理
"""

import os
import sys
import time
import json
import asyncio
import logging
import traceback as tb  # 重命名为tb，避免变量名冲突
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from dotenv import load_dotenv

import telethon
from telethon import TelegramClient, events
from telethon.tl.types import Channel, Chat, PeerChannel, PeerChat
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# 导入项目模块
from src.utils.logger import get_logger
from src.core.channel_manager import ChannelManager, DEFAULT_CHANNELS
from src.database.models import engine, TelegramChannel, init_db
from src.database.db_handler import (
    save_telegram_message, extract_promotion_info, 
    save_token_info, process_message_batch, token_batch, update_token_info,
    process_batches, cleanup_batch_tasks
)
import config.settings as config
from src.utils.utils import parse_market_cap, format_market_cap
from src.core.channel_discovery import ChannelDiscovery
import sqlite3
import traceback
from functools import wraps
import time

# 将现有日志替换为我们的统一日志模块
try:
    from src.utils.logger import get_logger
    logger = get_logger(__name__)
except ImportError:
    # 兼容性代码，使用原有的logger
    logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv()

# 重试装饰器，用于处理异步操作中的临时性错误
def async_retry(max_retries=3, delay=1, backoff=2, exceptions=(Exception,)):
    """
    异步重试装饰器，带有智能的延迟增长和错误处理
    
    Args:
        max_retries: 最大重试次数
        delay: 初始延迟时间（秒）
        backoff: 退避系数，每次失败后延迟时间增加的倍数
        exceptions: 要捕获的异常类型
        
    Returns:
        装饰器函数
    """
    def decorator(func):
        # 为每个函数单独存储上次失败时间
        last_failure_time = {}
        failure_count = {}
        
        @wraps(func)
        async def wrapper(*args, **kwargs):
            func_name = func.__name__
            retries = 0
            current_delay = delay
            
            # 初始化失败计数器
            if func_name not in failure_count:
                failure_count[func_name] = 0
                
            # 检查是否在短时间内多次失败
            if func_name in last_failure_time:
                time_since_last_failure = (datetime.now() - last_failure_time[func_name]).total_seconds()
                # 如果最近5分钟内有多次失败，增加初始延迟时间
                if time_since_last_failure < 300 and failure_count[func_name] > 2:
                    current_delay = max(delay * failure_count[func_name], 30)  # 最少30秒
                    logger.warning(f"函数 {func_name} 最近频繁失败，增加延迟至 {current_delay} 秒")
                # 如果超过30分钟没有失败，重置失败计数
                elif time_since_last_failure > 1800:
                    failure_count[func_name] = 0
            
            # 开始重试循环
            while True:
                try:
                    result = await func(*args, **kwargs)
                    # 成功执行，部分重置失败计数
                    if failure_count[func_name] > 0:
                        failure_count[func_name] = max(0, failure_count[func_name] - 1)
                    return result
                except exceptions as e:
                    retries += 1
                    failure_count[func_name] += 1
                    last_failure_time[func_name] = datetime.now()
                    
                    # 分析错误类型，对特定错误采取特殊处理
                    error_type = type(e).__name__
                    
                    # 对于网络相关错误，可能需要额外等待
                    if "Connection" in error_type or "Timeout" in error_type or "Network" in error_type:
                        # 网络错误可能需要更长的等待时间
                        current_delay = max(current_delay, 15) * backoff
                        logger.warning(f"网络错误: {error_type}, 增加等待时间")
                    # 对于API限制相关错误，需要较长时间等待
                    elif "Flood" in error_type or "TooMany" in error_type or "Wait" in error_type:
                        current_delay = max(current_delay * 2, 60)  # 至少等待60秒
                        logger.warning(f"API限制错误: {error_type}, 增加等待时间至 {current_delay} 秒")
                    
                    if retries > max_retries:
                        logger.error(f"函数 {func_name} 达到最大重试次数 {max_retries}，放弃重试: {str(e)}")
                        # 记录更详细的错误信息
                        logger.debug(f"失败的函数参数: args={args}, kwargs={kwargs}")
                        if hasattr(e, "__traceback__"):
                            logger.debug(tb.format_exc())
                        raise
                    
                    # 计算指数退避延迟，但设置上限
                    current_delay = min(current_delay * backoff, 300)  # 最大等待5分钟
                    
                    logger.warning(f"函数 {func_name} 执行失败 ({retries}/{max_retries}): {str(e)}, 将在 {current_delay:.1f} 秒后重试")
                    await asyncio.sleep(current_delay)
            
        return wrapper
        
    return decorator

class TelegramListener:
    """Telegram 消息监听器类"""
    
    def __init__(self):
        # Telegram API 认证信息
        self.api_id = os.getenv('TG_API_ID')
        self.api_hash = os.getenv('TG_API_HASH')
        
        # 确保会话目录存在
        session_dir = os.path.join(os.getcwd(), 'data', 'sessions')
        os.makedirs(session_dir, exist_ok=True)
        
        # 设置数据库会话
        from sqlalchemy.orm import sessionmaker
        from src.database.models import engine
        self.Session = sessionmaker(bind=engine)
        
        # 生成唯一的会话名称，避免冲突
        session_name = f'tg_session_{os.getpid()}_{int(time.time())}'
        self.session_path = os.path.join(session_dir, session_name)
        
        # 设置Telethon的SQLite连接参数
        self.connection_retries = 10  # 增加重试次数
        self.auto_reconnect = True
        self.retry_delay = 5  # 增加延迟时间，避免频繁重试
        self.request_retries = 5  # 请求重试次数
        self.flood_sleep_threshold = 60  # 被限流时等待时间（秒）
        
        # 初始化客户端，添加更强健的连接参数（移除不兼容参数）
        self.client = TelegramClient(
            self.session_path,
            self.api_id, 
            self.api_hash,
            connection_retries=self.connection_retries,
            auto_reconnect=self.auto_reconnect,
            retry_delay=self.retry_delay,
            request_retries=self.request_retries,
            flood_sleep_threshold=self.flood_sleep_threshold,
            timeout=30  # 设置更长的超时时间
        )
        
        # 初始化频道管理器
        self.channel_manager = ChannelManager(self.client)
        
        # 初始化频道发现器
        self.channel_discovery = None
        
        # 活跃的频道映射
        self.chain_map = {}
        
        # 频道实体映射 - 频道ID到实体对象的映射
        self.channel_entities = {}
        
        # 事件处理器映射，用于动态添加和移除事件处理器
        self.event_handlers = {}
        
        # 监控状态
        self.is_running = False
        self.last_error_time = None
        self.error_count = 0
        
        # 数据库批处理任务
        self.batch_task = None
        
        # 自动发现频道配置
        self.auto_discovery_enabled = config.auto_channel_discovery if hasattr(config, 'auto_channel_discovery') else True
        self.discovery_interval = config.discovery_interval if hasattr(config, 'discovery_interval') else 3600
        self.min_members = config.min_channel_members if hasattr(config, 'min_channel_members') else 500
        self.max_auto_channels = config.max_auto_channels if hasattr(config, 'max_auto_channels') else 10
    
    @async_retry(max_retries=3, delay=2, exceptions=(ConnectionError, TimeoutError))
    async def setup_channels(self):
        """设置频道监听，添加重试机制"""
        try:
            # 等待客户端连接
            if not self.client.is_connected():
                await self.client.connect()
                logger.info("客户端已成功连接")
                
            # 更新频道信息
            self.chain_map, self.channel_entities = await self.channel_manager.update_channels(DEFAULT_CHANNELS)
            logger.info(f"已加载 {len(self.chain_map)} 个活跃频道")
            
            # 初始化频道发现器
            self.channel_discovery = ChannelDiscovery(self.client, self.channel_manager)
            
            # 注册消息处理程序
            await self.register_handlers()
            return True
        except Exception as e:
            logger.error(f"设置频道监听时出错: {str(e)}")
            logger.debug(tb.format_exc())
            raise  # 让装饰器捕获异常并处理重试
    
    @async_retry(max_retries=2, delay=1)
    async def register_handlers(self):
        """注册所有活跃频道和群组的消息处理程序，添加重试机制"""
        try:
            # 移除旧的处理程序
            for handler in list(self.event_handlers.values()):
                self.client.remove_event_handler(handler)
            self.event_handlers.clear()
            
            # 构建监听实体列表
            chat_entities = []
            
            # 如果有channel_entities，使用实体进行监听 
            if hasattr(self, 'channel_entities') and self.channel_entities:
                chat_entities = list(self.channel_entities.values())
                logger.info(f"将使用 {len(chat_entities)} 个频道/群组实体进行消息监听")
            else:
                # 向后兼容：尝试使用用户名列表
                channel_list = list(self.chain_map.keys())
                if channel_list:
                    chat_entities = channel_list
                    logger.info(f"将使用 {len(channel_list)} 个频道/群组用户名进行消息监听")
            
            if not chat_entities:
                logger.warning("没有活跃的频道或群组可监听")
                return False
                
            # 注册新消息处理程序
            handler = self.client.add_event_handler(
                self.handle_new_message,
                events.NewMessage(chats=chat_entities)
            )
            self.event_handlers['new_message'] = handler
            
            # 日志记录监听的频道和群组
            if hasattr(self, 'channel_entities') and self.channel_entities:
                # 使用实体名称或ID记录日志
                entity_names = []
                for entity_id, entity in self.channel_entities.items():
                    # 判断是否为群组
                    is_group = False
                    if hasattr(entity, 'broadcast') and not entity.broadcast:
                        is_group = True
                    
                    if hasattr(entity, 'username') and entity.username:
                        entity_names.append(f"@{entity.username}{'(群组)' if is_group else ''}")
                    elif hasattr(entity, 'title'):
                        entity_names.append(f"{entity.title}{'(群组)' if is_group else ''} (ID: {entity_id})")
                    else:
                        entity_names.append(f"ID: {entity_id}{'(群组)' if is_group else ''}")
                        
                logger.info(f"已注册消息处理程序，监听频道和群组: {', '.join(entity_names)}")
            else:
                # 向后兼容：使用用户名列表记录日志
                logger.info(f"已注册消息处理程序，监听频道和群组: {', '.join(channel_list)}")
                
            return True
        except Exception as e:
            logger.error(f"注册处理程序时出错: {str(e)}")
            logger.debug(tb.format_exc())
            raise  # 让装饰器捕获异常并处理重试
    
    async def get_channel_members_count(self, channel_id, is_group=False, is_supergroup=False):
        """获取频道或群组的成员数量
        
        Args:
            channel_id: 频道或群组ID
            is_group: 是否为群组
            is_supergroup: 是否为超级群组
            
        Returns:
            int: 成员数量，失败则返回0
        """
        try:
            from telethon.tl.functions.channels import GetFullChannelRequest
            from telethon.tl.functions.messages import GetFullChatRequest
            
            if is_group:
                if is_supergroup:
                    # 超级群组使用GetFullChannelRequest
                    full_channel = await self.client(GetFullChannelRequest(
                        channel=channel_id
                    ))
                    return getattr(full_channel.full_chat, 'participants_count', 0)
                else:
                    # 普通群组使用GetFullChatRequest
                    full_chat = await self.client(GetFullChatRequest(
                        chat_id=channel_id
                    ))
                    return getattr(full_chat.full_chat, 'participants_count', 0)
            else:
                # 普通频道
                full_channel = await self.client(GetFullChannelRequest(
                    channel=channel_id
                ))
                return getattr(full_channel.full_chat, 'participants_count', 0)
        except Exception as e:
            logger.warning(f"获取频道 {channel_id} 的成员数时出错: {str(e)}")
            return 0
    
    async def handle_new_message(self, event):
        """处理新消息事件，支持频道和群组，增加错误处理和恢复机制"""
        start_time = time.time()
        message = event.message
        
        # 获取频道/群组标识符
        channel_identifier = None
        channel_id = None
        
        # 增强频道类型判断
        is_group = False
        is_supergroup = False
        
        # 尝试获取频道ID和类型
        if hasattr(event.chat, 'id'):
            channel_id = event.chat.id
        
        # 使用Telethon库的正确判断方式
        from telethon.tl.types import Channel, Chat
        
        if isinstance(event.chat, Channel):
            if event.chat.megagroup:
                # 超级群组
                is_supergroup = True
                is_group = True
            elif event.chat.broadcast:
                # 普通频道
                is_group = False
                is_supergroup = False
            else:
                # 其他Channel类型
                is_group = False
        elif isinstance(event.chat, Chat):
            # 普通群组
            is_group = True
            is_supergroup = False
        
        # 获取并更新频道成员数
        member_count = 0
        if channel_id:
            try:
                # 获取成员数量
                member_count = await self.get_channel_members_count(
                    channel_id=channel_id,
                    is_group=is_group,
                    is_supergroup=is_supergroup
                )
                
                # 更新数据库中的成员数
                if member_count > 0:
                    session = self.Session()
                    try:
                        channel = session.query(TelegramChannel).filter(
                            TelegramChannel.channel_id == channel_id
                        ).first()
                        
                        if channel and channel.member_count != member_count:
                            logger.info(f"更新频道 {channel_id} 的成员数: {channel.member_count} -> {member_count}")
                            channel.member_count = member_count
                            channel.last_updated = datetime.now()
                            session.commit()
                    except Exception as e:
                        logger.error(f"更新频道 {channel_id} 成员数时出错: {str(e)}")
                        if 'session' in locals():
                            session.rollback()
                    finally:
                        if 'session' in locals():
                            session.close()
            except Exception as e:
                logger.error(f"处理频道 {channel_id} 成员数时出错: {str(e)}")
        
        # 获取标识符（用户名或ID）
        if hasattr(event.chat, 'username') and event.chat.username:
            channel_identifier = event.chat.username
        else:
            # 如果没有用户名，则使用ID作为标识符
            channel_identifier = f"id_{channel_id}" if channel_id else "unknown"
            
        # 获取链信息
        chain = None
        # 先尝试通过ID获取链信息
        if channel_id and hasattr(self, 'channel_entities'):
            # 如果ID在channel_entities中有对应的实体，获取它的链信息
            entity_key = str(channel_id)
            if entity_key in self.chain_map:
                chain = self.chain_map[entity_key]
                
        # 如果通过ID没有找到，则尝试通过用户名获取
        if not chain and channel_identifier in self.chain_map:
            chain = self.chain_map[channel_identifier]
            
        # 如果都没找到，使用默认值
        if not chain:
            chain = 'UNKNOWN'
        
        channel_type = "普通频道"
        if is_supergroup:
            channel_type = "超级群组"
        elif is_group:
            channel_type = "普通群组"
            
        logger.info(f"收到新消息 - {channel_type}: {channel_identifier}, ID: {channel_id}, 链: {chain}, 消息ID: {message.id}")
        
        try:
            # 打印完整消息内容
            if message.text:
                logger.info(f"消息内容:\n{'-' * 50}\n{message.text[:500]}...\n{'-' * 50}")
            else:
                logger.info("消息没有文本内容")
            
            # 保存媒体文件
            media_path = None
            if message.media:
                # 添加超时控制和错误处理
                try:
                    media_dir = f'media/{chain}'
                    os.makedirs(media_dir, exist_ok=True)
                    media_path = f'{media_dir}/{message.id}'
                    
                    # 设置下载超时
                    download_task = asyncio.create_task(self.client.download_media(message, media_path))
                    await asyncio.wait_for(download_task, timeout=60)  # 60秒超时
                    logger.info(f"保存了媒体文件: {media_path}")
                except asyncio.TimeoutError:
                    logger.warning(f"下载媒体文件超时: {channel_type}={channel_identifier}, 消息ID={message.id}")
                    media_path = None
                except Exception as e:
                    logger.error(f"下载媒体文件失败: {str(e)}")
                    media_path = None
            
            # 使用新的数据库函数保存消息
            saved = save_telegram_message(
                chain=chain,
                message_id=message.id,
                date=message.date,
                text=message.text,
                media_path=media_path,
                channel_id=channel_id  # 只使用channel_id字段，移除is_group和is_supergroup
            )
            
            # 如果消息已存在，则不继续处理
            if not saved:
                return
            
            # 从消息中提取 promotion 信息
            promo = None
            if message.text:
                try:
                    promo = extract_promotion_info(message.text, message.date, chain)
                    logger.debug(f"extract_promotion_info 返回值: {promo}")
                except Exception as e:
                    logger.error(f"提取 promotion 信息时出错: {str(e)}")
                    logger.debug(tb.format_exc())
            
            # 更新 tokens 表
            if promo and promo.contract_address:
                try:
                    market_cap_value = parse_market_cap(promo.market_cap) if promo.market_cap else 0
                    market_cap_formatted = format_market_cap(market_cap_value)
                    
                    # 转换时间到 UTC+8
                    utc_time = message.date
                    utc8_time = utc_time + timedelta(hours=8)
                    current_time = utc8_time.strftime('%Y-%m-%d %H:%M:%S')
                    
                    # 使用新的token保存函数
                    token_data = {
                        'chain': chain,
                        'token_symbol': promo.token_symbol,
                        'contract': promo.contract_address,
                        'message_id': message.id,
                        'market_cap': market_cap_value,
                        'market_cap_formatted': market_cap_formatted,
                        'first_market_cap': market_cap_value,  # 第一次推荐时的市值
                        'promotion_count': getattr(promo, 'promotion_count', 0),
                        'likes_count': 0,
                        'telegram_url': getattr(promo, 'telegram_url', ''),
                        'twitter_url': getattr(promo, 'twitter_url', ''),
                        'website_url': getattr(promo, 'website_url', ''),
                        'latest_update': current_time,
                        'first_update': current_time,
                        'from_group': is_group,  # 添加是否来自群组的标记
                        'channel_name': channel_identifier,  # 添加channel_name字段
                        'channel_id': channel_id  # 直接保存channel_id值
                    }
                    
                    # 添加情感分析相关字段
                    if hasattr(promo, 'sentiment_score') and promo.sentiment_score is not None:
                        token_data['sentiment_score'] = promo.sentiment_score
                    if hasattr(promo, 'positive_words') and promo.positive_words is not None:
                        token_data['positive_words'] = ','.join(promo.positive_words)
                    if hasattr(promo, 'negative_words') and promo.negative_words is not None:
                        token_data['negative_words'] = ','.join(promo.negative_words)
                    if hasattr(promo, 'hype_score') and promo.hype_score is not None:
                        token_data['hype_score'] = promo.hype_score
                    if hasattr(promo, 'risk_level') and promo.risk_level is not None:
                        token_data['risk_level'] = promo.risk_level
                        
                    # 保存代币数据
                    # 获取数据库连接
                    try:
                        import sqlite3
                        from config.settings import DATABASE_URI
                        
                        # 转换SQLAlchemy的URI为sqlite3能接受的路径
                        db_path = DATABASE_URI.replace('sqlite:///', '')
                        conn = sqlite3.connect(db_path)
                        
                        # 使用正确的参数调用update_token_info
                        update_token_info(conn, token_data)
                        logger.info(f"成功更新代币信息: {promo.token_symbol}")
                        
                        # 确保关闭连接
                        conn.close()
                    except Exception as e:
                        logger.error(f"获取数据库连接或更新代币信息时出错: {str(e)}")
                        logger.debug(tb.format_exc())
                    
                    # 使用 DexScreener API 更新代币市值和流动性数据
                    try:
                        # 导入token_market_updater模块
                        from src.api.token_market_updater import update_token_market_data
                        from sqlalchemy.orm import sessionmaker
                        from src.database.models import engine
                        
                        # 创建数据库会话
                        Session = sessionmaker(bind=engine)
                        session = Session()
                        
                        # 调用更新函数
                        result = update_token_market_data(session, chain, promo.contract_address)
                        
                        if "error" not in result:
                            logger.info(f"成功更新代币 {promo.token_symbol} ({chain}/{promo.contract_address}) 的市值和流动性数据")
                            logger.info(f"市值: {result.get('marketCap', 'N/A')}, 流动性: {result.get('liquidity', 'N/A')}")
                        else:
                            logger.warning(f"更新代币 {promo.token_symbol} ({chain}/{promo.contract_address}) 的市值和流动性数据失败: {result['error']}")
                            
                        # 关闭会话
                        session.close()
                    except Exception as e:
                        logger.error(f"调用 DexScreener API 更新代币市值和流动性数据时出错: {str(e)}")
                        logger.debug(tb.format_exc())
                    
                    # 保存推广渠道信息
                    # for channel in getattr(promo, 'promotion_channels', []):
                    #     data = {
                    #         'chain': chain,
                    #         'message_id': message.id,
                    #         'channel_info': channel
                    #     }
                    #     save_promotion_channel(data)
                    
                except Exception as e:
                    logger.error(f"处理代币信息时出错: {str(e)}")
                    logger.debug(tb.format_exc())
            
            # 重置错误计数，表示处理成功
            self.error_count = 0
            self.last_error_time = None
            
            # 记录处理时间
            process_time = time.time() - start_time
            logger.debug(f"消息处理完成，耗时: {process_time:.2f}秒")
            
        except Exception as e:
            # 记录错误并更新错误计数
            self.error_count += 1
            self.last_error_time = datetime.now()
            logger.error(f"处理新消息时出错 (错误计数: {self.error_count}): {str(e)}")
            logger.debug(tb.format_exc())
            
            # 如果错误过多，尝试重新初始化处理程序
            if self.error_count >= 5:
                logger.warning("错误过多，尝试重新初始化处理程序...")
                asyncio.create_task(self.reinitialize_handlers())
    
    async def reinitialize_handlers(self):
        """当发生多次错误时，重新初始化处理程序"""
        try:
            logger.info("开始重新初始化处理程序...")
            # 重置错误计数
            self.error_count = 0
            
            # 重新设置频道监听
            success = await self.setup_channels()
            if success:
                logger.info("成功重新初始化处理程序")
            else:
                logger.error("重新初始化处理程序失败")
        except Exception as e:
            logger.error(f"重新初始化处理程序时出错: {str(e)}")
            logger.debug(tb.format_exc())
    
    async def auto_discover_channels(self):
        """定期自动发现并添加新频道"""
        if not self.auto_discovery_enabled or not self.channel_discovery:
            logger.info("自动发现频道功能已禁用")
            return
            
        try:
            logger.info("开始自动发现新频道")
            new_channels = await self.channel_discovery.auto_add_channels(
                min_members=self.min_members, 
                max_channels=self.max_auto_channels
            )
            
            if new_channels:
                logger.info(f"自动添加了 {len(new_channels)} 个新频道")
                # 更新活跃频道映射
                self.chain_map = self.channel_manager.get_active_channels()
                # 重新注册消息处理程序
                await self.register_handlers()
            else:
                logger.info("没有发现符合条件的新频道")
                
        except Exception as e:
            logger.error(f"自动发现频道时出错: {str(e)}")
            logger.debug(tb.format_exc())
    
    async def start(self):
        """启动服务"""
        max_connection_attempts = 5
        connection_attempt = 0
        
        try:
            # 设置连接超时
            connection_timeout = 60  # 秒
            
            while connection_attempt < max_connection_attempts:
                try:
                    # 尝试连接Telegram
                    logger.info(f"尝试连接Telegram (尝试 {connection_attempt+1}/{max_connection_attempts})...")
                    
                    # 使用超时包装
                    try:
                        await asyncio.wait_for(
                            self.client.connect(),
                            timeout=connection_timeout
                        )
                        # 连接成功，跳出循环
                        logger.info("成功连接到Telegram服务器")
                        break
                    except asyncio.TimeoutError:
                        connection_attempt += 1
                        logger.warning(f"连接Telegram超时 ({connection_attempt}/{max_connection_attempts})")
                        if connection_attempt >= max_connection_attempts:
                            logger.error("多次连接超时，无法连接到Telegram服务器")
                            return False
                        continue
                    
                except Exception as e:
                    connection_attempt += 1
                    logger.error(f"连接Telegram时出错 ({connection_attempt}/{max_connection_attempts}): {str(e)}")
                    
                    if connection_attempt >= max_connection_attempts:
                        logger.critical("多次连接失败，放弃连接")
                        return False
                    
                    # 增加延迟，避免频繁重试
                    wait_time = min(30, 5 * connection_attempt) 
                    logger.info(f"等待 {wait_time} 秒后重试...")
                    await asyncio.sleep(wait_time)
            
            # 检查是否已经授权
            authorization_timeout = 60
            try:
                is_authorized = await asyncio.wait_for(
                    self.client.is_user_authorized(), 
                    timeout=authorization_timeout
                )
            except asyncio.TimeoutError:
                logger.error(f"检查授权状态超时")
                is_authorized = False
                
            if not is_authorized:
                logger.info("用户未登录，开始登录流程...")
                
                try:
                    # 提示用户输入手机号码
                    phone = input("请输入您的手机号码 (包含国家代码，如 +86xxxxxxxxxx): ")
                    
                    # 发送验证码请求（带超时）
                    try:
                        await asyncio.wait_for(
                            self.client.send_code_request(phone),
                            timeout=60
                        )
                    except asyncio.TimeoutError:
                        logger.error("发送验证码请求超时")
                        return False
                    
                    # 提示用户输入验证码
                    code = input("请输入您收到的验证码 (输入'cancel'取消): ")
                    if code.lower() == 'cancel':
                        logger.info("用户取消登录")
                        return False
                        
                    # 登录（带超时）
                    try:
                        await asyncio.wait_for(
                            self.client.sign_in(phone, code),
                            timeout=60
                        )
                    except asyncio.TimeoutError:
                        logger.error("登录请求超时")
                        return False
                    
                    # 检查是否需要两步验证
                    try:
                        is_authorized = await asyncio.wait_for(
                            self.client.is_user_authorized(),
                            timeout=30
                        )
                    except asyncio.TimeoutError:
                        logger.error("检查授权状态超时")
                        is_authorized = False
                        
                    if not is_authorized:
                        # 可能需要两步验证密码
                        password = input("请输入您的两步验证密码 (输入'cancel'取消): ")
                        if password.lower() == 'cancel':
                            logger.info("用户取消登录")
                            return False
                            
                        # 密码登录（带超时）
                        try:
                            await asyncio.wait_for(
                                self.client.sign_in(password=password),
                                timeout=60
                            )
                        except asyncio.TimeoutError:
                            logger.error("两步验证登录请求超时")
                            return False
                    
                    # 登录成功，获取用户信息
                    try:
                        me = await asyncio.wait_for(
                            self.client.get_me(),
                            timeout=30
                        )
                        logger.info(f"登录成功! 已登录为: {me.first_name} (ID: {me.id})")
                    except asyncio.TimeoutError:
                        logger.warning("获取用户信息超时，但登录可能已成功")
                except Exception as e:
                    logger.error(f"登录过程中出错: {str(e)}")
                    logger.debug(tb.format_exc())
                    return False
                
                # 再次检查是否已登录
                try:
                    is_authorized = await asyncio.wait_for(
                        self.client.is_user_authorized(),
                        timeout=30
                    )
                except asyncio.TimeoutError:
                    logger.error("最终检查授权状态超时")
                    is_authorized = False
                    
                if not is_authorized:
                    logger.error("登录失败，请检查凭据后重试")
                    return False
            
            # 设置频道
            try:
                setup_result = await self.setup_channels()
                if not setup_result:
                    logger.warning("设置频道失败，但将继续尝试启动服务")
            except Exception as e:
                logger.error(f"设置频道时出错: {str(e)}")
                logger.debug(tb.format_exc())
                # 即使设置频道失败，也继续启动服务
                
            # 设置运行状态
            self.is_running = True
            
            # 启动批处理任务
            self.batch_task = asyncio.create_task(process_batches())
            
            # 启动健康检查和自动发现任务
            self.health_check_task = asyncio.create_task(self.health_check())
            self.discovery_task = asyncio.create_task(self.discovery_loop())
            
            logger.info("监听服务已启动")
            
            # 返回self，而不是进入无限循环
            return self
            
        except Exception as e:
            self.is_running = False
            logger.error(f"启动监听服务时出错: {str(e)}")
            logger.debug(tb.format_exc())
            return False
    
    async def discovery_loop(self):
        """自动发现频道的循环任务"""
        logger.info(f"启动自动发现频道循环，间隔: {self.discovery_interval}秒")
        
        while self.is_running:
            try:
                # 先等待一段时间，避免启动后立即开始发现
                await asyncio.sleep(60)  # 启动后等待60秒再开始发现
                
                # 执行自动发现
                if self.auto_discovery_enabled:
                    await self.auto_discover_channels()
                
                # 等待下一次执行
                logger.info(f"下一次自动发现将在 {self.discovery_interval} 秒后进行")
                await asyncio.sleep(self.discovery_interval)
                
            except asyncio.CancelledError:
                logger.info("自动发现频道循环已取消")
                break
            except Exception as e:
                logger.error(f"自动发现频道循环出错: {str(e)}")
                logger.debug(tb.format_exc())
                await asyncio.sleep(60)  # 出错后等待一分钟再继续
    
    async def health_check(self):
        """定期检查监听器的健康状态，确保其正常运行"""
        check_interval = 180  # 减少为3分钟检查一次
        reconnect_attempts = 0
        max_reconnect_attempts = 8  # 增加最大重试次数
        
        # 记录启动时间
        if not hasattr(self, '_start_time'):
            self._start_time = datetime.now()
            
        # 创建健康状态文件
        health_file = os.path.join("./logs", "health_status.txt")
        last_reconnect_time = None  # 记录上次重连时间
        
        while self.is_running:
            try:
                # 先检查连接状态，如果断开则立即处理
                if not self.client.is_connected():
                    now = datetime.now()
                    # 记录重连尝试
                    if last_reconnect_time:
                        time_since_last_reconnect = (now - last_reconnect_time).total_seconds()
                        # 如果距离上次重连不到30秒，增加等待时间避免频繁重连
                        if time_since_last_reconnect < 30:
                            await asyncio.sleep(30 - time_since_last_reconnect)
                    
                    logger.warning("检测到客户端已断开连接，正在尝试重新连接...")
                    last_reconnect_time = datetime.now()
                    
                    try:
                        # 首先尝试简单重连
                        await self.client.connect()
                        logger.info("客户端已重新连接")
                        reconnect_attempts = 0
                    except Exception as e:
                        reconnect_attempts += 1
                        logger.error(f"重新连接失败 (尝试 {reconnect_attempts}/{max_reconnect_attempts}): {str(e)}")
                        
                        # 如果多次重连失败，尝试完全重启客户端
                        if reconnect_attempts >= max_reconnect_attempts:
                            logger.critical(f"多次重连失败，尝试重新启动客户端...")
                            
                            try:
                                # 先尝试安全断开
                                try:
                                    if self.client.is_connected():
                                        await self.client.disconnect()
                                except:
                                    pass  # 忽略断开连接的错误
                                
                                # 等待一段较长时间，让网络或服务器状态恢复
                                await asyncio.sleep(30)
                                
                                # 删除旧的session文件，避免可能的损坏
                                session_files = [f"{self.session_path}.session", f"{self.session_path}.session-journal"]
                                for file in session_files:
                                    if os.path.exists(file):
                                        try:
                                            os.remove(file)
                                            logger.info(f"已删除可能损坏的会话文件: {file}")
                                        except:
                                            pass
                                
                                # 重新创建会话文件路径
                                session_name = f'tg_session_{os.getpid()}_{int(time.time())}'
                                self.session_path = os.path.join(os.path.dirname(self.session_path), session_name)
                                
                                # 重新创建客户端
                                self.client = TelegramClient(
                                    self.session_path,
                                    self.api_id, 
                                    self.api_hash,
                                    connection_retries=self.connection_retries,
                                    auto_reconnect=self.auto_reconnect,
                                    retry_delay=self.retry_delay,
                                    request_retries=self.request_retries,
                                    flood_sleep_threshold=self.flood_sleep_threshold,
                                    timeout=30  # 设置更长的超时时间
                                )
                                
                                # 连接并重新初始化
                                await self.client.connect()
                                
                                if not await self.client.is_user_authorized():
                                    logger.error("重新连接后用户未授权，请检查认证状态")
                                    # 继续执行，让用户手动处理认证问题
                                
                                await self.setup_channels()
                                logger.info("客户端已成功重启和重新初始化")
                                reconnect_attempts = 0
                                
                            except Exception as restart_error:
                                logger.critical(f"重启客户端失败: {str(restart_error)}")
                                logger.debug(tb.format_exc())
                                # 长时间等待后再尝试
                                await asyncio.sleep(60)
                
                # 记录健康检查开始
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"开始健康检查 - {now}")
                
                # 健康状态
                health_status = {
                    "timestamp": now,
                    "client_connected": self.client.is_connected(),
                    "active_channels": len(self.chain_map),
                    "registered_handlers": len(self.event_handlers),
                    "error_count": self.error_count,
                    "last_error_time": str(self.last_error_time) if self.last_error_time else "无",
                    "uptime_hours": (datetime.now() - self._start_time).total_seconds() / 3600,
                    "reconnect_attempts": reconnect_attempts,
                    "last_reconnect_time": str(last_reconnect_time) if last_reconnect_time else "无"
                }
                
                # 保存健康状态到文件
                with open(health_file, "w") as f:
                    for key, value in health_status.items():
                        f.write(f"{key}: {value}\n")
                
                # 验证活跃频道
                if not self.chain_map and self.client.is_connected():
                    logger.warning("没有活跃的频道，尝试重新设置...")
                    try:
                        await self.setup_channels()
                        logger.info("频道设置已更新")
                    except Exception as e:
                        logger.error(f"重新设置频道时出错: {str(e)}")
                
                # 检查消息处理器
                if (not self.event_handlers or 'new_message' not in self.event_handlers) and self.client.is_connected():
                    logger.warning("消息处理器未注册，尝试重新注册...")
                    try:
                        await self.register_handlers()
                        logger.info("消息处理器已重新注册")
                    except Exception as e:
                        logger.error(f"重新注册处理器时出错: {str(e)}")
                
                # 检查错误情况
                if self.last_error_time and self.error_count > 0:
                    time_since_error = (datetime.now() - self.last_error_time).total_seconds()
                    if time_since_error > 3600:  # 1小时内没有新错误，重置计数
                        self.error_count = 0
                        self.last_error_time = None
                        logger.info("错误计数已重置")
                    # 如果错误计数过高但未达到自动重初始化的阈值，减少一些计数
                    elif self.error_count > 2 and time_since_error > 1800:  # 30分钟
                        self.error_count -= 1
                        logger.info(f"错误计数减少: {self.error_count+1} -> {self.error_count}")
                
                # 检查系统资源
                try:
                    import psutil
                    process = psutil.Process(os.getpid())
                    memory_usage_mb = process.memory_info().rss / 1024 / 1024
                    logger.info(f"当前内存使用: {memory_usage_mb:.2f} MB")
                    
                    # 如果内存使用超过1GB，记录警告
                    if memory_usage_mb > 1024:
                        logger.warning(f"内存使用较高: {memory_usage_mb:.2f} MB")
                except ImportError:
                    logger.info("psutil未安装，无法获取系统资源信息")
                except Exception as e:
                    logger.error(f"获取系统资源信息出错: {str(e)}")
                
                # 健康检查成功
                logger.info(f"健康检查完成，监听器状态正常 - 活跃频道: {len(self.chain_map)}, 消息处理器: {len(self.event_handlers)}")
                
            except asyncio.CancelledError:
                logger.info("健康检查任务被取消")
                break
            except Exception as e:
                logger.error(f"健康检查时出错: {str(e)}")
                logger.debug(tb.format_exc())
            
            # 等待下一次检查
            try:
                await asyncio.sleep(check_interval)
            except asyncio.CancelledError:
                logger.info("健康检查睡眠被取消")
                break
    
    async def stop(self):
        """停止监听服务并释放资源"""
        logger.info("正在停止Telegram监听服务...")
        
        # 设置运行状态为False，使各循环能够正常退出
        self.is_running = False
        
        # 移除所有事件处理器
        for handler in list(self.event_handlers.values()):
            self.client.remove_event_handler(handler)
        self.event_handlers.clear()
        logger.info("已移除所有事件处理器")
        
        # 取消批处理任务
        if self.batch_task and not self.batch_task.done():
            self.batch_task.cancel()
            try:
                await self.batch_task
            except asyncio.CancelledError:
                pass
            logger.info("批处理任务已取消")
            
        # 取消健康检查和自动发现任务
        for task_name, task in [('health_check_task', getattr(self, 'health_check_task', None)), 
                               ('discovery_task', getattr(self, 'discovery_task', None))]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                logger.info(f"{task_name}已取消")
        
        # 断开与Telegram的连接
        if self.client and self.client.is_connected():
            await self.client.disconnect()
            logger.info("已断开与Telegram的连接")
        
        # 清理会话文件
        try:
            session_files = [f"{self.session_path}.session", f"{self.session_path}.session-journal"]
            for file in session_files:
                if os.path.exists(file):
                    os.remove(file)
                    logger.info(f"已删除会话文件: {file}")
        except Exception as e:
            logger.warning(f"清理会话文件时出错: {str(e)}")
        
        logger.info("Telegram监听服务已完全停止")
        return True

# 设置日志格式
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("logs/telegram_listener.log"),
            logging.StreamHandler()
        ]
    )

# 全局客户端实例，用于兼容旧代码
client = None

def run_listener():
    """启动 Telegram 监听服务"""
    # 设置日志
    setup_logging()
    
    # 确保数据库表存在
    init_db()
    
    # 创建必要目录
    os.makedirs('./media', exist_ok=True)
    os.makedirs('./data', exist_ok=True)
    os.makedirs('./logs', exist_ok=True)
    
    # 使用新的类
    listener = TelegramListener()
    loop = asyncio.get_event_loop()
    
    try:
        loop.run_until_complete(listener.start())
    except KeyboardInterrupt:
        logger.info("接收到键盘中断，正在优雅关闭...")
        loop.run_until_complete(listener.stop())
        logger.info("监听器已完全停止")
    except Exception as e:
        logger.error(f"监听器出错: {str(e)}")
        logger.debug(tb.format_exc())
        # 仍然尝试优雅关闭
        try:
            loop.run_until_complete(listener.stop())
        except Exception as stop_error:
            logger.error(f"停止监听器时出错: {str(stop_error)}")
    finally:
        # 确保循环关闭
        if not loop.is_closed():
            loop.close()