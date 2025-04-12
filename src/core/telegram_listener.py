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
from functools import wraps

import telethon
from telethon import TelegramClient, events
from telethon.tl.types import Channel, Chat, PeerChannel, PeerChat, PeerUser
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest
# 添加Telethon错误类型导入，用于处理登录限流问题
from telethon.errors.rpcerrorlist import (
    FloodWaitError, 
    PhoneCodeInvalidError, 
    PhoneCodeExpiredError, 
    PasswordHashInvalidError,
    SessionPasswordNeededError
)

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# 导入项目模块
from src.utils.logger import get_logger
from src.core.channel_manager import ChannelManager
from src.database.models import TelegramChannel
from src.database.db_handler import (
    extract_promotion_info, 
    process_message_batch, token_batch,
    process_batches, cleanup_batch_tasks
)
import config.settings as config
from src.utils.utils import parse_market_cap, format_market_cap
from src.core.channel_discovery import ChannelDiscovery
import traceback

# 将现有日志替换为我们的统一日志模块
try:
    from src.utils.logger import get_logger
    logger = get_logger(__name__)
except ImportError:
    # 兼容性代码，使用原有的logger
    logger = logging.getLogger(__name__)

# 加载环境变量

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
        """初始化Telegram监听器"""
        
        # 从配置中获取API认证信息
        try:
            # 直接从config模块获取配置
            import config.settings as config
            self.api_id = config.env_config.API_ID
            self.api_hash = config.env_config.API_HASH
            logger.info("已从环境变量中获取API ID和API HASH")
            
            # 验证API ID和API HASH是否有效
            if not self.api_id or not self.api_hash:
                logger.error("API ID或API HASH无效，请检查.env文件")
                raise ValueError("API ID或API HASH无效，请检查.env文件")
                
        except Exception as e:
            logger.error(f"获取API认证信息时出错: {str(e)}")
            # 尝试直接从环境变量获取，作为最后的尝试
            self.api_id = int(os.getenv('TG_API_ID', '0'))
            self.api_hash = os.getenv('TG_API_HASH', '')
            if not self.api_id or not self.api_hash:
                logger.critical("无法获取有效的API ID和API HASH，无法初始化Telegram客户端")
                raise ValueError("无法获取有效的API ID和API HASH，无法初始化Telegram客户端")
        
        # 初始化会话参数
        self.session_dir = os.path.join('./data')
        # 使用进程ID和时间戳创建唯一的会话名称
        session_name = f'tg_session_{os.getpid()}_{int(time.time())}'
        self.session_path = os.path.join(self.session_dir, session_name)
        
        # 确保目录存在
        os.makedirs(self.session_dir, exist_ok=True)
        
        # 设置连接参数
        self.connection_retries = 5  # 连接重试次数
        self.auto_reconnect = True   # 自动重连
        self.retry_delay = 5         # 重试延迟时间（秒）
        self.request_retries = 5     # 请求重试次数
        self.flood_sleep_threshold = 60  # 洪水睡眠阈值
        
        # 初始化Telegram客户端
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
        self.auto_discovery_enabled = config.AUTO_CHANNEL_DISCOVERY
        self.discovery_interval = config.DISCOVERY_INTERVAL
        self.min_members = config.MIN_CHANNEL_MEMBERS
        self.max_auto_channels = config.MAX_AUTO_CHANNELS
    
    @async_retry(max_retries=3, delay=2, exceptions=(ConnectionError, TimeoutError))
    async def setup_channels(self):
        """设置频道监听"""
        try:
            logger.info("开始设置频道监听...")
            
            # 确保客户端已连接
            if not self.client.is_connected():
                logger.warning("客户端未连接，尝试连接...")
                await self.client.connect()
                
            # 验证连接状态
            if not self.client.is_connected():
                logger.error("无法连接到Telegram服务器，请检查网络连接")
                return False
                
            # 获取活跃的频道
            active_channels = self.channel_manager.get_active_channels()
            
            if not active_channels:
                logger.warning("未找到活跃频道，请手动添加频道")
                return False
            
            # 注册消息处理器
            self.chain_map = active_channels
            await self.register_handlers(active_channels)
            logger.info(f"已注册消息处理器，监听 {len(active_channels)} 个活跃频道")
            return True
            
        except Exception as e:
            logger.error(f"设置频道监听时出错: {str(e)}")
            logger.debug(tb.format_exc())
            return False
    
    @async_retry(max_retries=2, delay=1)
    async def register_handlers(self, active_channels=None):
        """注册消息处理程序
        
        Args:
            active_channels: 可选的活跃频道列表，如果为None则自动获取
            
        Returns:
            bool: 是否成功注册处理程序
        """
        try:
            # 获取活跃频道
            if active_channels is None:
                # 使用channel_manager获取所有活跃频道
                active_channels = self.channel_manager.get_active_channels()
            
            if not active_channels:
                logger.warning("没有活跃的频道，无法注册消息处理程序")
                return False
                
            # 打印当前活跃频道数量
            logger.info(f"注册处理程序: {len(active_channels)} 个活跃频道")
                
            # 准备要监听的实体
            entities = []
            entity_names = []
            
            # 构建实体字典
            self.channel_entities = {}
            self.chain_map = {}
            
            # 添加每个频道或群组
            for channel in active_channels:
                try:
                    # 优先使用channel_id
                    channel_id = channel.get('channel_id')
                    channel_username = channel.get('channel_username')
                    chain = channel.get('chain')
                    
                    if not channel_id and not channel_username:
                        logger.warning(f"跳过缺少ID和用户名的频道: {channel}")
                        continue
                        
                    # 记录频道ID格式
                    if channel_id:
                        if isinstance(channel_id, int):
                            id_type = "正整数" if channel_id > 0 else "负整数"
                            logger.info(f"频道ID格式: {id_type}, 值: {channel_id}")
                        else:
                            logger.info(f"频道ID类型: {type(channel_id)}, 值: {channel_id}")
                    
                    # 添加到监听实体列表
                    entity = None
                    
                    # 尝试初始化实体类型
                    if channel_id:
                        # 优先使用ID
                        try:
                            # 如果是正整数，尝试转换为频道格式
                            if isinstance(channel_id, int) and channel_id > 0:
                                try:
                                    # 尝试将正数ID作为频道处理（添加-100前缀）
                                    entity = await self.client.get_entity(PeerChannel(-1000000000000 - channel_id))
                                    logger.info(f"成功将正整数ID {channel_id} 转换为频道实体")
                                except Exception as e:
                                    logger.warning(f"无法将正整数ID {channel_id} 转换为频道实体: {str(e)}")
                                    # 回退到原始ID
                                    entity = await self.client.get_entity(PeerChannel(channel_id))
                            else:
                                # 直接使用ID
                                entity = await self.client.get_entity(PeerChannel(channel_id))
                        except ValueError as e:
                            logger.warning(f"无法通过ID {channel_id} 获取频道实体: {str(e)}")
                            if channel_username:
                                try:
                                    # 尝试使用用户名
                                    entity = await self.client.get_entity(channel_username)
                                except Exception as e2:
                                    logger.error(f"无法通过用户名 {channel_username} 获取频道实体: {str(e2)}")
                                    continue
                            else:
                                continue
                    elif channel_username:
                        # 如果没有ID但有用户名
                        try:
                            entity = await self.client.get_entity(channel_username)
                        except Exception as e:
                            logger.error(f"无法通过用户名 {channel_username} 获取频道实体: {str(e)}")
                            continue
                    
                    if entity:
                        entities.append(entity)
                        entity_name = getattr(entity, 'title', None) or channel_username or f"ID:{channel_id}"
                        entity_names.append(entity_name)
                        
                        # 保存实体和链的映射
                        entity_key = str(entity.id)
                        self.channel_entities[entity_key] = entity
                        self.chain_map[entity_key] = chain
                except Exception as e:
                    logger.error(f"处理频道时出错: {channel.get('channel_name', 'unknown')}, 错误: {str(e)}")
                    continue
            
            # 检查是否有需要监听的实体
            if not entities:
                logger.warning("没有有效的频道实体，无法注册消息处理程序")
                return False
                
            # 注册新消息处理程序
            @self.client.on(events.NewMessage(chats=entities))
            async def handler(event):
                await self.handle_new_message(event)
                
            logger.info(f"已注册消息处理程序，监听实体: {', '.join(entity_names)}")
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
            from telethon.tl.types import PeerUser, PeerChannel, PeerChat
            
            # 检查是否为用户ID
            if isinstance(channel_id, PeerUser):
                logger.warning(f"尝试获取用户实体的成员数，这是不可能的: {channel_id}")
                return 0
                
            # 处理整数ID
            if isinstance(channel_id, int):
                # 在数据库中，频道ID可能被存储为正数
                # 如果是正数ID，尝试转换为频道格式后再获取
                if channel_id > 0:
                    logger.info(f"检测到正整数频道ID: {channel_id}，尝试转换为频道格式")
                    try:
                        # 尝试将正整数ID转换为频道格式（添加-100前缀）
                        # 这是Telegram内部存储格式的一种处理方式
                        channel_entity = await self.client.get_entity(PeerChannel(-1000000000000 - channel_id))
                        full_channel = await self.client(GetFullChannelRequest(channel=channel_entity))
                        members_count = getattr(full_channel.full_chat, 'participants_count', 0)
                        logger.info(f"成功获取转换后频道 {channel_id} 的成员数: {members_count}")
                        return members_count
                    except Exception as e:
                        logger.warning(f"尝试将 {channel_id} 转换为频道格式后获取成员数失败: {str(e)}")
                        # 如果转换失败，尝试原始方法
            
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
            # 记录频道ID格式
            id_type = "正整数" if channel_id > 0 else "负整数"
            logger.debug(f"处理消息: 频道ID格式: {id_type}, 值: {channel_id}")
        
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
                    # 使用数据库适配器获取频道信息
                    from src.database.db_factory import get_db_adapter
                    db_adapter = get_db_adapter()
                    
                    try:
                        # 尝试获取频道
                        channel = await db_adapter.get_channel_by_id(channel_id)
                        
                        if channel and channel.get('member_count') != member_count:
                            logger.info(f"更新频道 {channel_id} 的成员数: {channel.get('member_count')} -> {member_count}")
                            channel['member_count'] = member_count
                            channel['last_updated'] = datetime.now()
                            await db_adapter.save_channel(channel)
                    except Exception as e:
                        logger.error(f"更新频道 {channel_id} 成员数时出错: {str(e)}")
            except Exception as e:
                logger.warning(f"获取频道 {channel_id} 成员数时出错: {str(e)}")
        
        # 获取消息的基本信息
        message_id = message.id
        date = message.date
        text = message.text or message.message or ""
        
        # 确定该消息所属的区块链
        channel_chain = None
        
        try:
            # 由于ChannelManager已修改，获取所有活跃频道
            active_channels = self.channel_manager.get_active_channels()
            
            # 查找与当前消息匹配的频道
            for channel in active_channels:
                if (channel.get('channel_id') and channel.get('channel_id') == channel_id) or \
                   (channel.get('channel_username') and hasattr(event.chat, 'username') and 
                    channel.get('channel_username') == event.chat.username):
                    channel_chain = channel.get('chain')
                    break
                
            if not channel_chain:
                # 尝试从消息内容中提取区块链信息
                from src.database.db_handler import extract_chain_from_message
                extracted_chain = extract_chain_from_message(text)
                if extracted_chain:
                    channel_chain = extracted_chain
                    logger.info(f"从消息内容中提取到区块链: {channel_chain}")
                else:
                    logger.warning(f"无法确定消息的区块链，跳过处理: channel_id={channel_id}")
                    return
        except Exception as e:
            logger.error(f"确定消息区块链时出错: {str(e)}")
            return
        
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
                
        # 如果通过ID没有找到，则尝试通过上下文从活跃频道中查找
        if not chain:
            try:
                active_channels = self.channel_manager.get_active_channels()
                for active_channel in active_channels:
                    if (active_channel.get('channel_id') == channel_id or 
                        (active_channel.get('channel_username') and 
                         hasattr(event.chat, 'username') and 
                         active_channel.get('channel_username') == event.chat.username)):
                        chain = active_channel.get('chain')
                        # 添加到chain_map以加速后续查询
                        if channel_id:
                            self.chain_map[str(channel_id)] = chain
                        break
            except Exception as e:
                logger.error(f"查找频道链信息时出错: {str(e)}")
            
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
            
            try:
                # 使用数据库适配器获取连接
                from src.database.db_factory import get_db_adapter
                db_adapter = get_db_adapter()
                
                # 使用新的数据库函数保存消息
                message_data = {
                    'chain': chain,
                    'message_id': message.id,
                    'date': message.date,
                    'text': message.text,
                    'media_path': media_path,
                    'channel_id': channel_id  # 只使用channel_id字段
                }
                saved = await db_adapter.save_message(message_data)
                
                # 如果消息已存在，则不继续处理
                if not saved:
                    return
                
                # 从消息中提取 promotion 信息
                promo = None
                if message.text:
                    try:
                        promo = extract_promotion_info(message.text, message.date, chain, message.id, channel_id)
                        logger.debug(f"extract_promotion_info 返回值: {promo}")
                    except Exception as e:
                        logger.error(f"提取 promotion 信息时出错: {str(e)}")
                        logger.debug(tb.format_exc())
                
                # 如果找到合约地址，调用DEX API获取完整信息
                if promo and promo.contract_address:
                    logger.info(f"找到合约地址: {promo.contract_address}，开始通过DEX API获取完整信息")
                    try:
                        # 使用合约地址调用DEX API
                        from src.api.token_market_updater import update_token_market_data_async

                        # 确保chain值有效
                        if not promo.chain or promo.chain == "UNKNOWN":
                            # 尝试从消息中再次提取链信息
                            from src.database.db_handler import extract_chain_from_message
                            extracted_chain = extract_chain_from_message(message.text)
                            if extracted_chain:
                                promo.chain = extracted_chain
                                logger.info(f"从消息中提取到链信息: {promo.chain}")
                            else:
                                # 如果未确定链，尝试常见链
                                test_chains = ["solana", "ethereum", "bsc", "arbitrum", "base", "optimism"]
                                for test_chain in test_chains:
                                    try:
                                        # 尝试调用DEX API
                                        from src.api.token_market_updater import _normalize_chain_id
                                        chain_id = _normalize_chain_id(test_chain)
                                        if chain_id:
                                            logger.info(f"尝试在链 {test_chain} 上查询合约地址 {promo.contract_address}")
                                            result = await update_token_market_data_async(test_chain, promo.contract_address)
                                            if "error" not in result:
                                                # 找到有效链
                                                chain_map = {
                                                    "solana": "SOL",
                                                    "ethereum": "ETH",
                                                    "bsc": "BSC",
                                                    "arbitrum": "ARB",
                                                    "base": "BASE",
                                                    "optimism": "OP"
                                                }
                                                promo.chain = chain_map.get(test_chain, test_chain.upper())
                                                logger.info(f"通过DEX API确定链为: {promo.chain}")
                                                break
                                    except Exception as e_inner:
                                        logger.warning(f"尝试在链 {test_chain} 上查询时出错: {str(e_inner)}")
                        
                        # 使用确定的链调用API
                        if promo.chain and promo.chain != "UNKNOWN":
                            # 传递所有必要的参数
                            result = await update_token_market_data_async(
                                promo.chain, 
                                promo.contract_address, 
                                message_id=message.id, 
                                channel_id=channel_id,
                                risk_level=promo.risk_level if hasattr(promo, 'risk_level') else None,
                                promotion_count=promo.promotion_count if hasattr(promo, 'promotion_count') else 1
                            )
                            
                            if "error" not in result:
                                # 检查是否是新创建的代币 (is_new字段仅用于API响应和日志区分，不存储在数据库中)
                                is_new_token = result.get("is_new", False)
                                if is_new_token:
                                    logger.info(f"成功创建新代币 {promo.chain}/{promo.contract_address} 的信息")
                                else:
                                    logger.info(f"成功更新代币 {promo.chain}/{promo.contract_address} 的信息")
                                
                                # 获取当前时间格式化为字符串
                                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                
                                # 保存代币信息到数据库 - 由于我们已经在update_token_market_data_async中处理了保存逻辑
                                # 这里我们只需要保存代币标记信息
                                token_symbol = result.get('symbol', '未知')
                                
                                # 保存代币标记信息
                                token_mark_data = {
                                    'chain': promo.chain,
                                    'token_symbol': token_symbol,
                                    'contract': promo.contract_address,
                                    'message_id': message.id,
                                    'market_cap': result.get('marketCap', 0),
                                    'channel_id': channel_id
                                }
                                
                                # 保存代币标记
                                mark_result = await db_adapter.save_token_mark(token_mark_data)
                                if mark_result:
                                    logger.info(f"成功保存代币标记信息: {token_symbol}")
                                else:
                                    logger.warning(f"保存代币标记信息失败: {token_symbol}")
                            else:
                                logger.warning(f"通过DEX API获取代币信息失败: {result.get('error')}")
                        else:
                            logger.warning(f"未能确定合约地址 {promo.contract_address} 所属的链，无法调用DEX API")
                    except Exception as e:
                        logger.error(f"处理代币信息时出错: {str(e)}")
                        logger.debug(tb.format_exc())
                else:
                    logger.info("未从消息中提取到合约地址，跳过代币信息处理")
                
                # 重置错误计数，表示处理成功
                self.error_count = 0
                self.last_error_time = None
                
                # 记录处理时间
                process_time = time.time() - start_time
                logger.debug(f"消息处理完成，耗时: {process_time:.2f}秒")
                
            except Exception as e:
                logger.error(f"获取数据库连接或更新代币信息时出错: {str(e)}")
                logger.debug(tb.format_exc())
            
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
        if not self.auto_discovery_enabled:
            logger.info("自动发现频道功能已在配置中禁用")
            return
            
        if not self.channel_discovery:
            logger.warning("频道发现器未初始化，无法执行自动发现功能")
            try:
                # 尝试重新初始化频道发现器
                from src.core.channel_discovery import ChannelDiscovery
                self.channel_discovery = ChannelDiscovery(self.client, self.channel_manager)
                logger.info("已重新初始化频道发现器")
            except Exception as e:
                logger.error(f"重新初始化频道发现器失败: {str(e)}")
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
            # 处理操作系统差异
            import platform
            is_windows = platform.system() == 'Windows'
            
            # 在Windows环境下优化会话文件路径和名称
            if is_windows:
                # Windows环境下使用更简单的会话名称，避免使用进程ID
                session_name = f'tg_session_win_{int(time.time())}'
                self.session_path = os.path.join(self.session_dir, session_name)
                
                # 重新初始化客户端使用新的会话路径
                logger.info(f"Windows环境：使用简化的会话名称: {session_name}")
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
                    
                    # 发送验证码请求（带超时和FloodWaitError处理）
                    try:
                        await asyncio.wait_for(
                            self.client.send_code_request(phone),
                            timeout=60
                        )
                    except telethon.errors.rpcerrorlist.FloodWaitError as flood_error:
                        # 解析错误信息中的等待时间（单位为秒）
                        wait_seconds = getattr(flood_error, 'seconds', 0)
                        if not wait_seconds:
                            # 如果无法从错误属性中获取等待时间，尝试从错误消息中提取
                            import re
                            match = re.search(r'wait of (\d+) seconds', str(flood_error))
                            if match:
                                wait_seconds = int(match.group(1))
                            else:
                                wait_seconds = 3600  # 默认等待1小时
                        
                        # 转换为易读的时间格式
                        wait_minutes = wait_seconds // 60
                        wait_hours = wait_minutes // 60
                        remaining_minutes = wait_minutes % 60
                        
                        if wait_hours > 0:
                            wait_msg = f"{wait_hours}小时{remaining_minutes}分钟"
                        else:
                            wait_msg = f"{wait_minutes}分钟"
                        
                        logger.error(f"Telegram API限流: 需要等待{wait_msg}后才能继续。错误: {str(flood_error)}")
                        print(f"\n⚠️ 您的账号或IP已被Telegram限流，需要等待{wait_msg}后才能重试登录。")
                        print(f"⚠️ 限流原因: 可能是短时间内多次尝试登录或存在异常活动。")
                        print(f"⚠️ 请稍后再试或使用其他账号/网络环境。")
                        
                        # 将限流信息保存到文件，以便后续查看
                        try:
                            with open("./logs/flood_wait_info.txt", "w") as f:
                                f.write(f"限流发生时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                                f.write(f"需要等待时间: {wait_msg} ({wait_seconds}秒)\n")
                                f.write(f"限流错误详情: {str(flood_error)}\n")
                                f.write(f"手机号: {phone}\n")
                        except Exception as file_error:
                            logger.error(f"保存限流信息到文件时出错: {str(file_error)}")
                        
                        return False
                    except asyncio.TimeoutError:
                        logger.error("发送验证码请求超时")
                        print("\n⚠️ 发送验证码请求超时，请检查网络连接后重试。")
                        return False
                    except Exception as e:
                        logger.error(f"发送验证码时发生错误: {str(e)}")
                        print(f"\n⚠️ 发送验证码失败: {str(e)}")
                        print("请检查手机号是否正确，或网络连接是否稳定后重试。")
                        return False
                    
                    # 提示用户输入验证码
                    code = input("请输入您收到的验证码 (输入'cancel'取消): ")
                    if code.lower() == 'cancel':
                        logger.info("用户取消登录")
                        return False
                        
                    # 登录（带超时和FloodWaitError处理）
                    try:
                        await asyncio.wait_for(
                            self.client.sign_in(phone, code),
                            timeout=60
                        )
                    except telethon.errors.rpcerrorlist.FloodWaitError as flood_error:
                        # 处理FloodWaitError，类似于上面的处理
                        wait_seconds = getattr(flood_error, 'seconds', 3600)  # 默认1小时
                        wait_minutes = wait_seconds // 60
                        wait_hours = wait_minutes // 60
                        remaining_minutes = wait_minutes % 60
                        
                        if wait_hours > 0:
                            wait_msg = f"{wait_hours}小时{remaining_minutes}分钟"
                        else:
                            wait_msg = f"{wait_minutes}分钟"
                        
                        logger.error(f"登录时遇到限流: 需要等待{wait_msg}。错误: {str(flood_error)}")
                        print(f"\n⚠️ 登录过程中被Telegram限流，需要等待{wait_msg}后才能重试")
                        return False
                    except telethon.errors.PhoneCodeInvalidError:
                        logger.error("验证码无效")
                        print("\n⚠️ 验证码无效，请确保输入了正确的验证码")
                        return False
                    except telethon.errors.PhoneCodeExpiredError:
                        logger.error("验证码已过期")
                        print("\n⚠️ 验证码已过期，请重新获取验证码")
                        return False
                    except asyncio.TimeoutError:
                        logger.error("登录请求超时")
                        print("\n⚠️ 登录请求超时，请检查网络连接后重试")
                        return False
                    except Exception as e:
                        logger.error(f"登录过程中出错: {str(e)}")
                        print(f"\n⚠️ 登录失败: {str(e)}")
                        logger.debug(tb.format_exc())
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
                            
                        # 密码登录（带超时和FloodWaitError处理）
                        try:
                            await asyncio.wait_for(
                                self.client.sign_in(password=password),
                                timeout=60
                            )
                        except telethon.errors.rpcerrorlist.FloodWaitError as flood_error:
                            wait_seconds = getattr(flood_error, 'seconds', 3600)
                            wait_minutes = wait_seconds // 60
                            wait_hours = wait_minutes // 60
                            remaining_minutes = wait_minutes % 60
                            
                            if wait_hours > 0:
                                wait_msg = f"{wait_hours}小时{remaining_minutes}分钟"
                            else:
                                wait_msg = f"{wait_minutes}分钟"
                            
                            logger.error(f"两步验证时遇到限流: 需要等待{wait_msg}。错误: {str(flood_error)}")
                            print(f"\n⚠️ 两步验证过程中被Telegram限流，需要等待{wait_msg}后才能重试")
                            return False
                        except telethon.errors.PasswordHashInvalidError:
                            logger.error("两步验证密码无效")
                            print("\n⚠️ 两步验证密码无效，请确保输入了正确的密码")
                            return False
                        except asyncio.TimeoutError:
                            logger.error("两步验证登录请求超时")
                            print("\n⚠️ 两步验证登录请求超时，请检查网络连接后重试")
                            return False
                        except Exception as e:
                            logger.error(f"两步验证过程中出错: {str(e)}")
                            print(f"\n⚠️ 两步验证失败: {str(e)}")
                            return False
                    
                    # 登录成功，获取用户信息
                    try:
                        me = await asyncio.wait_for(
                            self.client.get_me(),
                            timeout=30
                        )
                        logger.info(f"登录成功! 已登录为: {me.first_name} (ID: {me.id})")
                        print(f"\n✅ 登录成功! 您已登录为: {me.first_name} (ID: {me.id})")
                    except asyncio.TimeoutError:
                        logger.warning("获取用户信息超时，但登录可能已成功")
                        print("\n⚠️ 获取用户信息超时，但登录可能已成功")
                except telethon.errors.rpcerrorlist.FloodWaitError as flood_error:
                    # 捕获整个登录流程中的FloodWaitError
                    wait_seconds = getattr(flood_error, 'seconds', 3600)
                    wait_minutes = wait_seconds // 60
                    wait_hours = wait_minutes // 60
                    remaining_minutes = wait_minutes % 60
                    
                    if wait_hours > 0:
                        wait_msg = f"{wait_hours}小时{remaining_minutes}分钟"
                    else:
                        wait_msg = f"{wait_minutes}分钟"
                    
                    logger.error(f"登录流程中遇到限流: 需要等待{wait_msg}。错误: {str(flood_error)}")
                    print(f"\n⚠️ 登录流程中被Telegram限流，需要等待{wait_msg}后才能重试")
                    return False
                except Exception as e:
                    logger.error(f"登录过程中出错: {str(e)}")
                    print(f"\n⚠️ 登录过程中发生错误: {str(e)}")
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
                    print("\n⚠️ 登录失败，请检查您的凭据后重试")
                    return False
            
            # 初始化频道发现器（用于自动发现新频道）
            if self.auto_discovery_enabled:
                try:
                    from src.core.channel_discovery import ChannelDiscovery
                    self.channel_discovery = ChannelDiscovery(self.client, self.channel_manager)
                    logger.info("频道发现器已初始化，自动发现功能已启用")
                except Exception as e:
                    logger.error(f"初始化频道发现器时出错，自动发现功能将禁用: {str(e)}")
                    self.auto_discovery_enabled = False
                    self.channel_discovery = None
            else:
                logger.info("自动发现频道功能已手动禁用")
                
            # 设置活跃频道并注册处理程序
            await self.setup_channels()
            
            # 设置运行状态
            self.is_running = True
            
            # 根据操作系统不同使用不同的批处理策略
            if is_windows:
                # Windows环境下在当前进程中运行批处理
                logger.info("Windows环境：在当前进程中运行批处理任务")
                self.batch_task = asyncio.create_task(process_batches())
            else:
                # Linux环境下可以安全使用进程间通信
                logger.info("非Windows环境：使用标准方式启动批处理任务")
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
        last_discovery_time = None
        
        while self.is_running:
            try:
                # 先等待一段时间，避免启动后立即开始发现
                await asyncio.sleep(60)  # 启动后等待60秒再开始发现
                
                current_time = datetime.now()
                # 限制自动发现的频率，避免短时间内多次执行
                if last_discovery_time and (current_time - last_discovery_time).total_seconds() < 300:
                    logger.info(f"距离上次自动发现仅过去了 {(current_time - last_discovery_time).total_seconds():.1f} 秒，跳过本次执行")
                    await asyncio.sleep(60)
                    continue
                
                # 更新现有频道的状态和信息
                logger.info("开始更新已有频道的状态和信息...")
                updated_channels = await self.channel_manager.update_channels()
                if updated_channels:
                    logger.info(f"已更新 {len(updated_channels)} 个频道的状态和信息")
                    # 更新活跃频道映射
                    self.chain_map = self.channel_manager.get_active_channels()
                    # 重新注册消息处理程序
                    await self.register_handlers()
                else:
                    logger.info("没有需要更新的频道")
                
                # 执行自动发现
                if self.auto_discovery_enabled:
                    await self.auto_discover_channels()
                    last_discovery_time = datetime.now()
                
                # 等待下一次执行
                logger.info(f"下一次自动发现和更新将在 {self.discovery_interval} 秒后进行")
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
                if (not self.event_handlers or len(self.event_handlers) == 0) and self.client.is_connected():
                    logger.warning("消息处理器未注册，尝试重新注册...")
                    try:
                        # 使用已缓存的活跃频道，避免重复查询数据库
                        existing_channels = self.channel_manager._active_channels_cache
                        if not existing_channels:
                            existing_channels = self.channel_manager.get_active_channels()
                            
                        if existing_channels:
                            await self.register_handlers(existing_channels)
                            logger.info("消息处理器已重新注册")
                        else:
                            logger.warning("没有活跃频道可供注册")
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
                active_channel_count = len(self.channel_manager.get_active_channels())
                logger.info(f"健康检查完成，监听器状态正常 - 活跃频道: {active_channel_count}, 消息处理器: {len(self.event_handlers)}")
                
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