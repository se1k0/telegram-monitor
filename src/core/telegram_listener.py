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
from src.core.telegram_client_factory import TelegramClientFactory
from src.database.models import TelegramChannel
from src.database.db_handler import (
    extract_promotion_info, 
    process_message_batch, token_batch,
    process_batches, cleanup_batch_tasks,
    save_telegram_message,
    extract_chain_from_message
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
        self.session_dir = os.path.join('./data/sessions')  # 专门的sessions目录
        self.session_backup_dir = os.path.join(self.session_dir, 'backups')  # 备份目录
        
        # 确保目录存在
        os.makedirs(self.session_dir, exist_ok=True)
        os.makedirs(self.session_backup_dir, exist_ok=True)
        
        # 初始化session路径
        self._init_session_path()
        
        # 设置连接参数
        self.connection_retries = 5
        self.auto_reconnect = True   # 自动重连
        self.retry_delay = 5         # 重试延迟时间（秒）
        self.request_retries = 5
        self.flood_sleep_threshold = 60  # 限流等待阈值
        self.max_reconnect_attempts = 3  # 最大重连尝试次数
        self.reconnect_cooldown = 30     # 重连冷却时间（秒）
        
        # 初始化客户端为None，等待start方法中创建
        self.client = None
        
        # 初始化频道管理器
        self.channel_manager = None  # 将在start方法中初始化
        
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
        
        # session管理相关属性
        self.max_session_backups = 5  # 最大备份数量
        self.last_session_backup = None
        self.session_backup_interval = 1800  # 备份间隔（秒）
        
        # session过期检测相关属性
        self.last_session_check = None
        self.session_check_interval = 300  # 每5分钟检查一次session状态
        self.session_expiry_threshold = 43200  # session过期阈值（12小时）
        self.last_authorized_check = None
        self.authorized_check_interval = 60  # 每1分钟检查一次授权状态
        self.max_errors = 5  # 增加最大错误次数阈值，防止属性不存在
        
        # 添加代币处理缓存
        self._token_processing_cache = {}  # 用于缓存正在处理的代币
        self._token_cache_lock = asyncio.Lock()  # 用于保护缓存的并发访问
    
    def _init_session_path(self):
        """初始化session路径,选择最新的可用session或创建新的"""
        try:
            # 处理操作系统差异
            import platform
            is_windows = platform.system() == 'Windows'
            
            # 查找现有的session文件
            existing_sessions = []
            for file in os.listdir(self.session_dir):
                if file.endswith('.session') and not file.endswith('.session-journal'):
                    full_path = os.path.join(self.session_dir, file)
                    existing_sessions.append((full_path, os.path.getmtime(full_path)))
            
            if existing_sessions:
                # 按修改时间排序，获取最新的session
                existing_sessions.sort(key=lambda x: x[1], reverse=True)
                latest_session = existing_sessions[0][0]
                logger.info(f"找到现有session文件: {os.path.abspath(latest_session)}")
                
                # 检查session文件是否被锁定
                if self._check_session_lock(latest_session):
                    # 使用现有的session文件
                    self.session_path = latest_session.replace('.session', '')
                    logger.info(f"将使用现有session文件: {os.path.abspath(self.session_path)}")
                    return
            
            # 如果没有可用的session文件,创建新的
            # 跨平台拼接session文件名，兼容Windows和Linux
            session_name = f"tg_session_{'win_' if is_windows else ''}{int(time.time())}"
            self.session_path = os.path.join(self.session_dir, session_name)
            logger.info(f"创建新的session文件: {os.path.abspath(self.session_path)}")
            
        except Exception as e:
            logger.error(f"初始化session路径时出错: {str(e)}")
            # 确保至少有一个默认的session路径
            self.session_path = os.path.join(self.session_dir, f'tg_session_default_{int(time.time())}')
            logger.info(f"使用默认session路径: {os.path.abspath(self.session_path)}")

    def _check_session_lock(self, session_path):
        """检查session文件是否被锁定,如果被锁定则尝试清理"""
        try:
            # 先检查journal文件
            journal_file = f"{session_path}-journal"
            if os.path.exists(journal_file):
                try:
                    os.remove(journal_file)
                    logger.info(f"已删除journal文件: {journal_file}")
                except Exception as e:
                    logger.warning(f"删除journal文件失败: {str(e)}")
                    return False
            
            # 尝试打开数据库连接
            import sqlite3
            try:
                conn = sqlite3.connect(session_path, timeout=1)
                conn.close()
                return True
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    logger.warning(f"session文件被锁定: {session_path}")
                    # 如果文件被锁定,尝试重命名或删除
                    try:
                        new_path = f"{session_path}.locked"
                        os.rename(session_path, new_path)
                        logger.info(f"已将锁定的session文件重命名为: {new_path}")
                        return False
                    except Exception as rename_error:
                        logger.error(f"重命名锁定的session文件失败: {str(rename_error)}")
                        return False
                return False
                
        except Exception as e:
            logger.error(f"检查session文件锁定状态时出错: {str(e)}")
            return False

    async def _check_session_validity(self):
        """检查session是否有效,只在建立连接前调用"""
        try:
            if not self.session_path:
                logger.error("session路径未设置")
                self._init_session_path()  # 尝试重新初始化session路径
                if not self.session_path:
                    return False
            
            # 检查session文件是否存在且可用
            session_file = os.path.abspath(f"{self.session_path}.session")
            if not os.path.exists(session_file):
                logger.warning(f"session文件不存在: {session_file}")
                return False
            
            # 检查session文件是否被锁定
            if not self._check_session_lock(session_file):
                logger.warning("session文件被锁定或损坏")
                return False
            
            # 如果有客户端实例,检查授权状态
            if self.client:
                try:
                    if not await self.client.is_user_authorized():
                        logger.warning("session未授权，需要重新登录")
                        return False
                except Exception as e:
                    logger.error(f"检查授权状态时出错: {str(e)}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"检查session有效性时出错: {str(e)}")
            return False

    async def _handle_session_expiry(self):
        """处理session异常情况（文件损坏、未授权等）"""
        try:
            logger.warning("检测到session异常,尝试恢复...")
            
            # 1. 首先尝试从备份恢复
            if await self.restore_session():
                logger.info("已从备份恢复session,尝试重新连接...")
                # 使用客户端工厂创建客户端
                self.client = await TelegramClientFactory.get_client(
                    self.session_path,
                    self.api_id, 
                    self.api_hash,
                    connection_retries=self.connection_retries,
                    auto_reconnect=self.auto_reconnect,
                    retry_delay=self.retry_delay,
                    request_retries=self.request_retries,
                    flood_sleep_threshold=self.flood_sleep_threshold,
                    timeout=30
                )
                
                try:
                    if not self.client.is_connected():
                        await self.client.connect()
                    if await self.client.is_user_authorized():
                        logger.info("从备份恢复session成功")
                        return True
                except Exception as e:
                    logger.error(f"从备份恢复后重连失败: {str(e)}")
            
            # 2. 如果备份恢复失败,创建新的session
            logger.info("备份恢复失败,创建新的session...")
            
            # 确保旧客户端的连接已完全关闭
            await TelegramClientFactory.disconnect_client()
            
            # 等待一段时间确保文件不再被占用
            await asyncio.sleep(5)
            
            # 删除旧的session文件，添加重试机制
            session_files = [os.path.abspath(f"{self.session_path}.session"), os.path.abspath(f"{self.session_path}.session-journal")]
            max_retries = 3
            retry_delay = 2
            
            for retry in range(max_retries):
                try:
                    for file in session_files:
                        if os.path.exists(file):
                            try:
                                os.remove(file)
                                logger.info(f"已删除旧的session文件: {file}")
                            except Exception as e:
                                if retry < max_retries - 1:
                                    logger.warning(f"删除session文件失败，将在{retry_delay}秒后重试: {str(e)}")
                                    await asyncio.sleep(retry_delay)
                                    continue
                                else:
                                    logger.error(f"删除旧的session文件失败: {str(e)}")
                    break  # 如果所有文件都成功删除，跳出重试循环
                except Exception as e:
                    if retry < max_retries - 1:
                        logger.warning(f"删除session文件时出错，将在{retry_delay}秒后重试: {str(e)}")
                        await asyncio.sleep(retry_delay)
                    else:
                        logger.error(f"删除session文件失败，已达到最大重试次数: {str(e)}")
            
            # 重新初始化session路径
            self._init_session_path()
            
            # 重新创建客户端
            self.client = await TelegramClientFactory.get_client(
                self.session_path,
                self.api_id, 
                self.api_hash,
                connection_retries=self.connection_retries,
                auto_reconnect=self.auto_reconnect,
                retry_delay=self.retry_delay,
                request_retries=self.request_retries,
                flood_sleep_threshold=self.flood_sleep_threshold,
                timeout=30
            )
            
            # 连接并重新登录
            if not self.client.is_connected():
                await self.client.connect()
            
            if not await self.client.is_user_authorized():
                logger.info("需要重新登录Telegram")
                try:
                    # 请求用户输入手机号
                    phone = input("请输入您的Telegram手机号（包含国家代码,如+86）：")
                    # 发送验证码
                    await self.client.send_code_request(phone)
                    # 请求用户输入验证码
                    code = input("请输入收到的验证码：")
                    # 登录
                    await self.client.sign_in(phone, code)
                    logger.info("Telegram登录成功")
                except Exception as e:
                    logger.error(f"Telegram登录失败: {str(e)}")
                    return False
            return True
        except Exception as e:
            logger.error(f"处理session异常时出错: {str(e)}")
            return False
    
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
    
    import asyncio
    # 全局异步锁，防止并发注册消息处理器
    _register_lock = asyncio.Lock()
    # 记录上次注册handler的时间戳
    _last_register_time = None
    # 记录上次注册时的频道实体快照（用于去重）
    _last_entities_snapshot = None

    async def register_handlers(self, active_channels=None, force=False):
        """
        注册消息处理程序（带防抖、去重、并发保护机制）
        本方法确保在高并发、定时任务、异常恢复等复杂场景下，
        不会重复注册handler，且注册前会彻底移除所有旧handler。
        
        参数：
            active_channels: 可选，活跃频道列表，若为None则自动获取
            force: 是否强制注册（如异常恢复、手动重载等场景）
        返回：
            bool: 是否成功注册消息处理程序
        """
        async with self._register_lock:
            import time
            now = time.time()
            # === 1. 获取活跃频道 ===
            if active_channels is None:
                # 若未指定，自动从频道管理器获取
                active_channels = self.channel_manager.get_active_channels()
            if not active_channels:
                logger.warning("没有活跃的频道，无法注册消息处理程序")
                return False
            # === 2. 生成当前频道实体快照（用于去重） ===
            entities_snapshot = tuple(sorted((c.get('channel_id'), c.get('channel_username')) for c in active_channels))
            # === 3. 防抖与去重判断 ===
            # 若频道实体未变且60秒内已注册过，则直接跳过，防止短时间重复注册
            if not force and hasattr(self, '_last_register_time') and hasattr(self, '_last_entities_snapshot'):
                if self._last_entities_snapshot == entities_snapshot and now - self._last_register_time < 3600:
                    logger.info(f"防抖：60秒内频道实体无变化，跳过重复注册handler。")
                    return True
            # === 4. 注册前彻底移除所有旧handler ===
            # 包括Telethon底层handler和self.event_handlers字典
            if hasattr(self, 'event_handlers') and self.event_handlers:
                for handler in list(self.event_handlers.values()):
                    try:
                        self.client.remove_event_handler(handler)
                    except Exception as e:
                        logger.warning(f"移除旧handler时出错: {str(e)}")
                self.event_handlers.clear()
                logger.info(f"已移除所有旧的消息处理器，准备注册新handler")
            # === 5. 构建监听实体列表与辅助结构 ===
            logger.info(f"注册处理程序: {len(active_channels)} 个活跃频道")
            entities = []  # Telethon实体对象列表
            entity_names = []  # 频道名称/用户名列表
            self.channel_entities = {}  # 频道ID到实体的映射
            self.chain_map = {}         # 频道ID到链的映射
            for channel in active_channels:
                try:
                    channel_id = channel.get('channel_id')
                    channel_username = channel.get('channel_username')
                    chain = channel.get('chain')
                    if not channel_id and not channel_username:
                        logger.warning(f"跳过缺少ID和用户名的频道: {channel}")
                        continue
                    entity = None
                    # === 5.1 优先通过ID获取实体 ===
                    if channel_id:
                        try:
                            # 正整数ID需特殊处理（加-100前缀）
                            if isinstance(channel_id, int) and channel_id > 0:
                                try:
                                    entity = await self.client.get_entity(PeerChannel(-1000000000000 - channel_id))
                                except Exception as e:
                                    logger.warning(f"无法将正整数ID {channel_id} 转换为频道实体: {str(e)}")
                                    # 权限问题直接跳过
                                    if "private and you lack permission" in str(e) or "banned from it" in str(e):
                                        logger.error(f"频道 {channel_id} 是私有的或已被禁止访问，跳过此频道")
                                        continue
                                    # 尝试用原始ID
                                    try:
                                        entity = await self.client.get_entity(PeerChannel(channel_id))
                                    except Exception as e2:
                                        logger.error(f"使用原始ID获取频道实体失败: {str(e2)}")
                                        # 若有用户名再尝试用户名
                                        if channel_username:
                                            try:
                                                entity = await self.client.get_entity(channel_username)
                                            except Exception as e3:
                                                logger.error(f"无法通过用户名 {channel_username} 获取频道实体: {str(e3)}")
                                                continue
                                        else:
                                            continue
                            else:
                                # 直接用ID
                                try:
                                    entity = await self.client.get_entity(PeerChannel(channel_id))
                                except Exception as e:
                                    logger.error(f"使用PeerChannel({channel_id})获取频道实体失败: {str(e)}")
                                    if "private and you lack permission" in str(e) or "banned from it" in str(e):
                                        logger.error(f"频道 {channel_id} 是私有的或已被禁止访问，跳过此频道")
                                        continue
                        except ValueError as e:
                            logger.warning(f"无法通过ID {channel_id} 获取频道实体: {str(e)}")
                            if channel_username:
                                try:
                                    entity = await self.client.get_entity(channel_username)
                                except Exception as e2:
                                    logger.error(f"无法通过用户名 {channel_username} 获取频道实体: {str(e2)}")
                                    continue
                            else:
                                continue
                    # === 5.2 若无ID则尝试用户名 ===
                    elif channel_username:
                        try:
                            entity = await self.client.get_entity(channel_username)
                        except Exception as e:
                            logger.error(f"无法通过用户名 {channel_username} 获取频道实体: {str(e)}")
                            continue
                    # === 5.3 实体有效则加入监听列表 ===
                    if entity:
                        entities.append(entity)
                        entity_name = getattr(entity, 'title', None) or channel_username or f"ID:{channel_id}"
                        entity_names.append(entity_name)
                        entity_key = str(entity.id)
                        self.channel_entities[entity_key] = entity
                        self.chain_map[entity_key] = chain
                except Exception as e:
                    logger.error(f"处理频道时出错: {channel.get('channel_name', 'unknown')}, 错误: {str(e)}")
                    continue
            # === 6. 检查是否有有效实体 ===
            if not entities:
                logger.warning("没有有效的频道实体，无法注册消息处理程序")
                return False
            # === 7. 注册新消息处理器 ===
            # 只注册一次，防止重复
            @self.client.on(events.NewMessage(chats=entities))
            async def handler(event):
                # 处理新消息事件，详见handle_new_message方法
                await self.handle_new_message(event)
            # === 8. 记录handler到本地映射，便于后续移除 ===
            if not hasattr(self, 'event_handlers'):
                self.event_handlers = {}
            self.event_handlers['new_message'] = handler
            # === 9. 记录本次注册的时间和实体快照 ===
            self._last_register_time = now
            self._last_entities_snapshot = entities_snapshot
            # === 10. 日志详细说明注册原因 ===
            logger.info(f"已注册消息处理程序，监听实体: {', '.join(entity_names)}，当前handler数量: {len(self.event_handlers)}，注册原因: {'强制' if force else '定时/变更'}")
            return True
    
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
                        try:
                            full_channel = await self.client(GetFullChannelRequest(channel=channel_entity))
                            members_count = getattr(full_channel.full_chat, 'participants_count', 0)
                            logger.info(f"成功获取转换后频道 {channel_id} 的成员数: {members_count}")
                            return members_count
                        except Exception as e:
                            if "private and you lack permission" in str(e) or "banned from it" in str(e):
                                logger.warning(f"频道 {channel_id} 是私有的或已被禁止访问，无法获取成员数")
                                return 0
                            else:
                                logger.warning(f"获取频道 {channel_id} 完整信息时出错: {str(e)}")
                                return 0
                    except Exception as e:
                        logger.warning(f"尝试将 {channel_id} 转换为频道格式后获取成员数失败: {str(e)}")
                        # 如果转换失败，尝试原始方法
            
            try:
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
                if "private and you lack permission" in str(e) or "banned from it" in str(e):
                    logger.warning(f"频道 {channel_id} 是私有的或已被禁止访问，无法获取成员数")
                    return 0
                else:
                    logger.warning(f"获取频道 {channel_id} 的成员数时出错: {str(e)}")
                    return 0
        except Exception as e:
            logger.warning(f"获取频道 {channel_id} 的成员数时出错: {str(e)}")
            return 0
    
    async def handle_new_message(self, event):
        """
        处理新消息事件
        
        Args:
            event: Telegram消息事件对象
        """
        try:
            # 获取消息对象
            message = event.message
            if not message:
                return
                
            # 获取频道信息
            channel_id = None
            channel_title = None
            
            if hasattr(message.peer_id, 'channel_id'):
                channel_id = message.peer_id.channel_id
                try:
                    chat = await self.client.get_entity(message.peer_id)
                    channel_title = chat.title
                except Exception as e:
                    logger.error(f"获取频道信息失败: {str(e)}")
                    channel_title = "未知频道"
            
            # 记录消息信息
            logger.info(f"收到新消息 - 普通频道: {channel_title}, ID: {channel_id}, 链: UNKNOWN, 消息ID: {message.id}")
            logger.info(f"消息内容:\n--------------------------------------------------\n{message.text[:500]}...\n--------------------------------------------------")
            
            # 从消息内容中提取链信息，确保不为None
            from src.database.db_handler import extract_chain_from_message
            chain = extract_chain_from_message(message.text) if message.text else None
            # 如果未能识别链，则使用'UNKNOWN'作为默认值，避免数据库插入失败
            chain = chain or 'UNKNOWN'
            
            # 保存消息到数据库
            save_result = await save_telegram_message(
                chain=chain,  # 使用提取的链信息，保证非None
                message_id=message.id,
                date=message.date,
                text=message.text,
                media_path=None,  # 不再保存媒体文件
                channel_id=channel_id
            )
            
            if save_result:
                logger.info(f"成功保存消息到数据库: {message.id}")
            else:
                logger.warning(f"消息 {chain}-{message.id} 已存在或保存失败，跳过本条消息")
                return  # 直接跳过后续处理
            
            # 提取并处理消息中的代币信息
            await self._process_token_in_message(message, channel_id, channel_title)
            
            # 重置错误计数，表示处理成功
            self.error_count = 0
            self.last_error_time = None
            
        except Exception as e:
            logger.error(f"处理新消息时出错: {str(e)}")
            import traceback as tb
            logger.debug(tb.format_exc())
            
            # 增加错误计数
            self.error_count += 1
            self.last_error_time = time.time()
            
            # 如果错误次数过多，尝试重新初始化
            if self.error_count >= self.max_errors:
                logger.warning(f"错误次数达到{self.max_errors}次，尝试重新初始化处理器...")
                await self.reinitialize_handlers()
    
    async def _process_token_in_message(self, message, channel_id, channel_title):
        """
        处理消息中的代币信息
        
        Args:
            message: 消息对象
            channel_id: 频道ID
            channel_title: 频道名称
        """
        try:
            # 提取代币信息，支持多合约
            contract_infos = await self._extract_contract_from_message(message.text, channel_id)
            if not contract_infos:
                logger.info("未从消息中提取到合约地址，跳过代币信息处理")
                return
            # 循环处理每个合约地址，兼容单合约情况
            for contract_info in contract_infos:
                contract_address = contract_info.get('contract_address')
                chain = contract_info.get('chain', 'UNKNOWN')
                # 如果链未知，尝试识别链
                if chain == 'UNKNOWN':
                    chain = await self._identify_chain_for_contract(contract_address)
                # 处理代币数据
                if chain and chain != 'UNKNOWN':
                    await self._update_or_create_token(
                        chain, 
                        contract_address, 
                        message.id, 
                        channel_id,
                        risk_level=contract_info.get('risk_level'),
                        promotion_count=contract_info.get('promotion_count', 1)
                    )
                else:
                    logger.warning(f"未能确定合约地址 {contract_address} 所属的链，无法调用DEX API")
        except Exception as e:
            logger.error(f"处理代币信息时出错: {str(e)}")
            import traceback as tb
            logger.debug(tb.format_exc())
    
    async def _extract_contract_from_message(self, message_text, channel_id):
        """
        从消息文本中提取合约地址信息，支持多合约提取
        
        Args:
            message_text: 消息文本
            channel_id: 频道ID
        Returns:
            List[Dict]: 包含多个合约地址和链信息的字典列表，如果未找到则返回空列表
        """
        try:
            from src.database.db_factory import get_db_adapter
            db_adapter = get_db_adapter()
            from datetime import datetime
            current_time = datetime.now()
            # 直接调用新版批量接口
            from src.database.db_handler import extract_promotion_info
            promos = extract_promotion_info(message_text, current_time, 'UNKNOWN', None, channel_id)
            contract_infos = []
            for promo in promos:
                if hasattr(promo, 'contract_address') and promo.contract_address:
                    contract_infos.append({
                        'contract_address': promo.contract_address,
                        'chain': promo.chain if hasattr(promo, 'chain') else 'UNKNOWN',
                        'risk_level': getattr(promo, 'risk_level', None),
                        'promotion_count': getattr(promo, 'promotion_count', 1)
                    })
            return contract_infos
        except Exception as e:
            logger.error(f"提取合约地址时出错: {str(e)}")
            import traceback as tb
            logger.debug(tb.format_exc())
            return []
    
    async def _identify_chain_for_contract(self, contract_address):
        """
        识别合约地址所属的链
        
        Args:
            contract_address: 合约地址
            
        Returns:
            str: 链ID，如果未识别出则返回'UNKNOWN'
        """
        try:
            # 尝试的链列表
            test_chains = ["solana", "ethereum", "bsc", "arbitrum", "base", "optimism", "avalanche", "polygon"]
            
            # 获取API助手
            from src.api.token_market_updater import update_token_market_data_async
            
            # 依次尝试各链
            for test_chain in test_chains:
                try:
                    logger.info(f"尝试在链 {test_chain} 上查询合约地址 {contract_address}")
                    result = await update_token_market_data_async(test_chain, contract_address)
                    
                    if "error" not in result:
                        # 找到有效链
                        chain_map = {
                            "solana": "SOL",
                            "ethereum": "ETH",
                            "bsc": "BSC",
                            "arbitrum": "ARB",
                            "base": "BASE",
                            "optimism": "OP",
                            "avalanche": "AVAX",
                            "polygon": "MATIC",
                            "zksync": "ZK",
                            "ton": "TON"
                        }
                        chain = chain_map.get(test_chain, test_chain.upper())
                        logger.info(f"通过DEX API确定链为: {chain}")
                        return chain
                        
                except Exception as e_inner:
                    logger.warning(f"尝试在链 {test_chain} 上查询时出错: {str(e_inner)}")
            
            # 未能识别链
            return "UNKNOWN"
            
        except Exception as e:
            logger.error(f"识别链时出错: {str(e)}")
            return "UNKNOWN"
    
    async def _update_or_create_token(self, chain, contract_address, message_id, channel_id, risk_level=None, promotion_count=1):
        """
        更新或创建代币信息
        
        Args:
            chain: 链ID
            contract_address: 合约地址
            message_id: 消息ID
            channel_id: 频道ID
            risk_level: 风险等级
            promotion_count: 推广次数
        """
        try:
            # 生成缓存键
            cache_key = f"{chain}:{contract_address}"
            
            # 检查缓存中是否正在处理该代币
            async with self._token_cache_lock:
                if cache_key in self._token_processing_cache:
                    logger.info(f"代币 {chain}/{contract_address} 正在处理中，跳过重复处理")
                    return
                # 将代币添加到处理缓存
                self._token_processing_cache[cache_key] = True
            
            try:
                # 获取API助手
                from src.api.token_market_updater import update_token_market_data_async
                
                # 更新代币数据
                result = await update_token_market_data_async(
                    chain, 
                    contract_address, 
                    message_id=message_id, 
                    channel_id=channel_id,
                    risk_level=risk_level,
                    promotion_count=promotion_count
                )
                
                # 检查API调用结果
                if "error" in result:
                    error_msg = result.get("error", "")
                    
                    # 只有在确认是"数据库中未找到该代币"且API成功获取到数据的情况下才创建新token
                    if "数据库中未找到该代币" in error_msg and not any(x in error_msg for x in ["无法获取代币数据", "API错误"]):
                        # 检查result中是否包含足够的数据来创建token
                        if result.get("marketCap") is not None or result.get("price") is not None or result.get("symbol"):
                            # 创建新代币
                            logger.info(f"未找到代币 {chain}/{contract_address}，开始创建新代币")
                            result["channel_id"] = channel_id
                            result["risk_level"] = risk_level
                            result["message_id"] = message_id
                            # 创建新代币
                            result = await self.create_token(result)
                            
                            if "error" in result:
                                logger.warning(f"创建新代币失败: {result.get('error')}")
                                return
                                
                            logger.info(f"成功创建新代币 {chain}/{contract_address}")
                        else:
                            logger.warning(f"未从API获取到足够的代币数据，跳过创建: {chain}/{contract_address}")
                            return
                    else:
                        # 其他API错误情况，记录错误并跳过
                        logger.warning(f"处理代币信息失败: {error_msg}")
                        return
                
                # 处理成功更新或创建的结果
                if "error" not in result:
                    await self._save_token_mark(chain, contract_address, message_id, channel_id, result)
                
            finally:
                # 无论处理成功与否，都从缓存中移除
                async with self._token_cache_lock:
                    self._token_processing_cache.pop(cache_key, None)
            
        except Exception as e:
            logger.error(f"更新或创建代币时出错: {str(e)}")
            import traceback as tb
            logger.debug(tb.format_exc())
            # 确保在发生异常时也从缓存中移除
            async with self._token_cache_lock:
                self._token_processing_cache.pop(cache_key, None)
    
    async def _save_token_mark(self, chain, contract_address, message_id, channel_id, token_result):
        """
        保存代币标记信息
        
        Args:
            chain: 链ID
            contract_address: 合约地址
            message_id: 消息ID
            channel_id: 频道ID
            token_result: 代币处理结果
        """
        try:
            # 获取数据库适配器
            from src.database.db_factory import get_db_adapter
            db_adapter = get_db_adapter()
            
            # 检查是否是新创建的代币
            is_new_token = token_result.get("is_new", False)
            if is_new_token:
                logger.info(f"成功创建新代币 {chain}/{contract_address} 的信息")
            else:
                logger.info(f"成功更新代币 {chain}/{contract_address} 的信息")
            
            # 保存代币标记信息
            token_symbol = token_result.get('symbol', '未知')
            token_mark_data = {
                'chain': chain,
                'token_symbol': token_symbol,
                'contract': contract_address,
                'message_id': message_id,
                'market_cap': token_result.get('marketCap', 0),
                'channel_id': channel_id
            }
            
            # 保存代币标记
            mark_result = await db_adapter.save_token_mark(token_mark_data)
            if mark_result:
                logger.info(f"成功保存代币标记信息: {token_symbol}")
                
                # 在保存成功后重新计算spread_count，确保计算最新值
                try:
                    # 查询该代币在所有频道的提及情况
                    mentions = await db_adapter.execute_query(
                        'tokens_mark', 
                        'select', 
                        filters={
                            'contract': contract_address,
                            'chain': chain
                        }
                    )
                    
                    # 计算不同频道的数量
                    unique_channels = set()
                    if mentions and isinstance(mentions, list):
                        for mention in mentions:
                            if mention and 'channel_id' in mention and mention['channel_id']:
                                unique_channels.add(mention['channel_id'])
                    
                    # 确保当前频道也被计算在内
                    if channel_id:
                        unique_channels.add(channel_id)
                    
                    # 计算实际的spread_count值
                    new_spread_count = len(unique_channels)
                    
                    # 获取当前代币信息
                    current_token = await db_adapter.get_token_by_contract(chain, contract_address)
                    if current_token:
                        current_spread_count = current_token.get('spread_count', 0) or 0
                        
                        # 只有当新值大于当前值时才更新
                        if new_spread_count > current_spread_count:
                            # 更新spread_count
                            update_result = await db_adapter.execute_query(
                                'tokens',
                                'update',
                                data={'spread_count': new_spread_count},
                                filters={'chain': chain, 'contract': contract_address}
                            )
                            
                            logger.info(f"更新代币 {token_symbol} ({chain}/{contract_address}) 的spread_count: {current_spread_count} -> {new_spread_count}")
                
                except Exception as spread_error:
                    logger.error(f"计算spread_count时出错: {str(spread_error)}")
                    import traceback
                    logger.debug(traceback.format_exc())
                    
            else:
                logger.warning(f"保存代币标记信息失败: {token_symbol}")
                
        except Exception as e:
            logger.error(f"保存代币标记时出错: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
    
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
        try:
            # 检查session是否有效
            if not await self._check_session_validity():
                logger.warning("当前session无效,尝试从备份恢复或创建新session...")
                if not await self._handle_session_expiry():
                    logger.error("无法恢复或创建有效的session")
                    return False
            
            # 使用客户端工厂初始化客户端
            self.client = await TelegramClientFactory.get_client(
                self.session_path,
                self.api_id, 
                self.api_hash,
                connection_retries=self.connection_retries,
                auto_reconnect=self.auto_reconnect,
                retry_delay=self.retry_delay,
                request_retries=self.request_retries,
                flood_sleep_threshold=self.flood_sleep_threshold,
                timeout=30
            )
            
            # 尝试连接
            try:
                if not self.client.is_connected():
                    await self.client.connect()
            except Exception as e:
                logger.error(f"连接Telegram服务器时出错: {str(e)}")
                return False
            
            # 检查是否已授权
            if not await self.client.is_user_authorized():
                logger.info("需要重新登录Telegram")
                try:
                    # 请求用户输入手机号
                    phone = input("请输入您的Telegram手机号（包含国家代码,如+86）：")
                    # 发送验证码
                    await self.client.send_code_request(phone)
                    # 请求用户输入验证码
                    code = input("请输入收到的验证码：")
                    # 登录
                    await self.client.sign_in(phone, code)
                    logger.info("Telegram登录成功")
                except Exception as e:
                    logger.error(f"Telegram登录失败: {str(e)}")
                    return False
            
            # 初始化频道管理器
            self.channel_manager = ChannelManager(self.client)
            
            # 设置频道监听
            if not await self.setup_channels():
                logger.error("设置频道监听失败")
                return False
            
            # 启动自动发现频道任务
            if self.auto_discovery_enabled:
                self.discovery_task = asyncio.create_task(self.discovery_loop())
            
            # 启动健康检查任务
            self.health_check_task = asyncio.create_task(self.health_check_loop())
            
            # 设置运行状态
            self.is_running = True
            
            # 定期备份session
            self.session_backup_task = asyncio.create_task(self._periodic_session_backup())
            
            logger.info("Telegram监听服务已启动")
            return True
            
        except Exception as e:
            logger.critical(f"启动服务时出错: {str(e)}")
            logger.debug(tb.format_exc())
            return False
    
    async def _periodic_session_backup(self):
        """定期备份session文件"""
        while self.is_running:
            try:
                # 检查是否需要备份
                current_time = time.time()
                if (self.last_session_backup is None or 
                    current_time - self.last_session_backup >= self.session_backup_interval):
                    if self.client and self.client.is_connected():
                        await self.backup_session()
                
                # 等待下一次备份检查
                await asyncio.sleep(60)  # 每分钟检查一次是否需要备份
                
            except Exception as e:
                logger.error(f"定期备份session时出错: {str(e)}")
                logger.debug(tb.format_exc())
                await asyncio.sleep(60)  # 出错后等待一分钟再继续
    
    async def _handle_disconnection(self):
        """处理断开连接的情况"""
        try:
            # 检查session是否有效
            if not await self._check_session_validity():
                logger.warning("当前session无效,尝试恢复...")
                if not await self._handle_session_expiry():
                    logger.error("无法恢复session")
                    return False
            
            # 尝试使用客户端工厂重新连接
            try:
                # 先尝试重连现有客户端
                if self.client and not self.client.is_connected():
                    await self.client.connect()
                    if await self.client.is_user_authorized():
                        logger.info("重新连接成功")
                        return True
                    else:
                        logger.warning("重新连接后未授权")
                
                # 如果重连失败，尝试通过工厂获取新客户端
                self.client = await TelegramClientFactory.get_client(
                    self.session_path,
                    self.api_id, 
                    self.api_hash,
                    connection_retries=self.connection_retries,
                    auto_reconnect=self.auto_reconnect,
                    retry_delay=self.retry_delay,
                    request_retries=self.request_retries,
                    flood_sleep_threshold=self.flood_sleep_threshold,
                    timeout=30
                )
                
                if self.client and await self.client.is_user_authorized():
                    logger.info("成功获取新客户端并已授权")
                    return True
                else:
                    logger.warning("获取新客户端后未授权")
                    return False
            except Exception as e:
                logger.error(f"重新连接失败: {str(e)}")
                return False
                
        except Exception as e:
            logger.error(f"处理断开连接时出错: {str(e)}")
            return False
    
    async def health_check_loop(self):
        """健康检查循环,只检查连接状态"""
        last_reconnect_time = None
        reconnect_attempts = 0
        
        while self.is_running:
            try:
                # 只检查连接状态
                if not self.client.is_connected():
                    now = datetime.now()
                    
                    # 检查是否需要等待冷却时间
                    if last_reconnect_time:
                        time_since_last_reconnect = (now - last_reconnect_time).total_seconds()
                        if time_since_last_reconnect < self.reconnect_cooldown:
                            await asyncio.sleep(self.reconnect_cooldown - time_since_last_reconnect)
                    
                    logger.warning("检测到客户端已断开连接,正在尝试重新连接...")
                    last_reconnect_time = datetime.now()
                    
                    if await self._handle_disconnection():
                        reconnect_attempts = 0
                        continue
                    
                    reconnect_attempts += 1
                    if reconnect_attempts >= self.max_reconnect_attempts:
                        logger.critical("多次重连失败,等待较长时间后重试...")
                        
                        # 先确保旧的连接已完全关闭
                        await TelegramClientFactory.disconnect_client()
                        
                        await asyncio.sleep(300)  # 等待5分钟后重试
                        reconnect_attempts = 0
                    else:
                        await asyncio.sleep(self.reconnect_cooldown)
                
                # 等待下一次检查
                await asyncio.sleep(10)  # 每10秒检查一次
                
            except Exception as e:
                logger.error(f"健康检查时出错: {str(e)}")
                logger.debug(tb.format_exc())
                await asyncio.sleep(10)
    
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
                    
                    # 检查是否需要等待冷却时间
                    if last_reconnect_time:
                        time_since_last_reconnect = (now - last_reconnect_time).total_seconds()
                        if time_since_last_reconnect < self.reconnect_cooldown:
                            await asyncio.sleep(self.reconnect_cooldown - time_since_last_reconnect)
                    
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
                                self.client = await TelegramClientFactory.get_client(
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
        
        # 使用工厂方法断开与Telegram的连接
        await TelegramClientFactory.disconnect_client()
        self.client = None
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

    # 添加新函数: 创建代币
    async def create_token(self, token_data):
        """
        创建新代币，如果已存在则更新
        
        Args:
            token_data: 包含代币信息的字典
            
        Returns:
            Dict: 包含创建结果的字典
        """
        try:
            logger.info(f"开始创建新代币 {token_data.get('chain')}/{token_data.get('contract')}")
            
            # 获取数据库适配器
            from src.database.db_factory import get_db_adapter
            db_adapter = get_db_adapter()
            
            # 获取当前时间格式化为字符串
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 计算代币传播统计的初始值
            spread_count = 1  # 默认至少有1次传播（当前消息）
            community_reach = 0  # 默认社群覆盖为0
            
            # 尝试获取频道的成员数量作为初始community_reach
            try:
                channel_id = token_data.get('channel_id')
                if channel_id:
                    # 尝试从数据库获取频道信息
                    channel_info = None
                    
                    # 从数据库查询频道信息
                    try:
                        channel_query = await db_adapter.execute_query(
                            'telegram_channels', 
                            'select', 
                            filters={'channel_id': channel_id}
                        )
                        if channel_query and len(channel_query) > 0:
                            channel_info = channel_query[0]
                    except Exception as db_error:
                        logger.warning(f"从数据库获取频道信息失败: {str(db_error)}")
                    
                    # 如果从数据库获取到了成员数
                    if channel_info and 'member_count' in channel_info and channel_info['member_count']:
                        community_reach = int(channel_info['member_count'])
                        logger.info(f"从数据库获取到频道 {channel_id} 的成员数: {community_reach}")
                    else:
                        # 尝试使用Telegram API获取成员数
                        try:
                            members_count = await self.get_channel_members_count(channel_id)
                            if members_count > 0:
                                community_reach = members_count
                                logger.info(f"从Telegram API获取到频道 {channel_id} 的成员数: {community_reach}")
                        except Exception as api_error:
                            logger.warning(f"从Telegram API获取频道成员数失败: {str(api_error)}")
                
                # 检查该代币是否已经在其他频道被提及
                try:
                    contract = token_data.get('contract')
                    chain = token_data.get('chain')
                    if contract and chain:
                        # 查询历史记录中该代币的提及次数
                        mentions = await db_adapter.execute_query(
                            'tokens_mark', 
                            'select', 
                            filters={
                                'contract': contract,
                                'chain': chain
                            }
                        )
                        
                        if mentions and len(mentions) > 0:
                            # 计算不同频道的数量
                            unique_channels = set()
                            for mention in mentions:
                                if 'channel_id' in mention and mention['channel_id']:
                                    unique_channels.add(mention['channel_id'])
                            
                            # 确保当前频道也被计算在内
                            if channel_id:
                                unique_channels.add(channel_id)
                                
                            # 更新spread_count为不同频道的数量
                            spread_count = len(unique_channels)
                                
                            logger.info(f"代币 {chain}/{contract} 已在 {spread_count} 个频道被提及")
                except Exception as mention_error:
                    logger.warning(f"获取代币提及次数时出错: {str(mention_error)}")
                    # 使用默认值
                    spread_count = 1
            except Exception as stats_error:
                logger.warning(f"计算代币传播统计数据时出错: {str(stats_error)}")
                # 使用默认值
                spread_count = 1
                community_reach = 0
            
            # 准备新代币数据
            new_token_data = {
                'chain': token_data.get('chain'),
                'contract': token_data.get('contract'),
                'token_symbol': token_data.get('symbol', ''),
                'message_id': token_data.get('message_id'),  # 添加消息ID
                'market_cap': token_data.get('marketCap', 0),
                'market_cap_1h': None,  # 初始设置为None，后续更新时会保存当前市值为1小时前市值
                'market_cap_formatted': self._format_market_cap(token_data.get('marketCap', 0)),
                'first_market_cap': token_data.get('marketCap', 0),  # 首次市值
                'promotion_count': 1,  # 初始推广次数为1
                'likes_count': 0,  # 初始点赞数为0
                'telegram_url': None,  # 暂无Telegram链接
                'twitter_url': None,  # 暂无Twitter链接
                'website_url': None,  # 暂无网站链接
                'latest_update': current_time,
                'first_update': current_time,
                'dexscreener_url': token_data.get('dexScreenerUrl'),
                'from_group': False,  # 默认不是来自群组
                'channel_id': token_data.get('channel_id'),
                'image_url': token_data.get('image_url'),
                'last_calculation_time': current_time,
                
                # 价格和市值趋势分析字段
                'price': token_data.get('price'),
                'first_price': token_data.get('first_price'),
                'price_change_24h': 0,  # 初始24小时价格变化为0
                'price_change_7d': 0,  # 初始7天价格变化为0
                'volume_24h': 0,  # 初始24小时交易量为0
                'volume_1h': token_data.get('volume_1h', 0),
                'liquidity': token_data.get('liquidity', 0),
                'holders_count': token_data.get('holders_count', 0),  # 使用API获取的持有者数量
                
                # 1小时交易数据
                'buys_1h': token_data.get('buys_1h', 0),
                'sells_1h': token_data.get('sells_1h', 0),
                
                # 代币传播统计 - 使用计算的值
                'spread_count': spread_count,  # 计算后的传播次数
                'community_reach': community_reach,  # 计算后的社群覆盖人数
                
                # 情感分析字段
                'sentiment_score': None,  # 暂无情感分析得分
                'positive_words': None,  # 暂无积极词汇
                'negative_words': None,  # 暂无消极词汇
                'is_trending': False,  # 初始非热门状态
                'hype_score': 0,  # 初始炒作评分为0
                'risk_level': token_data.get('risk_level')  # 风险等级
            }
            
            try:
                # 尝试插入新代币
                insert_result = await db_adapter.execute_query('tokens', 'insert', data=new_token_data)
                
                if isinstance(insert_result, dict) and insert_result.get('error'):
                    error_msg = insert_result.get('error', '')
                    
                    # 检查是否是唯一约束冲突（代币已存在）
                    if 'unique constraint' in error_msg.lower() or 'duplicate key' in error_msg.lower() or 'already exists' in error_msg.lower():
                        logger.info(f"代币 {token_data.get('chain')}/{token_data.get('contract')} 已存在，转为更新操作")
                        
                        # 获取现有代币信息
                        existing_token = await db_adapter.execute_query(
                            'tokens', 
                            'select', 
                            filters={
                                'chain': token_data.get('chain'),
                                'contract': token_data.get('contract')
                            },
                            limit=1
                        )
                        
                        if existing_token and len(existing_token) > 0:
                            existing_token = existing_token[0]
                            
                            # 准备更新数据
                            update_data = {
                                'latest_update': current_time,
                                'message_id': token_data.get('message_id'),  # 更新最新消息ID
                                'channel_id': token_data.get('channel_id'),  # 更新最新频道ID
                            }
                            
                            # 更新市值和价格（如果有新数据）
                            if token_data.get('marketCap'):
                                update_data['market_cap'] = token_data.get('marketCap')
                                update_data['market_cap_formatted'] = self._format_market_cap(token_data.get('marketCap'))
                            
                            if token_data.get('price'):
                                update_data['price'] = token_data.get('price')
                            
                            # 更新其他字段（如果有新数据）
                            if token_data.get('liquidity'):
                                update_data['liquidity'] = token_data.get('liquidity')
                            
                            if token_data.get('holders_count'):
                                update_data['holders_count'] = token_data.get('holders_count')
                            
                            # 增加推广次数
                            update_data['promotion_count'] = existing_token.get('promotion_count', 0) + 1
                            
                            # 更新spread_count（如果新值更大）
                            if spread_count > existing_token.get('spread_count', 0):
                                update_data['spread_count'] = spread_count
                            
                            # 执行更新
                            update_result = await db_adapter.execute_query(
                                'tokens',
                                'update',
                                filters={
                                    'chain': token_data.get('chain'),
                                    'contract': token_data.get('contract')
                                },
                                data=update_data
                            )
                            
                            if isinstance(update_result, dict) and update_result.get('error'):
                                logger.error(f"更新代币失败: {update_result.get('error')}")
                                return {"error": f"更新代币失败: {update_result.get('error')}"}
                            
                            logger.info(f"成功更新代币 {token_data.get('chain')}/{token_data.get('contract')}")
                            
                            # 返回更新结果
                            return {
                                "success": True,
                                "is_updated": True,
                                "chain": token_data.get('chain'),
                                "contract": token_data.get('contract'),
                                "symbol": token_data.get('symbol', ''),
                                "marketCap": token_data.get('marketCap', 0),
                                "liquidity": token_data.get('liquidity', 0),
                                "price": token_data.get('price'),
                                "dexScreenerUrl": token_data.get('dexScreenerUrl'),
                                "image_url": token_data.get('image_url')
                            }
                        else:
                            logger.warning(f"唯一约束冲突，但无法找到现有代币: {token_data.get('chain')}/{token_data.get('contract')}")
                            return {"error": "唯一约束冲突，但无法找到现有代币"}
                    else:
                        # 其他错误
                        logger.error(f"创建新代币失败: {error_msg}")
                        return {"error": f"创建新代币失败: {error_msg}"}
                
                logger.info(f"成功创建新代币 {token_data.get('chain')}/{token_data.get('contract')}")
                
                # 返回成功结果
                return {
                    "success": True,
                    "is_new": True,  # 标记为新创建的代币
                    "chain": token_data.get('chain'),
                    "contract": token_data.get('contract'),
                    "symbol": token_data.get('symbol', ''),
                    "marketCap": token_data.get('marketCap', 0),
                    "liquidity": token_data.get('liquidity', 0),
                    "price": token_data.get('price'),
                    "dexScreenerUrl": token_data.get('dexScreenerUrl'),
                    "image_url": token_data.get('image_url')
                }
            except Exception as db_error:
                # 尝试捕获数据库异常
                error_str = str(db_error).lower()
                
                # 检查是否是唯一约束冲突
                if 'unique constraint' in error_str or 'duplicate key' in error_str or 'already exists' in error_str:
                    logger.info(f"插入时发生冲突，代币 {token_data.get('chain')}/{token_data.get('contract')} 已存在，转为更新操作")
                    
                    # 获取现有代币信息
                    try:
                        existing_token = await db_adapter.execute_query(
                            'tokens', 
                            'select', 
                            filters={
                                'chain': token_data.get('chain'),
                                'contract': token_data.get('contract')
                            },
                            limit=1
                        )
                        
                        if existing_token and len(existing_token) > 0:
                            existing_token = existing_token[0]
                            
                            # 准备更新数据
                            update_data = {
                                'latest_update': current_time,
                                'message_id': token_data.get('message_id'),
                                'channel_id': token_data.get('channel_id'),
                            }
                            
                            # 更新市值和价格（如果有新数据）
                            if token_data.get('marketCap'):
                                update_data['market_cap'] = token_data.get('marketCap')
                                update_data['market_cap_formatted'] = self._format_market_cap(token_data.get('marketCap'))
                            
                            if token_data.get('price'):
                                update_data['price'] = token_data.get('price')
                            
                            # 更新其他字段（如果有新数据）
                            if token_data.get('liquidity'):
                                update_data['liquidity'] = token_data.get('liquidity')
                            
                            if token_data.get('holders_count'):
                                update_data['holders_count'] = token_data.get('holders_count')
                            
                            # 增加推广次数
                            update_data['promotion_count'] = existing_token.get('promotion_count', 0) + 1
                            
                            # 更新spread_count（如果新值更大）
                            if spread_count > existing_token.get('spread_count', 0):
                                update_data['spread_count'] = spread_count
                            
                            # 执行更新
                            update_result = await db_adapter.execute_query(
                                'tokens',
                                'update',
                                filters={
                                    'chain': token_data.get('chain'),
                                    'contract': token_data.get('contract')
                                },
                                data=update_data
                            )
                            
                            logger.info(f"成功更新代币 {token_data.get('chain')}/{token_data.get('contract')}")
                            
                            # 返回更新结果
                            return {
                                "success": True,
                                "is_updated": True,
                                "chain": token_data.get('chain'),
                                "contract": token_data.get('contract'),
                                "symbol": token_data.get('symbol', ''),
                                "marketCap": token_data.get('marketCap', 0),
                                "liquidity": token_data.get('liquidity', 0),
                                "price": token_data.get('price'),
                                "dexScreenerUrl": token_data.get('dexScreenerUrl'),
                                "image_url": token_data.get('image_url')
                            }
                        else:
                            logger.warning(f"唯一约束冲突，但无法找到现有代币: {token_data.get('chain')}/{token_data.get('contract')}")
                            return {"error": "唯一约束冲突，但无法找到现有代币"}
                    except Exception as update_error:
                        logger.error(f"冲突后尝试更新代币时出错: {str(update_error)}")
                        return {"error": f"冲突后尝试更新代币时出错: {str(update_error)}"}
                else:
                    # 重新抛出非冲突相关的异常
                    logger.error(f"创建代币时发生未知异常: {str(db_error)}")
                    return {"error": f"创建代币时发生未知异常: {str(db_error)}"}
                
        except Exception as e:
            logger.error(f"创建新代币时发生错误: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {"error": f"创建新代币时发生错误: {str(e)}"}
    
    def _format_market_cap(self, market_cap: float) -> str:
        """格式化市值显示
        
        Args:
            market_cap: 市值数字
            
        Returns:
            str: 格式化后的市值字符串
        """
        if not market_cap or market_cap <= 0:
            return "$0.00"
            
        if market_cap >= 1000000000:  # 十亿 (B)
            return f"${market_cap/1000000000:.2f}B"
        elif market_cap >= 1000000:   # 百万 (M)
            return f"${market_cap/1000000:.2f}M"
        elif market_cap >= 1000:      # 千 (K)
            return f"${market_cap/1000:.2f}K"
        return f"${market_cap:.2f}"

    async def backup_session(self):
        """备份当前session文件"""
        try:
            if not self.client or not self.client.is_connected():
                logger.warning("客户端未连接，无法备份session")
                return False
                
            # 创建备份文件名
            backup_name = f"session_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            backup_path = os.path.join(self.session_backup_dir, backup_name)
            
            # 复制session文件
            session_files = [f"{self.session_path}.session", f"{self.session_path}.session-journal"]
            for file in session_files:
                if os.path.exists(file):
                    backup_file = f"{backup_path}{os.path.splitext(file)[1]}"
                    import shutil
                    shutil.copy2(file, backup_file)
                    logger.info(f"已备份session文件: {file} -> {backup_file}")
            
            # 清理旧的备份
            self._cleanup_old_backups()
            
            self.last_session_backup = time.time()
            return True
            
        except Exception as e:
            logger.error(f"备份session文件时出错: {str(e)}")
            logger.debug(tb.format_exc())
            return False
    
    def _cleanup_old_backups(self):
        """清理旧的session备份文件"""
        try:
            # 获取所有备份文件
            backup_files = []
            for file in os.listdir(self.session_backup_dir):
                if file.startswith("session_backup_"):
                    full_path = os.path.join(self.session_backup_dir, file)
                    backup_files.append((full_path, os.path.getmtime(full_path)))
            
            # 按修改时间排序
            backup_files.sort(key=lambda x: x[1], reverse=True)
            
            # 删除多余的备份
            if len(backup_files) > self.max_session_backups:
                for file_path, _ in backup_files[self.max_session_backups:]:
                    try:
                        os.remove(file_path)
                        logger.info(f"已删除旧的session备份: {file_path}")
                    except Exception as e:
                        logger.warning(f"删除旧的session备份失败: {str(e)}")
                        
        except Exception as e:
            logger.error(f"清理旧的session备份时出错: {str(e)}")
            logger.debug(tb.format_exc())
    
    async def restore_session(self):
        """尝试从最新的备份恢复session"""
        try:
            # 获取所有备份文件
            backup_files = []
            for file in os.listdir(self.session_backup_dir):
                if file.startswith("session_backup_"):
                    full_path = os.path.join(self.session_backup_dir, file)
                    backup_files.append((full_path, os.path.getmtime(full_path)))
            
            if not backup_files:
                logger.warning("没有可用的session备份")
                return False
            
            # 按修改时间排序，获取最新的备份
            backup_files.sort(key=lambda x: x[1], reverse=True)
            latest_backup = backup_files[0][0]
            
            # 复制备份文件到当前session位置
            import shutil
            for ext in ['.session', '.session-journal']:
                backup_file = f"{latest_backup}{ext}"
                if os.path.exists(backup_file):
                    target_file = f"{self.session_path}{ext}"
                    shutil.copy2(backup_file, target_file)
                    logger.info(f"已从备份恢复session文件: {backup_file} -> {target_file}")
            
            return True
            
        except Exception as e:
            logger.error(f"恢复session备份时出错: {str(e)}")
            logger.debug(tb.format_exc())
            return False

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