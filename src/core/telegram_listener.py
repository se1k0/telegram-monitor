import asyncio
from telethon import TelegramClient, events
import config.settings as config
from src.database.models import init_db
from src.database.db_handler import (
    save_telegram_message, 
    extract_promotion_info, 
    save_token_info, 
    process_batches
)
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import sqlite3
from src.utils.utils import parse_market_cap, format_market_cap
from src.core.channel_manager import ChannelManager, DEFAULT_CHANNELS
from src.core.channel_discovery import ChannelDiscovery
import logging
import traceback
from functools import wraps
from typing import Callable, Any, Optional
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
    """异步函数重试装饰器
    
    Args:
        max_retries: 最大重试次数
        delay: 初始延迟时间（秒）
        backoff: 延迟时间的增长因子
        exceptions: 需要重试的异常类型
        
    Returns:
        装饰后的异步函数
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retry_count = 0
            current_delay = delay
            
            while True:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    retry_count += 1
                    if retry_count > max_retries:
                        logger.error(f"函数 {func.__name__} 在重试 {max_retries} 次后失败: {str(e)}")
                        raise
                    
                    logger.warning(f"函数 {func.__name__} 失败，正在重试 ({retry_count}/{max_retries}): {str(e)}")
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
        
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
        
        # 生成唯一的会话名称，避免冲突
        session_name = f'tg_session_{os.getpid()}_{int(time.time())}'
        self.session_path = os.path.join(session_dir, session_name)
        
        # 设置Telethon的SQLite连接参数
        self.connection_retries = 3
        self.auto_reconnect = True
        self.retry_delay = 1
        
        # 初始化客户端，添加SQLite连接参数
        self.client = TelegramClient(
            self.session_path,
            self.api_id, 
            self.api_hash,
            connection_retries=self.connection_retries,
            auto_reconnect=self.auto_reconnect,
            retry_delay=self.retry_delay
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
            logger.debug(traceback.format_exc())
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
            logger.debug(traceback.format_exc())
            raise  # 让装饰器捕获异常并处理重试
    
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
                is_group=is_group,
                is_supergroup=is_supergroup  # 添加超级群组标记
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
                    logger.debug(traceback.format_exc())
            
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
                        'channel_name': channel_identifier  # 添加channel_name字段
                    }
                    
                    # 添加情感分析相关字段
                    if hasattr(promo, 'sentiment_score') and promo.sentiment_score is not None:
                        token_data['sentiment_score'] = promo.sentiment_score
                    
                    if hasattr(promo, 'positive_words') and promo.positive_words:
                        token_data['positive_words'] = ', '.join(promo.positive_words)
                        
                    if hasattr(promo, 'negative_words') and promo.negative_words:
                        token_data['negative_words'] = ', '.join(promo.negative_words)
                        
                    if hasattr(promo, 'hype_score') and promo.hype_score is not None:
                        token_data['hype_score'] = promo.hype_score
                        
                    if hasattr(promo, 'risk_level') and promo.risk_level:
                        token_data['risk_level'] = promo.risk_level
                    
                    result = save_token_info(token_data)
                    if result:
                        logger.info(f"成功更新 token 信息: {promo.token_symbol}")
                        logger.info(f"首次推荐时间: {current_time}")
                        logger.info(f"当前市值: {market_cap_formatted}")
                    else:
                        logger.warning(f"保存 token 信息失败: {promo.token_symbol}")
                    
                except Exception as e:
                    logger.error(f"处理 token 数据时出错: {str(e)}")
                    logger.error(f"Token Symbol: {getattr(promo, 'token_symbol', 'Unknown')}")
                    logger.error(f"Market Cap: {getattr(promo, 'market_cap', 'Unknown')}")
                    logger.debug(traceback.format_exc())
            
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
            logger.debug(traceback.format_exc())
            
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
            logger.debug(traceback.format_exc())
    
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
            logger.debug(traceback.format_exc())
    
    async def start(self):
        """启动服务"""
        try:
            # 连接Telegram
            if not self.client.is_connected():
                await self.client.connect()
                
            if not await self.client.is_user_authorized():
                logger.info("用户未登录，开始登录流程...")
                
                try:
                    # 提示用户输入手机号码
                    phone = input("请输入您的手机号码 (包含国家代码，如 +86xxxxxxxxxx): ")
                    await self.client.send_code_request(phone)
                    
                    # 提示用户输入验证码
                    code = input("请输入您收到的验证码: ")
                    await self.client.sign_in(phone, code)
                    
                    # 检查是否需要两步验证
                    if not await self.client.is_user_authorized():
                        # 可能需要两步验证密码
                        password = input("请输入您的两步验证密码: ")
                        await self.client.sign_in(password=password)
                    
                    # 登录成功
                    me = await self.client.get_me()
                    logger.info(f"登录成功! 已登录为: {me.first_name} (ID: {me.id})")
                except Exception as e:
                    logger.error(f"登录过程中出错: {str(e)}")
                    return False
                
                # 再次检查是否已登录
                if not await self.client.is_user_authorized():
                    logger.error("登录失败，请检查凭据后重试")
                    return False
                
            # 设置频道
            await self.setup_channels()
            self.is_running = True
            
            # 启动批处理任务
            self.batch_task = asyncio.create_task(process_batches())
            logger.info("数据库批处理任务已启动")
            
            # 启动健康检查和自动发现任务
            self.health_check_task = asyncio.create_task(self.health_check())
            self.discovery_task = asyncio.create_task(self.discovery_loop())
            
            logger.info("监听服务已启动")
            
            # 返回self，而不是进入无限循环
            return self
            
        except Exception as e:
            self.is_running = False
            logger.error(f"启动监听服务时出错: {str(e)}")
            logger.debug(traceback.format_exc())
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
                logger.debug(traceback.format_exc())
                await asyncio.sleep(60)  # 出错后等待一分钟再继续
    
    async def health_check(self):
        """定期检查监听器的健康状态，确保其正常运行"""
        check_interval = 300  # 5分钟检查一次
        reconnect_attempts = 0
        max_reconnect_attempts = 5
        
        # 记录启动时间
        if not hasattr(self, '_start_time'):
            self._start_time = datetime.now()
            
        # 创建健康状态文件
        health_file = os.path.join("./logs", "health_status.txt")
        
        while self.is_running:
            await asyncio.sleep(check_interval)
            
            try:
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
                    "uptime_hours": (datetime.now() - self._start_time).total_seconds() / 3600
                }
                
                # 保存健康状态到文件
                with open(health_file, "w") as f:
                    for key, value in health_status.items():
                        f.write(f"{key}: {value}\n")
                    
                # 检查连接状态
                if not self.client.is_connected():
                    logger.warning("检测到客户端已断开连接，正在尝试重新连接...")
                    try:
                        await self.client.connect()
                        reconnect_attempts = 0
                        logger.info("客户端已重新连接")
                    except Exception as e:
                        reconnect_attempts += 1
                        logger.error(f"重新连接失败 (尝试 {reconnect_attempts}/{max_reconnect_attempts}): {str(e)}")
                        
                        # 如果多次重连失败，尝试完全重启客户端
                        if reconnect_attempts >= max_reconnect_attempts:
                            logger.critical(f"多次重连失败，尝试重新启动客户端...")
                            try:
                                await self.client.disconnect()
                                await asyncio.sleep(5)  # 等待一段时间
                                
                                # 重新创建客户端
                                self.client = TelegramClient(
                                    self.session_path,
                                    self.api_id, 
                                    self.api_hash,
                                    connection_retries=self.connection_retries,
                                    auto_reconnect=self.auto_reconnect,
                                    retry_delay=self.retry_delay
                                )
                                await self.client.connect()
                                await self.setup_channels()
                                logger.info("客户端已成功重启和重新初始化")
                                reconnect_attempts = 0
                            except Exception as restart_error:
                                logger.critical(f"重启客户端失败: {str(restart_error)}")
                
                # 验证活跃频道
                if not self.chain_map:
                    logger.warning("没有活跃的频道，尝试重新设置...")
                    await self.setup_channels()
                
                # 检查消息处理器
                if not self.event_handlers or 'new_message' not in self.event_handlers:
                    logger.warning("消息处理器未注册，尝试重新注册...")
                    await self.register_handlers()
                
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
                except ImportError:
                    logger.info("psutil未安装，无法获取系统资源信息")
                except Exception as e:
                    logger.error(f"获取系统资源信息出错: {str(e)}")
                
                # 健康检查成功
                logger.info(f"健康检查完成，监听器状态正常 - 活跃频道: {len(self.chain_map)}, 消息处理器: {len(self.event_handlers)}")
                
            except Exception as e:
                logger.error(f"健康检查时出错: {str(e)}")
                logger.debug(traceback.format_exc())

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
        logger.debug(traceback.format_exc())
        # 仍然尝试优雅关闭
        try:
            loop.run_until_complete(listener.stop())
        except Exception as stop_error:
            logger.error(f"停止监听器时出错: {str(stop_error)}")
    finally:
        # 确保循环关闭
        if not loop.is_closed():
            loop.close()