import logging
from src.database.models import TelegramChannel, Base
import config.settings as config
from telethon import TelegramClient
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest
from telethon.errors import ChatAdminRequiredError, ChannelPrivateError, UsernameNotOccupiedError
from telethon.tl.types import PeerChannel, PeerChat, Channel, Chat
from datetime import datetime
# 导入数据库工厂
from src.database.db_factory import get_db_adapter
import asyncio
from supabase import create_client
# 导入Telegram客户端工厂
from src.core.telegram_client_factory import TelegramClientFactory

# 创建日志记录器
logger = logging.getLogger(__name__)

class ChannelManager:
    """Telegram频道管理类，负责管理监听的频道信息"""
    
    def __init__(self, client=None):
        """初始化频道管理器
        
        Args:
            client: 可选的Telegram客户端实例，用于验证频道
        """
        # 使用数据库工厂获取适配器
        self.db_adapter = get_db_adapter()
        self.client = client
        
        # 添加缓存机制
        self._active_channels_cache = None
        self._cache_timestamp = None
        self._cache_ttl = 60  # 缓存有效期（秒）
        
    async def verify_channel(self, channel_username):
        """验证一个Telegram频道或群组是否存在且可访问
        
        注意：Telegram ID格式说明
        - 用户ID：通常是正数（如123456789）
        - 频道/超级群组ID：通常是负数（如-1001234567890，其中-100是前缀）
        - 普通群组ID：通常也是负数（如-12345678）
        
        在处理和存储ID时，应当保持原始格式，特别是保留负号。
        
        Args:
            channel_username: 频道用户名、频道ID或者群组ID
            
        Returns:
            dict: 包含频道/群组信息的字典，如果不存在则返回None
        """
        if not self.client:
            logger.warning("未提供Telegram客户端，无法验证频道或群组")
            return None
            
        # 检查客户端连接状态
        if not self.client.is_connected():
            logger.warning("客户端未连接，尝试重新连接")
            try:
                await self.client.connect()
                if not await self.client.is_user_authorized():
                    logger.error("客户端未授权，无法验证频道")
                    return None
            except Exception as e:
                logger.error(f"重新连接客户端失败: {str(e)}")
                return None
            
        try:
            # 判断是否是整数ID（处理没有用户名的频道或群组情况）
            try:
                if isinstance(channel_username, int) or (isinstance(channel_username, str) and channel_username.isdigit()):
                    channel_id = int(channel_username)
                    
                    # 尝试作为频道ID获取实体
                    try:
                        # 如果是正整数ID，记录日志但仍然尝试获取
                        if channel_id > 0:
                            logger.info(f"收到正整数ID {channel_id}，尝试直接获取实体")
                            
                        channel_entity = await self.client.get_entity(PeerChannel(channel_id))
                        # 获取完整的频道信息，然后检查是否为超级群组
                        full_channel = await self.client(GetFullChannelRequest(channel=channel_entity))
                        
                        # 获取原始ID（保留负号）
                        original_id = channel_entity.id
                        logger.info(f"成功获取频道实体，原始ID: {original_id}")
                        
                        # 检查是否为超级群组
                        is_group = getattr(channel_entity, 'megagroup', False)
                        is_supergroup = is_group  # 如果是megagroup，则也是supergroup
                    except ValueError:
                        # 如果不是频道ID，尝试作为普通群组ID获取实体
                        try:
                            channel_entity = await self.client.get_entity(PeerChat(channel_id))
                            # 获取原始ID（保留负号）
                            original_id = channel_entity.id
                            logger.info(f"成功获取群组实体，原始ID: {original_id}")
                            
                            # 普通群组
                            is_group = True
                            is_supergroup = False
                        except ValueError:
                            logger.error(f"无法解析ID为 {channel_id} 的频道或群组")
                            return {
                                'exists': False,
                                'error': f"无法解析ID为 {channel_id} 的频道或群组"
                            }
                else:
                    # 使用用户名获取实体（只有频道和超级群组有用户名）
                    channel_entity = await self.client.get_entity(channel_username)
                    # 获取原始ID（保留负号）
                    original_id = channel_entity.id
                    logger.info(f"成功通过用户名获取实体，原始ID: {original_id}")
                    
                    # 默认设置，后面会更新
                    is_group = False
                    is_supergroup = False
            except ValueError as e:
                logger.error(f"无法解析频道或群组: {channel_username}, 错误: {str(e)}")
                return {
                    'username': channel_username,
                    'exists': False,
                    'error': f"无法解析频道或群组: {channel_username}"
                }
                
            # 使用Telethon库正确判断频道类型
            if isinstance(channel_entity, Channel):
                if channel_entity.megagroup:
                    # 超级群组
                    is_group = True
                    is_supergroup = True
                elif channel_entity.broadcast:
                    # 普通频道
                    is_group = False
                    is_supergroup = False
                else:
                    # 其他Channel类型
                    is_group = False
                    is_supergroup = False
            elif isinstance(channel_entity, Chat):
                # 普通群组
                is_group = True
                is_supergroup = False
                
            # 根据实体类型获取完整信息
            if isinstance(channel_entity, Chat):
                # 普通群组类型
                try:
                    full_chat = await self.client(GetFullChatRequest(chat_id=channel_entity.id))
                    return {
                        'username': None,  # 群组没有用户名
                        'channel_id': original_id,  # 使用原始ID（保留负号）
                        'name': getattr(channel_entity, 'title', str(channel_entity.id)),
                        'exists': True,
                        'member_count': getattr(full_chat.full_chat, 'participants_count', 0),
                        'is_group': True,
                        'is_supergroup': False
                    }
                except Exception as e:
                    logger.error(f"获取普通群组信息时出错: {str(e)}")
                    return {
                        'username': None,
                        'channel_id': original_id,  # 使用原始ID（保留负号）
                        'name': getattr(channel_entity, 'title', str(channel_entity.id)),
                        'exists': True,
                        'member_count': 0,
                        'is_group': True,
                        'is_supergroup': False,
                        'error': str(e)
                    }
            else:
                # 频道类型 (包括普通频道和超级群组)
                try:
                    full_channel = await self.client(GetFullChannelRequest(channel=channel_entity))
                    return {
                        'username': getattr(channel_entity, 'username', None),
                        'channel_id': original_id,  # 使用原始ID（保留负号）
                        'name': getattr(channel_entity, 'title', str(channel_entity.id)),
                        'exists': True,
                        'member_count': getattr(full_channel.full_chat, 'participants_count', 0),
                        'is_group': is_group,
                        'is_supergroup': is_supergroup
                    }
                except Exception as e:
                    logger.error(f"获取频道信息时出错: {str(e)}")
                    return {
                        'username': getattr(channel_entity, 'username', None),
                        'channel_id': original_id,  # 使用原始ID（保留负号）
                        'name': getattr(channel_entity, 'title', str(channel_entity.id)),
                        'exists': True,
                        'member_count': 0,
                        'is_group': is_group,
                        'is_supergroup': is_supergroup,
                        'error': str(e)
                    }
        except (ChatAdminRequiredError, ChannelPrivateError, UsernameNotOccupiedError) as e:
            logger.error(f"验证频道或群组 {channel_username} 时出错: {str(e)}")
            return {
                'username': channel_username,
                'exists': False,
                'error': str(e)
            }
            
    def add_channel(self, channel_username, channel_name, chain, channel_id, is_group, is_supergroup, member_count):
        """添加频道/群组到数据库
        
        Args:
            channel_username: 频道用户名 (可以为None)
            channel_name: 频道名称
            chain: 所属区块链
            channel_id: 频道ID (可以为None，但如果username为None则必须提供)
            is_group: 是否为群组
            is_supergroup: 是否为超级群组
            member_count: 成员数量
            
        Returns:
            bool: 是否成功添加
        """
        # 验证参数
        if not channel_username and not channel_id:
            logger.error("添加频道失败：必须提供channel_username或channel_id中的至少一个")
            return False
            
        try:
            # 处理频道ID格式 - 确保存储标准化格式
            # 在Telegram API中，频道ID通常为负数（如-1001234567890）
            # 超级群组ID通常为 -100 开头
            # 普通群组ID通常为 -1 开头
            normalized_channel_id = channel_id
            
            # 如果频道ID是正数，记录日志以便追踪
            if isinstance(channel_id, int) and channel_id > 0:
                logger.info(f"注意: 添加频道时收到正数ID: {channel_id}，可能需要转换格式")
            
            # 直接使用 Supabase 客户端
            import config.settings as config
            from supabase import create_client
            
            supabase_url = config.SUPABASE_URL
            supabase_key = config.SUPABASE_SERVICE_KEY or config.SUPABASE_KEY
            
            if not supabase_url or not supabase_key:
                logger.error("缺少 Supabase 连接信息，无法添加频道")
                return False
                
            # 创建 Supabase 客户端
            supabase = create_client(supabase_url, supabase_key)
            
            # 首先检查频道是否已存在
            query = supabase.table('telegram_channels').select('*')
            
            # 根据提供的信息构建查询条件
            if channel_id:
                query = query.eq('channel_id', channel_id)
            elif channel_username:
                query = query.eq('channel_username', channel_username)
                
            # 执行查询
            response = query.execute()
            
            # 检查结果
            if hasattr(response, 'data') and response.data and len(response.data) > 0:
                existing_channel = response.data[0]
                
                # 检查频道是否活跃
                if existing_channel.get('is_active'):
                    logger.info(f"频道已存在且处于活跃状态: {channel_name}")
                    
                    # 如果有需要更新的信息，进行更新
                    if (existing_channel.get('channel_name') != channel_name or
                        existing_channel.get('member_count') != member_count):
                        
                        update_data = {
                            'channel_name': channel_name,
                            'member_count': member_count,
                            'last_updated': datetime.now().isoformat()
                        }
                        
                        # 更新频道信息
                        update_response = supabase.table('telegram_channels').update(update_data).eq('id', existing_channel['id']).execute()
                        
                        if hasattr(update_response, 'data') and update_response.data:
                            logger.info(f"已更新频道信息: {channel_name}")
                        else:
                            logger.warning(f"更新频道信息失败: {channel_name}")
                    
                    # 清除缓存，强制下次重新获取
                    self._active_channels_cache = None
                    self._cache_timestamp = None
                    
                    return True
                else:
                    # 频道存在但不活跃，重新激活它
                    update_data = {
                        'is_active': True,
                        'channel_name': channel_name,
                        'member_count': member_count,
                        'last_updated': datetime.now().isoformat()
                    }
                    
                    update_response = supabase.table('telegram_channels').update(update_data).eq('id', existing_channel['id']).execute()
                    
                    if hasattr(update_response, 'data') and update_response.data:
                        logger.info(f"已重新激活频道: {channel_name}")
                        # 清除缓存，强制下次重新获取
                        self._active_channels_cache = None
                        self._cache_timestamp = None
                        return True
                    else:
                        logger.error(f"重新激活频道失败: {channel_name}")
                        return False
            
            # 频道不存在，创建新频道
            channel_data = {
                'channel_username': channel_username,
                'channel_id': normalized_channel_id,
                'channel_name': channel_name,
                'chain': chain,
                'is_active': True,
                'is_group': is_group,
                'is_supergroup': is_supergroup,
                'member_count': member_count,
                'created_at': datetime.now().isoformat(),
                'last_updated': datetime.now().isoformat()
            }
            
            # 添加新频道
            response = supabase.table('telegram_channels').insert(channel_data).execute()
            
            if hasattr(response, 'data') and response.data:
                logger.info(f"已添加新频道: {channel_name}")
                # 清除缓存，强制下次重新获取
                self._active_channels_cache = None
                self._cache_timestamp = None
                return True
            else:
                logger.error(f"添加频道失败: {channel_name}, 无返回数据")
                logger.error(f"响应: {response}")
                return False
                
        except Exception as e:
            logger.error(f"添加频道 {channel_name} 时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False
            
    def remove_channel(self, channel_username):
        """从数据库移除频道
        
        Args:
            channel_username: 频道用户名
            
        Returns:
            bool: 是否成功移除
        """
        try:
            # 直接使用 Supabase 客户端获取数据，避免异步调用导致的问题
            import config.settings as config
            from supabase import create_client
            
            # 获取 Supabase 配置
            supabase_url = config.SUPABASE_URL
            supabase_key = config.SUPABASE_SERVICE_KEY or config.SUPABASE_KEY
            
            if not supabase_url or not supabase_key:
                logger.error("缺少 Supabase 连接信息，无法移除频道")
                return False
                
            # 创建 Supabase 客户端
            supabase = create_client(supabase_url, supabase_key)
            
            # 查询频道
            logger.info(f"查询频道: {channel_username}")
            response = supabase.table('telegram_channels').select('*').eq('channel_username', channel_username).limit(1).execute()
            
            if not response.data or len(response.data) == 0:
                logger.warning(f"未找到频道: {channel_username}")
                return False
            
            # 获取频道信息
            channel = response.data[0] if hasattr(response, 'data') and response.data and len(response.data) > 0 else None
            
            if not channel:
                logger.warning(f"无法获取频道数据: {channel_username}")
                return False
                
            # 更新频道状态为非活跃
            update_data = {
                'is_active': False,
                'last_updated': datetime.now().isoformat()
            }
            
            # 更新数据库
            update_response = supabase.table('telegram_channels').update(update_data).eq('id', channel['id']).execute()
            
            if hasattr(update_response, 'data') and update_response.data:
                logger.info(f"已移除频道: {channel.get('channel_name', channel_username)}")
                # 清除缓存，强制下次重新获取
                self._active_channels_cache = None
                self._cache_timestamp = None
                return True
            else:
                logger.error(f"移除频道失败: {channel_username}")
                logger.error(f"响应: {update_response}")
                return False
                
        except Exception as e:
            logger.error(f"移除频道 {channel_username} 时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def get_active_channels(self):
        """获取所有活跃的频道
        
        Returns:
            list: 活跃频道列表
        """
        try:
            # 检查缓存是否有效
            if self._active_channels_cache is not None and self._cache_timestamp is not None:
                now = datetime.now()
                cache_age = (now - self._cache_timestamp).total_seconds()
                if cache_age < self._cache_ttl:
                    logger.debug(f"使用缓存的活跃频道数据，缓存年龄: {cache_age:.1f}秒")
                    return self._active_channels_cache

            # 直接使用 Supabase 客户端获取数据，避免异步调用导致的问题
            import config.settings as config
            from supabase import create_client
            
            # 获取 Supabase 配置
            supabase_url = config.SUPABASE_URL
            supabase_key = config.SUPABASE_KEY
            
            if not supabase_url or not supabase_key:
                logger.error("缺少 SUPABASE_URL 或 SUPABASE_KEY 配置")
                return []
                
            # 创建 Supabase 客户端并直接获取数据
            supabase = create_client(supabase_url, supabase_key)
            
            # 执行查询
            logger.info("直接使用 Supabase 客户端获取活跃频道")
            response = supabase.table('telegram_channels').select('*').eq('is_active', True).execute()
            
            if hasattr(response, 'data'):
                channels = response.data
                logger.info(f"成功获取 {len(channels)} 个活跃频道")
                
                # 更新缓存
                self._active_channels_cache = channels
                self._cache_timestamp = datetime.now()
                
                return channels
            else:
                logger.error("获取活跃频道时，Supabase 未返回 data 字段")
                logger.error(f"响应: {response}")
                return []
                
        except Exception as e:
            logger.error(f"获取活跃频道时出错: {str(e)}")
            logger.error(f"错误类型: {type(e).__name__}")
            import traceback
            logger.error(traceback.format_exc())
            # 返回空列表作为后备方案
            return []
    
    def get_all_channels(self):
        """获取所有频道
        
        Returns:
            list: 所有频道列表
        """
        try:
            # 直接使用 Supabase 客户端获取数据，避免异步调用导致的问题
            import config.settings as config
            from supabase import create_client
            
            # 获取 Supabase 配置
            supabase_url = config.SUPABASE_URL
            supabase_key = config.SUPABASE_KEY
            
            if not supabase_url or not supabase_key:
                logger.error("缺少 SUPABASE_URL 或 SUPABASE_KEY 配置")
                return []
                
            # 创建 Supabase 客户端并直接获取数据
            supabase = create_client(supabase_url, supabase_key)
            
            # 执行查询
            logger.info("直接使用 Supabase 客户端获取所有频道")
            response = supabase.table('telegram_channels').select('*').execute()
            
            if hasattr(response, 'data'):
                channels = response.data
                logger.info(f"成功获取 {len(channels)} 个频道")
                return channels
            else:
                logger.error("获取频道时，Supabase 未返回 data 字段")
                logger.error(f"响应: {response}")
                return []
                
        except Exception as e:
            logger.error(f"获取所有频道时出错: {str(e)}")
            logger.error(f"错误类型: {type(e).__name__}")
            import traceback
            logger.error(traceback.format_exc())
            return []
            
    async def update_channels(self, default_channels=None):
        """
        更新现有频道的信息
        
        Args:
            default_channels: 默认频道列表
        
        Returns:
            list: 更新后的频道列表
        """
        try:
            if not self.client:
                logger.error("未提供Telegram客户端，无法更新频道信息")
                return []
                
            # 检查客户端连接状态
            if not self.client.is_connected():
                logger.warning("客户端未连接，尝试重新连接")
                try:
                    await self.client.connect()
                    if not await self.client.is_user_authorized():
                        logger.error("客户端未授权，无法更新频道")
                        # 尝试使用客户端工厂获取新客户端
                        try:
                            # 从配置中获取API认证信息
                            import config.settings as settings
                            api_id = settings.env_config.API_ID
                            api_hash = settings.env_config.API_HASH
                            # 获取当前session路径
                            session_path = self.client.session.filename.replace('.session', '')
                            
                            # 使用工厂获取新客户端
                            self.client = await TelegramClientFactory.get_client(
                                session_path,
                                api_id,
                                api_hash,
                                connection_retries=5,
                                auto_reconnect=True,
                                retry_delay=5,
                                request_retries=5,
                                flood_sleep_threshold=60,
                                timeout=30
                            )
                            
                            if not self.client or not await self.client.is_user_authorized():
                                logger.error("无法获取授权的客户端，更新频道失败")
                                return []
                        except Exception as client_error:
                            logger.error(f"获取新客户端失败: {str(client_error)}")
                            return []
                except Exception as e:
                    logger.error(f"重新连接客户端失败: {str(e)}")
                    return []
            
            # 获取所有频道
            channels = []
            try:
                # 首先从数据库获取频道列表
                db_channels = await self.db_adapter.execute_query(
                    'telegram_channels', 
                    'select',
                    order_by={'last_updated': 'asc'}
                )
                
                # 如果没有找到任何频道，并且提供了默认频道，则使用默认频道
                if (not db_channels or len(db_channels) == 0) and default_channels:
                    channels = default_channels
                    logger.info(f"使用提供的默认频道列表: {len(channels)} 个频道")
                else:
                    channels = db_channels
                    logger.info(f"从数据库获取到 {len(channels)} 个频道")
            except Exception as e:
                logger.error(f"从数据库获取频道列表时出错: {str(e)}")
                if default_channels:
                    channels = default_channels
                    logger.info(f"使用提供的默认频道列表: {len(channels)} 个频道")
                else:
                    return []
            
            # 更新频道信息
            updated_channels = []
            
            for channel in channels:
                try:
                    channel_id = channel.get('channel_id')
                    channel_username = channel.get('channel_username')
                    
                    # 跳过无效频道
                    if not channel_id and not channel_username:
                        continue
                        
                    identifier = channel_username or channel_id
                    logger.info(f"正在更新频道: {identifier}")
                    
                    # 验证频道
                    channel_info = await self.verify_channel(identifier)
                    
                    if not channel_info or not channel_info.get('exists', False):
                        logger.warning(f"频道 {identifier} 不存在或无法访问")
                        continue
                    
                    # 更新频道信息
                    update_data = {
                        'last_updated': datetime.now().isoformat(),
                        'channel_name': channel_info.get('name'),
                        'member_count': channel_info.get('member_count', 0),
                        'is_group': channel_info.get('is_group', False),
                        'is_supergroup': channel_info.get('is_supergroup', False)
                    }
                    
                    # 如果有username，更新username
                    if channel_info.get('username'):
                        update_data['channel_username'] = channel_info.get('username')
                    
                    # 更新数据库
                    try:
                        await self.db_adapter.execute_query(
                            'telegram_channels',
                            'update',
                            filters={'channel_id': channel_id},
                            data=update_data
                        )
                        logger.info(f"已更新频道 {identifier} 的信息")
                        
                        # 添加到更新后的频道列表
                        updated_channels.append({**channel, **update_data})
                    except Exception as db_error:
                        logger.error(f"更新频道 {identifier} 数据库信息时出错: {str(db_error)}")
                
                except Exception as e:
                    logger.error(f"更新频道 {channel.get('channel_id') or channel.get('channel_username')} 时出错: {str(e)}")
                    continue
            
            # 清除缓存，确保下次获取最新数据
            self._active_channels_cache = None
            self._cache_timestamp = None
            
            return updated_channels
                
        except Exception as e:
            logger.error(f"更新频道信息时出错: {str(e)}")
            return [] 