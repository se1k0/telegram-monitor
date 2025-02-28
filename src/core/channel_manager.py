import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.database.models import TelegramChannel, Base
import config.settings as config
from telethon import TelegramClient
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest
from telethon.errors import ChatAdminRequiredError, ChannelPrivateError, UsernameNotOccupiedError
from telethon.tl.types import PeerChannel, PeerChat, Channel, Chat
from datetime import datetime

# 创建日志记录器
logger = logging.getLogger(__name__)

class ChannelManager:
    """Telegram频道管理类，负责管理监听的频道信息"""
    
    def __init__(self, client=None):
        """初始化频道管理器
        
        Args:
            client: 可选的Telegram客户端实例，用于验证频道
        """
        self.engine = create_engine(config.DATABASE_URI)
        self.Session = sessionmaker(bind=self.engine)
        self.client = client
        
    async def verify_channel(self, channel_username):
        """验证一个Telegram频道或群组是否存在且可访问
        
        Args:
            channel_username: 频道用户名、频道ID或者群组ID
            
        Returns:
            dict: 包含频道/群组信息的字典，如果不存在则返回None
        """
        if not self.client:
            logger.warning("未提供Telegram客户端，无法验证频道或群组")
            return None
            
        try:
            # 判断是否是整数ID（处理没有用户名的频道或群组情况）
            try:
                if isinstance(channel_username, int) or (isinstance(channel_username, str) and channel_username.isdigit()):
                    channel_id = int(channel_username)
                    
                    # 尝试作为频道ID获取实体
                    try:
                        channel_entity = await self.client.get_entity(PeerChannel(channel_id))
                        # 获取完整的频道信息，然后检查是否为超级群组
                        full_channel = await self.client(GetFullChannelRequest(channel=channel_entity))
                        # 检查是否为超级群组
                        is_group = getattr(channel_entity, 'megagroup', False)
                        is_supergroup = is_group  # 如果是megagroup，则也是supergroup
                    except ValueError:
                        # 如果不是频道ID，尝试作为普通群组ID获取实体
                        try:
                            channel_entity = await self.client.get_entity(PeerChat(channel_id))
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
                        'channel_id': channel_entity.id,
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
                        'channel_id': channel_entity.id,
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
                        'channel_id': channel_entity.id,
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
                        'channel_id': channel_entity.id,
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
            # 检查是否已存在同名频道（根据用户名或ID）
            existing_channels = []
            session = self.Session()
            
            if channel_username:
                existing_username = session.query(TelegramChannel).filter(
                    TelegramChannel.channel_username == channel_username,
                ).first()
                if existing_username:
                    existing_channels.append(existing_username)
                    
            if channel_id:
                existing_id = session.query(TelegramChannel).filter(
                    TelegramChannel.channel_id == channel_id,
                ).first()
                if existing_id:
                    existing_channels.append(existing_id)
                    
            # 处理已存在的情况
            if existing_channels:
                for existing in existing_channels:
                    # 如果频道存在但被标记为非活跃，则重新激活
                    if not existing.is_active:
                        existing.is_active = True
                        # 更新最新信息
                        existing.channel_name = channel_name
                        existing.chain = chain
                        existing.is_group = is_group
                        existing.is_supergroup = is_supergroup
                        if member_count is not None:
                            existing.member_count = member_count
                        session.commit()
                        
                        # 确定频道类型描述
                        channel_type = "普通频道"
                        if is_supergroup:
                            channel_type = "超级群组"
                        elif is_group:
                            channel_type = "普通群组"
                            
                        logger.info(f"重新激活{channel_type}: {channel_name}")
                        return True
                    else:
                        # 如果频道已存在并且活跃，则记录信息
                        logger.info(f"{'超级群组' if is_supergroup else ('普通群组' if is_group else '普通频道')}已存在: {channel_name}")
                        return False
                        
            # 创建新频道记录
            new_channel = TelegramChannel(
                channel_username=channel_username,
                channel_id=channel_id,
                channel_name=channel_name,
                chain=chain,
                is_active=True,
                is_group=is_group,
                is_supergroup=is_supergroup,
                member_count=member_count,
                created_at=datetime.now(),  # 保持为datetime对象
                last_updated=None  # 将last_updated初始化为None
            )
            
            session.add(new_channel)
            session.commit()
            
            # 根据频道类型和标识符方式输出不同的日志
            if channel_username:
                identifier = f"@{channel_username}"
            else:
                identifier = f"ID: {channel_id}"
                
            channel_type = "普通频道"
            if is_supergroup:
                channel_type = "超级群组"
            elif is_group:
                channel_type = "普通群组"
                
            logger.info(f"成功添加{channel_type}: {channel_name} ({identifier})")
            return True
            
        except Exception as e:
            logger.error(f"添加{'超级群组' if is_supergroup else ('普通群组' if is_group else '普通频道')}时出错: {str(e)}")
            if 'session' in locals():
                session.rollback()
            return False
        finally:
            if 'session' in locals():
                session.close()
            
    def remove_channel(self, channel_username):
        """从监控列表中移除一个频道
        
        Args:
            channel_username: 要移除的频道用户名
            
        Returns:
            bool: 移除是否成功
        """
        session = self.Session()
        try:
            channel = session.query(TelegramChannel).filter_by(
                channel_username=channel_username
            ).first()
            
            if not channel:
                logger.warning(f"要移除的频道不存在: {channel_username}")
                return False
                
            # 标记为不活跃而不是删除
            channel.is_active = False
            session.commit()
            logger.info(f"已移除频道: {channel_username}")
            return True
            
        except Exception as e:
            session.rollback()
            logger.error(f"移除频道时出错: {str(e)}")
            return False
        finally:
            session.close()
            
    def get_active_channels(self):
        """获取所有活跃的频道
        
        Returns:
            dict: 频道标识符到链的映射字典
        """
        session = self.Session()
        try:
            channels = session.query(TelegramChannel).filter_by(is_active=True).all()
            channel_map = {}
            
            for channel in channels:
                # 使用用户名或ID作为标识符
                if channel.channel_username:
                    channel_map[channel.channel_username] = channel.chain
                    
                # 如果有ID，也添加ID到链的映射
                if channel.channel_id:
                    id_key = str(channel.channel_id)
                    channel_map[id_key] = channel.chain
                    
                # 如果既没有用户名也没有ID，尝试使用另一种标识符
                if not channel.channel_username and not channel.channel_id:
                    logger.warning(f"频道 ID {channel.id} 没有可用的标识符，无法添加到活跃频道映射")
                    
            return channel_map
        except Exception as e:
            logger.error(f"获取活跃频道时出错: {str(e)}")
            return {}
        finally:
            session.close()
            
    def get_all_channels(self):
        """获取所有频道，包括活跃和不活跃的
        
        Returns:
            list: 频道对象列表
        """
        session = self.Session()
        try:
            return session.query(TelegramChannel).all()
        except Exception as e:
            logger.error(f"获取所有频道时出错: {str(e)}")
            return []
        finally:
            session.close()
            
    async def update_channels(self, default_channels=None):
        """更新频道状态，验证所有频道和群组是否仍然存在
        
        Args:
            default_channels: 默认频道字典 {channel_username: chain}，
                             用于首次运行时初始化
        
        Returns:
            tuple: (chain_map, entity_map)
                - chain_map: 字典，映射频道标识符到链名
                - entity_map: 字典，映射频道ID到频道实体对象
        """
        # 首次运行时使用默认频道
        if default_channels:
            for username, chain in default_channels.items():
                # 默认用户名的频道，假定为普通频道而非群组
                self.add_channel(
                    channel_username=username, 
                    channel_name=username, 
                    chain=chain,
                    channel_id=None,  # 初始化时没有ID信息
                    is_group=False,   # 默认不是群组
                    is_supergroup=False,  # 默认不是超级群组
                    member_count=0    # 初始化时不知道成员数
                )
                
        if not self.client:
            logger.warning("未提供Telegram客户端，无法验证频道状态")
            return self.get_active_channels(), {}
            
        session = self.Session()
        entity_map = {}  # 频道ID到频道实体的映射
        chain_map = {}   # 频道标识符到链的映射
        
        try:
            channels = session.query(TelegramChannel).all()
            for channel in channels:
                try:
                    # 确定频道标识符（用户名或ID）
                    channel_identifier = channel.channel_username or (f"id_{channel.channel_id}" if channel.channel_id else None)
                    
                    # 跳过无法识别的频道
                    if not channel_identifier:
                        logger.warning(f"频道 ID {channel.id} 没有用户名或频道ID，无法识别")
                        continue

                    # 验证频道是否存在
                    if channel.is_active:
                        try:
                            entity = None
                            # 尝试获取实体
                            if channel.channel_username:
                                # 使用用户名获取频道
                                entity = await self.client.get_entity(channel.channel_username)
                            elif channel.channel_id:
                                # 根据是否为群组使用不同方式获取实体
                                if channel.is_group and not channel.is_supergroup:
                                    # 普通群组 - 使用PeerChat
                                    entity = await self.client.get_entity(PeerChat(channel.channel_id))
                                else:
                                    # 频道或超级群组 - 超级群组在技术上是Channel类型，使用PeerChannel
                                    # 注意：超级群组虽然标记为is_group=True，但它们应该用PeerChannel而不是PeerChat来获取
                                    entity = await self.client.get_entity(PeerChannel(channel.channel_id))
                                
                            if entity:
                                # 更新频道类型信息
                                is_group = False
                                is_supergroup = False
                                
                                # 使用Telethon库的正确判断方式
                                if isinstance(entity, Channel):
                                    if entity.megagroup:
                                        # 超级群组
                                        is_supergroup = True
                                        is_group = True
                                    elif entity.broadcast:
                                        # 普通频道
                                        is_group = False
                                        is_supergroup = False
                                    else:
                                        # 其他Channel类型
                                        is_group = False
                                elif isinstance(entity, Chat):
                                    # 普通群组
                                    is_group = True
                                    is_supergroup = False
                                
                                # 如果频道类型有变化，更新数据库
                                if channel.is_group != is_group or channel.is_supergroup != is_supergroup:
                                    logger.info(f"更新频道 {channel_identifier} 的类型信息: 群组={is_group}, 超级群组={is_supergroup}")
                                    channel.is_group = is_group
                                    channel.is_supergroup = is_supergroup
                                    channel.last_updated = datetime.now()
                                    session.commit()
                                
                                # 存储实体
                                entity_id = str(entity.id)
                                entity_map[entity_id] = entity
                                
                                # 添加映射关系
                                chain_map[entity_id] = channel.chain
                                if channel.channel_username:
                                    chain_map[channel.channel_username] = channel.chain
                                
                                logger.info(f"验证{'超级群组' if is_supergroup else ('普通群组' if is_group else '普通频道')} {channel_identifier} 成功")
                            else:
                                logger.warning(f"无法获取{'群组' if channel.is_group else '频道'} {channel_identifier} 的实体")
                                
                        except Exception as e:
                            logger.error(f"验证{'群组' if channel.is_group else '频道'} {channel_identifier} 时出错: {str(e)}")
                            # 保持频道活跃状态，即使验证失败
                            if channel.channel_username:
                                chain_map[channel.channel_username] = channel.chain
                            if channel.channel_id:
                                chain_map[str(channel.channel_id)] = channel.chain
                except Exception as e:
                    logger.error(f"处理频道ID {channel.id} 时出错: {str(e)}")
            
            return chain_map, entity_map
        except Exception as e:
            logger.error(f"更新频道状态时出错: {str(e)}")
            return {}, {}
        finally:
            session.close()

# 初始化需要导入的默认频道
DEFAULT_CHANNELS = {
    'MomentumTrackerCN': 'SOL',
    'ETH_Momentum_Tracker_CN': 'ETH'
} 