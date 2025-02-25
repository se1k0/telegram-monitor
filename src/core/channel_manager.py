import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.database.models import TelegramChannel, Base
import config.settings as config
from telethon import TelegramClient
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.errors import ChatAdminRequiredError, ChannelPrivateError, UsernameNotOccupiedError

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
        """验证一个Telegram频道是否存在且可访问
        
        Args:
            channel_username: 频道用户名
            
        Returns:
            dict: 包含频道信息的字典，如果频道不存在则返回None
        """
        if not self.client:
            logger.warning("未提供Telegram客户端，无法验证频道")
            return None
            
        try:
            channel_entity = await self.client.get_entity(channel_username)
            full_channel = await self.client(GetFullChannelRequest(channel=channel_entity))
            
            return {
                'username': channel_username,
                'name': getattr(channel_entity, 'title', channel_username),
                'exists': True,
                'member_count': getattr(full_channel.full_chat, 'participants_count', 0)
            }
        except (ChatAdminRequiredError, ChannelPrivateError, UsernameNotOccupiedError) as e:
            logger.error(f"验证频道 {channel_username} 时出错: {str(e)}")
            return {
                'username': channel_username,
                'exists': False,
                'error': str(e)
            }
            
    def add_channel(self, channel_username, channel_name, chain):
        """添加一个新的频道到数据库
        
        Args:
            channel_username: 频道用户名
            channel_name: 频道名称
            chain: 关联的区块链
            
        Returns:
            bool: 添加是否成功
        """
        session = self.Session()
        try:
            # 检查频道是否已存在
            existing_channel = session.query(TelegramChannel).filter_by(
                channel_username=channel_username
            ).first()
            
            if existing_channel:
                # 如果已存在但被标记为不活跃，则重新激活
                if not existing_channel.is_active:
                    existing_channel.is_active = True
                    existing_channel.chain = chain
                    existing_channel.channel_name = channel_name
                    session.commit()
                    logger.info(f"重新激活频道: {channel_username}")
                    return True
                else:
                    logger.info(f"频道已存在: {channel_username}")
                    return False
                    
            # 添加新频道
            new_channel = TelegramChannel(
                channel_username=channel_username,
                channel_name=channel_name,
                chain=chain,
                is_active=True
            )
            session.add(new_channel)
            session.commit()
            logger.info(f"添加新频道: {channel_username}, 链: {chain}")
            return True
            
        except Exception as e:
            session.rollback()
            logger.error(f"添加频道时出错: {str(e)}")
            return False
        finally:
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
            dict: 频道用户名到链的映射字典
        """
        session = self.Session()
        try:
            channels = session.query(TelegramChannel).filter_by(is_active=True).all()
            channel_map = {channel.channel_username: channel.chain for channel in channels}
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
        """更新频道状态，验证所有频道是否仍然存在
        
        Args:
            default_channels: 默认频道字典 {channel_username: chain}，
                             用于首次运行时初始化
        
        Returns:
            dict: 活跃的频道映射
        """
        # 首次运行时使用默认频道
        if default_channels:
            for username, chain in default_channels.items():
                self.add_channel(username, username, chain)
                
        if not self.client:
            logger.warning("未提供Telegram客户端，无法验证频道状态")
            return self.get_active_channels()
            
        session = self.Session()
        try:
            channels = session.query(TelegramChannel).all()
            for channel in channels:
                try:
                    channel_info = await self.verify_channel(channel.channel_username)
                    if channel_info and channel_info.get('exists', False):
                        # 更新频道信息
                        channel.channel_name = channel_info.get('name', channel.channel_name)
                        channel.is_active = True
                        logger.info(f"频道验证成功: {channel.channel_username}")
                    else:
                        # 标记为不活跃
                        channel.is_active = False
                        logger.warning(f"频道验证失败，标记为不活跃: {channel.channel_username}")
                except Exception as e:
                    logger.error(f"验证频道 {channel.channel_username} 时出错: {str(e)}")
                    
            session.commit()
            return self.get_active_channels()
            
        except Exception as e:
            session.rollback()
            logger.error(f"更新频道状态时出错: {str(e)}")
            return self.get_active_channels()
        finally:
            session.close()

# 初始化需要导入的默认频道
DEFAULT_CHANNELS = {
    'MomentumTrackerCN': 'SOL',
    'ETH_Momentum_Tracker_CN': 'ETH'
} 