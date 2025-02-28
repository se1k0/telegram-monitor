import logging
import asyncio
from telethon import TelegramClient, events
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty, Channel, Chat, ChannelFull, PeerChannel, PeerChat, PeerUser
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest
from telethon.errors import ChannelPrivateError, ChatAdminRequiredError
from typing import List, Dict, Optional, Tuple, Set
import re
from datetime import datetime, timedelta
from .channel_manager import ChannelManager
from config.settings import env_config

# 设置日志
try:
    from src.utils.logger import get_logger
    logger = get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

class ChannelDiscovery:
    """用于自动发现Telegram群聊频道的类"""
    
    def __init__(self, client: TelegramClient, channel_manager: ChannelManager):
        """初始化频道发现类
        
        Args:
            client: 已连接的Telegram客户端实例
            channel_manager: 频道管理器实例
        """
        self.client = client
        self.channel_manager = channel_manager
        # 要排除的频道列表（不需要自动添加的）
        self.excluded_channels = set(env_config.EXCLUDED_CHANNELS)
        # 频道分类的关键词规则（用于推断频道所属的区块链）
        self.chain_keywords = env_config.CHAIN_KEYWORDS
        
    async def discover_channels(self, limit: int = None) -> List[Dict]:
        """发现用户对话中的所有频道和群组
        
        Args:
            limit: 要获取的对话数量限制
            
        Returns:
            List[Dict]: 发现的频道和群组列表
        """
        # 如果未指定limit参数，使用配置中的值
        if limit is None:
            # 由于配置文件中没有这个具体参数，使用默认值100
            limit = 100
            
        logger.info(f"开始发现频道和群组，最大数量: {limit}")
        discovered_channels = []
        
        try:
            # 获取对话列表
            result = await self.client(GetDialogsRequest(
                offset_date=None,
                offset_id=0,
                offset_peer=InputPeerEmpty(),
                limit=limit,
                hash=0
            ))
            
            # 从Telethon库导入需要的类型
            from telethon.tl.types import Channel, Chat, User
            
            # 过滤出频道和群组类型的对话
            for dialog in result.dialogs:
                # 跳过用户对话
                if isinstance(dialog.peer, PeerUser):
                    continue
                    
                # 获取实体
                try:
                    entity = await self.client.get_entity(dialog.peer)
                except Exception as e:
                    logger.warning(f"获取对话实体时出错: {str(e)}")
                    continue
                    
                # 根据实体类型处理
                if isinstance(entity, Channel):
                    # 判断频道类型
                    is_supergroup = bool(getattr(entity, 'megagroup', False))
                    is_broadcast = bool(getattr(entity, 'broadcast', False))
                    
                    channel_type = "普通频道"
                    is_group = False
                    
                    if is_supergroup:
                        channel_type = "超级群组"
                        is_group = True
                    
                    # 获取频道的更多信息
                    channel_info = {
                        'id': entity.id,
                        'username': entity.username,
                        'title': entity.title,
                        'participants_count': 0,
                        'broadcast': is_broadcast,  # 是否为广播频道
                        'megagroup': is_supergroup,  # 是否为大型群组
                        'access_hash': entity.access_hash,
                        'type': 'supergroup' if is_supergroup else 'channel',  # 标记类型
                        'is_group': is_group,
                        'is_supergroup': is_supergroup
                    }
                    
                    # 尝试获取频道的完整信息（包括成员数）
                    try:
                        full_channel = await self.client(GetFullChannelRequest(channel=entity))
                        channel_info['participants_count'] = getattr(full_channel.full_chat, 'participants_count', 0)
                        channel_info['about'] = getattr(full_channel.full_chat, 'about', '')
                    except (ChannelPrivateError, ChatAdminRequiredError) as e:
                        logger.warning(f"无法获取{channel_type} {entity.title} 的完整信息: {str(e)}")
                    
                    # 如果用户名为空，设置为ID
                    if not channel_info['username']:
                        # 设置一个特殊标记，表明这个频道没有用户名，使用ID来识别
                        channel_info['username'] = None
                        channel_info['has_no_username'] = True
                        channel_info['channel_id'] = channel_info['id']
                        logger.info(f"{channel_type} {channel_info['title']} 没有用户名，将使用ID {channel_info['id']} 标识")
                    
                    # 添加到发现列表
                    discovered_channels.append(channel_info)
                    logger.info(f"发现{channel_type}: {channel_info['title']} (@{channel_info['username'] or '无用户名'}) - 成员: {channel_info['participants_count']}")
                
                elif isinstance(entity, Chat):
                    # 普通群组
                    try:
                        # 获取群组的基本信息
                        group_info = {
                            'id': entity.id,
                            'username': None,  # 普通群组没有用户名
                            'title': entity.title,
                            'participants_count': getattr(entity, 'participants_count', 0),
                            'broadcast': False,  # 普通群组不是广播频道
                            'megagroup': False,  # 普通群组不是大型群组
                            'type': 'group',  # 标记为普通群组类型
                            'has_no_username': True,
                            'channel_id': entity.id,  # 使用群组ID作为channel_id
                            'is_group': True,
                            'is_supergroup': False
                        }
                        
                        # 尝试获取群组的完整信息
                        try:
                            full_chat = await self.client(GetFullChatRequest(chat_id=entity.id))
                            if hasattr(full_chat, 'full_chat') and hasattr(full_chat.full_chat, 'participants_count'):
                                group_info['participants_count'] = full_chat.full_chat.participants_count
                            if hasattr(full_chat, 'full_chat') and hasattr(full_chat.full_chat, 'about'):
                                group_info['about'] = full_chat.full_chat.about
                        except Exception as e:
                            logger.warning(f"无法获取普通群组 {entity.title} 的完整信息: {str(e)}")
                        
                        # 添加到发现列表
                        discovered_channels.append(group_info)
                        logger.info(f"发现普通群组: {group_info['title']} (ID: {group_info['id']}) - 成员: {group_info['participants_count']}")
                    except Exception as e:
                        logger.warning(f"处理普通群组时出错: {str(e)}")
            
            logger.info(f"发现了 {len(discovered_channels)} 个频道和群组")
            return discovered_channels
            
        except Exception as e:
            logger.error(f"发现频道和群组时出错: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
            return []
    
    def guess_chain(self, channel_info: Dict) -> str:
        """根据频道信息推测其对应的区块链
        
        Args:
            channel_info: 频道信息字典
            
        Returns:
            str: 推测的链名称，默认为'UNKNOWN'
        """
        # 合并标题和描述以进行关键词搜索
        text = f"{channel_info['title']} {channel_info.get('about', '')}".lower()
        
        # 按关键词检查
        for chain, keywords in self.chain_keywords.items():
            for keyword in keywords:
                if keyword.lower() in text:
                    logger.info(f"频道 {channel_info['title']} 匹配链 {chain}")
                    return chain
                    
        # 检查用户名中是否包含链标识
        username = channel_info.get('username')
        if username:
            for chain, keywords in self.chain_keywords.items():
                for keyword in keywords:
                    if keyword.lower() in username.lower():
                        logger.info(f"频道用户名 {username} 匹配链 {chain}")
                        return chain
        
        # 处理频道标题，确保特殊字符不会导致编码错误
        # 使用更安全的方式处理标题，去除可能导致编码问题的字符
        try:
            # 尝试安全地记录日志，仅供日志使用
            logger.info(f"无法为频道 {channel_info['title']} 确定链，设置为UNKNOWN")
        except Exception:
            # 如果出现编码错误，使用一个更安全的版本
            safe_title = channel_info['title'].encode('utf-8', errors='replace').decode('utf-8', errors='replace')
            logger.info(f"无法为频道 {safe_title} 确定链，设置为UNKNOWN")
        
        return "UNKNOWN"
    
    async def auto_add_channels(self, min_members: int = None, max_channels: int = None) -> List[Dict]:
        """自动添加符合条件的频道和群组
        
        Args:
            min_members: 最小成员数量
            max_channels: 最多添加的频道数
            
        Returns:
            List[Dict]: 已添加的频道和群组列表
        """
        # 如果未指定参数，使用配置中的值
        if min_members is None:
            min_members = env_config.MIN_CHANNEL_MEMBERS
            
        if max_channels is None:
            max_channels = env_config.MAX_AUTO_CHANNELS
            
        logger.info(f"开始自动添加频道和群组，最小成员: {min_members}，最大数量: {max_channels}")
        
        # 获取当前活跃的频道
        active_channels = self.channel_manager.get_active_channels()
        active_usernames = set(active_channels.keys())
        
        # 获取所有发现的频道和群组
        discovered = await self.discover_channels(limit=200)
        
        # 按成员数排序
        discovered.sort(key=lambda x: x.get('participants_count', 0), reverse=True)
        
        # 筛选出符合条件的新频道和群组
        new_channels = []
        for channel in discovered:
            username = channel.get('username')
            channel_id = channel.get('id')
            is_group = channel.get('is_group', False)
            is_supergroup = channel.get('is_supergroup', False)
            
            # 如果没有用户名也没有ID，跳过
            if not username and not channel_id:
                channel_type = "频道"
                if is_supergroup:
                    channel_type = "超级群组"
                elif is_group:
                    channel_type = "普通群组"
                logger.warning(f"{channel_type} {channel.get('title', 'Unknown')} 没有用户名和ID，无法添加")
                continue
                
            # 构建用于检查的标识符
            identifier = username if username else f"id_{channel_id}"
            
            # 跳过已经添加或排除的频道
            if identifier in active_usernames or (username and username in self.excluded_channels):
                continue
                
            # 检查成员数是否符合要求
            if channel.get('participants_count', 0) <= min_members:
                continue
                
            # 推测频道所属的链
            chain = self.guess_chain(channel)
            
            # 添加到频道管理器
            success = False
            # 针对不同类型的实体使用不同的添加方式
            if not username:  # 对于没有用户名的频道或群组，使用ID
                success = self.channel_manager.add_channel(
                    channel_username=None,  # 明确设为None
                    channel_name=channel['title'],
                    chain=chain,
                    channel_id=channel_id,
                    is_group=is_group,
                    is_supergroup=is_supergroup,
                    member_count=channel.get('participants_count', 0)
                )
            else:  # 对于有用户名的频道，使用用户名
                success = self.channel_manager.add_channel(
                    channel_username=username,
                    channel_name=channel['title'],
                    chain=chain,
                    channel_id=channel_id,
                    is_group=is_group,
                    is_supergroup=is_supergroup,
                    member_count=channel.get('participants_count', 0)
                )
            
            if success:
                channel['chain'] = chain
                new_channels.append(channel)
                
                channel_type = "普通频道"
                if is_supergroup:
                    channel_type = "超级群组"
                elif is_group:
                    channel_type = "普通群组"
                    
                logger.info(f"自动添加新{channel_type}: {identifier} ({channel['title']}) - 成员: {channel['participants_count']}, 链: {chain}")
                
                # 如果达到最大数量，停止添加
                if len(new_channels) >= max_channels:
                    break
        
        logger.info(f"自动添加了 {len(new_channels)} 个新频道和群组")
        return new_channels
    
    def set_excluded_channels(self, excluded: List[str]):
        """设置要排除的频道列表
        
        Args:
            excluded: 要排除的频道用户名列表
        """
        self.excluded_channels = set(excluded)
        logger.info(f"已设置 {len(self.excluded_channels)} 个排除频道")
    
    def add_chain_keywords(self, chain: str, keywords: List[str]):
        """添加新的链关键词
        
        Args:
            chain: 链名称
            keywords: 关键词列表
        """
        if chain in self.chain_keywords:
            self.chain_keywords[chain].extend(keywords)
        else:
            self.chain_keywords[chain] = keywords
        logger.info(f"为链 {chain} 添加了关键词: {', '.join(keywords)}") 