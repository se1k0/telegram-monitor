import logging
import asyncio
from telethon import TelegramClient
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty, Channel, Chat, ChannelFull, PeerChannel, PeerChat, PeerUser
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.errors import ChannelPrivateError, ChatAdminRequiredError
from typing import List, Dict, Optional, Tuple, Set
import re
from datetime import datetime, timedelta
from .channel_manager import ChannelManager

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
        self.excluded_channels = set()
        # 频道分类的关键词规则（用于推断频道所属的区块链）
        self.chain_keywords = {
            'SOL': ['solana', 'sol', 'Solana', 'SOL','索拉纳'],
            'ETH': ['ethereum', 'eth', 'Ethereum', 'ETH','以太坊'],
            'BTC': ['bitcoin', 'btc', 'Bitcoin', 'BTC','比特币'],
            'BCH': ['bitcoin cash', 'bch', 'Bitcoin Cash', 'BCH','比特币现金'],
            'AVAX': ['avalanche', 'avax', 'Avalanche', 'AVAX','雪崩'],
            'BSC': ['binance', 'bnb', 'bsc', 'Binance','BNB','BSC','币安链','币安'],
            'MATIC': ['polygon', 'matic', 'Matic', 'MATIC','多边形'],
            'TRX': ['tron', 'trx', 'Tron', 'TRX','波场'],
            'TON': ['ton', 'Ton', 'TON','TON','TON链'],
            'ARB': ['arbitrum', 'arb', 'Arbitrum', 'ARB','Arbitrum链'],
            'OP': ['optimism', 'op', 'Optimism', 'OP','Optimism链'],
            'ZK': ['zksync', 'zks', 'ZKSync', 'ZK','ZKSync链'],
            'BASE': ['base', 'Base', 'BASE','Base链'],
            'LINE': ['line', 'Line', 'LINE','Line链'],
            'KLAY': ['klaytn', 'klay', 'Klaytn', 'KLAY','Klaytn链'],
            'FUSE': ['fuse', 'Fuse', 'FUSE','Fuse链'],
            'CELO': ['celo', 'Celo', 'CELO','Celo链'],
            'KCS': ['kucoin', 'kcs', 'KCS','KCS链'],
            'KSM': ['kusama', 'ksm', 'Kusama', 'KSM','Kusama链'],
            'DOT': ['polkadot', 'dot', 'Polkadot', 'DOT','波卡'],
            'ADA': ['cardano', 'ada', 'Cardano', 'ADA','卡尔达诺'],
            'XRP': ['ripple', 'xrp', 'Ripple', 'XRP','瑞波'],
            'LINK': ['chainlink', 'link', 'Chainlink', 'LINK','链链'],
            'XLM': ['stellar', 'xlm', 'Stellar', 'XLM','恒星'],
            'XMR': ['monero', 'xmr', 'Monero', 'XMR','门罗'],
            'LTC': ['litecoin', 'ltc', 'Litecoin', 'LTC','莱特币'],
        }
        
    async def discover_channels(self, limit: int = 100) -> List[Dict]:
        """发现用户对话中的所有频道
        
        Args:
            limit: 要获取的对话数量限制
            
        Returns:
            List[Dict]: 发现的频道列表
        """
        logger.info(f"开始发现频道，最大数量: {limit}")
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
            
            # 过滤出频道类型的对话
            for dialog in result.dialogs:
                # 修复：检查peer类型，避免访问PeerUser对象的channel_id属性
                if dialog.peer and isinstance(dialog.peer, PeerChannel):
                    entity = await self.client.get_entity(dialog.peer)
                    
                    if isinstance(entity, Channel):
                        # 获取频道的更多信息
                        channel_info = {
                            'id': entity.id,
                            'username': entity.username,
                            'title': entity.title,
                            'participants_count': 0,
                            'broadcast': getattr(entity, 'broadcast', False),  # 是否为广播频道
                            'mega_group': getattr(entity, 'megagroup', False),  # 是否为大型群组
                            'access_hash': entity.access_hash
                        }
                        
                        # 尝试获取频道的完整信息（包括成员数）
                        try:
                            if entity.username:
                                full_channel = await self.client(GetFullChannelRequest(channel=entity))
                                channel_info['participants_count'] = getattr(full_channel.full_chat, 'participants_count', 0)
                                channel_info['about'] = getattr(full_channel.full_chat, 'about', '')
                        except (ChannelPrivateError, ChatAdminRequiredError) as e:
                            logger.warning(f"无法获取频道 {entity.title} 的完整信息: {str(e)}")
                        
                        # 如果用户名为空，设置为ID
                        if not channel_info['username']:
                            channel_info['username'] = f"channel_{channel_info['id']}"
                        
                        # 只添加大型群组或广播频道
                        if channel_info['mega_group'] or channel_info['broadcast']:
                            discovered_channels.append(channel_info)
                            logger.info(f"发现频道: {channel_info['title']} (@{channel_info['username']}) - 成员: {channel_info['participants_count']}")
            
            logger.info(f"发现了 {len(discovered_channels)} 个频道")
            return discovered_channels
            
        except Exception as e:
            logger.error(f"发现频道时出错: {str(e)}")
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
        username = channel_info['username'].lower()
        for chain, keywords in self.chain_keywords.items():
            for keyword in keywords:
                if keyword.lower() in username:
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
    
    async def auto_add_channels(self, min_members: int = 0, max_channels: int = 20) -> List[Dict]:
        """自动添加符合条件的频道
        
        Args:
            min_members: 最小成员数量
            max_channels: 最多添加的频道数
            
        Returns:
            List[Dict]: 已添加的频道列表
        """
        logger.info(f"开始自动添加频道，最小成员: {min_members}，最大数量: {max_channels}")
        
        # 获取当前活跃的频道
        active_channels = self.channel_manager.get_active_channels()
        active_usernames = set(active_channels.keys())
        
        # 获取所有发现的频道
        discovered = await self.discover_channels(limit=200)
        
        # 按成员数排序
        discovered.sort(key=lambda x: x.get('participants_count', 0), reverse=True)
        
        # 筛选出符合条件的新频道
        new_channels = []
        for channel in discovered:
            username = channel['username']
            
            # 跳过已经添加或排除的频道
            if username in active_usernames or username in self.excluded_channels:
                continue
                
            # 检查成员数是否符合要求
            if channel.get('participants_count', 0) <= min_members:
                continue
                
            # 推测频道所属的链
            chain = self.guess_chain(channel)
            
            # 添加到频道管理器
            success = self.channel_manager.add_channel(
                channel_username=username,
                channel_name=channel['title'],
                chain=chain
            )
            
            if success:
                channel['chain'] = chain
                new_channels.append(channel)
                logger.info(f"自动添加新频道: {username} ({channel['title']}) - 成员: {channel['participants_count']}, 链: {chain}")
                
                # 如果达到最大数量，停止添加
                if len(new_channels) >= max_channels:
                    break
        
        logger.info(f"自动添加了 {len(new_channels)} 个新频道")
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