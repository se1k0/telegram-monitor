#!/usr/bin/env python3
import argparse
import asyncio
import os
import sys
import logging
from dotenv import load_dotenv
from telethon import TelegramClient

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.core.channel_manager import ChannelManager
from src.core.channel_discovery import ChannelDiscovery
from src.database.models import init_db
import config.settings as config
from src.utils.logger import get_logger

# 加载环境变量
load_dotenv()

# 获取日志记录器
logger = get_logger(__name__)

# Telegram API 认证信息
api_id = os.getenv('TG_API_ID')
api_hash = os.getenv('TG_API_HASH')

async def init_client():
    """初始化TelegramClient"""
    try:
        if not api_id or not api_hash:
            logger.error("缺少API凭据，请检查环境变量TG_API_ID和TG_API_HASH")
            return None
            
        client = TelegramClient('discover_session', api_id, api_hash)
        await client.start()
        logger.info("Telegram客户端初始化成功")
        return client
    except Exception as e:
        logger.error(f"初始化Telegram客户端时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

async def discover_channels(min_members=500, max_channels=20, auto_add=False, groups_only=False, channels_only=False):
    """发现并列出可用的频道和群组
    
    Args:
        min_members: 最小成员数
        max_channels: 最多显示/添加的数量
        auto_add: 是否自动添加发现的频道/群组
        groups_only: 是否只处理群组
        channels_only: 是否只处理频道
    """
    client = await init_client()
    if not client:
        logger.error("无法初始化Telegram客户端")
        return
        
    try:
        channel_manager = ChannelManager(client)
        discovery = ChannelDiscovery(client, channel_manager)
        
        # 设置排除的频道
        if hasattr(config, 'excluded_channels'):
            discovery.set_excluded_channels(config.excluded_channels)
            
        # 设置筛选条件
        if hasattr(config, 'env_config'):
            if hasattr(config.env_config, 'GROUPS_ONLY') and config.env_config.GROUPS_ONLY and not groups_only:
                groups_only = True
            if hasattr(config.env_config, 'CHANNELS_ONLY') and config.env_config.CHANNELS_ONLY and not channels_only:
                channels_only = True
                
        # 过滤条件确认
        if groups_only and channels_only:
            logger.warning("groups_only和channels_only不能同时为True，将忽略这两个参数")
            groups_only = channels_only = False
        
        # 发现频道和群组
        logger.info(f"开始发现{'群组' if groups_only else '频道' if channels_only else '频道和群组'}...")
        channels = await discovery.discover_channels(limit=200)
        
        # 根据类型筛选
        if groups_only:
            channels = [ch for ch in channels if ch.get('type') == 'group']
            logger.info(f"筛选出 {len(channels)} 个群组")
        elif channels_only:
            channels = [ch for ch in channels if ch.get('type') != 'group']
            logger.info(f"筛选出 {len(channels)} 个频道")
            
        # 按成员数排序
        channels.sort(key=lambda x: x.get('participants_count', 0), reverse=True)
        
        # 输出列表
        print(f"\n发现的{'群组' if groups_only else '频道' if channels_only else '频道和群组'}列表:")
        print("-" * 100)
        print(f"{'标题':<40} {'标识符':<20} {'成员数':<10} {'类型':<10} {'推测链':<10}")
        print("-" * 100)
        
        count = 0
        added_count = 0
        
        for channel in channels:
            # 检查成员数
            if channel.get('participants_count', 0) < min_members:
                continue
                
            # 获取频道类型
            if channel.get('type') == 'group':
                channel_type = "普通群组"
            else:
                channel_type = "广播频道" if channel.get('broadcast', False) else "群聊"
                if channel.get('mega_group', False):
                    channel_type = "大型群组"
                    
            # 推测链
            chain = discovery.guess_chain(channel)
            
            # 准备显示信息
            username_display = channel.get('username') if channel.get('username') else '(无用户名)'
            title = str(channel.get('title', '未知'))[:38]
            
            try:
                # 显示频道信息
                print(f"{title:<40} {username_display:<20} {channel.get('participants_count', 0):<10} {channel_type:<10} {chain:<10}")
            except Exception as e:
                # 如果格式化出错，使用更安全的方式显示
                safe_title = title.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                print(f"{safe_title:<40} {username_display:<20} {channel.get('participants_count', 0):<10} {channel_type:<10} {chain:<10}")
            
            count += 1
            
            # 自动添加频道
            if auto_add and count <= max_channels:
                # 添加频道或群组
                if channel.get('type') == 'group' or not channel.get('username'):
                    # 对于群组或没有用户名的频道，使用ID而不是用户名
                    is_group = channel.get('type') == 'group'
                    # 普通群组类型没有supergroup标记
                    is_supergroup = channel.get('megagroup', False)
                    
                    success = channel_manager.add_channel(
                        channel_username=None,  # 明确设置channel_username为None
                        channel_name=channel.get('title', 'Unknown Group'), 
                        chain=chain, 
                        channel_id=channel.get('id'), 
                        is_group=is_group,
                        is_supergroup=is_supergroup,
                        member_count=channel.get('participants_count', 0)
                    )
                else:
                    # 对于频道，使用用户名（如果有）
                    # 对于频道，需要判断是否为megagroup（超级群组）
                    is_supergroup = channel.get('megagroup', False)
                    is_group = is_supergroup  # 超级群组也是群组的一种
                    
                    success = channel_manager.add_channel(
                        channel_username=channel.get('username'), 
                        channel_name=channel.get('title', 'Unknown Channel'), 
                        chain=chain, 
                        channel_id=channel.get('id'), 
                        is_group=is_group,
                        is_supergroup=is_supergroup,
                        member_count=channel.get('participants_count', 0)
                    )
                                                         
                if success:
                    added_count += 1
                    identifier = channel.get('username') or f"ID: {channel.get('id', 'Unknown')}"
                    print(f"已自动添加{'群组' if channel.get('type') == 'group' else '频道'}: {identifier}")
            
            # 限制显示数量
            if count >= 50:  # 最多显示50个
                break
                
        print("-" * 100)
        print(f"总共发现 {count} 个符合条件的{'群组' if groups_only else '频道' if channels_only else '频道和群组'} (最小成员数: {min_members}).")
        
        if auto_add:
            print(f"已自动添加 {added_count} 个新{'群组' if groups_only else '频道' if channels_only else '频道和群组'}.")
            
    except Exception as e:
        logger.error(f"发现{'群组' if groups_only else '频道' if channels_only else '频道和群组'}时出错: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        await client.disconnect()

async def auto_add_channels(min_members=500, max_channels=10, groups_only=False, channels_only=False):
    """自动添加符合条件的频道和群组
    
    Args:
        min_members: 最小成员数量
        max_channels: 最多添加的数量
        groups_only: 是否只添加群组
        channels_only: 是否只添加频道
    """
    client = await init_client()
    if not client:
        logger.error("无法初始化Telegram客户端")
        return
        
    try:
        channel_manager = ChannelManager(client)
        discovery = ChannelDiscovery(client, channel_manager)
        
        # 设置排除的频道
        if hasattr(config, 'excluded_channels'):
            discovery.set_excluded_channels(config.excluded_channels)
        
        # 设置自动发现参数
        params = {}
        if hasattr(config, 'env_config'):
            if hasattr(config.env_config, 'GROUPS_ONLY') and config.env_config.GROUPS_ONLY:
                groups_only = True
            if hasattr(config.env_config, 'CHANNELS_ONLY') and config.env_config.CHANNELS_ONLY:
                channels_only = True
            
        # 过滤条件确认
        if groups_only and channels_only:
            logger.warning("groups_only和channels_only不能同时为True，将忽略这两个参数")
            groups_only = channels_only = False
            
        # 自动添加频道/群组
        logger.info(f"开始自动添加{'群组' if groups_only else '频道' if channels_only else '频道和群组'} (最小成员数: {min_members}, 最大数量: {max_channels})...")
        new_channels = await discovery.auto_add_channels(min_members, max_channels)
        
        # 根据需要过滤结果
        if groups_only:
            new_channels = [ch for ch in new_channels if ch.get('type') == 'group']
        elif channels_only:
            new_channels = [ch for ch in new_channels if ch.get('type') != 'group']
        
        if new_channels:
            print(f"\n成功添加了 {len(new_channels)} 个新{'群组' if groups_only else '频道' if channels_only else '频道和群组'}:")
            print("-" * 100)
            print(f"{'标题':<40} {'标识符':<20} {'成员数':<10} {'类型':<10} {'链':<10}")
            print("-" * 100)
            
            for channel in new_channels:
                # 确定显示标识符
                identifier = '未知'
                if channel.get('username'):
                    identifier = channel['username']
                elif channel.get('id'):
                    identifier = f"ID: {channel['id']}"
                elif channel.get('channel_id'):
                    identifier = f"ID: {channel['channel_id']}"
                    
                # 确定类型显示
                if channel.get('type') == 'group':
                    type_display = "群组"
                else:
                    is_broadcast = channel.get('broadcast', False)
                    is_mega = channel.get('mega_group', False)
                    if is_mega:
                        type_display = "大型群组"
                    elif is_broadcast:
                        type_display = "频道"
                    else:
                        type_display = "聊天"
                
                # 显示信息
                title = str(channel.get('title', '未知'))[:38]
                members = channel.get('participants_count', 0)
                chain = channel.get('chain', 'Unknown')
                
                try:
                    print(f"{title:<40} {identifier:<20} {members:<10} {type_display:<10} {chain:<10}")
                except Exception as e:
                    # 如果有特殊字符导致格式化错误，使用更安全的方式显示
                    safe_title = title.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
                    print(f"{safe_title:<40} {identifier:<20} {members:<10} {type_display:<10} {chain:<10}")
                
            print("-" * 100)
        else:
            print(f"没有添加任何新{'群组' if groups_only else '频道' if channels_only else '频道和群组'}")
            
    except Exception as e:
        logger.error(f"自动添加{'群组' if groups_only else '频道' if channels_only else '频道和群组'}时出错: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        await client.disconnect()

def main():
    """主程序入口"""
    # 确保目录存在
    os.makedirs('./logs', exist_ok=True)
    
    # 初始化数据库
    init_db()
    
    # 创建命令行解析器
    parser = argparse.ArgumentParser(description='Telegram频道和群组发现工具 - 支持自动发现和管理频道与群组')
    subparsers = parser.add_subparsers(dest='command', help='命令')
    
    # discover命令
    discover_parser = subparsers.add_parser('discover', 
                                            help='发现频道和群组并显示', 
                                            description='搜索用户已加入的频道和群组，并可选择自动添加到监控列表')
    discover_parser.add_argument('--min-members', type=int, default=500, help='最小成员数，筛选出成员数大于此值的频道和群组')
    discover_parser.add_argument('--auto-add', action='store_true', help='自动添加发现的频道和群组到监控列表')
    discover_parser.add_argument('--max-channels', type=int, default=10, help='最多添加的频道和群组数量')
    discover_parser.add_argument('--groups-only', action='store_true', help='只处理群组，不处理频道')
    discover_parser.add_argument('--channels-only', action='store_true', help='只处理频道，不处理群组')
    
    # auto-add命令
    add_parser = subparsers.add_parser('auto-add', 
                                      help='自动添加频道和群组', 
                                      description='直接自动发现并添加符合条件的频道和群组到监控列表')
    add_parser.add_argument('--min-members', type=int, default=500, help='最小成员数，筛选出成员数大于此值的频道和群组')
    add_parser.add_argument('--max-channels', type=int, default=10, help='最多添加的频道和群组数量')
    add_parser.add_argument('--groups-only', action='store_true', help='只添加群组，不添加频道')
    add_parser.add_argument('--channels-only', action='store_true', help='只添加频道，不添加群组')
    
    # 添加更多帮助信息
    parser.epilog = '''
示例:
  # 发现并列出所有频道和群组
  python scripts/discover_channels.py discover
  
  # 只发现群组
  python scripts/discover_channels.py discover --groups-only
  
  # 发现并自动添加成员数超过1000的频道和群组（最多10个）
  python scripts/discover_channels.py discover --min-members 1000 --auto-add --max-channels 10
  
  # 直接自动添加成员数超过1000的群组（不包括频道）
  python scripts/discover_channels.py auto-add --min-members 1000 --groups-only
    '''
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 执行对应的命令
    if args.command == 'discover':
        asyncio.run(discover_channels(args.min_members, args.max_channels, args.auto_add, 
                                       args.groups_only, args.channels_only))
    elif args.command == 'auto-add':
        asyncio.run(auto_add_channels(args.min_members, args.max_channels, args.groups_only, args.channels_only))
    else:
        parser.print_help()

if __name__ == '__main__':
    main() 