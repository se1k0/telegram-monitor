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

async def discover_channels(min_members=500, max_channels=20, auto_add=False):
    """发现并列出可用的频道"""
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
        
        # 发现频道
        logger.info("开始发现频道...")
        channels = await discovery.discover_channels(limit=200)
        
        # 按成员数排序
        channels.sort(key=lambda x: x.get('participants_count', 0), reverse=True)
        
        # 输出频道列表
        print("\n发现的频道列表:")
        print("-" * 100)
        print(f"{'标题':<40} {'用户名':<20} {'成员数':<10} {'类型':<10} {'推测链':<10}")
        print("-" * 100)
        
        count = 0
        added_count = 0
        
        for channel in channels:
            # 检查成员数
            if channel.get('participants_count', 0) < min_members:
                continue
                
            # 获取频道类型
            channel_type = "广播频道" if channel.get('broadcast', False) else "群聊"
            if channel.get('mega_group', False):
                channel_type = "大型群组"
                
            # 推测链
            chain = discovery.guess_chain(channel)
            
            # 显示频道信息
            print(f"{channel['title'][:38]:<40} {channel['username']:<20} {channel['participants_count']:<10} {channel_type:<10} {chain:<10}")
            count += 1
            
            # 自动添加频道
            if auto_add and count <= max_channels:
                success = channel_manager.add_channel(channel['username'], channel['title'], chain)
                if success:
                    added_count += 1
                    print(f"已自动添加频道: {channel['username']}")
            
            # 限制显示数量
            if count >= 50:  # 最多显示50个
                break
                
        print("-" * 100)
        print(f"总共发现 {count} 个符合条件的频道 (最小成员数: {min_members}).")
        
        if auto_add:
            print(f"已自动添加 {added_count} 个新频道.")
            
    except Exception as e:
        logger.error(f"发现频道时出错: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        await client.disconnect()

async def auto_add_channels(min_members=500, max_channels=10):
    """自动添加符合条件的频道"""
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
        
        # 自动添加频道
        logger.info(f"开始自动添加频道 (最小成员数: {min_members}, 最大数量: {max_channels})...")
        new_channels = await discovery.auto_add_channels(min_members, max_channels)
        
        if new_channels:
            print(f"\n成功添加了 {len(new_channels)} 个新频道:")
            print("-" * 80)
            print(f"{'标题':<40} {'用户名':<20} {'成员数':<10} {'链':<10}")
            print("-" * 80)
            
            for channel in new_channels:
                print(f"{channel['title'][:38]:<40} {channel['username']:<20} {channel['participants_count']:<10} {channel['chain']:<10}")
                
            print("-" * 80)
        else:
            print("没有添加任何新频道")
            
    except Exception as e:
        logger.error(f"自动添加频道时出错: {str(e)}")
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
    parser = argparse.ArgumentParser(description='Telegram频道发现工具')
    subparsers = parser.add_subparsers(dest='command', help='命令')
    
    # discover命令
    discover_parser = subparsers.add_parser('discover', help='发现频道并显示')
    discover_parser.add_argument('--min-members', type=int, default=500, help='最小成员数')
    discover_parser.add_argument('--auto-add', action='store_true', help='自动添加发现的频道')
    discover_parser.add_argument('--max-channels', type=int, default=10, help='最多添加的频道数量')
    
    # add命令
    add_parser = subparsers.add_parser('auto-add', help='自动添加频道')
    add_parser.add_argument('--min-members', type=int, default=500, help='最小成员数')
    add_parser.add_argument('--max-channels', type=int, default=10, help='最多添加的频道数量')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 执行对应的命令
    if args.command == 'discover':
        asyncio.run(discover_channels(args.min_members, args.max_channels, args.auto_add))
    elif args.command == 'auto-add':
        asyncio.run(auto_add_channels(args.min_members, args.max_channels))
    else:
        parser.print_help()

if __name__ == '__main__':
    main() 