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

from src.core.channel_manager import ChannelManager, DEFAULT_CHANNELS
from src.database.models import init_db

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/channel_manager.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv()

# Telegram API 认证信息
api_id = os.getenv('TG_API_ID')
api_hash = os.getenv('TG_API_HASH')

async def init_client():
    """初始化TelegramClient"""
    try:
        if not api_id or not api_hash:
            logger.error("缺少API凭据，请检查环境变量TG_API_ID和TG_API_HASH")
            return None
            
        client = TelegramClient('channel_manager_session', api_id, api_hash)
        await client.start()
        logger.info("Telegram客户端初始化成功")
        return client
    except Exception as e:
        logger.error(f"初始化Telegram客户端时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

async def list_channels():
    """列出所有频道"""
    manager = ChannelManager()
    channels = manager.get_all_channels()
    
    if not channels:
        print("没有找到任何频道")
        return
        
    print("\n当前所有频道:")
    print("-" * 80)
    print(f"{'ID':<5} {'频道用户名':<25} {'频道名称':<30} {'链':<10} {'状态':<10}")
    print("-" * 80)
    
    for channel in channels:
        status = "活跃" if channel.is_active else "不活跃"
        print(f"{channel.id:<5} {channel.channel_username:<25} {channel.channel_name:<30} {channel.chain:<10} {status:<10}")
    
    print("-" * 80)

async def add_channel(channel_username, chain_name):
    """添加一个新频道"""
    client = await init_client()
    if not client:
        print(f"错误: 无法初始化Telegram客户端，无法验证频道")
        return
        
    try:
        manager = ChannelManager(client)
        
        # 验证频道
        channel_info = await manager.verify_channel(channel_username)
        if not channel_info or not channel_info.get('exists', False):
            print(f"错误: 无法验证频道 '{channel_username}'")
            return
            
        # 添加频道
        channel_name = channel_info.get('name', channel_username)
        # 从channel_info中获取需要的信息
        channel_id = channel_info.get('channel_id')
        is_group = channel_info.get('is_group', False)
        is_supergroup = channel_info.get('is_supergroup', False)
        member_count = channel_info.get('member_count', 0)
        
        success = manager.add_channel(
            channel_username=channel_username, 
            channel_name=channel_name, 
            chain=chain_name,
            channel_id=channel_id,
            is_group=is_group,
            is_supergroup=is_supergroup,
            member_count=member_count
        )
        
        if success:
            print(f"成功添加{'群组' if is_group else '频道'}: {channel_username} ({channel_name}), 链: {chain_name}")
        else:
            print(f"频道 '{channel_username}' 已存在")
            
    except Exception as e:
        print(f"添加频道时出错: {str(e)}")
        logger.error(f"添加频道时出错: {str(e)}")
    finally:
        await client.disconnect()

async def remove_channel(channel_username):
    """移除一个频道"""
    manager = ChannelManager()
    success = manager.remove_channel(channel_username)
    
    if success:
        print(f"成功移除频道: {channel_username}")
    else:
        print(f"无法移除频道: {channel_username}")

async def update_channels():
    """更新所有频道状态"""
    client = await init_client()
    try:
        manager = ChannelManager(client)
        
        # 更新频道
        print("正在更新频道信息...")
        active_channels = await manager.update_channels(DEFAULT_CHANNELS)
        
        print(f"频道更新完成，共有 {len(active_channels)} 个活跃频道")
            
    finally:
        await client.disconnect()

def main():
    """主程序入口"""
    # 确保目录存在
    os.makedirs('./logs', exist_ok=True)
    
    # 初始化数据库
    init_db()
    
    # 创建命令行解析器
    parser = argparse.ArgumentParser(description='Telegram频道管理工具')
    subparsers = parser.add_subparsers(dest='command', help='命令')
    
    # list命令
    list_parser = subparsers.add_parser('list', help='列出所有频道')
    
    # add命令
    add_parser = subparsers.add_parser('add', help='添加一个新频道')
    add_parser.add_argument('channel', help='频道用户名')
    add_parser.add_argument('chain', help='关联的区块链名称')
    
    # remove命令
    remove_parser = subparsers.add_parser('remove', help='移除一个频道')
    remove_parser.add_argument('channel', help='要移除的频道用户名')
    
    # update命令
    update_parser = subparsers.add_parser('update', help='更新所有频道状态')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 执行对应的命令
    if args.command == 'list':
        asyncio.run(list_channels())
    elif args.command == 'add':
        asyncio.run(add_channel(args.channel, args.chain))
    elif args.command == 'remove':
        asyncio.run(remove_channel(args.channel))
    elif args.command == 'update':
        asyncio.run(update_channels())
    else:
        parser.print_help()

if __name__ == '__main__':
    main() 