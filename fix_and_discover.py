#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
综合脚本：修复日期格式问题并运行频道发现
使用方法：python fix_and_discover.py --min-members 0 --auto-add --max-channels 100
"""

import os
import sys
import argparse
import asyncio
import logging

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# 导入项目模块
from scripts.fix_date_format import fix_date_format
from scripts.discover_channels import discover_channels, main as discover_main
from src.utils.logger import setup_logger, get_logger
from src.database.models import TelegramChannel, Base, engine
from sqlalchemy.orm import sessionmaker
from telethon import TelegramClient
from telethon.tl.types import Channel, Chat, PeerChannel, PeerChat
from config.settings import load_config
from datetime import datetime

# 设置日志
setup_logger()
logger = get_logger(__name__)

# 创建会话工厂
Session = sessionmaker(bind=engine)

async def fix_channel_types():
    """修复数据库中所有频道的类型信息（is_group和is_supergroup字段）"""
    logger.info("开始修复频道类型信息...")
    
    # 加载配置获取API信息
    config = load_config()
    api_id = os.getenv('TG_API_ID')
    api_hash = os.getenv('TG_API_HASH')
    
    if not api_id or not api_hash:
        logger.error("缺少TG_API_ID或TG_API_HASH环境变量，无法修复频道类型")
        return False
    
    # 创建Telegram客户端
    session_path = 'data/sessions/fix_channel_types'
    client = TelegramClient(session_path, api_id, api_hash)
    
    try:
        # 连接到Telegram
        await client.connect()
        if not await client.is_user_authorized():
            logger.error("Telegram客户端未授权，请先运行main.py登录")
            await client.disconnect()
            return False
        
        logger.info("已连接到Telegram客户端")
        
        # 获取所有频道记录
        session = Session()
        channels = session.query(TelegramChannel).all()
        updated_count = 0
        
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
                    entity = None
                    # 尝试获取实体
                    if channel.channel_username:
                        # 使用用户名获取频道
                        entity = await client.get_entity(channel.channel_username)
                    elif channel.channel_id:
                        # 根据是否为群组使用不同方式获取实体
                        try:
                            if channel.is_group:
                                # 群组
                                entity = await client.get_entity(PeerChat(channel.channel_id))
                            else:
                                # 频道
                                entity = await client.get_entity(PeerChannel(channel.channel_id))
                        except ValueError:
                            # 如果失败，尝试另一种方式
                            try:
                                if not channel.is_group:
                                    entity = await client.get_entity(PeerChat(channel.channel_id))
                                else:
                                    entity = await client.get_entity(PeerChannel(channel.channel_id))
                            except Exception as e2:
                                logger.error(f"无法获取频道 {channel_identifier} 的实体: {str(e2)}")
                                continue
                    
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
                            previous_type = "未知"
                            if channel.is_supergroup:
                                previous_type = "超级群组"
                            elif channel.is_group:
                                previous_type = "普通群组" 
                            else:
                                previous_type = "普通频道"
                                
                            new_type = "未知"
                            if is_supergroup:
                                new_type = "超级群组"
                            elif is_group:
                                new_type = "普通群组" 
                            else:
                                new_type = "普通频道"
                                
                            logger.info(f"更新频道 {channel_identifier} 的类型信息: 从 {previous_type} 更新为 {new_type}")
                            channel.is_group = is_group
                            channel.is_supergroup = is_supergroup
                            channel.last_updated = datetime.now()
                            updated_count += 1
            except Exception as e:
                logger.error(f"处理频道 {channel.id} 时出错: {str(e)}")
                continue
        
        # 提交所有更改
        if updated_count > 0:
            session.commit()
            logger.info(f"已更新 {updated_count} 个频道的类型信息")
        else:
            logger.info("没有需要更新的频道类型信息")
        
        return True
    except Exception as e:
        logger.error(f"修复频道类型时出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        return False
    finally:
        if 'session' in locals():
            session.close()
        if 'client' in locals() and client.is_connected():
            await client.disconnect()

async def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='修复日期格式问题并发现Telegram频道和群组')
    parser.add_argument('--min-members', type=int, default=0, help='最小成员数，筛选出成员数大于此值的频道和群组')
    parser.add_argument('--auto-add', action='store_true', help='自动添加发现的频道和群组到监控列表')
    parser.add_argument('--max-channels', type=int, default=100, help='最多添加的频道和群组数量')
    parser.add_argument('--groups-only', action='store_true', help='只处理群组，不处理频道')
    parser.add_argument('--channels-only', action='store_true', help='只处理频道，不处理群组')
    parser.add_argument('--skip-fix', action='store_true', help='跳过日期格式修复步骤')
    parser.add_argument('--fix-channel-types', action='store_true', help='修复频道类型信息(is_group和is_supergroup字段)')
    args = parser.parse_args()
    
    # 修复频道类型信息
    if args.fix_channel_types:
        print("修复频道类型信息...")
        if await fix_channel_types():
            print("频道类型信息修复成功！")
        else:
            print("频道类型信息修复失败！")
        return
    
    # 第一步：修复日期格式问题
    if not args.skip_fix:
        print("第一步：修复数据库中的日期格式问题...")
        if fix_date_format():
            print("日期格式修复成功！")
        else:
            print("日期格式修复失败！将尝试继续执行频道发现...")
    
    # 第二步：运行频道发现
    print("第二步：发现Telegram频道和群组...")
    await discover_channels(
        min_members=args.min_members,
        max_channels=args.max_channels,
        auto_add=args.auto_add,
        groups_only=args.groups_only,
        channels_only=args.channels_only
    )
    
    print("操作完成！")

if __name__ == "__main__":
    asyncio.run(main()) 