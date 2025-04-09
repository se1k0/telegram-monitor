#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
检查 Supabase 连接脚本
用于测试 Supabase 数据库连接是否正常
"""

import os
import sys
import logging
import asyncio
from dotenv import load_dotenv

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("supabase-check")

# 加载环境变量
load_dotenv()

async def check_supabase_connection():
    """
    检查 Supabase 连接是否正常
    """
    try:
        # 获取 Supabase 配置
        from dotenv import load_dotenv
        load_dotenv()
        
        SUPABASE_URL = os.getenv('SUPABASE_URL', '')
        SUPABASE_KEY = os.getenv('SUPABASE_KEY', '')
        SUPABASE_SERVICE_KEY = os.getenv('SUPABASE_SERVICE_KEY', '')
        
        logger.info(f"Supabase URL: {SUPABASE_URL}")
        logger.info(f"Supabase KEY长度: {len(SUPABASE_KEY)} 字符")
        logger.info(f"Supabase SERVICE_KEY长度: {len(SUPABASE_SERVICE_KEY)} 字符")
        
        # 初始化 Supabase 客户端
        from supabase import create_client, Client
        
        logger.info("正在初始化 Supabase 客户端...")
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase 客户端初始化成功")
        
        # 尝试获取telegram_channels表的数据
        logger.info("尝试获取telegram_channels表的数据...")
        response = supabase.table('telegram_channels').select('*').limit(10).execute()
        
        if hasattr(response, 'data'):
            logger.info(f"获取到 {len(response.data)} 条记录")
            for i, channel in enumerate(response.data[:3], 1):  # 只显示前3条
                logger.info(f"频道 {i}: {channel.get('channel_name', '无名称')} - @{channel.get('channel_username', '无用户名')}")
            
            # 如果有更多数据，显示省略提示
            if len(response.data) > 3:
                logger.info(f"... 省略 {len(response.data) - 3} 条记录")
                
            logger.info("Supabase 数据库连接正常")
            return True
        else:
            logger.error("获取数据失败，没有返回data字段")
            logger.error(f"响应: {response}")
            return False
            
    except Exception as e:
        logger.error(f"检查 Supabase 连接时出错: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def check_telegram_connection():
    """
    检查 Telegram 连接是否正常
    """
    try:
        # 获取 Telegram API 配置
        from dotenv import load_dotenv
        load_dotenv()
        
        API_ID = os.getenv('TG_API_ID', '')
        API_HASH = os.getenv('TG_API_HASH', '')
        
        logger.info(f"Telegram API_ID: {API_ID}")
        logger.info(f"Telegram API_HASH长度: {len(API_HASH)} 字符")
        
        # 初始化 Telegram 客户端
        from telethon import TelegramClient
        
        # 设置会话文件路径
        session_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                    "data", "test_session")
        
        logger.info(f"使用会话文件: {session_path}")
        logger.info("正在初始化 Telegram 客户端...")
        
        client = TelegramClient(
            session_path,
            API_ID, 
            API_HASH,
            connection_retries=5,
            auto_reconnect=True,
            retry_delay=5,
            timeout=30
        )
        
        logger.info("尝试连接到 Telegram...")
        await client.connect()
        
        if await client.is_user_authorized():
            me = await client.get_me()
            logger.info(f"已登录为: {me.first_name} (ID: {me.id})")
        else:
            logger.warning("未登录 Telegram 账户")
            
        # 尝试获取频道信息
        logger.info("尝试获取默认频道 MomentumTrackerCN 的信息...")
        try:
            channel = await client.get_entity("MomentumTrackerCN")
            logger.info(f"成功获取频道信息: {channel.title} (ID: {channel.id})")
            
            # 尝试获取最新消息
            logger.info("尝试获取最新消息...")
            messages = await client.get_messages(channel, limit=1)
            if messages and len(messages) > 0:
                logger.info(f"获取到最新消息: {messages[0].text[:100]}...")
            else:
                logger.warning("没有获取到消息")
                
            logger.info("Telegram 连接正常")
            return True
        except Exception as e:
            logger.error(f"获取频道信息时出错: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        finally:
            await client.disconnect()
            
    except Exception as e:
        logger.error(f"检查 Telegram 连接时出错: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def main():
    """
    主函数
    """
    logger.info("=== 开始检查 Supabase 和 Telegram 连接 ===")
    
    # 检查 Supabase 连接
    logger.info("\n=== 检查 Supabase 连接 ===")
    supabase_result = await check_supabase_connection()
    
    # 检查 Telegram 连接
    logger.info("\n=== 检查 Telegram 连接 ===")
    telegram_result = await check_telegram_connection()
    
    # 总结检查结果
    logger.info("\n=== 检查结果汇总 ===")
    logger.info(f"Supabase 连接: {'正常 ✓' if supabase_result else '异常 ✗'}")
    logger.info(f"Telegram 连接: {'正常 ✓' if telegram_result else '异常 ✗'}")
    
    if not supabase_result:
        logger.error("Supabase 连接异常，建议检查以下内容:")
        logger.error("1. .env 文件中的 SUPABASE_URL, SUPABASE_KEY 和 SUPABASE_SERVICE_KEY 是否正确")
        logger.error("2. 网络连接是否正常")
        logger.error("3. supabase 库是否已正确安装")
        
    if not telegram_result:
        logger.error("Telegram 连接异常，建议检查以下内容:")
        logger.error("1. .env 文件中的 TG_API_ID 和 TG_API_HASH 是否正确")
        logger.error("2. 网络连接是否正常")
        logger.error("3. 登录状态是否有效")
    
if __name__ == "__main__":
    asyncio.run(main()) 