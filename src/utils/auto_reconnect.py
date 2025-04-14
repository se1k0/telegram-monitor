#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Telegram自动重连工具
当遇到API限流(FloodWaitError)或其他连接问题时，该模块会自动等待指定时间后重试连接
"""

import os
import sys
import time
import asyncio
import logging
import argparse
from datetime import datetime, timedelta

# 导入Telegram客户端和错误类型
import telethon
from telethon import TelegramClient
from telethon.errors.rpcerrorlist import (
    FloodWaitError, 
    PhoneCodeInvalidError, 
    PhoneCodeExpiredError, 
    PasswordHashInvalidError
)

# 导入项目模块
from src.utils.logger import get_logger
import config.settings as config

# 配置日志
logger = get_logger("auto_reconnect")

def setup_logging():
    """设置日志格式"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("logs/auto_reconnect.log"),
            logging.StreamHandler()
        ]
    )

async def check_flood_wait_status():
    """检查限流状态文件，确定是否需要继续等待"""
    flood_wait_file = "./logs/flood_wait_info.txt"
    
    if not os.path.exists(flood_wait_file):
        logger.info("未找到限流状态文件，无需等待")
        print("未找到限流状态文件，可以直接启动程序")
        return 0
    
    try:
        with open(flood_wait_file, "r") as f:
            lines = f.readlines()
        
        # 解析限流信息
        wait_seconds = 0
        flood_time_str = None
        
        for line in lines:
            if "限流发生时间:" in line:
                flood_time_str = line.split(":", 1)[1].strip()
            elif "需要等待时间:" in line and "秒" in line:
                # 从括号中提取秒数
                import re
                match = re.search(r'\((\d+)秒\)', line)
                if match:
                    wait_seconds = int(match.group(1))
        
        # 如果没有找到等待时间，无需等待
        if wait_seconds == 0:
            logger.info("限流状态文件中未找到等待时间，无需等待")
            print("无需等待，可以直接启动程序")
            return 0
            
        # 如果没有找到限流发生时间，使用文件修改时间作为替代
        if not flood_time_str:
            flood_time = datetime.fromtimestamp(os.path.getmtime(flood_wait_file))
        else:
            try:
                flood_time = datetime.strptime(flood_time_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                flood_time = datetime.fromtimestamp(os.path.getmtime(flood_wait_file))
        
        # 计算已经等待的时间
        now = datetime.now()
        elapsed_seconds = (now - flood_time).total_seconds()
        
        # 计算剩余等待时间
        remaining_seconds = max(0, wait_seconds - elapsed_seconds)
        
        if remaining_seconds <= 0:
            logger.info("限流等待时间已结束，可以重试连接")
            print("限流等待时间已结束，可以启动程序")
            
            # 重命名或删除限流状态文件，避免再次读取
            backup_file = f"{flood_wait_file}.{int(time.time())}.bak"
            os.rename(flood_wait_file, backup_file)
            logger.info(f"已将限流状态文件备份为: {backup_file}")
            
            return 0
        else:
            # 计算剩余时间的可读格式
            remaining_minutes = remaining_seconds // 60
            remaining_hours = remaining_minutes // 60
            remaining_mins = remaining_minutes % 60
            
            if remaining_hours > 0:
                wait_msg = f"{remaining_hours}小时{remaining_mins}分钟"
            else:
                wait_msg = f"{remaining_minutes}分钟"
                
            logger.info(f"API限流等待尚未结束，还需等待: {wait_msg} ({remaining_seconds:.0f}秒)")
            print(f"\n⚠️ API限流等待尚未结束，还需等待: {wait_msg} ({remaining_seconds:.0f}秒)")
            
            # 是否执行等待并自动重试
            return remaining_seconds
    
    except Exception as e:
        logger.error(f"读取限流状态文件时出错: {str(e)}")
        print(f"读取限流状态文件时出错: {str(e)}")
        return 0

async def test_connection():
    """测试Telegram连接是否正常"""
    try:
        # 从配置中获取API认证信息
        try:
            api_id = config.env_config.API_ID
            api_hash = config.env_config.API_HASH
        except:
            # 尝试直接从环境变量获取
            api_id = int(os.getenv('TG_API_ID', '0'))
            api_hash = os.getenv('TG_API_HASH', '')
        
        if not api_id or not api_hash:
            logger.error("API ID或API HASH无效")
            print("⚠️ API ID或API HASH无效，请检查配置")
            return False
            
        # 创建临时会话
        session_path = "./data/test_session"
        client = TelegramClient(
            session_path,
            api_id, 
            api_hash,
            connection_retries=2,
            auto_reconnect=True,
            timeout=30
        )
        
        # 尝试连接
        await client.connect()
        
        # 检查连接状态
        if await client.is_connected():
            logger.info("Telegram API连接测试成功")
            print("✅ Telegram API连接测试成功")
            await client.disconnect()
            
            # 删除测试会话文件
            for ext in ['.session', '.session-journal']:
                try:
                    if os.path.exists(f"{session_path}{ext}"):
                        os.remove(f"{session_path}{ext}")
                except:
                    pass
                    
            return True
        else:
            logger.error("Telegram API连接测试失败")
            print("❌ Telegram API连接测试失败")
            return False
            
    except FloodWaitError as e:
        # 处理FloodWaitError，记录到限流状态文件
        wait_seconds = getattr(e, 'seconds', 0)
        wait_minutes = wait_seconds // 60
        wait_hours = wait_minutes // 60
        remaining_minutes = wait_minutes % 60
        
        if wait_hours > 0:
            wait_msg = f"{wait_hours}小时{remaining_minutes}分钟"
        else:
            wait_msg = f"{wait_minutes}分钟"
            
        logger.error(f"API限流错误: 需要等待{wait_msg}。错误: {str(e)}")
        print(f"\n⚠️ API限流错误: 需要等待{wait_msg}")
        
        # 将限流信息保存到文件
        os.makedirs("./logs", exist_ok=True)
        with open("./logs/flood_wait_info.txt", "w") as f:
            f.write(f"限流发生时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"需要等待时间: {wait_msg} ({wait_seconds}秒)\n")
        
        return False
    except Exception as e:
        logger.error(f"Telegram连接测试失败: {str(e)}")
        print(f"❌ Telegram连接测试失败: {str(e)}")
        return False

async def auto_wait_and_retry(wait_on_flood=True, max_retries=3):
    """
    自动等待并重试连接，主要处理FloodWaitError
    
    Args:
        wait_on_flood: 是否等待FloodWaitError恢复
        max_retries: 最大重试次数
    """
    # 先检查是否需要等待FloodWaitError
    if wait_on_flood:
        remaining_seconds = await check_flood_wait_status()
        
        if remaining_seconds > 0:
            wait_minutes = remaining_seconds // 60
            wait_hours = wait_minutes // 60
            remaining_minutes = wait_minutes % 60
            
            if wait_hours > 0:
                wait_msg = f"{wait_hours}小时{remaining_minutes}分钟"
            else:
                wait_msg = f"{wait_minutes}分钟"
                
            logger.info(f"等待API限流恢复，需要等待: {wait_msg}...")
            print(f"\n⏳ 正在等待API限流恢复 ({wait_msg})...")
            
            # 等待完成后重新检查连接
            await asyncio.sleep(remaining_seconds)
            print(f"等待完成，重新测试连接...")
    
    # 测试连接
    retry_count = 0
    success = False
    
    while retry_count < max_retries and not success:
        if retry_count > 0:
            wait_time = 5 * retry_count
            logger.info(f"第 {retry_count} 次重试，等待 {wait_time} 秒...")
            print(f"第 {retry_count} 次重试，等待 {wait_time} 秒...")
            await asyncio.sleep(wait_time)
            
        logger.info(f"测试Telegram连接...")
        success = await test_connection()
        
        if success:
            logger.info("连接成功!")
            print("🎉 连接成功!")
            return True
        else:
            retry_count += 1
            
    if not success:
        logger.error(f"经过 {max_retries} 次重试后仍无法连接")
        print(f"❌ 经过 {max_retries} 次重试后仍无法连接")
    
    return success

def main():
    """
    主函数，用于命令行调用
    """
    parser = argparse.ArgumentParser(description='Telegram自动重连工具')
    parser.add_argument('--no-wait', action='store_true', help='不等待API限流恢复')
    parser.add_argument('--retries', type=int, default=3, help='最大重试次数')
    args = parser.parse_args()
    
    result = asyncio.run(auto_wait_and_retry(
        wait_on_flood=not args.no_wait,
        max_retries=args.retries
    ))
    
    return 0 if result else 1

if __name__ == "__main__":
    sys.exit(main()) 