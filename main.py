#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Telegram 监控服务
主程序入口
"""

import os
import sys
import time
import logging
import asyncio
import signal
import argparse
import platform
from datetime import datetime
from typing import Dict, Any, Optional

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 导入项目模块
from src.utils.logger import setup_logger, get_logger
from src.core.telegram_listener import TelegramListener
from src.database.models import init_db
from src.database.db_handler import cleanup_batch_tasks
from src.web.web_app import start_web_server
from src.utils.error_handler import ErrorMonitor, monitor_errors
from config.settings import load_config, CONFIG_FILE

# 设置日志
logger = get_logger(__name__)

# 全局变量
telegram_listener = None
web_server_process = None
error_monitor = None
shutdown_event = asyncio.Event()

# 记录启动时间
start_time = datetime.now()
logger.info(f"Telegram 监控服务启动于 {start_time}")

# 添加关闭时间记录函数
def log_runtime():
    end_time = datetime.now()
    runtime = end_time - start_time
    logger.info(f"Telegram 监控服务运行了 {runtime}")


def setup(config_file: str = CONFIG_FILE) -> Dict[str, Any]:
    """
    初始化程序环境
    
    Args:
        config_file: 配置文件路径
        
    Returns:
        配置字典
    """
    try:
        # 设置日志
        setup_logger()
        logger.info("正在初始化 Telegram 监控服务...")
        
        # 加载配置
        config = load_config(config_file)
        logger.info(f"已加载配置文件: {config_file}")
        
        # 初始化数据库
        init_db()
        logger.info("数据库初始化完成")
        
        # 修复数据库结构
        try:
            from scripts.repair_database import manually_add_columns
            logger.info("检查并修复数据库结构...")
            manually_add_columns()
            logger.info("数据库结构检查和修复完成")
        except Exception as e:
            logger.warning(f"数据库结构检查失败: {str(e)}")
            logger.warning("如果应用无法启动，请尝试运行: python scripts/repair_database.py")
        
        # 创建错误监控器
        global error_monitor
        error_monitor = ErrorMonitor("TelegramMonitor")
        logger.info("错误监控系统已启动")
        
        return config
    except Exception as e:
        logger.critical(f"初始化失败: {str(e)}")
        raise


async def start_telegram_listener(config: Dict[str, Any]) -> TelegramListener:
    """
    启动Telegram监听器
    
    Args:
        config: 配置字典
        
    Returns:
        TelegramListener实例
    """
    try:
        # 创建并启动Telegram监听器
        from src.core.telegram_listener import TelegramListener
        listener = TelegramListener()
        
        # 启动监听器
        result = await listener.start()
        
        # 检查启动结果
        if result is False:
            logger.error("Telegram监听器启动失败")
            return None
        elif isinstance(result, TelegramListener):
            # 如果start返回实例本身，使用它
            listener = result
        
        # 获取活跃频道数量
        active_channels = listener.channel_manager.get_active_channels()
        logger.info(f"Telegram监听器已启动，监控 {len(active_channels)} 个频道")
        
        return listener
    except Exception as e:
        logger.critical(f"启动Telegram监听器失败: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        return None


def start_web_interface(config: Dict[str, Any]) -> None:
    """
    启动Web界面
    
    Args:
        config: 配置字典
    """
    try:
        # 确保配置不为空
        if not config:
            logger.warning("配置为空，将使用默认配置")
            config = {
                "web_server": {
                    "host": "0.0.0.0",
                    "port": 5000,
                    "debug": False
                }
            }
            
        # 获取Web配置
        logger.info(f"获取Web配置")
        web_config = config.get('web_server', {})
        
        # 检查web_config是否为空，如果为空则使用默认值
        if not web_config:
            logger.warning("Web配置为空，将使用默认配置")
            web_config = {
                'host': '0.0.0.0',
                'port': 5000,
                'debug': False
            }
            
        # 获取具体配置参数
        host = web_config.get('host', '0.0.0.0')
        port = web_config.get('port', 5000)
        debug = web_config.get('debug', False)
        
        logger.info(f"Web配置: host={host}, port={port}, debug={debug}")
        
        # 调用start_web_server函数启动Web服务器
        from src.web.web_app import start_web_server
        logger.info(f"使用start_web_server启动Web服务器")
        
        global web_server_process
        web_server_process = start_web_server(host, port, debug)
        
        if web_server_process:
            logger.info(f"Web界面已启动: http://{host}:{port}")
        else:
            logger.error("Web服务器启动失败，尝试使用备用方式启动")
            # 使用备用启动方式
            try:
                from src.web.web_app import app
                import threading
                
                def run_flask():
                    app.run(host=host, port=port, debug=debug)
                
                web_thread = threading.Thread(target=run_flask)
                web_thread.daemon = True
                web_thread.start()
                web_server_process = web_thread
                logger.info(f"Web界面已通过备用方式启动: http://{host}:{port}")
            except Exception as e:
                logger.error(f"备用启动方式也失败: {str(e)}")
                
    except Exception as e:
        logger.error(f"启动Web界面失败: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        
        # 即使出错，也尝试使用最基本的配置启动
        try:
            logger.info("尝试使用最基本配置启动Web界面")
            from src.web.web_app import app
            import threading
            
            def run_flask():
                app.run(host='0.0.0.0', port=5000, debug=False)
            
            web_thread = threading.Thread(target=run_flask)
            web_thread.daemon = True
            web_thread.start()
            web_server_process = web_thread
            logger.info("使用基本配置成功启动Web界面")
        except Exception as e2:
            logger.error(f"使用基本配置启动Web界面也失败: {str(e2)}")


async def shutdown() -> None:
    """
    优雅关闭程序
    """
    logger.info("正在关闭 Telegram 监控服务...")
    
    # 设置关闭事件
    shutdown_event.set()
    
    # 关闭Telegram监听器
    global telegram_listener
    if telegram_listener:
        logger.info("正在关闭Telegram监听器...")
        try:
            await telegram_listener.stop()
            logger.info("Telegram监听器已关闭")
        except Exception as e:
            logger.error(f"关闭Telegram监听器时出错: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
        # 确保引用被释放
        telegram_listener = None
    
    # 关闭Web服务器
    global web_server_process
    if web_server_process:
        logger.info("正在关闭Web服务器...")
        try:
            # web_server_process现在是线程对象
            # Flask应用很难在运行中终止，只能设置daemon属性让主程序退出时自动终止
            logger.info("Web服务器将随主程序一起退出")
            
            # 等待一段时间让当前请求完成处理
            await asyncio.sleep(2)
                
        except Exception as e:
            logger.error(f"关闭Web服务器时出错: {str(e)}")
        # 确保引用被释放
        web_server_process = None
    
    # 清理批处理任务
    logger.info("正在清理批处理任务...")
    try:
        await cleanup_batch_tasks()
        logger.info("批处理任务已清理")
    except Exception as e:
        logger.error(f"清理批处理任务时出错: {str(e)}")
    
    # 生成最终错误报告
    global error_monitor
    if error_monitor:
        try:
            report = error_monitor.generate_report()
            logger.info(f"最终错误报告 - 总运行时间: {report['uptime_formatted']}")
            logger.info(f"总错误数: {report['error_stats']['total_errors']}")
        except Exception as e:
            logger.error(f"生成错误报告时出错: {str(e)}")
        # 确保引用被释放
        error_monitor = None
    
    logger.info("Telegram 监控服务已关闭")
    
    # 记录运行时间
    log_runtime()


def signal_handler() -> None:
    """
    信号处理函数
    """
    logger.info("接收到关闭信号，开始优雅关闭...")
    # 立即设置关闭事件
    shutdown_event.set()
    # 创建异步任务执行shutdown
    asyncio.create_task(shutdown())


async def periodic_tasks() -> None:
    """
    定期执行的任务
    """
    try:
        while not shutdown_event.is_set():
            try:
                # 监控错误
                monitor_errors()
                
                # 等待一段时间，但可以被中断
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=60)  # 每分钟检查一次，而不是每小时
                    if shutdown_event.is_set():
                        logger.info("检测到关闭事件，定期任务退出")
                        break
                except asyncio.TimeoutError:
                    # 超时，继续循环
                    pass
            except Exception as e:
                logger.error(f"执行定期任务时出错: {str(e)}")
                await asyncio.sleep(10)  # 出错后短暂休息
    except asyncio.CancelledError:
        logger.info("定期任务被取消")
    finally:
        logger.info("定期任务已退出")


async def main_async() -> None:
    """
    异步主函数
    """
    try:
        # 解析命令行参数
        parser = argparse.ArgumentParser(description='Telegram 监控服务')
        # 删除所有参数解析
        args = parser.parse_args()
        
        # 初始化
        config = setup(CONFIG_FILE)
        
        # 设置信号处理
        # Windows系统使用不同的信号处理方式
        if platform.system() == 'Windows':
            try:
                # Windows下改进的信号处理方式
                loop = asyncio.get_event_loop()
                
                def win_signal_handler(signum, frame):
                    logger.info(f"接收到信号 {signum}，开始优雅关闭...")
                    # 设置关闭事件
                    shutdown_event.set()
                    if loop.is_running():
                        loop.call_soon_threadsafe(lambda: asyncio.create_task(shutdown()))
                    else:
                        # 如果事件循环未运行，直接调用asyncio.run
                        asyncio.run(shutdown())
                
                # 注册信号处理函数
                signal.signal(signal.SIGINT, win_signal_handler)
                signal.signal(signal.SIGTERM, win_signal_handler)
                logger.debug("已设置Windows下的信号处理")
            except Exception as e:
                logger.warning(f"设置Windows信号处理失败: {e}, 程序将不能优雅关闭")
        else:
            # 在Unix系统下使用asyncio的信号处理器
            for sig in (signal.SIGINT, signal.SIGTERM):
                asyncio.get_event_loop().add_signal_handler(sig, signal_handler)
            logger.debug("已设置Unix下的信号处理")
        
        # 无条件启动Web界面，先启动Web服务，防止Telegram监听器阻塞
        logger.info(f"开始启动web服务")
        start_web_interface(config)
        
        # 启动定期任务
        periodic_task = asyncio.create_task(periodic_tasks())
        
        # 在后台启动Telegram监听器
        logger.info("开始启动Telegram监听器(后台运行)")
        global telegram_listener
        telegram_task = asyncio.create_task(start_telegram_listener_background(config))
        
        try:
            # 等待关闭事件
            await shutdown_event.wait()
        finally:
            # 取消定期任务
            logger.debug("取消定期任务...")
            if 'periodic_task' in locals():
                periodic_task.cancel()
                try:
                    await periodic_task
                except asyncio.CancelledError:
                    pass
                    
            # 取消Telegram监听器任务
            if 'telegram_task' in locals() and not telegram_task.done():
                logger.debug("取消Telegram监听器任务...")
                # 我们不直接取消任务，而是等待它自己完成关闭流程
                if telegram_listener:
                    await telegram_listener.stop()
    except KeyboardInterrupt:
        logger.info("在异步主函数中接收到键盘中断")
        # 设置关闭事件
        shutdown_event.set()
    except Exception as e:
        logger.critical(f"程序运行出错:")
        import traceback
        logger.debug(traceback.format_exc())
    finally:
        # 确保程序正常关闭
        if not shutdown_event.is_set():
            logger.debug("关闭事件未设置，执行关闭流程...")
        await shutdown()


async def start_telegram_listener_background(config: Dict[str, Any]) -> None:
    """
    在后台启动Telegram监听器，不阻塞主流程
    
    Args:
        config: 配置字典
    """
    global telegram_listener
    try:
        telegram_listener = await start_telegram_listener(config)
    except Exception as e:
        logger.error(f"Telegram监听器启动失败: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())


def main() -> None:
    """
    主函数
    """
    start_time = datetime.now()
    logger.info(f"Telegram 监控服务启动于 {start_time}")
    
    try:
        # 运行异步主函数
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("接收到键盘中断，正在优雅关闭...")
        # 确保在键盘中断时也会执行清理操作
        try:
            # 创建新的事件循环运行shutdown
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(shutdown())
            loop.close()
        except Exception as e:
            logger.error(f"关闭过程中出错: {str(e)}")
    except Exception as e:
        logger.critical(f"程序崩溃: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
    
    end_time = datetime.now()
    runtime = end_time - start_time
    logger.info(f"Telegram 监控服务运行了 {runtime}")


if __name__ == "__main__":
    main()
