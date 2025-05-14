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
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 导入项目模块
from src.utils.logger import setup_logger, get_logger
from src.core.telegram_listener import TelegramListener
from src.core.telegram_client_factory import TelegramClientFactory
from src.database.models import init_db
from src.database.db_handler import cleanup_batch_tasks
from src.web.web_app import start_web_server
from src.utils.error_handler import ErrorMonitor, monitor_errors
from config.settings import load_config, DATABASE_URI
# 导入调度器和代币更新器
from src.utils.scheduler import scheduler
from src.api.token_updater import token_update
# 导入数据库工厂
from src.database.db_factory import get_db_adapter

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

# 修改检查数据库连接函数
async def check_database_connection() -> bool:
    """
    检查Supabase数据库连接是否正常
    
    Returns:
        bool: 连接是否正常
    """
    try:
        # 从配置中获取数据库URI
        from config.settings import DATABASE_URI
        
        # 确认正在使用Supabase数据库
        if not DATABASE_URI or not DATABASE_URI.startswith('supabase://'):
            logger.error("未使用Supabase数据库，请检查配置")
            logger.error(f"当前DATABASE_URI: {DATABASE_URI or '未设置'}")
            logger.error("DATABASE_URI应以'supabase://'开头")
            return False
        
        logger.info("正在使用 Supabase 数据库")
        
        # 使用数据库工厂获取适配器
        db_adapter = get_db_adapter()
        logger.info("已获取Supabase数据库适配器")
        
        # 直接使用数据库适配器获取活跃频道
        channels = await db_adapter.get_active_channels()
        logger.info(f"通过数据库适配器获取到 {len(channels)} 个活跃频道")
        
        # 检查是否可以获取活跃频道
        if len(channels) >= 0:  # 允许0个频道的情况
            logger.info(f"数据库连接正常，准备就绪")
            return True
        else:
            logger.error("数据库连接检查失败，无法获取频道列表")
            return False
            
    except Exception as e:
        logger.error(f"数据库连接检查失败: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return False


def parse_arguments():
    """
    解析命令行参数
    
    Returns:
        argparse.Namespace: 解析后的参数
    """
    parser = argparse.ArgumentParser(description='Telegram 监控服务')
    parser.add_argument('--no-web', action='store_true', help='不启动Web服务器')
    parser.add_argument('--no-telegram', action='store_true', help='不启动Telegram监听器')
    parser.add_argument('--no-token-update', action='store_true', help='不启动代币更新器')
    parser.add_argument('--check-db', action='store_true', help='检查数据库连接并退出')
    return parser.parse_args()


def setup() -> Dict[str, Any]:
    """
    初始化程序环境
    
    Returns:
        配置字典
    """
    try:
        # 确保logs目录存在
        log_dir = Path(__file__).resolve().parent / 'logs'
        os.makedirs(log_dir, exist_ok=True)
        print(f"确保logs目录存在: {log_dir}")
        
        # 测试logs目录是否可写
        test_file = log_dir / "test_write.tmp"
        try:
            with open(test_file, 'w') as f:
                f.write("测试写入权限")
            os.remove(test_file)
            print("日志目录写入权限正常")
        except Exception as e:
            print(f"日志目录写入权限测试失败: {e}")
            sys.exit(1)
        
        # 重置所有日志处理器
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # 设置日志 - 强制重新初始化日志系统
        print("正在初始化日志系统...")
        logger = setup_logger(__name__)
        
        # 立即测试日志文件
        today = datetime.now().strftime('%Y-%m-%d')
        log_file = log_dir / f"{today}_monitor.log"
        if os.path.exists(log_file):
            print(f"日志文件已创建: {log_file} ({os.path.getsize(log_file)} 字节)")
        else:
            print(f"警告: 日志文件未创建: {log_file}")
            # 尝试直接写入
            try:
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [INFO] MAIN - 手动创建日志文件\n")
                print(f"已手动创建日志文件: {log_file}")
            except Exception as e:
                print(f"手动创建日志文件失败: {e}")
        
        # 记录重要的系统信息
        logger.info(f"日志系统初始化完成")
        logger.info(f"操作系统: {platform.system()} {platform.release()}")
        logger.info(f"Python版本: {sys.version}")
        logger.info(f"工作目录: {os.getcwd()}")
        logger.info("正在初始化 Telegram 监控服务...")
        
        # 加载配置
        config = load_config()
        logger.info("已从环境变量加载配置")
        
        # 设置常见库的日志级别，避免过多的调试信息
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
        logging.getLogger("asyncio").setLevel(logging.WARNING)
        logging.getLogger("telethon").setLevel(logging.INFO)
        
        # 导入数据库URI直接从settings中获取
        try:
            from config.settings import DATABASE_URI
            # 确认使用Supabase数据库
            if not DATABASE_URI or not DATABASE_URI.startswith('supabase://'):
                logger.error("未使用Supabase数据库，请检查配置")
                logger.error(f"当前DATABASE_URI: {DATABASE_URI or '未设置'}")
                logger.error("DATABASE_URI应以'supabase://'开头")
                sys.exit(1)
            # 将DATABASE_URI也加入config字典，保持一致性
            config['DATABASE_URI'] = DATABASE_URI
        except ImportError:
            logger.error("无法导入config.settings模块")
            sys.exit(1)
        
        # 初始化数据库连接
        try:
            from src.database.db_factory import get_db_adapter
            db_adapter = get_db_adapter()
            logger.info("Supabase数据库适配器初始化成功")
            
            # 不再检查tokens_mark表，假设它已经存在
            
        except Exception as e:
            logger.error(f"Supabase数据库适配器初始化失败: {e}")
            sys.exit(1)
        
        # 创建错误监控器
        global error_monitor
        error_monitor = ErrorMonitor("TelegramMonitor")
        logger.info("错误监控系统已启动")
        
        return config
    except Exception as e:
        logger.critical(f"初始化失败: {str(e)}")
        raise


async def start_telegram_listener(config: Dict[str, Any]):
    """
    启动Telegram监听器
    
    Args:
        config: 配置字典
        
    Returns:
        TelegramListener: 启动后的监听器实例，如果启动失败则返回None
    """
    try:
        # 导入TelegramListener类
        from src.core.telegram_listener import TelegramListener
        
        # 创建并启动监听器
        listener = TelegramListener()
        start_result = await listener.start()
        
        if start_result:
            logger.info("Telegram监听器已成功启动")
            return listener
        else:
            logger.error("Telegram监听器启动失败")
            return None
    except Exception as e:
        logger.error(f"启动Telegram监听器时出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        return None


def start_web_interface(config: Dict[str, Any]) -> None:
    """
    启动Web界面
    Linux下启用https，其它环境保持http
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
                import platform
                def run_flask():
                    # Linux环境，启用自签名证书
                    if platform.system().lower() == 'linux':
                        ssl_context = ('/home/ubuntu/certs/server.crt', '/home/ubuntu/certs/server.key')
                        app.run(host=host, port=port, debug=debug, ssl_context=ssl_context)
                    else:
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
            import platform
            def run_flask():
                if platform.system().lower() == 'linux':
                    ssl_context = ('/home/ubuntu/certs/server.crt', '/home/ubuntu/certs/server.key')
                    app.run(host='0.0.0.0', port=5000, debug=False, ssl_context=ssl_context)
                else:
                    app.run(host='0.0.0.0', port=5000, debug=False)
            web_thread = threading.Thread(target=run_flask)
            web_thread.daemon = True
            web_thread.start()
            web_server_process = web_thread
            logger.info("使用基本配置成功启动Web界面")
        except Exception as e2:
            logger.error(f"使用基本配置启动Web界面也失败: {str(e2)}")


async def start_scheduler(config: Dict[str, Any]) -> None:
    """
    启动调度器和定时任务
    
    Args:
        config: 配置字典
    """
    try:
        logger.info("正在启动调度器...")
        
        # 启动调度器
        await scheduler.start()
        
        # 获取代币更新配置
        token_update_config = config.get('token_update', {})
        token_limit = token_update_config.get('limit', 20)
        token_interval = token_update_config.get('interval', 2)
        
        # 注册按配置的间隔时间执行代币更新任务 - 已禁用
        logger.info(f"代币数据更新任务已被禁用")
        
        # # 确保token_updater模块被正确导入
        # try:
        #     from src.api.token_updater import token_update
        #     logger.info("成功导入token_update函数")
        # except Exception as e:
        #     logger.error(f"导入token_update函数失败: {str(e)}")
        #     import traceback
        #     logger.error(traceback.format_exc())
        #     return
        #     
        # # 计算间隔时间（秒）
        # interval_seconds = token_interval * 60
        # 
        # # 设置定时更新任务
        # scheduler.schedule_task(
        #     'token_update_task',
        #     token_update,
        #     args=(token_limit,),
        #     interval=interval_seconds
        # )
        # 
        # logger.info(f"代币数据更新任务已注册，每{token_interval}分钟执行一次，限制更新数量: {token_limit}")
        
    except Exception as e:
        logger.error(f"启动调度器和定时任务失败: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())


async def health_check():
    """
    执行应用的健康检查
    
    Returns:
        tuple: (是否健康, 消息)
    """
    try:
        # 检查全局变量
        global telegram_listener
        
        # 1. 检查监听器实例是否存在
        if telegram_listener is None:
            return False, "Telegram监听器未初始化"
            
        # 2. 检查监听器是否正在运行
        if not telegram_listener.is_running:
            return False, "Telegram监听器未运行"
            
        # 3. 检查客户端连接状态
        if not telegram_listener.client or not telegram_listener.client.is_connected():
            logger.warning("Telegram客户端未连接，尝试重新连接...")
            
            # 使用工厂类进行更有效的重连
            try:
                # 如果客户端存在，尝试直接重连
                if telegram_listener.client:
                    logger.info("尝试重新连接现有客户端...")
                    if not telegram_listener.client.is_connected():
                        await telegram_listener.client.connect()
                    if await telegram_listener.client.is_user_authorized():
                        logger.info("重新连接现有客户端成功")
                        return True, "重新连接现有客户端成功"
                
                # 如果直接重连失败，使用工厂类获取新客户端
                logger.info("使用客户端工厂重连...")
                telegram_listener.client = await TelegramClientFactory.get_client(
                    telegram_listener.session_path,
                    telegram_listener.api_id,
                    telegram_listener.api_hash,
                    connection_retries=telegram_listener.connection_retries,
                    auto_reconnect=telegram_listener.auto_reconnect,
                    retry_delay=telegram_listener.retry_delay,
                    request_retries=telegram_listener.request_retries,
                    flood_sleep_threshold=telegram_listener.flood_sleep_threshold,
                    timeout=30
                )
                
                # 检查新客户端是否连接成功
                if telegram_listener.client and telegram_listener.client.is_connected() and await telegram_listener.client.is_user_authorized():
                    logger.info("使用客户端工厂重连成功")
                    
                    # 重新注册消息处理器
                    await telegram_listener.reinitialize_handlers()
                    logger.info("已重新注册消息处理器")
                    
                    return True, "使用客户端工厂重连成功"
                else:
                    logger.error("重连失败，客户端未授权")
                    return False, "重连失败，客户端未授权"
                    
            except Exception as e:
                logger.error(f"重连过程中出错: {e}")
                return False, f"重连过程中出错: {str(e)}"
            
        # 4. 检查客户端授权状态
        try:
            if not await telegram_listener.client.is_user_authorized():
                logger.error("Telegram客户端未授权")
                return False, "Telegram客户端未授权"
        except Exception as e:
            logger.error(f"检查授权状态时出错: {str(e)}")
            return False, f"检查授权状态时出错: {str(e)}"
            
        # 5. 获取当前连接计数
        connection_count = TelegramClientFactory.get_connection_count()
        if connection_count > 5:  # 如果连接计数异常高，记录警告
            logger.warning(f"检测到高连接计数: {connection_count}，可能存在连接泄漏")
            
        # 一切正常
        return True, "Telegram监听服务正常运行"
            
    except Exception as e:
        logger.error(f"健康检查时出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        return False, f"健康检查时出错: {str(e)}"


async def shutdown() -> None:
    """
    优雅关闭程序
    """
    logger.info("正在关闭 Telegram 监控服务...")
    
    # 设置关闭事件
    shutdown_event.set()
    
    # 关闭调度器
    logger.info("正在关闭调度器...")
    try:
        await scheduler.stop()
        logger.info("调度器已关闭")
    except Exception as e:
        logger.error(f"关闭调度器时出错: {str(e)}")
    
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
    
    # 确保所有Telegram连接都已关闭
    try:
        await TelegramClientFactory.disconnect_client()
        logger.info("已断开所有Telegram连接")
    except Exception as e:
        logger.error(f"断开Telegram连接时出错: {str(e)}")
    
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
            # 检查连接状态
            if telegram_listener and telegram_listener.client and not telegram_listener.client.is_connected():
                logger.warning("检测到Telegram连接已断开，尝试重新连接...")
                
                # 使用客户端工厂进行重连
                telegram_listener.client = await TelegramClientFactory.get_client(
                    telegram_listener.session_path,
                    telegram_listener.api_id,
                    telegram_listener.api_hash,
                    connection_retries=telegram_listener.connection_retries,
                    auto_reconnect=telegram_listener.auto_reconnect,
                    retry_delay=telegram_listener.retry_delay,
                    request_retries=telegram_listener.request_retries,
                    flood_sleep_threshold=telegram_listener.flood_sleep_threshold,
                    timeout=30
                )
                
                # 重新注册消息处理器
                if telegram_listener.client and telegram_listener.client.is_connected():
                    logger.info("重连成功，重新注册消息处理器")
                    await telegram_listener.reinitialize_handlers()
            
            # 清理过多的会话
            try:
                # 如果连接计数过高，尝试进行一次连接重置
                connection_count = TelegramClientFactory.get_connection_count()
                if connection_count > 5:  # 如果连接计数异常高
                    logger.warning(f"检测到高连接计数: {connection_count}，正在尝试重置连接...")
                    await TelegramClientFactory.disconnect_client()
                    
                    # 重新获取连接
                    telegram_listener.client = await TelegramClientFactory.get_client(
                        telegram_listener.session_path,
                        telegram_listener.api_id,
                        telegram_listener.api_hash,
                        connection_retries=telegram_listener.connection_retries,
                        auto_reconnect=telegram_listener.auto_reconnect,
                        retry_delay=telegram_listener.retry_delay,
                        request_retries=telegram_listener.request_retries,
                        flood_sleep_threshold=telegram_listener.flood_sleep_threshold,
                        timeout=30
                    )
                    
                    # 重新注册消息处理器
                    if telegram_listener.client and telegram_listener.client.is_connected():
                        await telegram_listener.reinitialize_handlers()
                        logger.info("连接已重置，消息处理器已重新注册")
            except Exception as cleanup_error:
                logger.error(f"清理连接时出错: {str(cleanup_error)}")
            
            # 等待下一次检查
            await asyncio.sleep(60)  # 每分钟检查一次
            
    except asyncio.CancelledError:
        logger.info("定期任务已取消")
    except Exception as e:
        logger.error(f"执行定期任务时出错: {str(e)}")


async def main_async(config: Dict[str, Any], no_web: bool, no_telegram: bool) -> None:
    """
    异步主函数
    
    Args:
        config: 配置字典
        no_web: 是否不启动Web界面
        no_telegram: 是否不启动Telegram监听器
    """
    # 注册信号处理，根据不同操作系统使用不同方式
    register_signal_handlers()
    
    try:
        # 启动调度器和定时任务
        logger.info("启动调度器和定时任务...")
        await start_scheduler(config)
        
        # 先启动Web服务，防止Telegram监听器阻塞
        if not no_web:
            logger.info("开始启动Web服务")
            start_web_interface(config)
        
        # 启动定期任务
        periodic_task = asyncio.create_task(periodic_tasks())
        
        # 启动Telegram监听器
        telegram_task = None
        if not no_telegram:
            logger.info("开始启动Telegram监听器...")
            global telegram_listener
            
            # 不同操作系统使用统一的启动方式
            telegram_task = asyncio.create_task(start_telegram_listener_background(config))
            
            # 等待监听器启动完成
            try:
                await asyncio.wait_for(telegram_task, timeout=30)
                if not telegram_listener:
                    logger.error("Telegram监听器启动失败")
            except asyncio.TimeoutError:
                logger.error("Telegram监听器启动超时")
            except Exception as e:
                logger.error(f"Telegram监听器启动出错: {str(e)}")
            
        # 等待关闭事件
        try:
            await shutdown_event.wait()
        finally:
            # 取消定期任务
            if 'periodic_task' in locals():
                logger.debug("取消定期任务...")
                periodic_task.cancel()
                try:
                    await periodic_task
                except asyncio.CancelledError:
                    pass
                
            # 确保Telegram监听器正确关闭
            if not no_telegram and telegram_listener:
                logger.debug("关闭Telegram监听器...")
                try:
                    await telegram_listener.stop()
                except Exception as e:
                    logger.error(f"关闭Telegram监听器时出错: {str(e)}")
                    import traceback
                    logger.debug(traceback.format_exc())
    except KeyboardInterrupt:
        logger.info("接收到键盘中断")
        shutdown_event.set()
    except Exception as e:
        logger.critical(f"程序运行出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
    finally:
        # 确保程序优雅关闭
        await shutdown()


async def start_telegram_listener_background(config: Dict[str, Any]) -> None:
    """
    在后台启动Telegram监听器，不阻塞主流程
    
    Args:
        config: 配置字典
    """
    global telegram_listener
    try:
        # 直接异步调用，不使用asyncio.run，避免嵌套事件循环
        telegram_listener = await start_telegram_listener(config)
        if telegram_listener:
            logger.info("Telegram监听器成功在后台启动")
            # 获取活跃频道数量
            if hasattr(telegram_listener, 'channel_manager'):
                active_channels = telegram_listener.channel_manager.get_active_channels()
                logger.info(f"监控 {len(active_channels)} 个活跃频道")
    except Exception as e:
        logger.error(f"Telegram监听器启动失败: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())


def register_signal_handlers():
    """
    注册信号处理函数，根据不同操作系统使用不同方式
    """
    try:
        # Windows系统使用不同的信号处理方式
        if platform.system() == 'Windows':
            try:
                # Windows下使用signal模块直接处理信号
                loop = asyncio.get_running_loop()
                
                def win_signal_handler(signum, frame):
                    logger.info(f"Windows: 接收到信号 {signum}，开始优雅关闭...")
                    # 设置关闭事件
                    shutdown_event.set()
                    # 安全地创建异步任务
                    if loop.is_running():
                        loop.call_soon_threadsafe(lambda: asyncio.create_task(shutdown()))
                
                # 注册信号处理函数
                signal.signal(signal.SIGINT, win_signal_handler)
                signal.signal(signal.SIGTERM, win_signal_handler)
                logger.debug("已设置Windows下的信号处理")
            except Exception as e:
                logger.warning(f"设置Windows信号处理失败: {e}, 程序将不能优雅关闭")
        else:
            # 在Unix系统下使用asyncio的信号处理器，这种方式更可靠
            try:
                loop = asyncio.get_running_loop()
                for sig in (signal.SIGINT, signal.SIGTERM):
                    loop.add_signal_handler(sig, signal_handler)
                logger.debug("已设置Unix下的信号处理")
            except Exception as e:
                logger.warning(f"设置Unix信号处理失败: {e}，使用备用方法")
                # 备用方法：使用signal模块
                def unix_signal_handler(signum, frame):
                    logger.info(f"Unix: 接收到信号 {signum}，开始优雅关闭...")
                    shutdown_event.set()
                    
                signal.signal(signal.SIGINT, unix_signal_handler)
                signal.signal(signal.SIGTERM, unix_signal_handler)
    except Exception as e:
        logger.error(f"注册信号处理函数失败: {e}")


def main() -> None:
    """
    主函数，程序入口
    """
    try:
        # 解析命令行参数
        args = parse_arguments()
        
        # 初始化
        config = setup()
        
        # 处理只检查数据库连接的情况
        if args.check_db:
            logger.info("检查数据库连接...")
            # 创建一个新的事件循环来检查数据库连接
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(check_database_connection())
                if result:
                    logger.info("数据库连接正常")
                    sys.exit(0)
                else:
                    logger.error("数据库连接失败")
                    sys.exit(1)
            finally:
                loop.close()
        
        # 根据不同操作系统设置多进程启动方法
        if platform.system() == 'Windows':
            try:
                # Windows下需要显式设置多进程启动方法为'spawn'
                import multiprocessing
                multiprocessing.set_start_method('spawn', force=True)
                logger.info("Windows环境：设置多进程启动方法为'spawn'")
            except Exception as e:
                logger.warning(f"设置Windows多进程方法失败: {e}")
        
        # 运行主异步函数
        # 使用asyncio.run是最安全的方式，它会适当处理不同环境的差异
        asyncio.run(main_async(config, args.no_web, args.no_telegram))
        
    except KeyboardInterrupt:
        logger.info("收到用户中断，正在关闭...")
        # 记录运行时间
        log_runtime()
    except Exception as e:
        logger.critical(f"程序崩溃: {str(e)}")
        import traceback
        logger.critical(traceback.format_exc())
        # 记录运行时间
        log_runtime()
        sys.exit(1)


if __name__ == "__main__":
    main()
