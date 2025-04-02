#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
调度器模块
用于在项目运行期间安排并执行定时任务
支持每小时整点执行任务
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
import traceback
from typing import Callable, Dict, Any, Optional, List

# 设置日志
logger = logging.getLogger(__name__)

class TaskScheduler:
    """任务调度器，用于安排定时任务"""
    
    def __init__(self):
        """初始化调度器"""
        self.tasks = {}  # 存储任务的字典
        self.running = False  # 调度器运行状态
        self.scheduler_task = None  # 调度器任务对象
    
    async def start(self):
        """启动调度器"""
        if self.running:
            logger.warning("调度器已经在运行中")
            return
        
        self.running = True
        logger.info("调度器已启动")
        
        # 创建调度器任务
        self.scheduler_task = asyncio.create_task(self._scheduler_loop())
    
    async def stop(self):
        """停止调度器"""
        if not self.running:
            logger.warning("调度器未运行")
            return
        
        self.running = False
        
        if self.scheduler_task:
            try:
                self.scheduler_task.cancel()
                await asyncio.sleep(0.1)  # 给任务一点时间取消
                logger.info("调度器已停止")
            except Exception as e:
                logger.error(f"停止调度器时出错: {str(e)}")
    
    async def _scheduler_loop(self):
        """
        调度器主循环
        检查并执行到期的任务
        """
        try:
            while self.running:
                now = datetime.now()
                
                # 检查每个注册的任务
                for task_id, task_info in list(self.tasks.items()):
                    if self._is_task_due(task_info, now):
                        # 如果任务到期，创建新任务执行它
                        asyncio.create_task(self._execute_task(task_id, task_info))
                        
                        # 更新下次执行时间
                        if task_info["recurring"]:
                            if task_info["hourly"]:
                                # 如果是每小时任务，设置为下一个小时整点
                                next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
                                task_info["next_run"] = next_hour
                            else:
                                # 其他周期性任务，按间隔更新
                                task_info["next_run"] = now + timedelta(seconds=task_info["interval"])
                        else:
                            # 如果不是周期性任务，执行后移除
                            self.tasks.pop(task_id, None)
                
                # 计算到下一分钟的等待时间，保证精确检查
                # 这样每分钟检查一次，不会错过整点
                next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
                wait_seconds = (next_minute - now).total_seconds()
                
                # 等待到下一分钟，但可以被中断
                try:
                    await asyncio.sleep(wait_seconds)
                except asyncio.CancelledError:
                    logger.info("调度器循环被取消")
                    break
                
        except asyncio.CancelledError:
            logger.info("调度器循环被取消")
        except Exception as e:
            logger.error(f"调度器循环出错: {str(e)}")
            logger.error(traceback.format_exc())
    
    def _is_task_due(self, task_info: Dict[str, Any], now: datetime) -> bool:
        """
        检查任务是否到期
        
        Args:
            task_info: 任务信息字典
            now: 当前时间
            
        Returns:
            bool: 任务是否到期
        """
        # 如果任务已禁用，不执行
        if not task_info["enabled"]:
            return False
            
        # 检查下次执行时间是否已到
        return now >= task_info["next_run"]
    
    async def _execute_task(self, task_id: str, task_info: Dict[str, Any]):
        """
        执行任务
        
        Args:
            task_id: 任务ID
            task_info: 任务信息字典
        """
        try:
            logger.info(f"执行任务: {task_id}")
            
            # 记录任务开始时间
            start_time = datetime.now()
            task_info["last_run"] = start_time
            
            # 执行任务函数
            if asyncio.iscoroutinefunction(task_info["func"]):
                # 如果是协程函数，直接await
                result = await task_info["func"](*task_info["args"], **task_info["kwargs"])
            else:
                # 如果是普通函数，使用线程池执行
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None, 
                    lambda: task_info["func"](*task_info["args"], **task_info["kwargs"])
                )
            
            # 记录执行结果
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            task_info["last_result"] = result
            task_info["last_duration"] = duration
            task_info["last_error"] = None
            task_info["consecutive_errors"] = 0
            
            logger.info(f"任务 {task_id} 执行完成，用时 {duration:.2f} 秒")
            
        except Exception as e:
            # 记录错误信息
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            task_info["last_error"] = str(e)
            task_info["last_duration"] = duration
            task_info["consecutive_errors"] += 1
            
            logger.error(f"任务 {task_id} 执行出错: {str(e)}")
            logger.error(traceback.format_exc())
    
    def schedule_task(self, 
                      task_id: str, 
                      func: Callable, 
                      args: tuple = (), 
                      kwargs: dict = None,
                      run_at: Optional[datetime] = None,
                      interval: Optional[int] = None,
                      hourly: bool = False,
                      enabled: bool = True):
        """
        安排任务执行
        
        Args:
            task_id: 任务ID，用于标识和管理任务
            func: 要执行的函数
            args: 函数参数
            kwargs: 函数关键字参数
            run_at: 首次运行时间，如果为None，则立即运行
            interval: 重复间隔（秒），如果为None，则任务只运行一次
            hourly: 是否每小时整点运行
            enabled: 任务是否启用
        """
        if kwargs is None:
            kwargs = {}
            
        now = datetime.now()
        
        # 确定首次运行时间
        if run_at is None:
            if hourly:
                # 如果是每小时任务，设置为下一个整点
                next_hour = now.replace(minute=0, second=0, microsecond=0)
                if next_hour <= now:
                    next_hour += timedelta(hours=1)
                next_run = next_hour
            else:
                # 否则立即运行
                next_run = now
        else:
            next_run = run_at
        
        # 创建任务信息
        task_info = {
            "func": func,
            "args": args,
            "kwargs": kwargs,
            "next_run": next_run,
            "last_run": None,
            "last_result": None,
            "last_error": None,
            "last_duration": None,
            "consecutive_errors": 0,
            "recurring": hourly or interval is not None,
            "interval": interval,
            "hourly": hourly,
            "enabled": enabled
        }
        
        # 保存任务
        self.tasks[task_id] = task_info
        
        logger.info(f"已安排任务 {task_id}, 下次执行时间: {next_run}")
        return task_id
    
    def schedule_hourly_task(self, task_id: str, func: Callable, args: tuple = (), kwargs: dict = None):
        """
        安排每小时整点执行的任务（便捷方法）
        
        Args:
            task_id: 任务ID
            func: 要执行的函数
            args: 函数参数
            kwargs: 函数关键字参数
        """
        return self.schedule_task(
            task_id=task_id,
            func=func,
            args=args,
            kwargs=kwargs,
            hourly=True
        )
    
    def cancel_task(self, task_id: str) -> bool:
        """
        取消任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            bool: 是否成功取消任务
        """
        if task_id in self.tasks:
            self.tasks.pop(task_id)
            logger.info(f"已取消任务 {task_id}")
            return True
        
        logger.warning(f"未找到任务 {task_id}")
        return False
    
    def enable_task(self, task_id: str, enabled: bool = True) -> bool:
        """
        启用或禁用任务
        
        Args:
            task_id: 任务ID
            enabled: 是否启用
            
        Returns:
            bool: 操作是否成功
        """
        if task_id in self.tasks:
            self.tasks[task_id]["enabled"] = enabled
            status = "启用" if enabled else "禁用"
            logger.info(f"已{status}任务 {task_id}")
            return True
        
        logger.warning(f"未找到任务 {task_id}")
        return False
    
    def get_task_info(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        获取任务信息
        
        Args:
            task_id: 任务ID
            
        Returns:
            Optional[Dict]: 任务信息字典，如果任务不存在则返回None
        """
        return self.tasks.get(task_id)
    
    def get_all_tasks(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有任务
        
        Returns:
            Dict: 所有任务信息字典
        """
        return self.tasks

# 创建全局调度器实例
scheduler = TaskScheduler() 