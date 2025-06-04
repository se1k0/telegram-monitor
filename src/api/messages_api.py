#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Messages API接口
提供查询messages表内容的API功能
"""

import os
import sys
import logging
import json
import time
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta
import asyncio

# 添加路径以确保能够导入项目模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# 导入数据库适配器
from src.database.db_factory import get_db_adapter
from src.database.models import Message

# 添加日志支持
try:
    from src.utils.logger import get_logger
    logger = get_logger(__name__)
except ImportError:
    # 如果导入失败，则使用基本日志配置
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    logger = logging.getLogger(__name__)

class MessagesAPI:
    """提供消息查询的API功能类"""
    
    def __init__(self):
        """初始化API类"""
        self.db_adapter = get_db_adapter()
        
    async def query_messages(self, 
                            channel_ids: Optional[List[int]] = None, 
                            start_date: Optional[datetime] = None, 
                            end_date: Optional[datetime] = None, 
                            limit: int = 1000) -> Dict[str, Any]:
        """
        查询消息表中的内容
        
        Args:
            channel_ids: 频道ID列表，如果为None则查询所有频道
            start_date: 开始日期，如果为None则不限制开始日期
            end_date: 结束日期，如果为None则不限制结束日期
            limit: 返回结果的最大数量，默认为1000
            
        Returns:
            Dict[str, Any]: 包含查询结果的字典
        """
        try:
            # 构建过滤条件
            filters = {}
            
            # 添加频道ID过滤
            if channel_ids and len(channel_ids) > 0:
                if len(channel_ids) == 1:
                    # 单个频道ID使用等于条件
                    filters["channel_id"] = channel_ids[0]
                else:
                    # 多个频道ID - 使用OR条件
                    # 对于多个channel_id，我们需要分别进行查询并合并结果
                    all_messages = []
                    for channel_id in channel_ids:
                        channel_filters = filters.copy()
                        channel_filters["channel_id"] = channel_id
                        
                        # 添加日期过滤
                        if start_date:
                            channel_filters["date"] = (">=", start_date.isoformat())
                        
                        # 执行每个频道的查询
                        channel_result = await self.db_adapter.execute_query(
                            table="messages",
                            query_type="select",
                            filters=channel_filters,
                            limit=limit,  # 每个频道应用相同的limit
                            order_by={"date": "desc"}
                        )
                        
                        # 处理查询结果
                        if isinstance(channel_result, dict) and "data" in channel_result:
                            channel_messages = channel_result.get("data", [])
                        else:
                            channel_messages = channel_result if channel_result else []
                        
                        all_messages.extend(channel_messages)
                    
                    # 按日期降序排序所有消息
                    all_messages.sort(key=lambda x: x.get("date", ""), reverse=True)
                    
                    # 应用end_date过滤
                    if end_date:
                        filtered_by_end_date = []
                        for message in all_messages:
                            if "date" in message and message["date"]:
                                try:
                                    msg_date = datetime.fromisoformat(message["date"].replace("Z", "+00:00"))
                                    if msg_date <= end_date:
                                        filtered_by_end_date.append(message)
                                except Exception as e:
                                    logger.warning(f"日期解析错误: {str(e)}")
                        all_messages = filtered_by_end_date
                    
                    # 限制总结果数量
                    if len(all_messages) > limit:
                        all_messages = all_messages[:limit]
                    
                    # 格式化日期
                    for message in all_messages:
                        if "date" in message and message["date"]:
                            try:
                                date_obj = datetime.fromisoformat(message["date"].replace("Z", "+00:00"))
                                message["date"] = date_obj.strftime("%Y-%m-%d %H:%M:%S")
                            except Exception as e:
                                logger.warning(f"日期格式化错误: {str(e)}")
                    
                    return {
                        "success": True,
                        "count": len(all_messages),
                        "messages": all_messages,
                        "query_params": {
                            "channel_ids": channel_ids,
                            "start_date": start_date.isoformat() if start_date else None,
                            "end_date": end_date.isoformat() if end_date else None,
                            "limit": limit
                        }
                    }
            
            # 添加日期过滤 - 使用元组形式的比较操作符
            if start_date:
                filters["date"] = (">=", start_date.isoformat())
            
            # 执行查询 (单个频道或无频道过滤的情况)
            result = await self.db_adapter.execute_query(
                table="messages",
                query_type="select",
                filters=filters,
                limit=limit,
                order_by={"date": "desc"}  # 按日期降序排序
            )
            
            # 处理查询结果
            if isinstance(result, dict) and "data" in result:
                messages = result.get("data", [])
            else:
                messages = result if result else []
            
            # 应用end_date过滤 (如果有)
            filtered_messages = []
            for message in messages:
                # 过滤end_date
                if end_date and "date" in message and message["date"]:
                    try:
                        msg_date = datetime.fromisoformat(message["date"].replace("Z", "+00:00"))
                        if msg_date > end_date:
                            continue
                    except Exception as e:
                        logger.warning(f"日期解析错误: {str(e)}")
                
                # 通过了所有过滤条件，添加到结果中
                filtered_messages.append(message)
            
            # 限制结果数量
            if len(filtered_messages) > limit:
                filtered_messages = filtered_messages[:limit]
                
            # 格式化日期
            for message in filtered_messages:
                if "date" in message and message["date"]:
                    # 将ISO日期字符串转换为更易读的格式
                    try:
                        date_obj = datetime.fromisoformat(message["date"].replace("Z", "+00:00"))
                        message["date"] = date_obj.strftime("%Y-%m-%d %H:%M:%S")
                    except Exception as e:
                        logger.warning(f"日期格式化错误: {str(e)}")
            
            return {
                "success": True,
                "count": len(filtered_messages),
                "messages": filtered_messages,
                "query_params": {
                    "channel_ids": channel_ids,
                    "start_date": start_date.isoformat() if start_date else None,
                    "end_date": end_date.isoformat() if end_date else None,
                    "limit": limit
                }
            }
            
        except Exception as e:
            logger.error(f"查询消息失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            
            return {
                "success": False,
                "error": f"查询消息失败: {str(e)}",
                "count": 0,
                "messages": []
            }

# 创建API单例
messages_api = MessagesAPI()

# 导出查询函数
async def query_messages(channel_ids=None, start_date=None, end_date=None, limit=1000):
    """
    查询消息的导出函数
    
    Args:
        channel_ids: 频道ID列表，如果为None则查询所有频道
        start_date: 开始日期，如果为None则不限制开始日期
        end_date: 结束日期，如果为None则不限制结束日期
        limit: 返回结果的最大数量，默认为1000
        
    Returns:
        Dict[str, Any]: 包含查询结果的字典
    """
    return await messages_api.query_messages(channel_ids, start_date, end_date, limit) 