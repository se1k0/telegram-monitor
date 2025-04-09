#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库工厂模块
根据配置返回适当的数据库适配器
"""

import os
import logging
from typing import Any, Dict, Optional

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

# 从配置中获取数据库类型
try:
    import config.settings as config
    DATABASE_URI = config.DATABASE_URI
except ImportError:
    # 尝试从环境变量加载
    from dotenv import load_dotenv
    load_dotenv()
    DATABASE_URI = os.getenv('DATABASE_URI', '')

# 数据库适配器存储
_adapters = {}

def get_db_adapter():
    """
    获取数据库适配器
    
    Returns:
        数据库适配器实例
    """
    global _adapters
    
    # 检查是否已创建适配器
    if DATABASE_URI in _adapters:
        return _adapters[DATABASE_URI]
        
    # 确保DATABASE_URI是supabase类型
    if not DATABASE_URI.startswith('supabase://'):
        logger.error(f"不支持的数据库类型: {DATABASE_URI}")
        logger.error("目前只支持Supabase数据库，DATABASE_URI必须以supabase://开头")
        raise ValueError(f"不支持的数据库类型: {DATABASE_URI}")
        
    # 使用Supabase适配器
    try:
        from .supabase_adapter import get_adapter as get_supabase_adapter
        adapter = get_supabase_adapter()
        _adapters[DATABASE_URI] = adapter
        logger.info("使用Supabase数据库适配器")
        return adapter
    except ImportError as e:
        logger.error(f"无法导入Supabase适配器: {str(e)}")
        logger.error("请运行: pip install supabase")
        raise
            
    return None

# 获取默认适配器
def get_default_adapter():
    """
    获取默认数据库适配器
    
    Returns:
        默认数据库适配器实例
    """
    return get_db_adapter() 