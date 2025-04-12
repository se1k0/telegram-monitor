#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库工厂模块
专门用于创建和管理Supabase数据库适配器
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

# 从配置中获取数据库连接信息
try:
    import config.settings as config
    DATABASE_URI = config.DATABASE_URI
    logger.info(f"成功从config.settings加载DATABASE_URI，值为: {DATABASE_URI[:10]}...")
except ImportError:
    # 如果无法导入配置，尝试直接从环境变量加载
    from dotenv import load_dotenv
    load_dotenv()
    DATABASE_URI = os.getenv('DATABASE_URI', '')
    logger.warning(f"无法从config.settings加载配置，直接从环境变量加载DATABASE_URI")

if not DATABASE_URI:
    logger.error("DATABASE_URI未设置，请检查config/settings.py文件或环境变量")

# 数据库适配器单例实例
_adapter = None

def get_db_adapter():
    """
    获取Supabase数据库适配器的单例实例
    
    如果DATABASE_URI不是supabase类型，将抛出异常
    
    Returns:
        SupabaseAdapter: 数据库适配器实例
    """
    global _adapter
    
    # 检查是否已创建适配器
    if _adapter is not None:
        return _adapter
        
    # 确保DATABASE_URI是supabase类型
    if not DATABASE_URI.startswith('supabase://'):
        logger.error(f"不支持的数据库类型: {DATABASE_URI}")
        logger.error("项目只支持Supabase数据库，DATABASE_URI必须以supabase://开头")
        raise ValueError(f"不支持的数据库类型: {DATABASE_URI}")
        
    # 创建Supabase适配器
    try:
        from .supabase_adapter import get_adapter as get_supabase_adapter
        _adapter = get_supabase_adapter()
        logger.info("成功创建Supabase数据库适配器")
        return _adapter
    except ImportError as e:
        logger.error(f"无法导入Supabase适配器: {str(e)}")
        logger.error("请运行: pip install supabase")
        raise
            
# 获取默认适配器 (保留兼容性)
def get_default_adapter():
    """
    获取默认数据库适配器
    
    Returns:
        Supabase数据库适配器实例
    """
    return get_db_adapter() 