#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
环境变量检查工具
用于检查.env文件加载和环境变量设置情况
"""

import os
import sys
import logging
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# 添加项目根目录到sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)
    logger.info(f"添加项目根目录到sys.path: {parent_dir}")

def check_env_files():
    """检查可能的.env文件位置"""
    # 项目根目录
    root_dir = Path(parent_dir)
    
    # 检查常见的.env文件位置
    possible_paths = [
        root_dir / '.env',                   # 项目根目录
        root_dir.parent / '.env',            # 上级目录
        Path.cwd() / '.env',                 # 当前工作目录
        Path(os.path.expanduser('~')) / '.telegram-monitor.env'  # 用户主目录
    ]
    
    logger.info("检查可能的.env文件位置:")
    found = False
    
    for path in possible_paths:
        if path.exists():
            logger.info(f"✓ 找到.env文件: {path}")
            # 读取文件前几行(不包含敏感信息)检查格式
            try:
                with open(path, 'r') as f:
                    lines = f.readlines()[:5]  # 只读取前5行
                logger.info(f"  文件格式预览 (前5行):")
                for i, line in enumerate(lines, 1):
                    # 过滤密钥等敏感信息
                    if any(key in line.lower() for key in ['key', 'secret', 'password', 'token']):
                        logger.info(f"  {i}: {line.split('=')[0]}=******** (已隐藏敏感信息)")
                    else:
                        logger.info(f"  {i}: {line.strip()}")
            except Exception as e:
                logger.error(f"  读取文件失败: {e}")
            found = True
        else:
            logger.info(f"✗ 未找到.env文件: {path}")
    
    if not found:
        logger.warning("未找到任何.env文件！")
    
    return found

def check_env_vars():
    """检查关键环境变量的设置情况"""
    # 关键环境变量列表
    key_vars = [
        'DATABASE_URI',
        'SUPABASE_URL', 
        'SUPABASE_KEY',
        'TG_API_ID',
        'TG_API_HASH'
    ]
    
    logger.info("\n检查关键环境变量:")
    
    # 直接从系统环境变量获取
    for var in key_vars:
        value = os.environ.get(var)
        if value:
            # 对敏感信息进行脱敏处理
            if any(key in var.lower() for key in ['key', 'hash', 'secret', 'password', 'token']):
                masked_value = value[:5] + '*' * (len(value) - 8) + value[-3:] if len(value) > 8 else '******'
                logger.info(f"✓ {var} = {masked_value}")
            elif var == 'DATABASE_URI':
                # 只显示前10个字符，保护可能包含的凭证
                masked_value = value[:10] + '...' if len(value) > 10 else value
                logger.info(f"✓ {var} = {masked_value}")
                # 检查是否以supabase://开头
                if not value.startswith('supabase://'):
                    logger.error(f"✗ {var} 不是有效的Supabase连接字符串，必须以supabase://开头")
            else:
                logger.info(f"✓ {var} = {value}")
        else:
            logger.warning(f"✗ {var} 未设置")

def check_config_loading():
    """测试配置加载逻辑"""
    logger.info("\n测试config.settings模块加载:")
    
    try:
        import config.settings as settings
        logger.info("✓ 成功导入config.settings模块")
        
        # 检查DATABASE_URI值
        if hasattr(settings, 'DATABASE_URI'):
            db_uri = settings.DATABASE_URI
            if db_uri:
                # 保护敏感信息
                masked_uri = db_uri[:10] + '...' if len(db_uri) > 10 else db_uri
                logger.info(f"✓ settings.DATABASE_URI = {masked_uri}")
                
                # 检查是否以supabase://开头
                if not db_uri.startswith('supabase://'):
                    logger.error(f"✗ settings.DATABASE_URI 不是有效的Supabase连接字符串，必须以supabase://开头")
            else:
                logger.warning("✗ settings.DATABASE_URI 为空")
        else:
            logger.error("✗ settings模块中不存在DATABASE_URI属性")
        
        # 尝试加载配置字典
        try:
            from config.settings import load_config
            config = load_config()
            logger.info("✓ 成功加载配置字典")
            
            # 检查配置字典中的DATABASE_URI
            if 'DATABASE_URI' in config:
                db_uri = config['DATABASE_URI']
                masked_uri = db_uri[:10] + '...' if db_uri and len(db_uri) > 10 else db_uri
                logger.info(f"✓ config['DATABASE_URI'] = {masked_uri}")
            else:
                logger.warning("✗ 配置字典中不存在'DATABASE_URI'键")
                
            # 检查配置字典中的database.uri
            if 'database' in config and 'uri' in config['database']:
                db_uri = config['database']['uri']
                masked_uri = db_uri[:10] + '...' if db_uri and len(db_uri) > 10 else db_uri
                logger.info(f"✓ config['database']['uri'] = {masked_uri}")
            else:
                logger.warning("✗ 配置字典中不存在'database.uri'键")
                
        except Exception as e:
            logger.error(f"✗ 加载配置字典失败: {e}")
        
    except ImportError as e:
        logger.error(f"✗ 导入config.settings模块失败: {e}")
    except Exception as e:
        logger.error(f"✗ 测试config.settings时出错: {e}")

def check_db_adapter():
    """测试数据库适配器加载"""
    logger.info("\n测试数据库适配器加载:")
    
    try:
        from src.database.db_factory import get_db_adapter
        logger.info("✓ 成功导入数据库适配器工厂函数")
        
        # 检查数据库适配器工厂中的DATABASE_URI
        try:
            from src.database.db_factory import DATABASE_URI
            if DATABASE_URI:
                masked_uri = DATABASE_URI[:10] + '...' if len(DATABASE_URI) > 10 else DATABASE_URI
                logger.info(f"✓ db_factory.DATABASE_URI = {masked_uri}")
                
                # 检查是否以supabase://开头
                if not DATABASE_URI.startswith('supabase://'):
                    logger.error(f"✗ db_factory.DATABASE_URI 不是有效的Supabase连接字符串")
            else:
                logger.warning("✗ db_factory.DATABASE_URI 为空")
        except ImportError:
            logger.warning("✗ 无法导入db_factory.DATABASE_URI")
        
        # 不尝试创建适配器，这会建立实际连接
        logger.info("数据库适配器加载检查完成")
        
    except ImportError as e:
        logger.error(f"✗ 导入数据库适配器工厂失败: {e}")
    except Exception as e:
        logger.error(f"✗ 测试数据库适配器时出错: {e}")

def check_models():
    """测试数据库模型模块加载"""
    logger.info("\n测试数据库模型模块加载:")
    
    try:
        from src.database.models import DATABASE_URI
        logger.info("✓ 成功导入数据库模型模块")
        
        # 检查模型模块中的DATABASE_URI
        if DATABASE_URI:
            masked_uri = DATABASE_URI[:10] + '...' if len(DATABASE_URI) > 10 else DATABASE_URI
            logger.info(f"✓ models.DATABASE_URI = {masked_uri}")
            
            # 检查是否以supabase://开头
            if not DATABASE_URI.startswith('supabase://'):
                logger.error(f"✗ models.DATABASE_URI 不是有效的Supabase连接字符串")
        else:
            logger.warning("✗ models.DATABASE_URI 为空")
            
    except ImportError as e:
        logger.error(f"✗ 导入数据库模型模块失败: {e}")
    except Exception as e:
        logger.error(f"✗ 测试数据库模型时出错: {e}")

def main():
    """主函数"""
    logger.info("="*60)
    logger.info("Telegram监控系统环境变量检查工具")
    logger.info("="*60)
    
    # 检查系统环境
    logger.info(f"Python版本: {sys.version}")
    logger.info(f"当前工作目录: {os.getcwd()}")
    logger.info(f"脚本位置: {os.path.abspath(__file__)}")
    logger.info(f"项目根目录: {parent_dir}")
    
    # 执行各项检查
    check_env_files()
    check_env_vars()
    check_config_loading()
    check_db_adapter()
    check_models()
    
    logger.info("\n检查完成！")
    
    # 返回检查结果
    if 'DATABASE_URI' in os.environ and os.environ['DATABASE_URI'].startswith('supabase://'):
        logger.info("✓ 环境变量配置正常")
        return 0
    else:
        logger.error("✗ 环境变量配置存在问题，请检查上述输出")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 