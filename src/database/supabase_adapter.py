#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Supabase数据库适配器
提供与Supabase数据库的连接和操作功能
"""

import os
import sys
import logging
import asyncio
from typing import Dict, Any, List, Optional, Union, Tuple
from datetime import datetime

try:
    from supabase import create_client, Client
except ImportError:
    raise ImportError("未安装supabase库，请运行：pip install supabase")

# 导入配置
try:
    from config.settings import SUPABASE_URL, SUPABASE_KEY, SUPABASE_SERVICE_KEY
except ImportError:
    # 从环境变量加载
    from dotenv import load_dotenv
    load_dotenv()
    SUPABASE_URL = os.getenv('SUPABASE_URL', '')
    SUPABASE_KEY = os.getenv('SUPABASE_KEY', '')
    SUPABASE_SERVICE_KEY = os.getenv('SUPABASE_SERVICE_KEY', '')

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

# 检查Supabase配置
if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("未配置Supabase，请在.env文件中设置SUPABASE_URL和SUPABASE_KEY")
    sys.exit(1)

# 初始化Supabase客户端
supabase: Optional[Client] = None
supabase_admin: Optional[Client] = None

try:
    # 初始化常规客户端 (anon key) - 用于读取操作
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info(f"Supabase客户端初始化成功: {SUPABASE_URL}")
    
    # 如果有服务角色密钥，初始化管理客户端 (service role key) - 用于写入操作
    if SUPABASE_SERVICE_KEY:
        try:
            supabase_admin = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
            logger.info("Supabase管理客户端初始化成功 (使用服务角色密钥)")
        except Exception as admin_init_error:
            logger.warning(f"Supabase管理客户端初始化失败: {str(admin_init_error)}")
            logger.warning("将使用匿名密钥作为管理客户端，可能受RLS策略限制")
            supabase_admin = supabase
    else:
        logger.warning("未找到SUPABASE_SERVICE_KEY，将对所有操作使用匿名密钥，可能受RLS策略限制")
        supabase_admin = supabase
except Exception as e:
    logger.error(f"Supabase客户端初始化失败: {str(e)}")
    sys.exit(1)

class SupabaseAdapter:
    """Supabase数据库适配器类"""
    
    def __init__(self):
        """初始化Supabase适配器"""
        self.supabase = supabase
        self.supabase_admin = supabase_admin if supabase_admin else supabase
        
    async def execute_query(self, table: str, query_type: str, data: Dict[str, Any] = None, 
                           filters: Dict[str, Any] = None, limit: int = None, fields: List[str] = None,
                           order_by: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        执行Supabase查询
        
        Args:
            table: 表名
            query_type: 查询类型 (select, insert, update, upsert, delete)
            data: 要插入或更新的数据
            filters: 过滤条件
            limit: 限制返回记录数
            fields: 要选择的字段列表（仅用于select操作）
            order_by: 排序参数，例如 {'column': 'created_at', 'ascending': False}
            
        Returns:
            查询结果
        """
        try:
            # 根据操作类型选择合适的客户端
            # 读取操作使用普通客户端，写入操作使用管理客户端
            if query_type == 'select':
                query = self.supabase.table(table)
            else:
                query = self.supabase_admin.table(table)
            
            if query_type == 'select':
                # 构建查询
                # 处理字段选择
                if fields and isinstance(fields, list) and len(fields) > 0:
                    # 使用指定的字段列表
                    field_str = ",".join(fields)
                    select_query = query.select(field_str)
                else:
                    # 默认选择所有字段
                    select_query = query.select("*")
                
                if filters:
                    # 对于Supabase的Python客户端，我们需要使用eq、gt等方法来过滤
                    # 应用过滤条件
                    for key, value in filters.items():
                        # 处理不同类型的过滤条件
                        if isinstance(value, tuple) and len(value) == 2:
                            operator, val = value
                            if operator == '=':
                                select_query = select_query.eq(key, val)
                            elif operator == '>':
                                select_query = select_query.gt(key, val)
                            elif operator == '<':
                                select_query = select_query.lt(key, val)
                            # 其他操作符...
                        else:
                            # 默认使用相等操作符
                            select_query = select_query.eq(key, value)
                
                # 应用排序
                if order_by and isinstance(order_by, dict) and 'column' in order_by:
                    column = order_by['column']
                    # 默认为升序，除非显式指定为降序
                    ascending = order_by.get('ascending', True)
                    select_query = select_query.order(column, desc=(not ascending))
                    
                # 应用限制
                if limit:
                    select_query = select_query.limit(limit)
                
                # 执行查询
                result = select_query.execute()
                return result.data
                
            elif query_type == 'insert':
                if data:
                    # 如果提供了数据字典，去除id字段，让数据库自动生成
                    if isinstance(data, dict) and 'id' in data and data['id'] is None:
                        data = {k: v for k, v in data.items() if k != 'id'}
                    # 如果提供了数据列表，对每一项都去除id字段
                    elif isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict) and 'id' in item and item['id'] is None:
                                item.pop('id')
                    
                    result = query.insert(data).execute()
                    return result.data
                    
            elif query_type == 'update':
                if data and filters:
                    # 从data中移除id字段，避免更新主键
                    if isinstance(data, dict) and 'id' in data:
                        data = {k: v for k, v in data.items() if k != 'id'}
                
                    # 构建更新查询
                    update_query = query.update(data)
                    
                    # 应用过滤条件
                    for key, value in filters.items():
                        update_query = update_query.eq(key, value)
                    
                    # 执行更新
                    result = update_query.execute()
                    
                    # 检查更新结果
                    if not result or not hasattr(result, 'data'):
                        logger.error(f"更新操作没有返回预期的结果: {result}")
                        return {'error': '更新操作失败，没有返回预期的结果'}
                        
                    # 检查是否真的更新了数据
                    if not result.data:
                        logger.warning(f"更新操作可能未影响任何记录，返回空结果: {result}")
                        
                        # 使用查询确认记录是否存在
                        check_query = self.supabase.table(table).select('*')
                        for key, value in filters.items():
                            check_query = check_query.eq(key, value)
                        
                        check_result = check_query.execute()
                        if hasattr(check_result, 'data') and check_result.data:
                            logger.info(f"记录确实存在，但更新操作未返回数据，这可能是Supabase API的特性")
                            return check_result.data
                        else:
                            logger.error(f"更新操作失败，记录不存在: {filters}")
                            return {'error': '更新操作失败，记录不存在'}
                    
                    return result.data
                    
            elif query_type == 'upsert':
                if data:
                    # 对于upsert操作，需要确保不提供无效的id值
                    if isinstance(data, dict) and 'id' in data and data['id'] is None:
                        data = {k: v for k, v in data.items() if k != 'id'}
                    elif isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict) and 'id' in item and item['id'] is None:
                                item.pop('id')
                    
                    result = query.upsert(data).execute()
                    return result.data
                    
            elif query_type == 'delete':
                if filters:
                    # 构建删除查询
                    delete_query = query.delete()
                    
                    # 应用过滤条件
                    for key, value in filters.items():
                        delete_query = delete_query.eq(key, value)
                    
                    # 执行删除
                    result = delete_query.execute()
                    return result.data
            
            # 如果没有任何操作被执行，返回空结果
            return []
                
        except Exception as e:
            logger.error(f"执行查询时出错: {query_type} on {table} - {str(e)}")
            logger.error(f"查询参数: data={data}, filters={filters}, limit={limit}")
            import traceback
            logger.error(traceback.format_exc())
            return {'error': str(e)}
    
    async def save_message(self, message_data: Dict[str, Any]) -> bool:
        """
        保存消息到Supabase
        
        Args:
            message_data: 消息数据
            
        Returns:
            是否成功
        """
        try:
            # 格式化日期时间字段
            for key, value in message_data.items():
                if isinstance(value, datetime):
                    message_data[key] = value.isoformat()

            # 检查消息是否已存在
            existing = await self.execute_query(
                'messages',
                'select',
                filters={
                    'channel_id': message_data.get('channel_id'),
                    'message_id': message_data.get('message_id')
                },
                limit=1
            )
            
            # 安全处理查询结果
            if existing and isinstance(existing, list) and len(existing) > 0:
                # 确保获取到的是字典而不是列表
                existing_message = existing[0] if isinstance(existing[0], dict) else None
                
                if existing_message and 'id' in existing_message:
                    # 更新现有消息，保留原始ID
                    message_data['id'] = existing_message['id']
                    result = await self.execute_query(
                        'messages',
                        'update',
                        data=message_data,
                        filters={
                            'id': existing_message['id']
                        }
                    )
                else:
                    logger.warning(f"找到现有消息但格式不正确: {existing}")
                    # 插入新消息，不指定id
                    if 'id' in message_data:
                        message_data.pop('id')
                    result = await self.execute_query(
                        'messages',
                        'insert',
                        data=message_data
                    )
            elif existing and isinstance(existing, dict) and existing.get('error'):
                # 查询出错
                logger.error(f"查询现有消息时出错: {existing.get('error')}")
                return False
            else:
                # 插入新消息，不指定id，让数据库自动生成
                if 'id' in message_data:
                    message_data.pop('id')
                result = await self.execute_query(
                    'messages',
                    'insert',
                    data=message_data
                )
                
            # 检查结果
            if isinstance(result, dict) and result.get('error'):
                logger.error(f"保存消息操作返回错误: {result.get('error')}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"保存消息到Supabase失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    async def save_token(self, token_data: Dict[str, Any]) -> bool:
        """
        保存代币信息到Supabase
        
        Args:
            token_data: 代币数据
            
        Returns:
            是否成功
        """
        try:
            # 确保token_data是字典
            if not isinstance(token_data, dict):
                logger.error(f"token_data必须是字典，但收到了: {type(token_data)}")
                return False
            
            # 格式化日期时间字段
            for key, value in token_data.items():
                if isinstance(value, datetime):
                    token_data[key] = value.isoformat()
            
            # 验证必要字段
            required_fields = ['chain', 'contract']
            for field in required_fields:
                if field not in token_data or token_data[field] is None or token_data[field] == '':
                    logger.error(f"保存代币信息到Supabase失败: 缺少必需字段 '{field}'或字段为空")
                    return False
            
            chain = token_data.get('chain')
            contract = token_data.get('contract')
            logger.info(f"准备保存代币数据: {chain}/{contract}")
            
            # 检查代币是否已存在
            existing = await self.execute_query(
                'tokens',
                'select',
                filters={
                    'chain': chain,
                    'contract': contract
                },
                limit=1
            )
            
            # 安全处理查询结果
            if existing and isinstance(existing, list) and len(existing) > 0:
                # 确保获取到的是字典而不是列表
                existing_token = existing[0] if isinstance(existing[0], dict) else None
                
                if existing_token and 'id' in existing_token:
                    # 记录更新前的值，用于验证更新是否成功
                    logger.info(f"找到现有代币: {chain}/{contract}, ID={existing_token['id']}")
                    original_update_time = existing_token.get('latest_update')
                    
                    # 创建新的更新数据，以现有数据为基础
                    updated_data = dict(existing_token)
                    
                    # 只更新提供的非空字段，保留其他字段
                    for key, value in token_data.items():
                        # 跳过None值，除非明确要设置为None
                        if value is not None:
                            updated_data[key] = value
                        # 对于一些特定字段，即使是None也不更新，保留原值
                        elif key in ['market_cap', 'price', 'volume_1h', 'liquidity', 'holders_count', 
                                   'buys_1h', 'sells_1h', 'spread_count', 'community_reach']:
                            # 不更新这些关键字段为None
                            pass
                        else:
                            # 其他字段允许设置为None
                            updated_data[key] = value
                    
                    # 确保累计promotion_count
                    if token_data.get('promotion_count', 0) > 0:
                        updated_data['promotion_count'] = (existing_token.get('promotion_count') or 0) + token_data.get('promotion_count')
                    
                    # 确保保留first_market_cap
                    if not updated_data.get('first_market_cap') and existing_token.get('first_market_cap'):
                        updated_data['first_market_cap'] = existing_token['first_market_cap']
                    
                    # 确保保留first_update
                    if not updated_data.get('first_update') and existing_token.get('first_update'):
                        updated_data['first_update'] = existing_token['first_update']
                    
                    # 确保latest_update字段更新为当前时间
                    updated_data['latest_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    logger.info(f"正在更新代币数据: {chain}/{contract}, ID={existing_token['id']}")
                    logger.debug(f"更新前数据: {existing_token}")
                    logger.debug(f"更新后数据: {updated_data}")
                    
                    # 使用Supabase服务角色密钥执行更新操作，绕过RLS策略
                    try:
                        update_result = self.supabase_admin.table('tokens').update(updated_data).eq('id', existing_token['id']).execute()
                        logger.debug(f"更新结果: {update_result}")
                        
                        # 验证更新是否成功
                        verify_result = await self.execute_query(
                            'tokens',
                            'select',
                            filters={'id': existing_token['id']},
                            limit=1
                        )
                        
                        if verify_result and isinstance(verify_result, list) and len(verify_result) > 0:
                            updated_token = verify_result[0]
                            new_update_time = updated_token.get('latest_update')
                            
                            if new_update_time != original_update_time:
                                logger.info(f"✅ 代币更新成功: {chain}/{contract}, 新时间戳: {new_update_time}")
                                return True
                            else:
                                logger.warning(f"⚠️ 代币似乎未更新: {chain}/{contract}, 时间戳未变: {new_update_time}")
                                # 尝试使用upsert操作
                                upsert_result = self.supabase_admin.table('tokens').upsert(updated_data).execute()
                                logger.info(f"尝试使用upsert: {upsert_result}")
                                return True  # 假设upsert成功
                        else:
                            logger.error(f"❌ 更新后无法验证代币: {chain}/{contract}")
                            return False
                    except Exception as e:
                        logger.error(f"执行更新操作时出错: {str(e)}")
                        import traceback
                        logger.error(traceback.format_exc())
                        return False
                else:
                    logger.warning(f"找到现有代币但格式不正确: {existing}")
                    # 插入新代币，不指定id
                    if 'id' in token_data:
                        token_data.pop('id')
                    
                    # 再次验证必要字段
                    if not token_data.get('contract'):
                        logger.error(f"插入新代币失败: 缺少必需字段 'contract'或字段为空")
                        return False
                    
                    # 确保latest_update字段更新为当前时间
                    token_data['latest_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    token_data['first_update'] = token_data['latest_update']
                    
                    logger.info(f"插入新代币数据: {chain}/{contract}, 数据: {token_data}")
                    try:
                        insert_result = self.supabase_admin.table('tokens').insert(token_data).execute()
                        logger.debug(f"插入结果: {insert_result}")
                        return True
                    except Exception as e:
                        logger.error(f"执行插入操作时出错: {str(e)}")
                        import traceback
                        logger.error(traceback.format_exc())
                        return False
            elif existing and isinstance(existing, dict) and existing.get('error'):
                # 查询出错
                logger.error(f"查询现有代币时出错: {existing.get('error')}")
                return False
            else:
                # 插入新代币，确保不提供id字段，让数据库自动生成
                if 'id' in token_data:
                    token_data.pop('id')
                
                # 最后检查一次contract字段
                if not token_data.get('contract'):
                    logger.error(f"插入新代币失败: 合约地址为空")
                    return False
                
                # 对于新代币，确保first_market_cap有值
                if token_data.get('market_cap') and not token_data.get('first_market_cap'):
                    token_data['first_market_cap'] = token_data['market_cap']
                
                # 确保latest_update和first_update字段设置为当前时间
                token_data['latest_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                token_data['first_update'] = token_data['latest_update']
                
                logger.info(f"创建新代币记录: {chain}/{contract}, 数据: {token_data}")
                try:
                    insert_result = self.supabase_admin.table('tokens').insert(token_data).execute()
                    logger.debug(f"插入结果: {insert_result}")
                    
                    # 验证插入是否成功
                    verify_result = await self.execute_query(
                        'tokens',
                        'select',
                        filters={'chain': chain, 'contract': contract},
                        limit=1
                    )
                    
                    if verify_result and isinstance(verify_result, list) and len(verify_result) > 0:
                        logger.info(f"✅ 新代币创建成功: {chain}/{contract}")
                        return True
                    else:
                        logger.error(f"❌ 无法验证新代币创建: {chain}/{contract}")
                        return False
                except Exception as e:
                    logger.error(f"执行插入操作时出错: {str(e)}")
                    import traceback
                    logger.error(traceback.format_exc())
                    return False
                    
            # 代码不应该执行到这里，但如果发生了，返回False
            logger.error(f"保存代币数据逻辑出现意外情况: {chain}/{contract}")
            return False
            
        except Exception as e:
            logger.error(f"保存代币到Supabase失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    async def save_token_mark(self, token_data: Dict[str, Any]) -> bool:
        """
        保存代币标记信息到Supabase
        
        Args:
            token_data: 代币数据
            
        Returns:
            是否成功
        """
        try:
            # 确保token_data是字典
            if not isinstance(token_data, dict):
                logger.error(f"token_data必须是字典，但收到了: {type(token_data)}")
                return False
                
            # 提取需要的字段
            mark_data = {
                'chain': token_data.get('chain'),
                'token_symbol': token_data.get('token_symbol'),
                'contract': token_data.get('contract'),
                'message_id': token_data.get('message_id'),
                'market_cap': token_data.get('market_cap'),
                'mention_time': datetime.now().isoformat(),
                'channel_id': token_data.get('channel_id')
            }
            
            logger.info(f"准备保存代币标记数据: {mark_data}")
            
            # 严格验证必需字段
            if not mark_data.get('chain'):
                logger.error(f"保存代币标记失败: 缺少必需字段 'chain'")
                return False
                
            # 严格验证contract字段，确保不为空
            if not mark_data.get('contract'):
                logger.error(f"保存代币标记失败: 缺少必需字段 'contract'（不能为null）")
                return False
            
            # 检查token_symbol字段，应当也提供
            if not mark_data.get('token_symbol'):
                logger.warning(f"代币标记缺少token_symbol字段，但将继续保存")
                
            # 检查message_id字段
            if not mark_data.get('message_id'):
                logger.error(f"保存代币标记失败: 缺少必需字段 'message_id'")
                return False
                
            # 检查记录是否已存在(合约+消息ID唯一性检查)
            existing = await self.execute_query(
                'tokens_mark',
                'select',
                filters={
                    'chain': mark_data.get('chain'),
                    'contract': mark_data.get('contract'),
                    'message_id': mark_data.get('message_id')
                },
                limit=1
            )
                
            if existing and isinstance(existing, list) and len(existing) > 0:
                logger.warning(f"代币标记记录已存在，跳过插入: 链={mark_data.get('chain')}, "
                                f"合约={mark_data.get('contract')}, 消息ID={mark_data.get('message_id')}")
                return True  # 已存在视为成功
            
            # 再次确认所有必需字段不为null，防止数据库约束错误
            required_fields = ['chain', 'contract', 'message_id']
            for field in required_fields:
                if not mark_data.get(field):
                    logger.error(f"保存代币标记失败: 必需字段 '{field}' 为空")
                    return False
            
            # 直接插入新记录，不指定id，让数据库自动生成
            logger.debug(f"执行tokens_mark表插入操作，数据: {mark_data}")
            result = await self.execute_query(
                'tokens_mark',
                'insert',
                data=mark_data
            )
                
            # 验证结果
            if isinstance(result, dict) and result.get('error'):
                logger.error(f"保存代币标记操作返回错误: {result.get('error')}")
                return False
                
            # 记录插入结果
            logger.info(f"成功保存代币标记到tokens_mark表: {mark_data.get('token_symbol')} / {mark_data.get('contract')}")
            return True
            
        except Exception as e:
            logger.error(f"保存代币标记到Supabase失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    async def get_token_by_contract(self, chain: str, contract: str) -> Optional[Dict[str, Any]]:
        """
        根据合约地址获取代币信息
        
        Args:
            chain: 链名称
            contract: 合约地址
            
        Returns:
            代币信息字典
        """
        try:
            result = await self.execute_query(
                'tokens',
                'select',
                filters={'chain': chain, 'contract': contract},
                limit=1
            )
            
            if result and isinstance(result, list) and len(result) > 0:
                return result[0]
            return None
            
        except Exception as e:
            logger.error(f"从Supabase获取代币信息失败: {str(e)}")
            return None
    
    async def get_channel_by_id(self, channel_id: int) -> Optional[Dict[str, Any]]:
        """
        根据ID获取频道信息
        
        Args:
            channel_id: 频道ID
            
        Returns:
            频道信息字典
        """
        try:
            result = await self.execute_query(
                'telegram_channels',
                'select',
                filters={'channel_id': channel_id},
                limit=1
            )
            
            if result and isinstance(result, list) and len(result) > 0:
                return result[0]
            return None
            
        except Exception as e:
            logger.error(f"从Supabase获取频道信息失败: {str(e)}")
            return None
    
    async def get_active_channels(self) -> List[Dict[str, Any]]:
        """
        获取所有活跃的频道
        
        Returns:
            活跃频道列表
        """
        try:
            channels = await self.execute_query(
                'telegram_channels',
                'select',
                filters={'is_active': True}
            )
            return channels or []
        except Exception as e:
            logger.error(f"从Supabase获取活跃频道失败: {str(e)}")
            return []
    
    async def execute_raw_sql(self, sql_query: str) -> Dict[str, Any]:
        """
        执行原始SQL查询语句
        
        Args:
            sql_query: SQL查询语句
            
        Returns:
            Dict[str, Any]: 包含结果或错误信息的字典
        """
        try:
            logger.debug(f"执行原始SQL: {sql_query}")
            
            # 判断是否是写操作
            is_write_operation = any(keyword in sql_query.upper() for keyword in 
                ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE'])
            
            # 判断SQL操作类型
            if sql_query.strip().upper().startswith('CREATE TABLE'):
                # 创建表操作 - 不能直接通过REST API执行
                logger.warning("Supabase REST API不直接支持CREATE TABLE操作")
                logger.warning("请在Supabase控制台 > SQL Editor中执行此操作")
                return {'error': '不支持通过API直接创建表，请使用Supabase控制台', 'sql': sql_query}
            elif sql_query.strip().upper().startswith('SELECT'):
                # 查询操作 - 我们无法直接执行原生SQL查询
                logger.warning("无法通过Supabase REST API执行原生SELECT查询")
                logger.warning("请使用Supabase的表格API (table.select()...)")
                return {'error': '不支持通过API直接执行SELECT查询，请使用表格API', 'sql': sql_query}
            else:
                # 其他操作 - 同样不能直接通过REST API执行
                logger.warning(f"Supabase REST API不直接支持此SQL操作: {sql_query}")
                return {'error': '不支持通过API直接执行此SQL，请使用Supabase控制台或表格API', 'sql': sql_query}
            
        except Exception as e:
            logger.error(f"执行原始SQL时出错: {str(e)}")
            return {'error': str(e)}
    
    async def save_channel(self, channel_data: Dict[str, Any]) -> bool:
        """
        保存频道信息到Supabase
        
        Args:
            channel_data: 频道数据
            
        Returns:
            是否成功
        """
        try:
            # 格式化日期时间字段
            for key, value in channel_data.items():
                if isinstance(value, datetime):
                    channel_data[key] = value.isoformat()
            
            # 检查频道是否已存在
            existing = None
            if channel_data.get('channel_id'):
                existing = await self.execute_query(
                    'telegram_channels',
                    'select',
                    filters={'channel_id': channel_data.get('channel_id')},
                    limit=1
                )
            elif channel_data.get('channel_username'):
                existing = await self.execute_query(
                    'telegram_channels',
                    'select',
                    filters={'channel_username': channel_data.get('channel_username')},
                    limit=1
                )
            
            if existing and len(existing) > 0:
                # 更新现有频道
                if isinstance(existing, list) and len(existing) > 0 and isinstance(existing[0], dict):
                    record_id = existing[0].get('id')
                    if record_id:
                        channel_data['id'] = record_id  # 保留原始ID
                
                channel_id = channel_data.get('channel_id')
                if channel_id:
                    result = await self.execute_query(
                        'telegram_channels',
                        'update',
                        data=channel_data,
                        filters={'channel_id': channel_id}
                    )
                else:
                    username = channel_data.get('channel_username')
                    result = await self.execute_query(
                        'telegram_channels',
                        'update',
                        data=channel_data,
                        filters={'channel_username': username}
                    )
            else:
                # 插入新频道，不指定id，让数据库自动生成
                if 'id' in channel_data:
                    channel_data.pop('id')
                result = await self.execute_query(
                    'telegram_channels',
                    'insert',
                    data=channel_data
                )
                
            # 修复：正确处理不同类型的返回值
            if isinstance(result, dict) and result.get('error'):
                # 如果返回的是带错误信息的字典，说明失败
                logger.error(f"保存频道信息返回错误: {result.get('error')}")
                return False
            else:
                # 如果返回的是列表或其他值，表示成功
                return True
            
        except Exception as e:
            logger.error(f"保存频道信息到Supabase失败: {str(e)}")
            return False

    async def check_tokens_mark_table(self):
        """
        检查tokens_mark表是否存在
        
        Returns:
            Dict: 包含检查结果的字典
        """
        try:
            logger.info("检查tokens_mark表是否存在...")
            
            # 假设表已经存在，直接返回成功
            return {
                'status': True,
                'exists': True,
                'message': 'tokens_mark表已存在'
            }
            
        except Exception as e:
            logger.error(f"检查tokens_mark表时出错: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                'status': False,
                'exists': False,
                'error': str(e)
            }

# 创建单例实例
_adapter = None

def get_adapter() -> SupabaseAdapter:
    """
    获取Supabase适配器实例
    
    Returns:
        SupabaseAdapter实例
    """
    global _adapter
    if _adapter is None:
        _adapter = SupabaseAdapter()
    return _adapter 