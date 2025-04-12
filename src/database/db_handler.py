#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库操作处理模块
仅支持Supabase数据库
"""

import asyncio
import traceback
import time
import functools
import json
import os
import re
import inspect
from typing import Tuple, Optional, List, Dict, Any, Callable
from datetime import datetime, timezone, timedelta
from contextlib import contextmanager

# 导入数据库工厂（已经移除SQLAlchemy会话）
from src.database.db_factory import get_db_adapter
# 导入必要的数据模型
from src.database.models import PromotionInfo

# 添加日志支持
try:
    from src.utils.logger import get_logger
    logger = get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

# 导入代币分析器
try:
    from src.analysis.token_analyzer import get_analyzer
    token_analyzer = get_analyzer()
    HAS_ANALYZER = True
except ImportError:
    logger.warning("无法导入代币分析器，将使用基本分析")
    token_analyzer = None
    HAS_ANALYZER = False

# 批处理消息队列
message_batch = []
token_batch = []

# 从配置文件或环境变量中获取批处理设置
try:
    from config.settings import BATCH_SIZE, BATCH_INTERVAL
    MAX_BATCH_SIZE = BATCH_SIZE if hasattr(BATCH_SIZE, '__int__') else 50
    BATCH_TIMEOUT = BATCH_INTERVAL if hasattr(BATCH_INTERVAL, '__int__') else 10
except (ImportError, AttributeError):
    # 默认值
    MAX_BATCH_SIZE = 50
    BATCH_TIMEOUT = 10  # 秒

# 重试设置
OPERATION_RETRIES = 5  # 重试次数
OPERATION_RETRY_DELAY = 1.0  # 重试间隔(秒)

# 添加数据库性能监控相关的变量
db_performance_stats = {
    'operation_counts': {},
    'operation_times': {},
    'lock_errors': 0,
    'total_retries': 0
}

@contextmanager
def session_scope():
    """提供事务范围的会话上下文管理器
    
    注意：此函数仅作为兼容层。在Supabase中没有事务的概念，
    而是直接使用adapter进行操作。
    """
    # 导入全局配置
    import config.settings as config
    
    # 检查是否使用Supabase
    if not config.DATABASE_URI.startswith('supabase://'):
        logger.error("未使用Supabase数据库，请检查配置")
        logger.error(f"当前DATABASE_URI: {config.DATABASE_URI}")
        logger.error("DATABASE_URI应以'supabase://'开头")
        raise ValueError("必须使用Supabase数据库")
        
    try:
        # 使用Supabase适配器，不再创建SQLAlchemy会话
        from src.database.db_factory import get_db_adapter
        adapter = get_db_adapter()
        logger.debug("使用Supabase适配器创建会话")
        
        # 返回适配器实例而不是会话对象
        yield adapter
        
    except Exception as e:
        # 发生错误时记录但不再进行回滚（Supabase没有会话概念）
        logger.error(f"Supabase操作出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        # 重新抛出异常
        raise e

async def process_batches():
    """定期处理批处理队列的消息和代币"""
    global message_batch, token_batch
    
    while True:
        try:
            if message_batch:
                local_batch = message_batch.copy()
                message_batch = []
                
                try:
                    # 使用Supabase适配器处理批量消息
                    from src.database.db_factory import get_db_adapter
                    db_adapter = get_db_adapter()
                    
                    for msg_data in local_batch:
                        try:
                            # 构建消息数据
                            message_data = {
                                'chain': msg_data.get('chain'),
                                'message_id': msg_data.get('message_id'),
                                'date': msg_data.get('date'),
                                'text': msg_data.get('text'),
                                'media_path': msg_data.get('media_path'),
                                'channel_id': msg_data.get('channel_id')
                            }
                            
                            # 使用Supabase适配器保存消息
                            await db_adapter.save_message(message_data)
                        except Exception as e:
                            logger.error(f"处理消息批次时出错: {str(e)}")
                            logger.debug(traceback.format_exc())
                            continue
                    
                    logger.info(f"批量处理了 {len(local_batch)} 条消息")
                except Exception as e:
                    logger.error(f"消息批处理失败: {str(e)}")
                    import traceback
                    logger.error(traceback.format_exc())
                
            if token_batch:
                local_batch = token_batch.copy()
                token_batch = []
                
                try:
                    # 使用Supabase适配器处理批量代币信息
                    from src.database.db_factory import get_db_adapter
                    db_adapter = get_db_adapter()
                    
                    for token_data in local_batch:
                        try:
                            # 简单验证代币数据
                            if not all(key in token_data for key in ['chain', 'token_symbol', 'contract']):
                                logger.warning(f"无效的代币数据: 缺少必要字段，数据: {token_data}")
                                continue
                                
                            # 使用Supabase适配器保存代币信息
                            await db_adapter.save_token(token_data)
                        except Exception as e:
                            logger.error(f"处理代币批次时出错: {str(e)}")
                            logger.debug(traceback.format_exc())
                            continue
                    
                    logger.info(f"批量处理了 {len(local_batch)} 条代币信息")
                except Exception as e:
                    logger.error(f"代币批处理失败: {str(e)}")
                    import traceback
                    logger.error(traceback.format_exc())
                
            await asyncio.sleep(BATCH_TIMEOUT)
        except Exception as e:
            logger.error(f"批处理过程中出错: {str(e)}")
            logger.debug(traceback.format_exc())
            await asyncio.sleep(5)  # 出错后等待短暂时间再继续

def monitor_db_operation(operation_name):
    """装饰器函数：监控数据库操作性能
    
    Args:
        operation_name: 操作名称，用于统计
        
    Returns:
        装饰器函数
    """
    def decorator(func):
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                return func(*args, **kwargs)
            finally:
                execution_time = time.time() - start_time
                # 更新统计信息
                if operation_name not in db_performance_stats['operation_counts']:
                    db_performance_stats['operation_counts'][operation_name] = 0
                    db_performance_stats['operation_times'][operation_name] = 0
                
                db_performance_stats['operation_counts'][operation_name] += 1
                db_performance_stats['operation_times'][operation_name] += execution_time
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                return await func(*args, **kwargs)
            finally:
                execution_time = time.time() - start_time
                # 更新统计信息
                if operation_name not in db_performance_stats['operation_counts']:
                    db_performance_stats['operation_counts'][operation_name] = 0
                    db_performance_stats['operation_times'][operation_name] = 0
                
                db_performance_stats['operation_counts'][operation_name] += 1
                db_performance_stats['operation_times'][operation_name] += execution_time
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator

@monitor_db_operation('save_messages_batch')
async def save_messages_batch(messages: List[Dict]):
    """批量保存消息到数据库
    
    Args:
        messages: 消息字典列表，每个字典包含消息的所有必要字段
    
    Returns:
        int: 成功保存的消息数量
    """
    if not messages:
        return 0
        
    try:
        # 使用数据库适配器
        from src.database.db_factory import get_db_adapter
        db_adapter = get_db_adapter()
        
        # 使用适配器批量保存消息
        successful = 0
        for msg in messages:
            # 初始检查，确保必须的字段存在
            if not all(key in msg for key in ['chain', 'message_id', 'date']):
                logger.warning(f"消息缺少必要字段: {msg}")
                continue
                
            # 准备消息数据
            message_data = {
                'chain': msg['chain'],
                'message_id': msg['message_id'],
                'date': msg['date'].isoformat() if isinstance(msg['date'], datetime) else msg['date'],
                'text': msg.get('text'),
                'media_path': msg.get('media_path'),
                'channel_id': msg.get('channel_id')
            }
            
            # 保存消息
            result = await db_adapter.save_message(message_data)
            if result:
                successful += 1
                
        # 返回成功添加的数量
        return successful
    except Exception as e:
        logger.error(f"批量保存消息失败: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        return 0

async def save_messages_individually(messages: List[Dict]):
    """当批量保存失败时，尝试逐个保存消息
    
    Args:
        messages: 消息数据列表
    """
    successful = 0
    for msg_data in messages:
        try:
            # 使用已有的保存函数
            if save_telegram_message(
                chain=msg_data['chain'],
                message_id=msg_data['message_id'],
                date=msg_data['date'],
                text=msg_data['text'],
                media_path=msg_data.get('media_path'),
                channel_id=msg_data.get('channel_id')
            ):
                successful += 1
        except Exception as individual_error:
            logger.error(f"单独保存消息 {msg_data['message_id']} 时出错: {individual_error}")
    
    logger.info(f"逐个保存: 成功 {successful}/{len(messages)} 条消息")

def save_telegram_message(
    chain: str,
    message_id: int,
    date: datetime,
    text: str,
    media_path: Optional[str] = None,
    channel_id: Optional[int] = None
) -> bool:
    """保存Telegram消息到数据库
    
    Args:
        chain: 区块链名称
        message_id: 消息ID
        date: 消息日期
        text: 消息文本
        media_path: 媒体文件路径
        channel_id: 频道或群组ID
        
    Returns:
        bool: 操作是否成功
    """
    # 如果大批量处理队列已开启，将消息添加到队列
    global message_batch
    if MAX_BATCH_SIZE > 0:
        message_batch.append({
            'chain': chain,
            'message_id': message_id,
            'date': date,
            'text': text,
            'media_path': media_path,
            'channel_id': channel_id
        })
        # 如果队列达到最大值，立即处理
        if len(message_batch) >= MAX_BATCH_SIZE:
            asyncio.create_task(process_message_batch())
        return True
    
    try:
        # 使用数据库适配器
        from src.database.db_factory import get_db_adapter
        db_adapter = get_db_adapter()
        
        # 准备消息数据
        message_data = {
            'chain': chain,
            'message_id': message_id,
            'date': date.isoformat() if isinstance(date, datetime) else date,
            'text': text,
            'media_path': media_path,
            'channel_id': channel_id
        }
        
        # 使用异步方式保存消息
        # 创建新的事件循环执行异步操作
        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(db_adapter.save_message(message_data))
            return result
        finally:
            loop.close()
    except Exception as e:
        logger.error(f"保存消息时出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        return False

async def process_message_batch():
    """处理消息批处理队列"""
    global message_batch
    
    if not message_batch:
        return
        
    # 复制当前队列，并清空全局队列
    current_batch = message_batch.copy()
    message_batch = []
    
    logger.info(f"处理消息批处理队列，共 {len(current_batch)} 条消息")
    
    try:
        saved_count = await save_messages_batch(current_batch)
        logger.info(f"成功批量保存 {saved_count}/{len(current_batch)} 条消息")
        
        # 如果批量保存失败，尝试逐个保存
        if saved_count < len(current_batch):
            logger.warning("批量保存部分失败，尝试逐个保存剩余消息")
            await save_messages_individually(current_batch)
    except Exception as e:
        logger.error(f"处理消息批处理时出错: {str(e)}")
        # 出错时尝试逐个保存
        try:
            await save_messages_individually(current_batch)
        except Exception as e2:
            logger.error(f"逐个保存消息时也出错: {str(e2)}")
            import traceback
            logger.debug(traceback.format_exc())

def save_tokens_batch(tokens: List[Dict]):
    """批量保存代币信息到数据库
    
    警告：此函数已被废弃，不应再使用。
    此函数原本用于批量处理代币信息，但现在已由process_batches函数替代。
    在代码中直接使用process_batches处理token_batch变量，而不要调用此函数。
    
    仅为了保持向后兼容性而保留。将在下一次主要版本更新中彻底移除。
    
    Args:
        tokens: 代币信息列表
        
    Returns:
        更新的代币数量
    """
    logger.warning("调用了废弃的save_tokens_batch函数，请改用process_batches处理token_batch")
    
    if not tokens:
        return 0
    
    # 使用数据库适配器
    from src.database.db_factory import get_db_adapter
    db_adapter = get_db_adapter()
    
    # 使用重试机制
    for attempt in range(OPERATION_RETRIES):
        try:
            # 处理每个代币信息
            updated_count = 0
            for token_data in tokens:
                token_symbol = token_data.get('token_symbol')
                chain = token_data.get('chain')
                contract = token_data.get('contract')
                
                if not token_symbol or not chain:
                    logger.warning(f"跳过无效的代币数据: 缺少token_symbol或chain")
                    continue
                
                # 标准化风险等级值
                if 'risk_level' in token_data:
                    risk_level = token_data['risk_level']
                    # 确保风险等级是有效值
                    if risk_level not in ['low', 'medium', 'high', 'medium-high', 'low-medium', 'unknown']:
                        # 处理中文风险等级，统一转为英文
                        if risk_level == '低':
                            token_data['risk_level'] = 'low'
                        elif risk_level == '中':
                            token_data['risk_level'] = 'medium'
                        elif risk_level == '高':
                            token_data['risk_level'] = 'high'
                        elif not risk_level:
                            token_data['risk_level'] = 'unknown'
                
                # 处理日期时间类型
                for key, value in token_data.items():
                    if isinstance(value, datetime):
                        token_data[key] = value.isoformat()
                
                # 使用事件循环执行异步保存方法
                loop = asyncio.new_event_loop()
                try:
                    result = loop.run_until_complete(db_adapter.save_token(token_data))
                    if result:
                        updated_count += 1
                        
                    # 如果有contract字段，保存token mark信息
                    if contract:
                        token_exists = loop.run_until_complete(db_adapter.get_token_by_contract(chain, contract))
                        if token_exists:
                            # 保存代币标记
                            loop.run_until_complete(db_adapter.save_token_mark(token_data))
                finally:
                    loop.close()
            
            logger.debug(f"更新/添加了 {updated_count} 条代币信息")
            return updated_count
            
        except Exception as e:
            logger.error(f"批量保存代币信息时出错(尝试 {attempt+1}/{OPERATION_RETRIES}): {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
            
            # 如果还有重试次数，等待后重试
            if attempt < OPERATION_RETRIES - 1:
                time.sleep(OPERATION_RETRY_DELAY * (attempt + 1))  # 指数退避
            else:
                logger.error(f"批量保存代币信息失败，达到最大重试次数")
                return 0

def save_token_info(token_data: Dict[str, Any]) -> bool:
    """保存代币信息
    
    Args:
        token_data: 代币数据字典
        
    Returns:
        bool: 是否成功
    """
    # 增强数据验证
    if not isinstance(token_data, dict):
        logger.error(f"token_data必须是字典，但收到了: {type(token_data)}")
        return False
        
    # 验证必需字段
    required_fields = ['chain', 'token_symbol']
    for field in required_fields:
        if field not in token_data or not token_data[field]:
            logger.error(f"保存代币信息失败: 缺少必需字段 '{field}'")
            return False
            
    # 特别验证contract字段 - 这是一个关键字段
    if 'contract' not in token_data or not token_data['contract']:
        logger.error(f"保存代币信息失败: 缺少必需字段 'contract'（不能为null）")
        return False
        
    # 常规验证
    valid, message = validate_token_data(token_data)
    if not valid:
        logger.warning(f"代币数据验证失败: {message}")
        return False
        
    # 使用数据库适配器
    from src.database.db_factory import get_db_adapter
    db_adapter = get_db_adapter()
    
    try:
        # 处理日期时间类型
        for key, value in token_data.items():
            if isinstance(value, datetime):
                token_data[key] = value.isoformat()
        
        # 使用事件循环执行异步保存方法
        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(db_adapter.save_token(token_data))
            return result
        finally:
            loop.close()
    except Exception as e:
        logger.error(f"保存代币信息时出错: {str(e)}")
        logger.debug(f"问题数据: {token_data}")
        import traceback
        logger.debug(traceback.format_exc())
        return False

def extract_promotion_info(message_text: str, date: datetime, chain: str = None, message_id: int = None, channel_id: int = None) -> Optional[PromotionInfo]:
    """从消息文本中提取合约地址信息
    
    专注于提取合约地址，其他信息通过DEX API获取
    
    Args:
        message_text: 消息文本
        date: 消息日期
        chain: 可选的链名称
        message_id: 消息ID
        channel_id: 频道ID
        
    Returns:
        PromotionInfo: 包含合约地址和链信息的数据对象，如果未提取到则返回None
    """
    
    try:
        # 清理消息文本
        cleaned_text = re.sub(r'\s+', ' ', message_text)
        cleaned_text = re.sub(r'[\u200b\u200c\u200d\ufeff]', '', cleaned_text)  # 移除零宽字符
        
        # 如果未提供链信息，尝试从消息中提取
        if not chain or chain == "UNKNOWN":
            # 先尝试检测市值单位，这是最可靠的链标识
            mc_pattern = re.search(r'(\bmc\b|\bmarket\s*cap\b|市值)[：:]*\s*[`\'"]*\d+(?:\.\d+)?\s*(?:bnb|BNB)', cleaned_text, re.IGNORECASE)
            if mc_pattern:
                logger.info(f"从市值单位(BNB)判断为BSC链")
                chain = 'BSC'
            else:
                # 不是BSC，尝试其他链的提取
                chain_from_message = extract_chain_from_message(message_text)
                if chain_from_message:
                    logger.info(f"从消息中提取到链信息: {chain_from_message}")
                    chain = chain_from_message
        
        # 提取代币符号
        # 寻找常见格式的代币符号，如$XYZ
        token_symbol = None
        symbol_match = re.search(r'\$([A-Za-z0-9_]{1,20})\b', cleaned_text)
        if symbol_match:
            token_symbol = symbol_match.group(1).upper()
            logger.info(f"从消息中提取到代币符号: {token_symbol}")
        
        # 专注于提取合约地址
        contract_address = None
        
        # 使用增强的合约地址提取模式
        contract_patterns = [
            # 带标记的合约地址
            r'(?:📝|合约[：:]|[Cc]ontract[：:])[ ]*([0-9a-fA-FxX]{8,})',
            r'合约地址[：:][ ]*([0-9a-fA-FxX]{8,})',
            r'地址[：:][ ]*([0-9a-fA-FxX]{8,})',
            # 标准以太坊地址格式
            r'\b(0x[0-9a-fA-F]{40})\b',
            # 其他可能的合约地址格式
            r'\b([a-zA-Z0-9]{32,50})\b'
        ]
        
        # 尝试所有模式提取合约地址
        for pattern in contract_patterns:
            match = re.search(pattern, cleaned_text)
            if match:
                potential_address = match.group(1) if '(' in pattern else match.group(0)
                logger.info(f"从消息中提取到潜在合约地址: {potential_address}")
                
                # 验证地址格式
                if re.match(r'^0x[a-fA-F0-9]{40}$', potential_address):
                    contract_address = potential_address
                    if not chain or chain == "UNKNOWN":
                        # 检查是否有明确的链指示器
                        if re.search(r'(\bmc\b|\bmarket\s*cap\b|市值)[：:]*\s*[`\'"]*\d+(?:\.\d+)?\s*(?:bnb|BNB)', cleaned_text, re.IGNORECASE):
                            logger.info("从市值单位(BNB)判断为BSC链")
                            chain = 'BSC'
                        elif re.search(r'(\bmc\b|\bmarket\s*cap\b|市值)[：:]*\s*[`\'"]*\d+(?:\.\d+)?\s*(?:eth|ETH)', cleaned_text, re.IGNORECASE):
                            logger.info("从市值单位(ETH)判断为ETH链")
                            chain = 'ETH'
                        elif 'bsc' in cleaned_text.lower() or 'bnb' in cleaned_text.lower() or 'pancake' in cleaned_text.lower() or 'binance' in cleaned_text.lower():
                            logger.info("从上下文关键词判断为BSC链")
                            chain = 'BSC'
                        elif 'eth' in cleaned_text.lower() or 'ethereum' in cleaned_text.lower() or 'uniswap' in cleaned_text.lower():
                            logger.info("从上下文关键词判断为ETH链")
                            chain = 'ETH'
                        else:
                            logger.info("检测到EVM类地址，但无法确定具体链，尝试通过DEX API确定具体链")
                    break
                elif re.match(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$', potential_address):
                    contract_address = potential_address
                    if not chain or chain == "UNKNOWN":
                        logger.info("检测到类似SOL的地址，设置链为SOL")
                        chain = "SOL"
                    break
                elif potential_address.startswith('0x'):
                    # 尝试修正不完整的EVM地址
                    full_address = re.search(r'0x[0-9a-fA-F]{40}', cleaned_text)
                    if full_address:
                        contract_address = full_address.group(0)
                        break
        
        # 如果未找到合约地址，尝试从URL中提取
        if not contract_address:
            urls = re.findall(r'https?://[^\s<>"]+|www\.[^\s<>"]+', cleaned_text)
            for url in urls:
                url_clean = url.strip()
                # 处理URL末尾可能的标点符号
                for marker in [' ', '\n', '\t', ',', ')', ']', '}', '"', "'", '。', '，', '：', '；']:
                    if marker in url_clean:
                        url_clean = url_clean.split(marker)[0]
                
                contract_from_url, chain_from_url = extract_contract_from_url(url_clean)
                if contract_from_url:
                    contract_address = contract_from_url
                    if chain_from_url and (not chain or chain == "UNKNOWN"):
                        chain = chain_from_url
                    logger.info(f"从URL提取到合约地址: {contract_address}, 链: {chain}")
                    break
        
        # 如果找到了合约地址，创建并返回PromotionInfo对象
        if contract_address:
            # 如果在此阶段仍然没有确定链，并且是EVM地址，再尝试一次从消息上下文推断
            if (not chain or chain == "UNKNOWN") and contract_address.startswith('0x'):
                chain_from_context = extract_chain_from_message(message_text)
                if chain_from_context:
                    chain = chain_from_context
                    logger.info(f"从上下文推断合约地址 {contract_address} 所在链为: {chain}")
                else:
                    # 仍然无法确定链，分析上下文中是否有明确的BSC/ETH关键词
                    text_lower = cleaned_text.lower()
                    if 'bsc' in text_lower or 'bnb' in text_lower or 'pancake' in text_lower or 'binance' in text_lower:
                        chain = 'BSC'
                        logger.info(f"从关键词判断合约地址 {contract_address} 所在链为BSC")
                    elif 'eth' in text_lower or 'ethereum' in text_lower or 'uniswap' in text_lower:
                        chain = 'ETH'
                        logger.info(f"从关键词判断合约地址 {contract_address} 所在链为ETH")
                    else:
                        logger.warning(f"无法确定合约地址 {contract_address} 所在的链，设置为UNKNOWN")
                        chain = "UNKNOWN"
            
            logger.info(f"成功提取合约地址: {contract_address}, 链: {chain}")
            
            # 创建推广信息对象
            info = PromotionInfo(
                token_symbol=token_symbol,
                contract_address=contract_address,
                chain=chain,
                promotion_count=1,  # 默认为1，表示首次见到
                first_trending_time=date
            )
            
            # 添加新的必要字段
            info.message_id = message_id
            info.channel_id = channel_id
            
            # 尝试从消息中提取风险评级
            risk_level = None
            risk_patterns = [
                r'[Rr]isk[：:]\s*([A-Za-z]+)',
                r'风险[：:]\s*([A-Za-z高中低]+)',
                r'[Ss]afe[：:]\s*([A-Za-z]+)',
                r'安全[：:]\s*([A-Za-z是否]+)'
            ]
            
            for pattern in risk_patterns:
                match = re.search(pattern, cleaned_text)
                if match:
                    risk_text = match.group(1).strip().lower()
                    if risk_text in ['high', '高', 'high risk']:
                        risk_level = 'high'
                    elif risk_text in ['medium', 'mid', 'moderate', '中']:
                        risk_level = 'medium'
                    elif risk_text in ['low', '低', 'safe', 'yes', '是']:
                        risk_level = 'low'
                    break
                    
            info.risk_level = risk_level
            
            # 尝试从消息中提取市值信息
            from src.utils.utils import parse_market_cap
            market_cap_text = re.search(r'([Mm]arket\s*[Cc]ap|市值|[Mm][Cc])[：:]\s*[`\'"]?([^,\n]+)', cleaned_text)
            if market_cap_text:
                mc_value = market_cap_text.group(2).strip()
                try:
                    parsed_mc = parse_market_cap(mc_value)
                    if parsed_mc:
                        info.market_cap = str(parsed_mc)
                        # 第一次见到的市值就是first_market_cap
                        info.first_market_cap = parsed_mc
                        
                        # 从市值单位判断链
                        mc_lower = mc_value.lower()
                        if 'bnb' in mc_lower and (not chain or chain == "UNKNOWN"):
                            info.chain = 'BSC'
                            logger.info("从市值单位(BNB)修正链信息为BSC")
                        elif 'eth' in mc_lower and (not chain or chain == "UNKNOWN"):
                            info.chain = 'ETH'
                            logger.info("从市值单位(ETH)修正链信息为ETH")
                        elif 'sol' in mc_lower and (not chain or chain == "UNKNOWN"):
                            info.chain = 'SOL'
                            logger.info("从市值单位(SOL)修正链信息为SOL")
                except:
                    pass
            
            return info
            
        logger.info("未能从消息中提取到合约地址")
        return None
            
    except Exception as e:
        logger.error(f"提取代币信息时发生错误: {str(e)}")
        logger.debug(traceback.format_exc())
        return None

def extract_chain_from_message(message_text: str) -> Optional[str]:
    """从消息文本中提取区块链信息
    
    Args:
        message_text: 需要解析的消息文本
        
    Returns:
        str: 提取到的链名称，未找到则返回None
    """
    # 清理消息文本，便于匹配
    text = message_text.lower()
    
    # 定义不同链的关键词匹配规则
    chain_patterns = {
        'SOL': [r'\bsol\b', r'\bsolana\b', r'@solana', r'solanas', r'솔라나', r'索拉纳', 
                r'solscan\.io', r'explorer\.solana\.com', r'solana_trojanbot', r'sol链'],
        'BSC': [r'\bbsc\b', r'\bbinance smart chain\b', r'\bbnb\b', r'\bbnb chain\b', r'币安链', r'바이낸스', 
                r'bscscan\.com', r'pancakeswap', r'poocoin', r'bsc链', r'\bbnb:'],
        'ETH': [r'\beth\b', r'\bethereum\b', r'@ethereum', r'以太坊', r'이더리움', 
                r'etherscan\.io', r'uniswap', r'sushiswap', r'eth链', r'\beth:'],
        'ARB': [r'\barb\b', r'\barbitrum\b', r'arbitrums', r'阿比特龙', r'아비트럼', 
                r'arbiscan\.io', r'arb链'],
        'BASE': [r'\bbase\b', r'basechain', r'coinbase', r'贝斯链', r'베이스', 
                 r'basescan\.org', r'base链'],
        'AVAX': [r'\bavax\b', r'\bavalanche\b', r'雪崩链', r'아발란체', 
                 r'snowtrace\.io', r'traderjoe', r'avax链'],
        'MATIC': [r'\bmatic\b', r'\bpolygon\b', r'波利冈', r'폴리곤', 
                  r'polygonscan\.com', r'matic链'],
        'OP': [r'\boptimism\b', r'\bop\b', r'乐观链', r'옵티미즘', 
               r'optimistic\.etherscan\.io', r'op链']
    }
    
    # 优先检查是否明确提到市值单位为BNB，这是BSC链的最明确标志
    if re.search(r'(\bmc\b|\bmarket\s*cap\b|市值)[：:]*\s*[`\'"]*\d+(?:\.\d+)?\s*(?:bnb|BNB)', text, re.IGNORECASE):
        logger.info("从市值单位(BNB)判断为BSC链")
        return 'BSC'
    
    # 提取dexscreener URL并解析，这是比匹配简单关键词更明确的信息
    # 处理格式: dexscreener.com/solana/xxx 或 dexscreener.com/ethereum/xxx等
    dexscreener_match = re.search(r'(?:https?://)?(?:www\.)?dexscreener\.com/([a-zA-Z0-9]+)(?:/[^/\s]+)?', text)
    if dexscreener_match:
        chain_str = dexscreener_match.group(1).upper()
        # 映射DEX Screener URL路径到链标识
        dexscreener_map = {
            'SOLANA': 'SOL',
            'ETHEREUM': 'ETH',
            'BSC': 'BSC',
            'ARBITRUM': 'ARB',
            'BASE': 'BASE',
            'AVALANCHE': 'AVAX',
            'POLYGON': 'MATIC',
            'OPTIMISM': 'OP'
        }
        if chain_str in dexscreener_map:
            logger.info(f"从DEX Screener URL提取到链信息: {dexscreener_map[chain_str]}")
            return dexscreener_map[chain_str]
    
    # 处理更复杂的dexscreener URL格式，例如完整的交易对地址URL
    complex_dexscreener = re.search(r'dexscreener\.com/([a-zA-Z0-9]+)/[a-zA-Z0-9]{10,}', text, re.IGNORECASE)
    if complex_dexscreener:
        chain_str = complex_dexscreener.group(1).upper()
        dexscreener_map = {
            'SOLANA': 'SOL',
            'ETHEREUM': 'ETH',
            'BSC': 'BSC',
            'ARBITRUM': 'ARB',
            'BASE': 'BASE',
            'AVALANCHE': 'AVAX',
            'POLYGON': 'MATIC',
            'OPTIMISM': 'OP'
        }
        if chain_str in dexscreener_map:
            logger.info(f"从复杂的DEX Screener URL提取到链信息: {dexscreener_map[chain_str]}")
            return dexscreener_map[chain_str]
    
    # 检查区块浏览器链接，这也是强有力的证据
    explorer_patterns = {
        'SOL': [r'solscan\.io', r'explorer\.solana\.com'],
        'ETH': [r'etherscan\.io'],
        'BSC': [r'bscscan\.com'],
        'ARB': [r'arbiscan\.io'],
        'BASE': [r'basescan\.org'],
        'AVAX': [r'snowtrace\.io'],
        'MATIC': [r'polygonscan\.com'],
        'OP': [r'optimistic\.etherscan\.io']
    }
    
    for chain, patterns in explorer_patterns.items():
        for pattern in patterns:
            if re.search(pattern, text):
                logger.info(f"从区块浏览器URL提取到链信息: {chain}, 匹配模式: {pattern}")
                return chain
    
    # 检查特定的DEX关键词
    dex_patterns = {
        'SOL': [r'raydium', r'orca\.so', r'jupiter'],
        'ETH': [r'uniswap', r'sushiswap'],
        'BSC': [r'pancakeswap', r'poocoin']
    }
    
    for chain, patterns in dex_patterns.items():
        for pattern in patterns:
            if re.search(pattern, text):
                logger.info(f"从DEX关键词提取到链信息: {chain}, 匹配模式: {pattern}")
                return chain
    
    # 最后再检查一般关键词匹配，这个优先级较低
    for chain, patterns in chain_patterns.items():
        for pattern in patterns:
            if re.search(pattern, text):
                logger.info(f"从关键词匹配提取到链信息: {chain}, 匹配模式: {pattern}")
                return chain
    
    # 处理中文环境
    chinese_chains = {
        'SOL': ['solana', 'sol', '索拉纳', '索兰纳'],
        'ETH': ['ethereum', 'eth', '以太坊', '以太'],
        'BSC': ['binance', 'bsc', 'bnb', '币安'],
        'AVAX': ['avalanche', 'avax', '雪崩'],
        'MATIC': ['polygon', 'matic', '波利冈']
    }
    
    for chain, keywords in chinese_chains.items():
        for keyword in keywords:
            if keyword in text:
                logger.info(f"从中文环境提取到链信息: {chain}, 关键词: {keyword}")
                return chain
    
    # 检查是否包含特定的机器人引用
    bot_patterns = {
        'SOL': [r'solana_trojanbot'],
        'BSC': [r'ape\.bot', r'sigma_buybot.*bsc', r'pancakeswap_bot'],
        'ETH': [r'uniswap_bot', r'sigma_buybot.*eth']
    }
    
    for chain, patterns in bot_patterns.items():
        for pattern in patterns:
            if re.search(pattern, text):
                logger.info(f"从机器人引用提取到链信息: {chain}, 匹配模式: {pattern}")
                return chain
    
    # 尝试从MC（市值）单位判断链
    mc_patterns = {
        'ETH': [r'(\bmc\b|\bmarket\s*cap\b|市值)[：:]*\s*[`\'"]*\d+(?:\.\d+)?\s*(?:eth|ETH)'],
        'BSC': [r'(\bmc\b|\bmarket\s*cap\b|市值)[：:]*\s*[`\'"]*\d+(?:\.\d+)?\s*(?:bnb|BNB)'],
        'SOL': [r'(\bmc\b|\bmarket\s*cap\b|市值)[：:]*\s*[`\'"]*\d+(?:\.\d+)?\s*(?:sol|SOL)']
    }
    
    for chain, patterns in mc_patterns.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                logger.info(f"从市值单位提取到链信息: {chain}, 匹配模式: {pattern}")
                return chain
    
    # 最后才从合约地址格式推断，且需要结合其他上下文信息
    if re.search(r'\b0x[0-9a-fA-F]{40}\b', text):
        # 尝试从其他上下文判断具体是哪种EVM链
        if 'bnb' in text or 'bsc' in text or 'binance' in text or 'pancake' in text:
            logger.info("从合约地址格式和上下文信息(BSC关键词)推断为BSC链")
            return 'BSC'
        elif 'eth' in text or 'ethereum' in text or 'uniswap' in text:
            logger.info("从合约地址格式和上下文信息(ETH关键词)推断为ETH链")
            return 'ETH'
        elif 'arb' in text or 'arbitrum' in text:
            logger.info("从合约地址格式和上下文信息(ARB关键词)推断为ARB链")
            return 'ARB'
        elif 'matic' in text or 'polygon' in text:
            logger.info("从合约地址格式和上下文信息(MATIC关键词)推断为MATIC链")
            return 'MATIC'
        else:
            # 不再默认返回ETH，而是返回None表示无法确定
            logger.warning("从合约地址格式推断为EVM链，但无法确定具体是哪条链，需要更多上下文")
            return None
        
    if re.search(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b', text) and ('sol' in text or 'solana' in text):
        # Solana Base58格式地址
        logger.info("从合约地址格式和SOL关键词推断为SOL链")
        return 'SOL'
    
    # 如果所有方法都失败，返回None
    logger.debug("无法从消息中提取链信息")
    return None

def extract_url_from_text(text: str, keyword: str = '') -> Optional[str]:
    """从文本中提取URL链接
    
    Args:
        text: 要处理的文本
        keyword: 可选的关键词，用于筛选URL
        
    Returns:
        提取出的URL，或None
    """
    if not text:
        return None
    
    try:
        # 定义URL正则表达式模式
        url_patterns = [
            r'https?://\S+',  # 标准HTTP/HTTPS URL
            r'www\.\S+',      # 以www开头的URL
            r't\.me/\S+',     # Telegram链接
            r'twitter\.com/\S+'  # Twitter链接
        ]
        
        # 合并所有模式
        combined_pattern = '|'.join(url_patterns)
        
        # 查找所有匹配的URL
        urls = re.findall(combined_pattern, text)
        
        if not urls:
            return None
            
        if keyword:
            # 如果指定了关键词，优先返回包含关键词的URL
            for url in urls:
                if keyword.lower() in url.lower():
                    # 处理URL末尾可能的标点符号
                    markers = [' ', '\n', '\t', ',', ')', ']', '}', '"', "'", '。', '，', '：', '；']
                    url_part = url
                    
                    # 查找最早出现的标点符号位置
                    end_idx = len(url_part)
                    for marker in markers:
                        marker_idx = url_part.find(marker)
                        if marker_idx > 0 and marker_idx < end_idx:
                            end_idx = marker_idx
                    
                    url = url_part[:end_idx].strip()
                    return url
        
        # 如果没有指定关键词或没有找到包含关键词的URL，返回第一个URL
        url_part = urls[0]
        # 处理URL末尾可能的标点符号
        markers = [' ', '\n', '\t', ',', ')', ']', '}', '"', "'", '。', '，', '：', '；']
        
        # 查找最早出现的标点符号位置
        end_idx = len(url_part)
        for marker in markers:
            marker_idx = url_part.find(marker)
            if marker_idx > 0 and marker_idx < end_idx:
                end_idx = marker_idx
        
        url = url_part[:end_idx].strip()
        return url

    except Exception as e:
        logger.error(f"从文本中提取URL时出错: {str(e)}")
        return None
    
    return None

def extract_contract_from_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    """从URL中提取合约地址和链信息
    
    Args:
        url: 网址
        
    Returns:
        Tuple[Optional[str], Optional[str]]: 合约地址和链信息的元组，未找到则返回(None, None)
    """
    if not url:
        return None, None
    
    try:
        # 处理各种常见区块浏览器和DEX的URL
        # DexScreener格式
        # 例如: https://dexscreener.com/solana/8WJ2ngd7FpHVkWiQTNyJ3N9j1oDmjR5e6MFdDAKQNinF
        dexscreener_pattern = r'(?:https?://)?(?:www\.)?dexscreener\.com/([a-zA-Z0-9]+)/([a-zA-Z0-9]{20,})'
        match = re.search(dexscreener_pattern, url)
        if match:
            chain_str = match.group(1).lower()
            contract = match.group(2)
            
            # 映射到内部链标识
            dexscreener_map = {
                'solana': 'SOL',
                'ethereum': 'ETH',
                'bsc': 'BSC',
                'arbitrum': 'ARB',
                'base': 'BASE',
                'avalanche': 'AVAX',
                'polygon': 'MATIC',
                'optimism': 'OP'
            }
            
            chain = dexscreener_map.get(chain_str)
            logger.info(f"从DexScreener URL提取到合约地址: {contract}, 链: {chain}")
            return contract, chain
        
        # 特殊模式：币安链浏览器
        # 例如: https://bscscan.com/token/0x123456789...
        bscscan_pattern = r'(?:https?://)?(?:www\.)?bscscan\.com/(?:token|address)/([a-zA-Z0-9]{20,})'
        match = re.search(bscscan_pattern, url)
        if match:
            contract = match.group(1)
            logger.info(f"从BSCScan URL提取到合约地址: {contract}, 链: BSC")
            return contract, 'BSC'
        
        # Etherscan格式
        # 例如: https://etherscan.io/token/0x123456789...
        etherscan_pattern = r'(?:https?://)?(?:www\.)?etherscan\.io/(?:token|address)/([a-zA-Z0-9]{20,})'
        match = re.search(etherscan_pattern, url)
        if match:
            contract = match.group(1)
            logger.info(f"从Etherscan URL提取到合约地址: {contract}, 链: ETH")
            return contract, 'ETH'
        
        # Solscan格式
        # 例如: https://solscan.io/token/8WJ2ngd7FpHVkWiQTNyJ3N9j1oDmjR5e6MFdDAKQNinF
        solscan_pattern = r'(?:https?://)?(?:www\.)?solscan\.io/(?:token|account)/([a-zA-Z0-9]{20,})'
        match = re.search(solscan_pattern, url)
        if match:
            contract = match.group(1)
            logger.info(f"从Solscan URL提取到合约地址: {contract}, 链: SOL")
            return contract, 'SOL'
        
        # Polygonscan格式
        polygonscan_pattern = r'(?:https?://)?(?:www\.)?polygonscan\.com/(?:token|address)/([a-zA-Z0-9]{20,})'
        match = re.search(polygonscan_pattern, url)
        if match:
            contract = match.group(1)
            logger.info(f"从Polygonscan URL提取到合约地址: {contract}, 链: MATIC")
            return contract, 'MATIC'

        # Arbiscan格式
        arbiscan_pattern = r'(?:https?://)?(?:www\.)?arbiscan\.io/(?:token|address)/([a-zA-Z0-9]{20,})'
        match = re.search(arbiscan_pattern, url)
        if match:
            contract = match.group(1)
            logger.info(f"从Arbiscan URL提取到合约地址: {contract}, 链: ARB")
            return contract, 'ARB'
        
        # Basescan格式
        basescan_pattern = r'(?:https?://)?(?:www\.)?basescan\.org/(?:token|address)/([a-zA-Z0-9]{20,})'
        match = re.search(basescan_pattern, url)
        if match:
            contract = match.group(1)
            logger.info(f"从Basescan URL提取到合约地址: {contract}, 链: BASE")
            return contract, 'BASE'
        
        # Snowtrace (Avalanche) 格式
        snowtrace_pattern = r'(?:https?://)?(?:www\.)?snowtrace\.io/(?:token|address)/([a-zA-Z0-9]{20,})'
        match = re.search(snowtrace_pattern, url)
        if match:
            contract = match.group(1)
            logger.info(f"从Snowtrace URL提取到合约地址: {contract}, 链: AVAX")
            return contract, 'AVAX'
        
        # 处理Raydium、Orca等Solana DEX的URL
        solana_dex_pattern = r'(?:https?://)?(?:www\.)?(raydium\.io|orca\.so|jup\.ag)/(?:\w+)/([a-zA-Z0-9]{20,})'
        match = re.search(solana_dex_pattern, url)
        if match:
            contract = match.group(2)
            logger.info(f"从Solana DEX URL提取到合约地址: {contract}, 链: SOL")
            return contract, 'SOL'
        
        # 处理Uniswap、Sushiswap等以太坊DEX的URL
        eth_dex_pattern = r'(?:https?://)?(?:www\.)?(uniswap\.org|app\.uniswap\.org|sushi\.com)/(?:\w+)/([a-zA-Z0-9]{20,})'
        match = re.search(eth_dex_pattern, url)
        if match:
            contract = match.group(2)
            logger.info(f"从ETH DEX URL提取到合约地址: {contract}, 链: ETH")
            return contract, 'ETH'
        
        logger.debug(f"未能从URL中提取合约地址: {url}")
        return None, None
        
    except Exception as e:
        logger.error(f"从URL中提取合约地址时出错: {str(e)}")
        return None, None

def format_token_history(history: list) -> str:
    """格式化代币历史数据为易读的字符串"""
    if not history:
        return "未找到该代币的历史数据"
    
    output = []
    output.append("=== 代币历史数据 ===\n")
    
    # 获取第一条数据中的代币信息
    _, first_promo = history[0]
    if first_promo:
        output.append(f"代币符号: {first_promo.token_symbol}")
        output.append(f"合约地址: {first_promo.contract_address}\n")
    
    # 添加每条记录的详细信息
    for message, promo in history:
        # 正确处理时区转换
        date = message['date']
        if isinstance(date, (int, float)):
            # 假设时间戳是UTC时间
            utc_time = datetime.fromtimestamp(date, timezone.utc)
        else:
            # 如果是datetime对象，确保它有UTC时区信息
            utc_time = timezone.utc.localize(date) if not date.tzinfo else date
            
        # 转换为北京时间 (UTC+8)
        beijing_tz = timezone(timedelta(hours=8))
        beijing_time = utc_time.astimezone(beijing_tz)
        
        # 输出北京时间，明确标注时区
        output.append(f"时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)")
        if promo:
            if promo.market_cap is not None:
                output.append(f"市值: ${promo.market_cap:,.2f}")
            if promo.promotion_count is not None:
                output.append(f"推广次数: {promo.promotion_count}")
        output.append(f"消息ID: {message['message_id']}")
        output.append("消息内容:")
        output.append(message['text'])
        output.append("-" * 50 + "\n")
    
    return "\n".join(output)

def get_db_performance_stats():
    """获取数据库性能统计信息
    
    Returns:
        性能统计数据字典
    """
    stats = db_performance_stats.copy()
    
    # 计算每个操作的平均执行时间
    avg_times = {}
    for op_name, total_time in stats['operation_times'].items():
        count = stats['operation_counts'].get(op_name, 0)
        if count > 0:
            avg_times[op_name] = total_time / count
        else:
            avg_times[op_name] = 0
    
    stats['average_times'] = avg_times
    
    # 添加Supabase状态信息
    try:
        from src.database.db_factory import get_db_adapter
        db_adapter = get_db_adapter()
        stats['adapter_type'] = 'supabase'
        stats['database_uri'] = 'supabase://*****' # 隐藏敏感信息
    except Exception as e:
        logger.error(f"获取数据库状态信息时出错: {e}")
        stats['adapter_error'] = str(e)
    
    return stats

def reset_db_performance_stats():
    """重置数据库性能统计信息"""
    global db_performance_stats
    db_performance_stats = {
        'operation_counts': {},
        'operation_times': {},
        'lock_errors': 0,
        'total_retries': 0
    }

# 添加缺失的清理批处理任务函数
async def cleanup_batch_tasks():
    """清理所有批处理任务队列，确保所有待处理数据都被保存
    
    在程序关闭前调用此函数，以防止数据丢失
    """
    global message_batch, token_batch
    
    try:
        # 处理消息批处理队列
        if message_batch:
            logger.info(f"清理 {len(message_batch)} 条未处理的消息...")
            await process_message_batch()
        
        # 处理代币批处理队列
        if token_batch:
            logger.info(f"清理 {len(token_batch)} 条未处理的代币信息...")
            try:
                # 复制当前队列并清空全局队列
                local_batch = token_batch.copy()
                token_batch = []
                
                # 安全地处理每个代币数据
                processed_count = 0
                for token_data in local_batch:
                    try:
                        if isinstance(token_data, dict) and 'contract' in token_data and token_data['contract']:
                            # 使用增强的save_token_info函数处理单个代币信息
                            if save_token_info(token_data):
                                processed_count += 1
                        else:
                            logger.warning(f"跳过无效的代币数据: {token_data}")
                    except Exception as e:
                        logger.error(f"处理单个代币数据时出错: {str(e)}")
                
                logger.info(f"成功清理 {processed_count}/{len(local_batch)} 条代币信息")
            except Exception as e:
                logger.error(f"清理代币信息队列时出错: {str(e)}")
                import traceback
                logger.debug(traceback.format_exc())
        
        logger.info("批处理任务队列清理完成")
        return True
    except Exception as e:
        logger.error(f"清理批处理任务队列时出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        return False

def calculate_community_reach(token_symbol: str, session=None):
    """计算代币的社群覆盖人数
    
    计算方式：
    1. 根据token_symbol查询tokens_mark表中的所有条目
    2. 统计查询结果中的唯一channel_id
    3. 根据channel_id查询telegram_channels表中的member_count
    4. 将member_count相加得到community_reach
    
    Args:
        token_symbol: 代币符号
        session: 废弃参数，为了兼容性保留
        
    Returns:
        int: 计算得到的社群覆盖人数
    """
    # 使用缓存避免频繁计算相同的token
    cache_key = f"community_reach_{token_symbol}"
    if hasattr(calculate_community_reach, 'cache') and cache_key in calculate_community_reach.cache:
        # 缓存有效期为5分钟
        cache_time, value = calculate_community_reach.cache[cache_key]
        if (datetime.now() - cache_time).total_seconds() < 300:  # 5分钟内
            return value

    # 初始化缓存字典(如果不存在)
    if not hasattr(calculate_community_reach, 'cache'):
        calculate_community_reach.cache = {}
    
    try:
        # 使用Supabase适配器
        from src.database.db_factory import get_db_adapter
        db_adapter = get_db_adapter()
        
        # 使用异步调用但在同步环境中执行
        async def get_community_reach():
            # 1. 获取所有提及该代币的token_mark记录
            token_marks = await db_adapter.execute_query(
                'tokens_mark',
                'select',
                filters={'token_symbol': token_symbol}
            )
            
            # 如果没有记录，返回0
            if not token_marks:
                return 0
                
            # 2. 提取唯一的channel_id
            channel_ids = []
            for mark in token_marks:
                if isinstance(mark, dict) and mark.get('channel_id') and mark['channel_id'] not in channel_ids:
                    channel_ids.append(mark['channel_id'])
            
            # 如果没有channel_id，返回0
            if not channel_ids:
                return 0
                
            # 3. 获取这些频道的成员数
            total_reach = 0
            for channel_id in channel_ids:
                channel_info = await db_adapter.get_channel_by_id(channel_id)
                if channel_info and channel_info.get('member_count'):
                    total_reach += channel_info['member_count']
            
            return total_reach
        
        # 执行异步函数，使用事件循环
        loop = asyncio.new_event_loop()
        try:
            total_reach = loop.run_until_complete(get_community_reach())
        finally:
            loop.close()
                
        # 存入缓存
        calculate_community_reach.cache[cache_key] = (datetime.now(), total_reach)
        
        # 限制缓存大小，防止内存泄漏
        if len(calculate_community_reach.cache) > 1000:  # 最多缓存1000个token
            # 删除最早的20%缓存
            sorted_keys = sorted(
                calculate_community_reach.cache.keys(),
                key=lambda k: calculate_community_reach.cache[k][0]
            )
            for key in sorted_keys[:200]:  # 删除最早的200个
                del calculate_community_reach.cache[key]
        
        # 返回总覆盖人数
        return total_reach
        
    except Exception as e:
        logger.error(f"计算代币 {token_symbol} 的社群覆盖人数时出错: {str(e)}")
        logger.debug(traceback.format_exc())
        return 0

# 添加简单的代币数据验证函数
def validate_token_data(token_data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    验证代币数据的完整性
    
    Args:
        token_data: 代币数据
        
    Returns:
        (bool, str): 是否有效，错误信息
    """
    required_fields = ['chain', 'token_symbol', 'contract']
    
    # 检查必要字段
    for field in required_fields:
        if field not in token_data or not token_data[field]:
            return False, f"缺少必要字段: {field}"
    
    return True, ""
