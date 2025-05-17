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

# 添加常量定义 - 支持的区块链及其代码
CHAINS = {
    'SOL': ['solana', 'sol', '索拉纳', '索兰纳', 'raydium', 'orca', 'jupiter'],
    'ETH': ['ethereum', 'eth', '以太坊', '以太', 'uniswap', 'sushiswap'],
    'BSC': ['binance', 'bsc', 'bnb', '币安', 'pancakeswap', 'poocoin'],
    'ARB': ['arbitrum', 'arb', '阿比特龙', '아비트럼'],
    'BASE': ['base', 'basechain', 'coinbase', '贝斯链', '베이스'],
    'AVAX': ['avalanche', 'avax', '雪崩链', '아발란체'],
    'MATIC': ['polygon', 'matic', '波利冈', '폴리곤'],
    'OP': ['optimism', 'op', '乐观链', '옵티미즘']
}

# 添加EVM链列表常量
EVM_CHAINS = ['ETH', 'BSC', 'ARB', 'BASE', 'MATIC', 'AVAX', 'OP']
NON_EVM_CHAINS = ['SOL']

# 链的区块浏览器
CHAIN_EXPLORERS = {
    'SOL': ['solscan.io', 'explorer.solana.com'],
    'ETH': ['etherscan.io'],
    'BSC': ['bscscan.com'],
    'ARB': ['arbiscan.io'],
    'BASE': ['basescan.org'],
    'AVAX': ['snowtrace.io'],
    'MATIC': ['polygonscan.com'],
    'OP': ['optimistic.etherscan.io']
}

# DEX平台URL匹配
DEX_PATTERNS = {
    'SOL': [r'raydium\.io', r'orca\.so', r'jup\.ag'],
    'ETH': [r'uniswap\.org', r'app\.uniswap\.org', r'sushi\.com'],
    'BSC': [r'pancakeswap\.finance', r'poocoin\.app']
}

# 推特账号与链的映射
TWITTER_CHAIN_MAP = {
    'cz_binance': 'BSC',
    'binance': 'BSC',
    'bnbchain': 'BSC',
    'ethereum': 'ETH',
    'vitalikbuterin': 'ETH',
    'solana': 'SOL',
    'arbitrum': 'ARB',
    'optimism': 'OP',
    'avalancheavax': 'AVAX',
    'polygonlabs': 'MATIC',
    'base': 'BASE'
}

# URL正则表达式模式
URL_PATTERNS = [
    r'https?://\S+',  # 标准HTTP/HTTPS URL
    r'www\.\S+',      # 以www开头的URL
    r't\.me/\S+',     # Telegram链接
    r'twitter\.com/\S+',  # Twitter链接
    r'x\.com/\S+'     # X.com链接
]

# 合约地址匹配模式
CONTRACT_PATTERNS = [
    # 带标记的合约地址
    r'(?:📝|合约[：:]|[Cc]ontract[：:])[ ]*([0-9a-fA-FxX]{8,})',
    r'合约地址[：:][ ]*([0-9a-fA-FxX]{8,})',
    r'地址[：:][ ]*([0-9a-fA-FxX]{8,})',
    # 标准以太坊地址格式
    r'\b(0x[0-9a-fA-F]{40})\b',
    # 其他可能的合约地址格式
    r'\b([a-zA-Z0-9]{32,50})\b'
]

# 辅助函数：查找文本中的所有URL
def find_urls_in_text(text: str) -> List[str]:
    """
    从文本中提取所有URL
    
    Args:
        text: 要处理的文本
        
    Returns:
        List[str]: 提取出的URL列表
    """
    if not text:
        return []
    
    # 合并所有URL模式
    combined_pattern = '|'.join(URL_PATTERNS)
    
    # 提取所有URL
    urls = re.findall(combined_pattern, text)
    
    # 清理URL
    clean_urls = []
    for url in urls:
        # 处理URL末尾可能的标点符号
        markers = [' ', '\n', '\t', ',', ')', ']', '}', '"', "'", '。', '，', '：', '；']
        end_idx = len(url)
        for marker in markers:
            marker_idx = url.find(marker)
            if marker_idx > 0 and marker_idx < end_idx:
                end_idx = marker_idx
        
        clean_url = url[:end_idx].strip()
        if clean_url:
            clean_urls.append(clean_url)
    
    return clean_urls

# 辅助函数：根据URL判断链
def get_chain_from_url(url: str) -> Optional[str]:
    """
    从URL中判断区块链类型
    
    Args:
        url: URL字符串
        
    Returns:
        Optional[str]: 链名称或None
    """
    url_lower = url.lower()
    
    # DexScreener URL格式
    dexscreener_match = re.search(r'(?:https?://)?(?:www\.)?dexscreener\.com/([a-zA-Z0-9]+)(?:/[^/\s]+)?', url_lower)
    if dexscreener_match:
        chain_str = dexscreener_match.group(1).upper()
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
            return dexscreener_map[chain_str]
    
    # GMGN.ai URL格式
    gmgn_match = re.search(r'gmgn\.ai(?:/[^/]+)?/([^/]+)/token/', url_lower)
    if gmgn_match:
        chain_id = gmgn_match.group(1).upper()
        if chain_id in ['BSC', 'ETH', 'ARBITRUM', 'BASE', 'POLYGON', 'OPTIMISM']:
            if chain_id == 'ARBITRUM':
                return 'ARB'
            elif chain_id == 'POLYGON':
                return 'MATIC'
            elif chain_id == 'OPTIMISM':
                return 'OP'
            return chain_id
    
    # 检查区块浏览器域名
    for chain, explorers in CHAIN_EXPLORERS.items():
        for explorer in explorers:
            if explorer in url_lower:
                return chain
    
    # 检查DEX域名
    for chain, patterns in DEX_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, url_lower):
                return chain
    
    # 检查常见关键词
    for chain, keywords in CHAINS.items():
        for keyword in keywords:
            if keyword.lower() in url_lower:
                return chain
    
    # 推特账号判断
    twitter_match = re.search(r'(?:twitter\.com|x\.com)/([^/\s]+)', url_lower)
    if twitter_match:
        twitter_user = twitter_match.group(1).lower()
        if twitter_user in TWITTER_CHAIN_MAP:
            return TWITTER_CHAIN_MAP[twitter_user]
    
    return None

# 辅助函数：判断地址格式对应的链
def get_chain_from_address(address: str) -> Optional[str]:
    """
    根据合约地址格式判断可能的链
    
    Args:
        address: 合约地址
        
    Returns:
        Optional[str]: 链名称、'EVM'表示以太坊风格地址(需要进一步确定具体链)，或None
    """
    if not address:
        return None
        
    # EVM格式地址(以太坊、BSC等) - 0x开头的42位16进制数
    if re.match(r'^0x[a-fA-F0-9]{40}$', address):
        return 'EVM'
    
    # Solana格式地址 - Base58编码，不以0x开头，通常32-44位
    if re.match(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$', address) and not address.startswith('0x'):
        return 'SOL'
    
    # 检查是否可能是不完整的EVM地址
    if address.startswith('0x') and len(address) >= 10:
        logger.warning(f"发现可能不完整的EVM格式地址: {address}")
        return 'EVM_PARTIAL'
    
    # 针对特殊格式的地址，可以扩展更多判断
    # 例如: Arweave、NEAR、Cosmos等
    
    return None

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
                    # 只记录日志，不抛出异常，保证主流程继续
                    continue
                
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
                    # 只记录日志，不抛出异常，保证主流程继续
                    continue
                
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
        # 新增：批量查重，过滤已存在的消息
        filtered_msgs = []
        for msg in messages:
            if not all(key in msg for key in ['chain', 'message_id', 'date']):
                logger.warning(f"消息缺少必要字段: {msg}")
                continue
            exists = await db_adapter.check_message_exists(msg['chain'], msg['message_id'])
            if exists:
                logger.info(f"消息 {msg['chain']}-{msg['message_id']} 已存在，批量保存时跳过")
                continue
            filtered_msgs.append(msg)
        successful = 0
        for msg in filtered_msgs:
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
    from src.database.db_factory import get_db_adapter
    db_adapter = get_db_adapter()
    for msg_data in messages:
        try:
            exists = await db_adapter.check_message_exists(msg_data['chain'], msg_data['message_id'])
            if exists:
                logger.info(f"消息 {msg_data['chain']}-{msg_data['message_id']} 已存在，逐个保存时跳过")
                continue
            # 使用异步方式保存
            if await save_telegram_message(
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
            import traceback
            logger.debug(traceback.format_exc())
    logger.info(f"逐个保存: 成功 {successful}/{len(messages)} 条消息")

async def save_telegram_message(
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
    global message_batch
    if MAX_BATCH_SIZE > 0:
        from src.database.db_factory import get_db_adapter
        db_adapter = get_db_adapter()
        exists = await db_adapter.check_message_exists(chain, message_id)
        if exists:
            logger.info(f"消息 {chain}-{message_id} 已存在，跳过入队和保存")
            return False
        message_batch.append({
            'chain': chain,
            'message_id': message_id,
            'date': date,
            'text': text,
            'media_path': media_path,
            'channel_id': channel_id
        })
        if len(message_batch) >= MAX_BATCH_SIZE:
            asyncio.create_task(process_message_batch())
        return True
    try:
        from src.database.db_factory import get_db_adapter
        db_adapter = get_db_adapter()
        exists = await db_adapter.check_message_exists(chain, message_id)
        if exists:
            logger.info(f"消息 {chain}-{message_id} 已存在，跳过保存")
            return False
        message_data = {
            'chain': chain,
            'message_id': message_id,
            'date': date.isoformat() if isinstance(date, datetime) else date,
            'text': text,
            'media_path': media_path,
            'channel_id': channel_id
        }
        result = await db_adapter.save_message(message_data)
        return result
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

def extract_promotion_info(message_text: str, date: datetime, chain: str = None, message_id: int = None, channel_id: int = None) -> List[PromotionInfo]:
    """
    批量提取所有合约地址，并对每个合约地址独立走一遍原有严密的链推断、符号、风险、市值等主流程，返回PromotionInfo对象列表。
    保证每个PromotionInfo的chain字段都为具体链（ETH/BSC/ARB等），绝不会为EVM。
    日志输出全部迁移到主流程，底层函数只保留异常和调试日志。
    日志输出严格区分调用目的：收集新地址时输出"发现新地址"日志，链推断时只输出链推断相关日志。
    """
    results = []
    try:
        # 1. 清理消息文本
        cleaned_text = re.sub(r'\s+', ' ', message_text)
        cleaned_text = re.sub(r'[\u200b\u200c\u200d\ufeff]', '', cleaned_text)  # 移除零宽字符
        # 2. 批量提取所有合约地址（URL+正则），并记录来源和链信息，便于后续唯一日志输出
        urls = find_urls_in_text(cleaned_text)
        address_info_map = dict()  # 合约地址 -> {'source': 'url'/'regex', 'url': url, 'chain': chain}
        # 2.1 从URL中提取合约地址，并在主流程输出日志（目的：收集新地址）
        for url in urls:
            contract, detected_chain = extract_contract_from_url(url)
            if contract:
                # 只保留第一次出现的来源
                if contract not in address_info_map:
                    address_info_map[contract] = {'source': 'url', 'url': url, 'chain': detected_chain}
                    # === 只在"收集新地址"阶段输出日志 ===
                    if detected_chain:
                        logger.info(f"从URL '{url}' 检测到链: {detected_chain}")
                        if detected_chain == 'SOL':
                            logger.info(f"从URL直接提取到Solana格式地址: {contract}")
                        elif detected_chain in EVM_CHAINS:
                            logger.info(f"从URL直接提取到EVM格式地址: {contract}, 链: {detected_chain}")
        # 2.2 从文本中正则提取合约地址
        for pattern in CONTRACT_PATTERNS:
            for match in re.finditer(pattern, cleaned_text):
                potential_address = match.group(1) if '(' in pattern else match.group(0)
                if potential_address and potential_address not in address_info_map:
                    address_info_map[potential_address] = {'source': 'regex', 'url': None, 'chain': None}
        # 3. 对每个唯一合约地址，独立走一遍原有主流程
        for contract_address, info_dict in address_info_map.items():
            # === 以下为原有extract_promotion_info主流程，针对当前contract_address ===
            # 3.1 独立链推断
            local_chain = chain
            # 优先从URL判断链信息
            chain_from_url = None
            for url in urls:
                c, detected_chain = extract_contract_from_url(url)
                if c == contract_address and detected_chain:
                    # === 只在链推断发生变化或补充信息时输出链推断相关日志 ===
                    if (not info_dict['chain']) or (info_dict['chain'] != detected_chain):
                        logger.info(f"链推断：合约地址{contract_address}在URL '{url}' 检测到链信息: {detected_chain}")
                        if detected_chain == 'SOL':
                            logger.info(f"链推断：合约地址{contract_address}为Solana格式")
                        elif detected_chain in EVM_CHAINS:
                            logger.info(f"链推断：合约地址{contract_address}为EVM格式，链: {detected_chain}")
                    chain_from_url = detected_chain
                    break
            if chain_from_url:
                local_chain = chain_from_url
            # 如果未提供链信息或为UNKNOWN，尝试从消息中提取
            if not local_chain or local_chain == "UNKNOWN":
                # 先检查消息中的市值单位来判断链
                if re.search(r'(\bmc\b|\bmarket\s*cap\b|市值)[：:]*\s*[`\'"]*\d+(?:\.\d+)?\s*(?:bnb|BNB)', cleaned_text, re.IGNORECASE):
                    logger.info("从市值单位(BNB)判断为BSC链")
                    local_chain = 'BSC'
                elif re.search(r'(\bmc\b|\bmarket\s*cap\b|市值)[：:]*\s*[`\'"]*\d+(?:\.\d+)?\s*(?:eth|ETH)', cleaned_text, re.IGNORECASE):
                    logger.info("从市值单位(ETH)判断为ETH链")
                    local_chain = 'ETH'
                elif re.search(r'(\bmc\b|\bmarket\s*cap\b|市值)[：:]*\s*[`\'"]*\d+(?:\.\d+)?\s*(?:sol|SOL)', cleaned_text, re.IGNORECASE):
                    logger.info("从市值单位(SOL)判断为SOL链")
                    local_chain = 'SOL'
                else:
                    chain_from_message = extract_chain_from_message(message_text)
                    if chain_from_message:
                        logger.info(f"从消息中提取到链信息: {chain_from_message}")
                        local_chain = chain_from_message
            # 3.2 提取代币符号（多格式）
            token_symbol = None
            symbol_match = re.search(r'\$([A-Za-z0-9_]{1,20})\b', cleaned_text)
            if symbol_match:
                token_symbol = symbol_match.group(1).upper()
                logger.info(f"从消息中提取到代币符号: {token_symbol}")
            else:
                # 尝试从第一行或其他格式中提取代币符号
                first_line = cleaned_text.split('\n')[0] if '\n' in cleaned_text else cleaned_text
                symbol_patterns = [
                    r'"([A-Za-z0-9_]{1,10})"',
                    r'\'([A-Za-z0-9_]{1,10})\'',
                    r'\*\*([A-Za-z0-9_]{1,10})\*\*',
                    r'`([A-Za-z0-9_]{1,10})`'
                ]
                for pattern in symbol_patterns:
                    match = re.search(pattern, first_line)
                    if match:
                        potential_symbol = match.group(1).upper()
                        common_words = ['NEW', 'TOKEN', 'CONTRACT', 'ADDRESS', 'LINK', 'ALPHA']
                        if potential_symbol not in common_words and len(potential_symbol) >= 2:
                            token_symbol = potential_symbol
                            logger.info(f"从消息格式中提取到代币符号: {token_symbol}")
                            break
            # 3.3 合约格式判断和链修正
            address_chain = get_chain_from_address(contract_address)
            if address_chain == 'EVM':
                if local_chain not in EVM_CHAINS:
                    for evm_chain in EVM_CHAINS:
                        for keyword in CHAINS.get(evm_chain, []):
                            if keyword.lower() in cleaned_text.lower():
                                logger.warning(f"最终检查: 合约地址{contract_address}是EVM格式，但链为{local_chain}，上下文暗示应为{evm_chain}链")
                                local_chain = evm_chain
                                break
                        if local_chain in EVM_CHAINS:
                            break
                    if local_chain not in EVM_CHAINS:
                        logger.warning(f"最终检查: 合约地址{contract_address}是EVM格式，但链为{local_chain}，这不匹配。修正为BSC")
                        local_chain = 'BSC'
            elif address_chain == 'SOL':
                if local_chain != 'SOL':
                    logger.warning(f"最终检查: 合约地址{contract_address}是SOL格式，但链为{local_chain}，这不匹配。修正为SOL")
                    local_chain = 'SOL'
            elif not local_chain or local_chain == 'UNKNOWN':
                if address_chain == 'EVM':
                    for evm_chain in EVM_CHAINS:
                        for keyword in CHAINS.get(evm_chain, []):
                            if keyword.lower() in cleaned_text.lower():
                                logger.info(f"通过关键词将EVM地址归类为{evm_chain}链")
                                local_chain = evm_chain
                                break
                        if local_chain and local_chain != 'UNKNOWN':
                            break
                    if not local_chain or local_chain == 'UNKNOWN':
                        local_chain = 'BSC'
                        logger.info("没有找到匹配的EVM链关键词，默认设置为BSC")
                elif address_chain == 'SOL':
                    local_chain = 'SOL'
            # 3.4 PromotionInfo对象构建
            info = PromotionInfo(
                token_symbol=token_symbol,
                contract_address=contract_address,
                chain=local_chain,
                promotion_count=1,
                first_trending_time=date
            )
            info.message_id = message_id
            info.channel_id = channel_id
            # 3.5 风险评级提取
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
            # 3.6 市值提取与链修正
            from src.utils.utils import parse_market_cap
            # 统一市值字段提取逻辑，不区分USD市值与市值，所有市值相关字段一视同仁
            # 匹配"市值"、"Market Cap"、"Mc"等常见写法
            market_cap_text = re.search(r'([Mm]arket\s*[Cc]ap|市值|[Mm][Cc])[：:]*\s*[`\'\"]?([^,\n]+)', cleaned_text)
            if market_cap_text:
                mc_value = market_cap_text.group(2).strip()
                # === 只保留第一个"数字+单位"片段，丢弃后续所有非市值内容 ===
                # 允许格式如 3.35M、3369587、100万、2.1B 等，防止混杂内容导致解析失败
                mc_match = re.match(r'([0-9.,]+\s*[a-zA-Z万亿KMB]*)', mc_value)
                if mc_match:
                    mc_clean = mc_match.group(1).replace(' ', '')
                else:
                    # 兜底：只取第一个单词，最大程度保证健壮性
                    mc_clean = mc_value.split()[0]
                try:
                    # 只将纯净的市值字符串传入解析函数，极大降低异常概率
                    parsed_mc = parse_market_cap(mc_clean)
                    if parsed_mc:
                        info.market_cap = str(parsed_mc)
                        mc_lower = mc_clean.lower()
                        # 链推断逻辑保留
                        if 'bnb' in mc_lower and (not info.chain or info.chain == 'UNKNOWN'):
                            info.chain = 'BSC'
                        elif 'eth' in mc_lower and (not info.chain or info.chain == 'UNKNOWN'):
                            info.chain = 'ETH'
                        elif 'sol' in mc_lower and (not info.chain or info.chain == 'UNKNOWN'):
                            info.chain = 'SOL'
                except Exception as e:
                    # 若解析失败，详细记录原始内容和异常信息，便于后续排查
                    logger.warning(f"解析市值出错: {mc_value}, 错误: {str(e)}")
            # 3.7 最终保险，chain绝不为EVM
            if info.chain == 'EVM' or not info.chain or info.chain == 'UNKNOWN':
                chain_from_msg = extract_chain_from_message(message_text)
                if chain_from_msg in EVM_CHAINS:
                    info.chain = chain_from_msg
                else:
                    info.chain = 'ETH'
            results.append(info)
        return results
    except Exception as e:
        logger.error(f"批量提取合约地址时出错: {str(e)}")
        logger.debug(traceback.format_exc())
        return results

def extract_single_promotion_info(message_text: str, date: datetime, chain: str = None, message_id: int = None, channel_id: int = None) -> Optional[PromotionInfo]:
    """
    兼容旧接口，只返回第一个PromotionInfo对象
    """
    infos = extract_promotion_info(message_text, date, chain, message_id, channel_id)
    return infos[0] if infos else None

def extract_chain_from_message(message_text: str) -> Optional[str]:
    """从消息文本中提取区块链信息
    
    Args:
        message_text: 需要解析的消息文本
        
    Returns:
        str: 提取到的链名称，未找到则返回None
    """
    if not message_text:
        return None
        
    # 清理消息文本，便于匹配
    text = message_text.lower()
    
    # 首先检查URL中是否包含链信息，这通常最可靠
    urls = find_urls_in_text(text)
    for url in urls:
        chain = get_chain_from_url(url)
        if chain:
            logger.info(f"从URL '{url}' 检测到链: {chain}")
            return chain
    
    # 通过市值单位判断链（高优先级判断）
    mc_patterns = {
        'BSC': r'(\bmc\b|\bmarket\s*cap\b|市值)[：:]*\s*[`\'"]*\d+(?:\.\d+)?\s*(?:bnb|BNB)',
        'ETH': r'(\bmc\b|\bmarket\s*cap\b|市值)[：:]*\s*[`\'"]*\d+(?:\.\d+)?\s*(?:eth|ETH)',
        'SOL': r'(\bmc\b|\bmarket\s*cap\b|市值)[：:]*\s*[`\'"]*\d+(?:\.\d+)?\s*(?:sol|SOL)',
        'ARB': r'(\bmc\b|\bmarket\s*cap\b|市值)[：:]*\s*[`\'"]*\d+(?:\.\d+)?\s*(?:arb|ARB)',
        'AVAX': r'(\bmc\b|\bmarket\s*cap\b|市值)[：:]*\s*[`\'"]*\d+(?:\.\d+)?\s*(?:avax|AVAX)',
        'MATIC': r'(\bmc\b|\bmarket\s*cap\b|市值)[：:]*\s*[`\'"]*\d+(?:\.\d+)?\s*(?:matic|MATIC|polygon)',
        'OP': r'(\bmc\b|\bmarket\s*cap\b|市值)[：:]*\s*[`\'"]*\d+(?:\.\d+)?\s*(?:op|OP|optimism)'
    }
    
    for chain, pattern in mc_patterns.items():
        if re.search(pattern, text, re.IGNORECASE):
            logger.info(f"从市值单位判断为{chain}链")
            return chain
    
    # 检查推特账号关键词
    twitter_match = re.search(r'(?:twitter\.com|x\.com)/([^/\s]+)', text)
    if twitter_match:
        twitter_user = twitter_match.group(1).lower()
        if twitter_user in TWITTER_CHAIN_MAP:
            logger.info(f"从推特账号 '{twitter_user}' 识别链为: {TWITTER_CHAIN_MAP[twitter_user]}")
            return TWITTER_CHAIN_MAP[twitter_user]
    
    # 检查机器人引用或频道名称
    bot_patterns = {
        'SOL': [r'solana_trojanbot', r'solana.*bot', r'sol.*alert'],
        'BSC': [r'ape\.bot', r'sigma_buybot.*bsc', r'pancakeswap_bot', r'bnb.*bot', r'bsc.*alert'],
        'ETH': [r'uniswap_bot', r'sigma_buybot.*eth', r'eth.*alert', r'ethereum.*bot'],
        'ARB': [r'arb.*bot', r'arbitrum.*alert'],
        'AVAX': [r'avax.*bot', r'avalanche.*alert'],
        'MATIC': [r'polygon.*bot', r'matic.*alert'],
        'BASE': [r'base.*bot', r'base.*alert'],
        'OP': [r'optimism.*bot', r'op.*alert']
    }
    
    for chain, patterns in bot_patterns.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                logger.info(f"从机器人/频道名称引用提取到链信息: {chain}, 匹配模式: {pattern}")
                return chain
    
    # 检查明确的链标识符（如#BSC、#ETH、#SOL等)
    tag_patterns = {
        'BSC': r'(?:^|\s)#(?:bsc|bnb|binance)(?:\s|$)',
        'ETH': r'(?:^|\s)#(?:eth|ethereum)(?:\s|$)',
        'SOL': r'(?:^|\s)#(?:sol|solana)(?:\s|$)',
        'ARB': r'(?:^|\s)#(?:arb|arbitrum)(?:\s|$)',
        'AVAX': r'(?:^|\s)#(?:avax|avalanche)(?:\s|$)',
        'MATIC': r'(?:^|\s)#(?:matic|polygon)(?:\s|$)',
        'BASE': r'(?:^|\s)#(?:base)(?:\s|$)',
        'OP': r'(?:^|\s)#(?:op|optimism)(?:\s|$)'
    }
    
    for chain, pattern in tag_patterns.items():
        if re.search(pattern, text, re.IGNORECASE):
            logger.info(f"从标签提取到链信息: {chain}")
            return chain
    
    # 检查消息中是否包含地址格式，用于推断链类型
    evm_address = re.search(r'\b0x[0-9a-fA-F]{40}\b', text)
    if evm_address:
        # 如果找到EVM地址，尝试从上下文确定具体链
        for chain, keywords in CHAINS.items():
            if chain in EVM_CHAINS:  # 仅检查EVM链
                for keyword in keywords:
                    if keyword.lower() in text:
                        logger.info(f"从上下文({keyword})和EVM地址格式推断为{chain}链")
                        return chain
        
        # 如果没有特定关键词，尝试检查网络费用相关术语
        fee_patterns = {
            'ETH': [r'gas\s+(?:fee|price)', r'gwei', r'gas\s+limit'],
            'BSC': [r'bnb\s+(?:fee|gas)', r'gwei.*bnb'],
            'ARB': [r'arb\s+(?:fee|gas)', r'gwei.*arb'],
            'MATIC': [r'matic\s+(?:fee|gas)', r'gwei.*matic'],
        }
        
        for chain, patterns in fee_patterns.items():
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    logger.info(f"从网络费用术语推断为{chain}链")
                    return chain
        
        # 检查币对描述
        pair_patterns = {
            'BSC': [r'\b(?:bnb|busd)/[a-z0-9]+\b', r'\b[a-z0-9]+/(?:bnb|busd)\b'],
            'ETH': [r'\b(?:eth|usdt)/[a-z0-9]+\b', r'\b[a-z0-9]+/(?:eth|usdt)\b'],
            'SOL': [r'\b(?:sol|usdc)/[a-z0-9]+\b', r'\b[a-z0-9]+/(?:sol|usdc)\b']
        }
        
        for chain, patterns in pair_patterns.items():
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    logger.info(f"从交易对描述推断为{chain}链")
                    return chain
        
        # 如果还是没有识别到，默认返回BSC（作为最常见的EVM链）
        logger.warning("检测到EVM格式地址但无法确定具体链，默认设置为BSC(最常见的EVM链)")
        return 'BSC'
    
    # 检查是否有Solana格式地址
    solana_address = re.search(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b', text)
    if solana_address and ('sol' in text or 'solana' in text):
        logger.info("从合约地址格式和SOL关键词推断为SOL链")
        return 'SOL'
    
    # 从消息文本中寻找最频繁出现的链相关词汇
    chain_mentions = {}
    for chain, keywords in CHAINS.items():
        mentions = 0
        for keyword in keywords:
            mentions += len(re.findall(rf'\b{re.escape(keyword.lower())}\b', text))
        if mentions > 0:
            chain_mentions[chain] = mentions
    
    # 如果存在链提及，返回提及最多的链
    if chain_mentions:
        most_mentioned = max(chain_mentions.items(), key=lambda x: x[1])
        logger.info(f"从关键词频率分析，'{most_mentioned[0]}'链被提及{most_mentioned[1]}次，判定为该链")
        return most_mentioned[0]
    
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
        # 使用辅助函数获取所有URL
        urls = find_urls_in_text(text)
        
        if not urls:
            return None
            
        if keyword:
            # 如果指定了关键词，优先返回包含关键词的URL
            for url in urls:
                if keyword.lower() in url.lower():
                    return url
        
        # 如果没有指定关键词或没有找到包含关键词的URL，返回第一个URL
        return urls[0] if urls else None

    except Exception as e:
        logger.error(f"从文本中提取URL时出错: {str(e)}")
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
        # 处理URL中常见的非法字符和格式问题
        url = url.split('#')[0].split('?')[0]  # 移除URL中的fragment和query部分
        url_lower = url.lower()
        # 1. 处理专门的代币信息平台URL
        # GMGN.ai格式的URL
        # 格式如: https://gmgn.ai/bsc/token/0x04e8f6a9e5765df0e5105bbc7ba6b562f8104444
        gmgn_match = re.search(r'(?:https?://)?(?:www\.)?gmgn\.ai(?:/[^/]+)?/([^/]+)/token/([a-zA-Z0-9]{20,})', url, re.IGNORECASE)
        if gmgn_match:
            chain_str = gmgn_match.group(1).upper()
            contract = gmgn_match.group(2)
            # 映射到标准链标识
            chain_map = {
                'BSC': 'BSC',
                'ETH': 'ETH',
                'ETHEREUM': 'ETH',
                'ARBITRUM': 'ARB',
                'BASE': 'BASE',
                'SOLANA': 'SOL',
                'POLYGON': 'MATIC',
                'OPTIMISM': 'OP',
                'AVALANCHE': 'AVAX'
            }
            chain = chain_map.get(chain_str, chain_str)
            return contract, chain
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
            return contract, chain
        # GeckoTerminal格式
        # 例如: https://www.geckoterminal.com/eth/pools/0x1234...
        geckoterminal_pattern = r'(?:https?://)?(?:www\.)?geckoterminal\.com/([a-zA-Z0-9]+)/(?:pools|tokens)/([a-zA-Z0-9]{20,})'
        match = re.search(geckoterminal_pattern, url_lower)
        if match:
            chain_str = match.group(1).lower()
            contract = match.group(2)
            geckoterminal_map = {
                'sol': 'SOL',
                'solana': 'SOL',
                'eth': 'ETH',
                'ethereum': 'ETH',
                'bsc': 'BSC',
                'arb': 'ARB',
                'arbitrum': 'ARB',
                'base': 'BASE',
                'avax': 'AVAX',
                'avalanche': 'AVAX',
                'matic': 'MATIC',
                'polygon': 'MATIC',
                'op': 'OP',
                'optimism': 'OP'
            }
            chain = geckoterminal_map.get(chain_str)
            return contract, chain
        # CoinGecko格式
        # 例如: https://www.coingecko.com/en/coins/ethereum/0x1234...
        coingecko_pattern = r'(?:https?://)?(?:www\.)?coingecko\.com/[^/]+/coins/([a-zA-Z0-9-]+)/([a-zA-Z0-9]{20,})'
        match = re.search(coingecko_pattern, url_lower)
        if match:
            chain_str = match.group(1).lower()
            contract = match.group(2)
            coingecko_map = {
                'solana': 'SOL',
                'ethereum': 'ETH',
                'binance-smart-chain': 'BSC',
                'arbitrum-one': 'ARB',
                'base': 'BASE',
                'avalanche': 'AVAX',
                'polygon-pos': 'MATIC',
                'optimistic-ethereum': 'OP'
            }
            chain = coingecko_map.get(chain_str)
            return contract, chain
        # 2. 处理区块浏览器URL
        # 循环检查各个区块浏览器
        for chain, explorers in CHAIN_EXPLORERS.items():
            for explorer in explorers:
                if explorer in url_lower:
                    # 提取合约地址
                    explorer_pattern = rf'(?:https?://)?(?:www\.)?{re.escape(explorer)}/(?:token|address|account|contracts)/([a-zA-Z0-9]{{20,}})'
                    explorer_match = re.search(explorer_pattern, url_lower)
                    if explorer_match:
                        contract = explorer_match.group(1)
                        # 检查合约地址格式
                        if chain != 'SOL' and contract.startswith('0x') and len(contract) >= 40:
                            return contract, chain
                        elif chain == 'SOL' and not contract.startswith('0x'):
                            return contract, chain
                        else:
                            # 尝试在URL中寻找正确格式的地址
                            if chain != 'SOL':
                                evm_address = re.search(r'0x[a-fA-F0-9]{40}', url)
                                if evm_address:
                                    return evm_address.group(0), chain
                            else:
                                solana_address = re.search(r'[1-9A-HJ-NP-Za-km-z]{32,44}', url)
                                if solana_address:
                                    return solana_address.group(0), 'SOL'
        # 3. 处理DEX和流动性平台URL
        # 检查DEX平台URL
        for chain, patterns in DEX_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, url_lower):
                    # 提取合约地址
                    dex_contract = re.search(r'/([a-zA-Z0-9]{20,})', url)
                    if dex_contract:
                        contract = dex_contract.group(1)
                        return contract, chain
                    # 如果没有直接找到，尝试根据链类型寻找相应格式的地址
                    if chain != 'SOL':
                        evm_address = re.search(r'0x[a-fA-F0-9]{40}', url)
                        if evm_address:
                            return evm_address.group(0), chain
                    else:
                        solana_address = re.search(r'[1-9A-HJ-NP-Za-km-z]{32,44}', url)
                        if solana_address:
                            return solana_address.group(0), 'SOL'
        # 4. 处理其他常见的代币信息URL格式
        # 比如: https://coinmarketcap.com/currencies/[token-name]/
        # 或 https://www.mexc.com/exchange/[TOKEN]_USDT
        exchange_patterns = [
            # Coinmarketcap - 不包含合约地址，但可能有助于确定链
            (r'(?:https?://)?(?:www\.)?coinmarketcap\.com/currencies/([a-zA-Z0-9-]+)', None),
            # Binance
            (r'(?:https?://)?(?:www\.)?binance\.com/[^/]+/trade/([A-Z0-9]+)_([A-Z0-9]+)', 'BSC'),
            # MEXC
            (r'(?:https?://)?(?:www\.)?mexc\.com/exchange/([A-Z0-9]+)_([A-Z0-9]+)', None)
        ]
        for pattern, default_chain in exchange_patterns:
            match = re.search(pattern, url)
            if match:
                if default_chain:
                    # 尝试从URL的其他部分提取合约地址
                    evm_address = re.search(r'0x[a-fA-F0-9]{40}', url)
                    if evm_address:
                        return evm_address.group(0), default_chain
                    solana_address = re.search(r'[1-9A-HJ-NP-Za-km-z]{32,44}', url)
                    if solana_address and default_chain == 'SOL':
                        return solana_address.group(0), 'SOL'
        # 5. 最后尝试直接从URL中提取合约地址格式
        # 获取URL中暗示的链信息
        chain_from_url = get_chain_from_url(url)
        # 根据链类型尝试提取对应格式的地址
        if chain_from_url == 'SOL':
            solana_match = re.search(r'[1-9A-HJ-NP-Za-km-z]{32,44}', url)
            if solana_match:
                contract = solana_match.group(0)
                return contract, 'SOL'
        elif chain_from_url in EVM_CHAINS:
            evm_match = re.search(r'0x[a-fA-F0-9]{40}', url)
            if evm_match:
                contract = evm_match.group(0)
                return contract, chain_from_url
        else:
            # 没有明确的链信息，尝试提取任何格式的地址
            evm_match = re.search(r'0x[a-fA-F0-9]{40}', url)
            if evm_match:
                contract = evm_match.group(0)
                # 尝试从URL关键词判断链
                if 'bsc' in url_lower or 'binance' in url_lower:
                    return contract, 'BSC'
                elif 'eth' in url_lower or 'ethereum' in url_lower:
                    return contract, 'ETH'
                # 扩展支持其他链
                elif 'arb' in url_lower or 'arbitrum' in url_lower:
                    return contract, 'ARB'
                elif 'base' in url_lower:
                    return contract, 'BASE'
                elif 'matic' in url_lower or 'polygon' in url_lower:
                    return contract, 'MATIC'
                elif 'avax' in url_lower or 'avalanche' in url_lower:
                    return contract, 'AVAX'
                elif 'op' in url_lower or 'optimism' in url_lower:
                    return contract, 'OP'
                else:
                    return contract, None
            # 尝试提取Solana格式地址
            solana_match = re.search(r'[1-9A-HJ-NP-Za-km-z]{32,44}', url)
            if solana_match and ('sol' in url_lower or 'solana' in url_lower):
                contract = solana_match.group(0)
                return contract, 'SOL'
        logger.debug(f"未能从URL中提取合约地址: {url}")
        return None, None
    except Exception as e:
        logger.error(f"从URL中提取合约地址时出错: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        return None, None

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

# 测试函数，用于验证重构是否正常工作
def test_message_extraction():
    """
    测试函数，用于验证重构后的信息提取功能是否正常工作
    此函数仅用于测试，不应该在生产环境中调用
    
    Returns:
        dict: 测试结果
    """
    test_results = {}
    
    # 测试URL提取
    test_results['url_extraction'] = {}
    url_tests = [
        {
            'text': '请查看以下链接: https://etherscan.io/token/0x1234567890abcdef1234567890abcdef12345678',
            'expected': 'https://etherscan.io/token/0x1234567890abcdef1234567890abcdef12345678'
        },
        {
            'text': '网站: www.example.com，联系我们',
            'expected': 'www.example.com'
        },
        {
            'text': '没有URL的文本',
            'expected': None
        }
    ]
    
    for i, test in enumerate(url_tests):
        result = extract_url_from_text(test['text'])
        test_results['url_extraction'][f'test_{i+1}'] = {
            'input': test['text'],
            'expected': test['expected'],
            'result': result,
            'passed': result == test['expected']
        }
    
    # 测试链提取
    test_results['chain_extraction'] = {}
    chain_tests = [
        {
            'text': '这是BSC上的新代币，链接: https://bscscan.com',
            'expected': 'BSC'
        },
        {
            'text': '索拉纳上的NFT项目很热门',
            'expected': 'SOL'
        },
        {
            'text': '市值: 100 BNB，价格...',
            'expected': 'BSC'
        },
        {
            'text': '合约地址: 0x1234567890abcdef1234567890abcdef12345678',
            'expected': 'BSC'  # 默认EVM地址为BSC
        }
    ]
    
    for i, test in enumerate(chain_tests):
        result = extract_chain_from_message(test['text'])
        test_results['chain_extraction'][f'test_{i+1}'] = {
            'input': test['text'],
            'expected': test['expected'],
            'result': result,
            'passed': result == test['expected']
        }
    
    # 测试合约地址提取
    test_results['contract_extraction'] = {}
    contract_tests = [
        {
            'url': 'https://etherscan.io/token/0x1234567890abcdef1234567890abcdef12345678',
            'expected_contract': '0x1234567890abcdef1234567890abcdef12345678',
            'expected_chain': 'ETH'
        },
        {
            'url': 'https://bscscan.com/address/0xabcdef1234567890abcdef1234567890abcdef12',
            'expected_contract': '0xabcdef1234567890abcdef1234567890abcdef12',
            'expected_chain': 'BSC'
        },
        {
            'url': 'https://solscan.io/token/8WJ2ngd7FpHVkWiQTNyJ3N9j1oDmjR5e6MFdDAKQNinF',
            'expected_contract': '8WJ2ngd7FpHVkWiQTNyJ3N9j1oDmjR5e6MFdDAKQNinF',
            'expected_chain': 'SOL'
        }
    ]
    
    for i, test in enumerate(contract_tests):
        contract, chain = extract_contract_from_url(test['url'])
        test_results['contract_extraction'][f'test_{i+1}'] = {
            'input': test['url'],
            'expected_contract': test['expected_contract'],
            'expected_chain': test['expected_chain'],
            'result_contract': contract,
            'result_chain': chain,
            'passed': contract == test['expected_contract'] and chain == test['expected_chain']
        }
    
    # 测试完整的代币信息提取
    test_results['promotion_info_extraction'] = {}
    promotion_tests = [
        {
            'text': 'New Token $ABC\n合约地址: 0x1234567890abcdef1234567890abcdef12345678\n链: BSC\n市值: 100 BNB',
            'date': datetime.now(),
            'expected_contract': '0x1234567890abcdef1234567890abcdef12345678',
            'expected_chain': 'BSC',
            'expected_symbol': 'ABC'
        },
        {
            'text': 'SOL代币，符号: $XYZ\n合约: 8WJ2ngd7FpHVkWiQTNyJ3N9j1oDmjR5e6MFdDAKQNinF\n市值: 50 SOL',
            'date': datetime.now(),
            'expected_contract': '8WJ2ngd7FpHVkWiQTNyJ3N9j1oDmjR5e6MFdDAKQNinF',
            'expected_chain': 'SOL',
            'expected_symbol': 'XYZ'
        }
    ]
    
    for i, test in enumerate(promotion_tests):
        info = extract_promotion_info(test['text'], test['date'])
        test_passed = False
        if info:
            test_passed = (
                info.contract_address == test['expected_contract'] and
                info.chain == test['expected_chain'] and
                info.token_symbol == test['expected_symbol']
            )
        
        test_results['promotion_info_extraction'][f'test_{i+1}'] = {
            'input': test['text'],
            'expected_contract': test['expected_contract'],
            'expected_chain': test['expected_chain'],
            'expected_symbol': test['expected_symbol'],
            'result': info.__dict__ if info else None,
            'passed': test_passed
        }
    
    # 统计测试结果
    total_tests = sum(len(category) for category in test_results.values())
    passed_tests = sum(
        sum(1 for test in category.values() if test['passed'])
        for category in test_results.values()
    )
    
    test_results['summary'] = {
        'total_tests': total_tests,
        'passed_tests': passed_tests,
        'success_rate': f"{(passed_tests / total_tests * 100):.2f}%" if total_tests > 0 else "N/A"
    }
    
    return test_results
