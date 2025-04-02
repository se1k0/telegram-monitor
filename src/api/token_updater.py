#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
代币数据更新模块
用于在项目运行期间自动更新代币数据
设计为每小时整点自动执行
"""

import logging
import random
import time
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional
from sqlalchemy.orm import Session

from src.database.models import engine, Token
from sqlalchemy.orm import sessionmaker
from src.api.token_market_updater import update_token_market_and_txn_data

# 设置日志
logger = logging.getLogger(__name__)

# 创建会话工厂
Session = sessionmaker(bind=engine)

class TokenDataUpdater:
    """代币数据更新器"""
    
    def __init__(self):
        """初始化更新器"""
        self.running = False
        self.default_batch_size = 50
        self.default_min_delay = 0.5
        self.default_max_delay = 2.0
    
    def get_tokens_to_update(self, limit: int = None) -> List[Dict[str, str]]:
        """
        获取需要更新的代币列表
        
        Args:
            limit: 最大返回数量，None表示不限制
            
        Returns:
            List[Dict]: 代币信息列表，每个字典包含chain和contract
        """
        session = Session()
        try:
            query = session.query(Token)
            
            if limit:
                query = query.limit(limit)
                
            tokens = query.all()
            
            return [{"chain": token.chain, "contract": token.contract, 
                    "symbol": token.token_symbol} for token in tokens]
        except Exception as e:
            logger.error(f"获取代币列表时发生错误: {str(e)}")
            return []
        finally:
            session.close()
    
    def update_token(self, chain: str, contract: str, symbol: str) -> Dict[str, Any]:
        """
        更新单个代币数据
        
        Args:
            chain: 区块链名称
            contract: 代币合约地址
            symbol: 代币符号，用于日志
            
        Returns:
            Dict: 更新结果字典
        """
        session = Session()
        try:
            logger.info(f"更新代币 {symbol} ({chain}/{contract})")
            result = update_token_market_and_txn_data(session, chain, contract)
            
            if "error" in result:
                logger.warning(f"更新代币 {symbol} 失败: {result['error']}")
                return {"success": False, "error": result["error"]}
            
            logger.info(f"成功更新代币 {symbol}: 市值={result.get('marketCap', 'N/A')}, "
                       f"1小时买入={result.get('buys_1h', 'N/A')}, "
                       f"1小时卖出={result.get('sells_1h', 'N/A')}")
            
            return {"success": True, "result": result}
        except Exception as e:
            logger.error(f"更新代币 {symbol} 时发生异常: {str(e)}")
            return {"success": False, "error": str(e)}
        finally:
            session.close()
    
    def hourly_update(self, limit: int = 500, 
                      batch_size: int = None, 
                      min_delay: float = None, 
                      max_delay: float = None) -> Dict[str, Any]:
        """
        执行每小时更新任务
        
        Args:
            limit: 最大更新代币数量
            batch_size: 每批处理的代币数量
            min_delay: 请求间最小延迟(秒)
            max_delay: 请求间最大延迟(秒)
            
        Returns:
            Dict: 包含更新结果的字典
        """
        self.running = True
        
        # 使用默认值
        if batch_size is None:
            batch_size = self.default_batch_size
        if min_delay is None:
            min_delay = self.default_min_delay
        if max_delay is None:
            max_delay = self.default_max_delay
        
        logger.info("="*50)
        logger.info(f"开始每小时代币数据更新: {datetime.now()}")
        
        results = {
            "start_time": datetime.now(),
            "end_time": None,
            "total": 0,
            "success": 0,
            "failed": 0,
            "details": []
        }
        
        try:
            # 获取需要更新的代币列表
            tokens = self.get_tokens_to_update(limit)
            
            if not tokens:
                logger.warning("没有找到需要更新的代币")
                results["end_time"] = datetime.now()
                results["duration"] = results["end_time"] - results["start_time"]
                return results
            
            results["total"] = len(tokens)
            logger.info(f"找到 {len(tokens)} 个代币需要更新")
            
            # 随机打乱代币列表顺序，避免按相同顺序请求
            random.shuffle(tokens)
            
            # 分批处理
            for i in range(0, len(tokens), batch_size):
                if not self.running:
                    logger.info("更新过程被中断")
                    break
                    
                batch = tokens[i:i+batch_size]
                logger.info(f"处理第 {i//batch_size + 1} 批，共 {len(batch)} 个代币")
                
                # 处理每个批次中的代币
                for token in batch:
                    if not self.running:
                        logger.info("更新过程被中断")
                        break
                        
                    # 随机延迟，模拟人工操作
                    delay = min_delay + random.random() * (max_delay - min_delay)
                    time.sleep(delay)
                    
                    # 更新代币数据
                    result = self.update_token(
                        token["chain"], 
                        token["contract"],
                        token["symbol"]
                    )
                    
                    # 记录结果
                    if result["success"]:
                        results["success"] += 1
                    else:
                        results["failed"] += 1
                        # 检查是否达到API限制
                        error = result.get("error", "")
                        if isinstance(error, str) and ("rate limit" in error.lower() or "too many requests" in error.lower()):
                            logger.warning("检测到API速率限制，增加等待时间...")
                            time.sleep(max_delay * 3)  # 遇到速率限制，等待更长时间
                    
                    results["details"].append({
                        "chain": token["chain"],
                        "symbol": token["symbol"],
                        "contract": token["contract"],
                        "success": result["success"],
                        "error": result.get("error")
                    })
                
                # 批次之间的延迟更长一些，避免触发API限制
                batch_delay = max_delay * 2
                logger.info(f"批次完成，等待 {batch_delay:.2f} 秒后继续...")
                time.sleep(batch_delay)
            
        except Exception as e:
            logger.error(f"执行每小时更新任务时发生错误: {str(e)}")
            logger.error(traceback.format_exc())
            
        finally:
            # 记录结束时间和持续时间
            results["end_time"] = datetime.now()
            results["duration"] = results["end_time"] - results["start_time"]
            
            # 打印结果摘要
            logger.info("="*30)
            logger.info(f"更新完成: {datetime.now()}")
            logger.info(f"总代币数: {results['total']}")
            logger.info(f"成功: {results['success']}")
            logger.info(f"失败: {results['failed']}")
            logger.info(f"总用时: {results['duration']}")
            logger.info("="*50)
            
            self.running = False
            return results
    
    def stop(self):
        """停止正在进行的更新任务"""
        if self.running:
            logger.info("停止更新任务...")
            self.running = False
        else:
            logger.info("没有正在运行的更新任务")

# 创建全局更新器实例
token_updater = TokenDataUpdater()

# 直接调用的便捷函数
def hourly_update(limit: int = 500) -> Dict[str, Any]:
    """
    执行每小时更新任务的便捷函数
    
    Args:
        limit: 最大更新代币数量
        
    Returns:
        Dict: 包含更新结果的字典
    """
    return token_updater.hourly_update(limit=limit) 