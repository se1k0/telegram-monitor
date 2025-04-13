#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
监控代币数据异常变化

此脚本用于监控token表数据的异常变化，特别关注以下情况：
1. 市值突然变为0或空值
2. 社群覆盖人数和传播次数突然变为0
3. 其他关键字段意外变化

自动发送报警通知并记录异常，便于及时处理

使用方法：
    python monitor_token_data.py [--alert-level {low,medium,high}]
    
参数：
    --alert-level: 报警级别，默认为medium
"""

import os
import sys
import argparse
import asyncio
import logging
import json
import smtplib
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# 添加项目根目录到Python路径
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

# 导入项目模块
from src.utils.logger import setup_logger, get_logger
from src.database.db_factory import get_db_adapter
import config.settings as settings

# 设置日志
logger = get_logger(__name__)

# 异常变化阈值 (视为异常的变化百分比)
THRESHOLDS = {
    'low': 50,     # 50%的变化
    'medium': 30,  # 30%的变化
    'high': 10     # 10%的变化
}

# 处理参数
def parse_args():
    parser = argparse.ArgumentParser(description='监控代币数据异常变化')
    parser.add_argument('--alert-level', choices=['low', 'medium', 'high'], default='medium',
                        help='报警级别，默认为medium')
    return parser.parse_args()

async def get_token_snapshots():
    """获取代币数据快照"""
    db_adapter = get_db_adapter()
    
    # 获取所有代币数据
    tokens = await db_adapter.execute_query('tokens', 'select')
    
    if not tokens or not isinstance(tokens, list):
        logger.error("获取代币列表失败或结果为空")
        return []
    
    # 获取最新备份
    backup_dir = Path(project_root) / 'data' / 'backups'
    if not backup_dir.exists():
        logger.warning(f"备份目录 {backup_dir} 不存在，无法进行比较")
        return tokens
    
    backup_files = sorted(list(backup_dir.glob("tokens_backup_*.json")))
    if not backup_files:
        logger.warning("未找到历史备份文件，无法进行比较")
        return tokens
    
    # 获取最新的备份文件
    latest_backup = backup_files[-1]
    
    try:
        # 加载备份数据
        with open(latest_backup, 'r', encoding='utf-8') as f:
            backup_tokens = json.load(f)
        
        logger.info(f"加载历史备份: {latest_backup.name}, 包含 {len(backup_tokens)} 个代币")
        
        # 将当前和历史数据组合成快照
        snapshots = []
        
        # 创建备份数据的id索引
        backup_tokens_map = {token.get('id'): token for token in backup_tokens if token.get('id')}
        
        for token in tokens:
            token_id = token.get('id')
            if not token_id:
                continue
                
            # 查找对应的备份数据
            backup_token = backup_tokens_map.get(token_id)
            
            snapshots.append({
                'current': token,
                'previous': backup_token
            })
        
        return snapshots
        
    except Exception as e:
        logger.error(f"处理备份数据时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return tokens

def detect_anomalies(snapshots, alert_level='medium'):
    """检测异常变化"""
    threshold = THRESHOLDS.get(alert_level, 30)  # 默认为medium级别
    
    anomalies = []
    
    for snapshot in snapshots:
        current = snapshot.get('current')
        previous = snapshot.get('previous')
        
        # 如果没有上一版本数据，则跳过
        if not current or not previous:
            continue
            
        token_id = current.get('id')
        chain = current.get('chain')
        contract = current.get('contract')
        symbol = current.get('token_symbol')
        
        # 检查关键标识字段
        if not all([token_id, chain, contract]):
            continue
            
        # 异常列表
        issues = []
        
        # 1. 检查市值异常变化
        current_market_cap = current.get('market_cap', 0) or 0
        previous_market_cap = previous.get('market_cap', 0) or 0
        
        if previous_market_cap > 0 and current_market_cap == 0:
            issues.append({
                'field': 'market_cap',
                'severity': 'high',
                'description': f"市值从 ${previous_market_cap:.2f} 变为 $0",
                'previous': previous_market_cap,
                'current': current_market_cap
            })
        elif previous_market_cap > 0:
            # 计算变化百分比
            change_pct = abs(current_market_cap - previous_market_cap) / previous_market_cap * 100
            if change_pct > threshold and current_market_cap < previous_market_cap:
                issues.append({
                    'field': 'market_cap',
                    'severity': 'medium',
                    'description': f"市值下降 {change_pct:.2f}%: ${previous_market_cap:.2f} -> ${current_market_cap:.2f}",
                    'previous': previous_market_cap,
                    'current': current_market_cap
                })
        
        # 2. 检查社群覆盖人数异常变化
        current_reach = current.get('community_reach', 0) or 0
        previous_reach = previous.get('community_reach', 0) or 0
        
        if previous_reach > 0 and current_reach == 0:
            issues.append({
                'field': 'community_reach',
                'severity': 'high',
                'description': f"社群覆盖人数从 {previous_reach} 变为 0",
                'previous': previous_reach,
                'current': current_reach
            })
        elif previous_reach > 0:
            # 计算变化百分比
            change_pct = abs(current_reach - previous_reach) / previous_reach * 100
            if change_pct > threshold and current_reach < previous_reach:
                issues.append({
                    'field': 'community_reach',
                    'severity': 'medium',
                    'description': f"社群覆盖人数下降 {change_pct:.2f}%: {previous_reach} -> {current_reach}",
                    'previous': previous_reach,
                    'current': current_reach
                })
        
        # 3. 检查传播次数异常变化
        current_spread = current.get('spread_count', 0) or 0
        previous_spread = previous.get('spread_count', 0) or 0
        
        if previous_spread > 0 and current_spread == 0:
            issues.append({
                'field': 'spread_count',
                'severity': 'high',
                'description': f"传播次数从 {previous_spread} 变为 0",
                'previous': previous_spread,
                'current': current_spread
            })
        elif previous_spread > 0:
            # 计算变化百分比
            change_pct = abs(current_spread - previous_spread) / previous_spread * 100
            if change_pct > threshold and current_spread < previous_spread:
                issues.append({
                    'field': 'spread_count',
                    'severity': 'medium',
                    'description': f"传播次数下降 {change_pct:.2f}%: {previous_spread} -> {current_spread}",
                    'previous': previous_spread,
                    'current': current_spread
                })
        
        # 如果有异常，添加到列表
        if issues:
            anomalies.append({
                'id': token_id,
                'chain': chain,
                'contract': contract,
                'symbol': symbol,
                'issues': issues
            })
    
    return anomalies

def send_alert_email(anomalies):
    """发送报警邮件"""
    if not anomalies:
        logger.info("没有异常，不发送报警邮件")
        return
        
    try:
        # 获取邮件配置
        email_host = settings.EMAIL_HOST
        email_port = settings.EMAIL_PORT
        email_user = settings.EMAIL_USER
        email_password = settings.EMAIL_PASSWORD
        alert_recipients = settings.ALERT_RECIPIENTS.split(',')
        
        if not all([email_host, email_port, email_user, email_password, alert_recipients]):
            logger.warning("邮件配置不完整，无法发送报警")
            return
            
        # 创建邮件内容
        msg = MIMEMultipart()
        msg['Subject'] = f"[告警] 检测到 {len(anomalies)} 个代币数据异常 - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        msg['From'] = email_user
        msg['To'] = ", ".join(alert_recipients)
        
        # 邮件正文
        email_body = f"""<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .high {{ color: red; font-weight: bold; }}
        .medium {{ color: orange; }}
        .low {{ color: blue; }}
    </style>
</head>
<body>
    <h2>代币数据异常报警</h2>
    <p>系统检测到 {len(anomalies)} 个代币的数据出现异常变化:</p>
    
    <table>
        <tr>
            <th>代币</th>
            <th>问题字段</th>
            <th>严重性</th>
            <th>描述</th>
            <th>变化前</th>
            <th>变化后</th>
        </tr>
"""
        
        for anomaly in anomalies:
            symbol = anomaly.get('symbol') or f"{anomaly.get('chain')}/{anomaly.get('contract')}"
            
            for issue in anomaly.get('issues', []):
                severity_class = issue.get('severity', 'medium')
                email_body += f"""
        <tr>
            <td>{symbol}</td>
            <td>{issue.get('field')}</td>
            <td class="{severity_class}">{severity_class.upper()}</td>
            <td>{issue.get('description')}</td>
            <td>{issue.get('previous')}</td>
            <td>{issue.get('current')}</td>
        </tr>"""
                
        email_body += """
    </table>
    
    <p>请检查数据库中的相关记录，并采取必要的修复措施。</p>
    <p>您可以运行以下命令来修复问题:</p>
    <pre>python scripts/fix_token_data.py</pre>
    
    <p>此邮件由系统自动发送，请勿回复。</p>
</body>
</html>
"""
        
        # 添加HTML内容
        msg.attach(MIMEText(email_body, 'html'))
        
        # 发送邮件
        with smtplib.SMTP(email_host, email_port) as server:
            server.starttls()
            server.login(email_user, email_password)
            server.send_message(msg)
            
        logger.info(f"已成功发送报警邮件到: {', '.join(alert_recipients)}")
        
    except Exception as e:
        logger.error(f"发送报警邮件时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

async def main_async():
    # 解析命令行参数
    args = parse_args()
    
    logger.info("="*50)
    logger.info(f"开始监控代币数据异常变化, 报警级别: {args.alert_level}")
    
    try:
        # 获取代币数据快照
        snapshots = await get_token_snapshots()
        
        # 检测异常
        anomalies = detect_anomalies(snapshots, args.alert_level)
        
        # 记录结果
        if anomalies:
            logger.warning(f"检测到 {len(anomalies)} 个代币数据异常")
            
            # 详细记录每个异常
            for anomaly in anomalies:
                symbol = anomaly.get('symbol') or f"{anomaly.get('chain')}/{anomaly.get('contract')}"
                logger.warning(f"代币 {symbol} (ID={anomaly.get('id')}) 存在以下问题:")
                
                for issue in anomaly.get('issues', []):
                    logger.warning(f" - [{issue.get('severity', 'medium').upper()}] {issue.get('field')}: {issue.get('description')}")
            
            # 发送报警
            send_alert_email(anomalies)
        else:
            logger.info("没有检测到异常变化，代币数据正常")
        
    except Exception as e:
        logger.error(f"监控过程中发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
    
    return 0

def main():
    # 设置日志
    log_dir = Path(project_root) / 'logs'
    os.makedirs(log_dir, exist_ok=True)
    setup_logger(__name__, log_file=log_dir / f"{datetime.now().strftime('%Y-%m-%d')}_token_monitor.log")
    
    # 运行异步主函数
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    return asyncio.run(main_async())

if __name__ == "__main__":
    sys.exit(main()) 