#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
代币历史数据分析脚本

此脚本用于分析token_history表中的历史数据，并生成统计报告
可以分析特定代币的历史趋势，也可以生成全局市场趋势报告

使用方法：
    python token_history_analysis.py [--chain <chain>] [--symbol <symbol>] [--days <days>] [--output <file>]

参数：
    --chain: 限制为特定链的代币
    --symbol: 限制为特定符号的代币
    --days: 分析最近几天的数据，默认为30天
    --output: 输出报告文件路径，默认为标准输出
    --format: 输出格式，支持text, json, html，默认为text
"""

import os
import sys
import argparse
import asyncio
import logging
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import io

# 添加项目根目录到Python路径
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

# 导入项目模块
from src.utils.logger import setup_logger, get_logger
from src.database.db_factory import get_db_adapter

# 设置日志
logger = get_logger(__name__)

# 处理参数
def parse_args():
    parser = argparse.ArgumentParser(description='分析代币历史数据并生成报告')
    parser.add_argument('--chain', help='限制为特定链的代币')
    parser.add_argument('--symbol', help='限制为特定符号的代币')
    parser.add_argument('--days', type=int, default=30, help='分析最近几天的数据，默认为30天')
    parser.add_argument('--output', help='输出报告文件路径，默认为标准输出')
    parser.add_argument('--format', choices=['text', 'json', 'html'], default='text', help='输出格式')
    return parser.parse_args()

async def get_token_history_data(db_adapter, chain=None, symbol=None, days=30):
    """获取代币历史数据，使用Supabase表格API而非原生SQL"""
    try:
        # 计算开始日期
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        # 构建查询过滤条件
        filters = {}
        if chain:
            filters['chain'] = chain
        if symbol:
            filters['token_symbol'] = symbol
            
        # 执行查询
        result = await db_adapter.execute_query(
            'token_history',
            'select',
            filters=filters
        )
        
        if not result or not isinstance(result, list):
            logger.warning("未找到符合条件的历史数据")
            return []
            
        # 手动筛选日期范围
        filtered_result = []
        start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
        
        for record in result:
            if 'timestamp' in record:
                try:
                    # 处理不同格式的时间戳
                    if isinstance(record['timestamp'], str):
                        record_time = datetime.strptime(record['timestamp'].split('.')[0], '%Y-%m-%d %H:%M:%S')
                    elif isinstance(record['timestamp'], datetime):
                        record_time = record['timestamp']
                    else:
                        continue
                        
                    # 只保留开始日期之后的记录
                    if record_time >= start_datetime:
                        filtered_result.append(record)
                except Exception as e:
                    logger.error(f"处理时间戳时出错: {str(e)}")
                    continue
        
        # 按链、合约和时间戳排序
        filtered_result.sort(key=lambda x: (x.get('chain', ''), x.get('contract', ''), 
                                           x.get('timestamp', '') if isinstance(x.get('timestamp', ''), str) else ''))
        
        logger.info(f"获取到 {len(filtered_result)} 条历史数据记录")
        return filtered_result
        
    except Exception as e:
        logger.error(f"查询历史数据时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return []

async def analyze_token_history(history_data):
    """分析代币历史数据，生成统计指标"""
    
    if not history_data:
        return {}
        
    # 转换为Pandas DataFrame以便分析
    df = pd.DataFrame(history_data)
    
    # 确保时间戳列是datetime类型
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # 按代币分组
    tokens_df = df.groupby(['chain', 'contract', 'token_symbol'])
    
    # 分析结果
    analysis_results = []
    
    # 分析每个代币的数据
    for (chain, contract, symbol), token_df in tokens_df:
        # 排序数据
        token_df = token_df.sort_values('timestamp')
        
        # 基本信息
        token_info = {
            'chain': chain,
            'contract': contract,
            'symbol': symbol,
            'data_points': len(token_df),
            'start_date': token_df['timestamp'].min().strftime('%Y-%m-%d'),
            'end_date': token_df['timestamp'].max().strftime('%Y-%m-%d')
        }
        
        # 市值分析
        market_cap_series = token_df['market_cap'].dropna()
        if not market_cap_series.empty:
            token_info['market_cap'] = {
                'latest': market_cap_series.iloc[-1],
                'min': market_cap_series.min(),
                'max': market_cap_series.max(),
                'mean': market_cap_series.mean(),
                'median': market_cap_series.median(),
                'std': market_cap_series.std(),
                'change': ((market_cap_series.iloc[-1] - market_cap_series.iloc[0]) / market_cap_series.iloc[0] * 100) if market_cap_series.iloc[0] > 0 else 0
            }
        
        # 价格分析
        price_series = token_df['price'].dropna()
        if not price_series.empty:
            token_info['price'] = {
                'latest': price_series.iloc[-1],
                'min': price_series.min(),
                'max': price_series.max(),
                'mean': price_series.mean(),
                'median': price_series.median(),
                'std': price_series.std(),
                'change': ((price_series.iloc[-1] - price_series.iloc[0]) / price_series.iloc[0] * 100) if price_series.iloc[0] > 0 else 0
            }
        
        # 社群覆盖分析
        community_series = token_df['community_reach'].dropna()
        if not community_series.empty:
            token_info['community_reach'] = {
                'latest': community_series.iloc[-1],
                'min': community_series.min(),
                'max': community_series.max(),
                'growth': community_series.iloc[-1] - community_series.iloc[0],
                'growth_pct': ((community_series.iloc[-1] - community_series.iloc[0]) / community_series.iloc[0] * 100) if community_series.iloc[0] > 0 else 0
            }
        
        # 传播次数分析
        spread_series = token_df['spread_count'].dropna()
        if not spread_series.empty:
            token_info['spread_count'] = {
                'latest': spread_series.iloc[-1],
                'min': spread_series.min(),
                'max': spread_series.max(),
                'growth': spread_series.iloc[-1] - spread_series.iloc[0],
                'growth_pct': ((spread_series.iloc[-1] - spread_series.iloc[0]) / spread_series.iloc[0] * 100) if spread_series.iloc[0] > 0 else 0
            }
        
        # 交易量分析
        volume_series = token_df['volume_24h'].dropna()
        if not volume_series.empty:
            token_info['volume_24h'] = {
                'latest': volume_series.iloc[-1],
                'mean': volume_series.mean(),
                'max': volume_series.max()
            }
        
        # 交易频率分析
        buys_series = token_df['buys_1h'].dropna()
        sells_series = token_df['sells_1h'].dropna()
        if not buys_series.empty and not sells_series.empty:
            token_info['trades'] = {
                'buys_1h_mean': buys_series.mean(),
                'sells_1h_mean': sells_series.mean(),
                'buy_sell_ratio': buys_series.mean() / sells_series.mean() if sells_series.mean() > 0 else 0
            }
        
        # 计算相关性
        if not market_cap_series.empty and not community_series.empty:
            token_info['correlations'] = {
                'market_cap_vs_community': market_cap_series.corr(community_series),
                'market_cap_vs_spread': market_cap_series.corr(spread_series) if not spread_series.empty else 0
            }
        
        # 添加到结果列表
        analysis_results.append(token_info)
    
    # 添加总体市场趋势
    market_trends = {
        'total_tokens': len(analysis_results),
        'avg_market_cap_change': np.mean([t['market_cap']['change'] for t in analysis_results if 'market_cap' in t]),
        'avg_community_growth': np.mean([t['community_reach']['growth_pct'] for t in analysis_results if 'community_reach' in t]),
        'tokens_with_positive_growth': sum(1 for t in analysis_results if 'market_cap' in t and t['market_cap']['change'] > 0),
        'tokens_with_negative_growth': sum(1 for t in analysis_results if 'market_cap' in t and t['market_cap']['change'] < 0)
    }
    
    # 排序结果 - 按市值变化率排序
    analysis_results.sort(key=lambda x: x['market_cap']['change'] if 'market_cap' in x else 0, reverse=True)
    
    return {
        'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'period_days': int((datetime.strptime(analysis_results[0]['end_date'], '%Y-%m-%d') - 
                         datetime.strptime(analysis_results[0]['start_date'], '%Y-%m-%d')).days) if analysis_results else 0,
        'market_trends': market_trends,
        'tokens': analysis_results
    }

def generate_text_report(analysis_result):
    """生成文本格式的报告"""
    if not analysis_result:
        return "没有可用的分析数据。"
        
    output = io.StringIO()
    
    print("========== 代币历史数据分析报告 ==========", file=output)
    print(f"分析日期: {analysis_result['analysis_date']}", file=output)
    print(f"分析周期: {analysis_result['period_days']} 天", file=output)
    print("\n---------- 市场趋势概述 ----------", file=output)
    print(f"总分析代币数: {analysis_result['market_trends']['total_tokens']}", file=output)
    print(f"平均市值变化率: {analysis_result['market_trends']['avg_market_cap_change']:.2f}%", file=output)
    print(f"平均社群增长率: {analysis_result['market_trends']['avg_community_growth']:.2f}%", file=output)
    print(f"市值增长代币数: {analysis_result['market_trends']['tokens_with_positive_growth']}", file=output)
    print(f"市值下降代币数: {analysis_result['market_trends']['tokens_with_negative_growth']}", file=output)
    
    print("\n---------- 代币详细分析 ----------", file=output)
    for idx, token in enumerate(analysis_result['tokens'][:10], 1):  # 仅显示前10个
        print(f"\n{idx}. {token['symbol']} ({token['chain']}/{token['contract'][:8]}...)", file=output)
        print(f"   数据点数: {token['data_points']}, 时间范围: {token['start_date']} 到 {token['end_date']}", file=output)
        
        if 'market_cap' in token:
            print(f"   市值变化: {token['market_cap']['change']:.2f}%, 当前值: ${token['market_cap']['latest']:.2f}", file=output)
            
        if 'community_reach' in token:
            print(f"   社群覆盖增长: {token['community_reach']['growth_pct']:.2f}%, 当前值: {token['community_reach']['latest']}", file=output)
            
        if 'correlations' in token:
            print(f"   市值与社群相关性: {token['correlations']['market_cap_vs_community']:.2f}", file=output)
    
    if len(analysis_result['tokens']) > 10:
        print(f"\n... 另外还有 {len(analysis_result['tokens']) - 10} 个代币的分析结果 ...", file=output)
    
    print("\n========== 报告结束 ==========", file=output)
    
    return output.getvalue()

def generate_json_report(analysis_result):
    """生成JSON格式的报告"""
    if not analysis_result:
        return "{}"
        
    return json.dumps(analysis_result, indent=2, ensure_ascii=False)

def generate_html_report(analysis_result):
    """生成HTML格式的报告"""
    if not analysis_result:
        return "<html><body><h1>没有可用的分析数据</h1></body></html>"
        
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>代币历史数据分析报告</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            color: #333;
        }}
        h1, h2, h3 {{
            color: #2c3e50;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        .report-header {{
            background-color: #f8f9fa;
            padding: 20px;
            border-radius: 5px;
            margin-bottom: 20px;
        }}
        .market-trends {{
            background-color: #e8f4f8;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 30px;
        }}
        .token-card {{
            background-color: #fff;
            border: 1px solid #ddd;
            border-radius: 5px;
            padding: 15px;
            margin-bottom: 15px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .token-header {{
            display: flex;
            justify-content: space-between;
            border-bottom: 1px solid #eee;
            padding-bottom: 10px;
            margin-bottom: 10px;
        }}
        .positive {{
            color: #28a745;
        }}
        .negative {{
            color: #dc3545;
        }}
        .token-stats {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
            gap: 10px;
        }}
        .stat-card {{
            background-color: #f8f9fa;
            padding: 10px;
            border-radius: 5px;
        }}
        .stat-title {{
            font-weight: bold;
            margin-bottom: 5px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
        }}
        th, td {{
            padding: 8px 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background-color: #f2f2f2;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="report-header">
            <h1>代币历史数据分析报告</h1>
            <p>分析日期: {analysis_result['analysis_date']}</p>
            <p>分析周期: {analysis_result['period_days']} 天</p>
        </div>
        
        <div class="market-trends">
            <h2>市场趋势概述</h2>
            <table>
                <tr>
                    <td>总分析代币数</td>
                    <td>{analysis_result['market_trends']['total_tokens']}</td>
                </tr>
                <tr>
                    <td>平均市值变化率</td>
                    <td class="{('positive' if analysis_result['market_trends']['avg_market_cap_change'] >= 0 else 'negative')}">
                        {analysis_result['market_trends']['avg_market_cap_change']:.2f}%
                    </td>
                </tr>
                <tr>
                    <td>平均社群增长率</td>
                    <td class="{('positive' if analysis_result['market_trends']['avg_community_growth'] >= 0 else 'negative')}">
                        {analysis_result['market_trends']['avg_community_growth']:.2f}%
                    </td>
                </tr>
                <tr>
                    <td>市值增长代币数</td>
                    <td>{analysis_result['market_trends']['tokens_with_positive_growth']}</td>
                </tr>
                <tr>
                    <td>市值下降代币数</td>
                    <td>{analysis_result['market_trends']['tokens_with_negative_growth']}</td>
                </tr>
            </table>
        </div>
        
        <h2>代币详细分析</h2>
    """
    
    for idx, token in enumerate(analysis_result['tokens'], 1):
        # 提取数据
        market_cap_change = token['market_cap']['change'] if 'market_cap' in token else 0
        market_cap_latest = token['market_cap']['latest'] if 'market_cap' in token else 0
        community_growth = token['community_reach']['growth_pct'] if 'community_reach' in token else 0
        community_latest = token['community_reach']['latest'] if 'community_reach' in token else 0
        
        html += f"""
        <div class="token-card">
            <div class="token-header">
                <h3>{token['symbol']} ({token['chain']}/{token['contract'][:8]}...)</h3>
                <div>
                    <span>数据点数: {token['data_points']}</span>
                    <span>时间范围: {token['start_date']} 到 {token['end_date']}</span>
                </div>
            </div>
            
            <div class="token-stats">
                <div class="stat-card">
                    <div class="stat-title">市值变化</div>
                    <div class="{('positive' if market_cap_change >= 0 else 'negative')}">{market_cap_change:.2f}%</div>
                    <div>当前值: ${market_cap_latest:,.2f}</div>
                </div>
                
                <div class="stat-card">
                    <div class="stat-title">社群覆盖增长</div>
                    <div class="{('positive' if community_growth >= 0 else 'negative')}">{community_growth:.2f}%</div>
                    <div>当前值: {community_latest:,}</div>
                </div>
        """
        
        if 'correlations' in token:
            corr = token['correlations']['market_cap_vs_community']
            html += f"""
                <div class="stat-card">
                    <div class="stat-title">市值与社群相关性</div>
                    <div>{corr:.2f}</div>
                </div>
            """
            
        # 交易数据
        if 'trades' in token:
            html += f"""
                <div class="stat-card">
                    <div class="stat-title">买卖比率</div>
                    <div>{token['trades']['buy_sell_ratio']:.2f}</div>
                    <div>平均买入: {token['trades']['buys_1h_mean']:.2f}/小时</div>
                    <div>平均卖出: {token['trades']['sells_1h_mean']:.2f}/小时</div>
                </div>
            """
            
        html += """
            </div>
        </div>
        """
    
    html += """
    </div>
</body>
</html>
    """
    
    return html

async def main_async():
    # 解析命令行参数
    args = parse_args()
    
    logger.info("="*50)
    logger.info("开始分析代币历史数据")
    
    if args.chain:
        logger.info(f"限制链: {args.chain}")
    if args.symbol:
        logger.info(f"限制符号: {args.symbol}")
    
    logger.info(f"分析周期: 最近 {args.days} 天")
    
    try:
        # 获取数据库适配器
        db_adapter = get_db_adapter()
        
        # 获取历史数据
        history_data = await get_token_history_data(db_adapter, args.chain, args.symbol, args.days)
        
        if not history_data:
            logger.warning("没有可用的历史数据，无法生成分析报告")
            return 1
            
        # 分析数据
        analysis_result = await analyze_token_history(history_data)
        
        # 生成报告
        if args.format == 'text':
            report = generate_text_report(analysis_result)
        elif args.format == 'json':
            report = generate_json_report(analysis_result)
        elif args.format == 'html':
            report = generate_html_report(analysis_result)
        else:
            report = generate_text_report(analysis_result)
        
        # 输出报告
        if args.output:
            # 创建目录
            os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
            # 写入文件
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(report)
            logger.info(f"分析报告已保存到: {args.output}")
        else:
            # 输出到标准输出
            print(report)
        
    except Exception as e:
        logger.error(f"分析过程中发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
    
    logger.info("分析完成")
    return 0

def main():
    # 设置日志
    log_dir = Path(project_root) / 'logs'
    os.makedirs(log_dir, exist_ok=True)
    setup_logger(__name__, log_file=log_dir / f"{datetime.now().strftime('%Y-%m-%d')}_token_analysis.log")
    
    # 运行异步主函数
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    return asyncio.run(main_async())

if __name__ == "__main__":
    sys.exit(main()) 