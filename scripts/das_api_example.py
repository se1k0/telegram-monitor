#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
DAS API 测试工具
用于测试并展示如何使用优化后的DAS API获取代币持有者信息
"""

import os
import sys
import argparse
import json
from decimal import Decimal
from typing import Dict, Any

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

# 导入API模块
from src.api.das_api import get_token_accounts, get_token_holders_count, get_token_holders_info

# 创建一个自定义的JSON编码器来处理Decimal类型
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def print_json(data: Dict[str, Any]):
    """
    美化打印JSON数据
    """
    print(json.dumps(data, indent=2, cls=DecimalEncoder, ensure_ascii=False))

def test_get_token_accounts(args):
    """
    测试获取代币账户信息
    """
    print("\n=== 测试获取代币账户信息 ===")
    
    params = {}
    
    if args.mint:
        params["mint"] = args.mint
    
    if args.owner:
        params["owner"] = args.owner
        
    if args.limit:
        params["limit"] = args.limit
        
    if args.page:
        params["page"] = args.page
        
    if args.show_zero_balance:
        params["show_zero_balance"] = args.show_zero_balance
    
    print(f"请求参数: {params}")
    
    result = get_token_accounts(**params)
    
    if "error" in result:
        print(f"错误: {result['error']}")
        return
    
    print(f"总持有者数量: {result.get('total', 0)}")
    print(f"返回结果条数: {len(result.get('token_accounts', []))}")
    
    if args.verbose:
        print("\n账户信息:")
        print_json(result)
    else:
        # 只打印部分信息
        print("\n账户信息预览 (前3条):")
        accounts = result.get("token_accounts", [])
        for i, account in enumerate(accounts[:3]):
            print(f"\n账户 {i+1}:")
            print(f"  地址: {account.get('address')}")
            print(f"  所有者: {account.get('owner')}")
            print(f"  铸币地址: {account.get('mint')}")
            print(f"  数量: {account.get('amount')}")

def test_get_token_holders_count(args):
    """
    测试获取代币持有者数量
    """
    print("\n=== 测试获取代币持有者数量 ===")
    
    if not args.mint:
        print("错误: 获取持有者数量需要提供mint参数")
        return
        
    print(f"代币铸币地址: {args.mint}")
    
    count = get_token_holders_count(args.mint)
    
    if count is None:
        print("获取持有者数量失败")
    else:
        print(f"持有者数量: {count}")

def test_get_token_holders_info(args):
    """
    测试获取代币持有者详细信息
    """
    print("\n=== 测试获取代币持有者详细信息 ===")
    
    if not args.mint:
        print("错误: 获取持有者信息需要提供mint参数")
        return
        
    print(f"代币铸币地址: {args.mint}")
    max_pages = args.max_pages or 3
    print(f"最大页数: {max_pages}")
    
    count, top_holders = get_token_holders_info(args.mint, max_pages)
    
    if count is None:
        print("获取持有者信息失败")
    else:
        print(f"持有者总数: {count}")
        print(f"\n前 {len(top_holders)} 大持有者:")
        
        for i, holder in enumerate(top_holders):
            print(f"\n持有者 {i+1}:")
            print(f"  地址: {holder.get('address')}")
            print(f"  数量: {holder.get('amount')}")
            print(f"  占比: {holder.get('percentage')}%")

def main():
    """
    主函数
    """
    parser = argparse.ArgumentParser(description="DAS API 测试工具")
    parser.add_argument("--mint", help="代币铸币地址")
    parser.add_argument("--owner", help="所有者地址")
    parser.add_argument("--limit", type=int, default=10, help="返回结果的最大数量")
    parser.add_argument("--page", type=int, default=1, help="返回结果的页码")
    parser.add_argument("--show-zero-balance", action="store_true", help="显示余额为零的账户")
    parser.add_argument("--verbose", "-v", action="store_true", help="显示详细信息")
    parser.add_argument("--test", choices=["accounts", "count", "holders", "all"], default="all", 
                        help="要测试的功能，默认为全部")
    parser.add_argument("--max-pages", type=int, help="获取持有者信息的最大页数")
    
    args = parser.parse_args()
    
    if not args.mint and not args.owner:
        parser.error("请提供 --mint 或 --owner 参数")
    
    # 根据测试类型执行对应的函数
    if args.test == "accounts" or args.test == "all":
        test_get_token_accounts(args)
        
    if args.test == "count" or args.test == "all":
        test_get_token_holders_count(args)
        
    if args.test == "holders" or args.test == "all":
        test_get_token_holders_info(args)
    
    print("\n测试完成!")

if __name__ == "__main__":
    main() 