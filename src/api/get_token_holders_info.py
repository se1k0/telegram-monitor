import requests
from typing import Dict, List, Optional, Tuple
from decimal import Decimal


def get_token_holders_info(token_mint: str, api_key: str) -> Tuple[Optional[int], Optional[List[Dict]]]:
    """
    获取代币持有者数量和前10大持有者信息
    
    Args:
        token_mint (str): 代币的 mint 地址
        api_key (str): Helius API key
    
    Returns:
        Tuple[Optional[int], Optional[List[Dict]]]: 
            - 持有者总数
            - 前10大持有者列表，每个持有者包含地址、数量和占比
    """
    try:
        url = f"https://mainnet.helius-rpc.com/?api-key={api_key}"
        headers = {"Content-Type": "application/json"}
        
        # 首先获取第一页数据
        payload = {
            "jsonrpc": "2.0",
            "id": "token-holders",
            "method": "getTokenAccounts",
            "params": {
                "mint": token_mint,
                "page": 1
            }
        }
        
        # 存储所有持有者信息
        all_holders = []
        total_supply = Decimal(0)
        
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        
        if "error" in data:
            print(f"API 错误: {data['error']}")
            return None, None
            
        result = data.get("result", {})
        total = result.get("total", 0)
        
        # 处理第一页数据
        token_accounts = result.get("token_accounts", [])
        for account in token_accounts:
            amount = Decimal(account.get("amount", 0))
            total_supply += amount
            all_holders.append({
                "address": account.get("owner"),
                "amount": amount
            })
        
        # 如果需要翻页且总数大于1000
        if total >= 1000:
            max_pages = 10
            current_page = 1
            
            while current_page < max_pages:
                current_page += 1
                payload["params"]["page"] = current_page
                
                try:
                    response = requests.post(url, headers=headers, json=payload)
                    response.raise_for_status()
                    data = response.json()
                    
                    if "error" in data:
                        print(f"获取第 {current_page} 页时发生错误: {data['error']}")
                        break
                        
                    result = data.get("result", {})
                    page_accounts = result.get("token_accounts", [])
                    
                    # 处理当前页数据
                    for account in page_accounts:
                        amount = Decimal(account.get("amount", 0))
                        total_supply += amount
                        all_holders.append({
                            "address": account.get("owner"),
                            "amount": amount
                        })
                except requests.exceptions.RequestException as e:
                    print(f"请求错误: {e}")
                    break
        
        # 计算前10大持有者
        all_holders.sort(key=lambda x: x["amount"], reverse=True)
        top_holders = all_holders[:10]
        
        # 计算每个持有者的占比
        for holder in top_holders:
            holder["percentage"] = (holder["amount"] / total_supply * 100).quantize(Decimal("0.01"))
        
        return total, top_holders

    except requests.exceptions.RequestException as e:
        print(f"请求错误: {e}")
        return None, None
    except Exception as e:
        print(f"未知错误: {e}")
        return None, None 