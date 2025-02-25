import requests
from typing import Dict


def get_token_info(token_address: str, api_key: str) -> Dict:
    """
    获取代币信息的函数
    
    Args:
        token_address (str): 代币地址
        api_key (str): Helius API key
    
    Returns:
        Dict: 包含代币信息的字典
    """
    try:
        # 构建请求
        url = f"https://mainnet.helius-rpc.com/?api-key={api_key}"
        payload = {
            "jsonrpc": "2.0",
            "id": "test",
            "method": "getAsset",
            "params": {
                "id": token_address
            }
        }
        headers = {"Content-Type": "application/json"}

        # 发送请求
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # 检查请求是否成功
        data = response.json()

        # 提取需要的信息
        result = data.get("result", {})
        content = result.get("content", {})
        metadata = content.get("metadata", {})
        links = content.get("links", {})
        token_info = result.get("token_info", {})
        price_info = token_info.get("price_info", {})

        # 构建返回的数据结构
        token_data = {
            "metadata": {
                "name": metadata.get("name"),
                "symbol": metadata.get("symbol"),
                "description": metadata.get("description"),
                "token_standard": metadata.get("token_standard")
            },
            "image_url": links.get("image"),
            "supply": {
                "amount": token_info.get("supply"),
                "decimals": token_info.get("decimals")
            },
            "price": {
                "price_per_token": price_info.get("price_per_token"),
                "currency": price_info.get("currency")
            }
        }

        return token_data

    except requests.exceptions.RequestException as e:
        print(f"请求错误: {e}")
        return {}
    except KeyError as e:
        print(f"数据解析错误: {e}")
        return {}
    except Exception as e:
        print(f"未知错误: {e}")
        return {} 