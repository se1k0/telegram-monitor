# DEX Screener API 模块

本模块提供了与DEX Screener API交互的功能，允许你获取各种去中心化交易所(DEX)的信息，如交易对数据、代币档案、流动池等。

## 功能特点

- 实现了所有DEX Screener API端点
- 提供简单易用的接口
- 包含错误处理机制
- 遵循API速率限制指南
- 支持单例模式和便捷函数

## 安装

该模块已经包含在项目中，无需额外安装。确保已安装`requests`库：

```bash
pip install requests
```

## 使用方法

### 导入模块

你可以直接导入需要的函数：

```python
from src.api.dex_screener_api import search_pairs, get_token_pools
```

或者导入整个模块：

```python
import src.api.dex_screener_api as dex_api
```

### 获取最新代币档案

```python
from src.api.dex_screener_api import get_latest_token_profiles

# 获取最新代币档案
profiles = get_latest_token_profiles()
print(profiles)
```

### 搜索交易对

```python
from src.api.dex_screener_api import search_pairs

# 搜索SOL/USDC交易对
results = search_pairs("SOL/USDC")
print(results)
```

### 获取代币流动池

```python
from src.api.dex_screener_api import get_token_pools

# 获取SOL代币的流动池
chain_id = "solana"
token_address = "So11111111111111111111111111111111111111112"  # SOL
pools = get_token_pools(chain_id, token_address)
print(pools)
```

### 通过多个代币地址获取交易对

```python
from src.api.dex_screener_api import get_pairs_by_token_address

# 获取SOL和USDC的交易对
chain_id = "solana"
token_addresses = [
    "So11111111111111111111111111111111111111112",  # SOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC
]
pairs = get_pairs_by_token_address(chain_id, token_addresses)
print(pairs)
```

## API函数列表

| 函数名 | 描述 | 速率限制 |
|--------|------|----------|
| `get_latest_token_profiles()` | 获取最新的代币档案 | 60次/分钟 |
| `get_latest_boosted_tokens()` | 获取最新的推广代币 | 60次/分钟 |
| `get_top_boosted_tokens()` | 获取最活跃推广的代币 | 60次/分钟 |
| `check_token_orders(chain_id, token_address)` | 检查代币的已支付订单 | 60次/分钟 |
| `get_pairs_by_chain_and_address(chain_id, pair_id)` | 通过区块链ID和交易对地址获取交易对信息 | 300次/分钟 |
| `search_pairs(query)` | 搜索匹配查询的交易对 | 300次/分钟 |
| `get_token_pools(chain_id, token_address)` | 获取指定代币地址的流动池 | 300次/分钟 |
| `get_pairs_by_token_address(chain_id, token_addresses)` | 通过代币地址获取交易对 | 300次/分钟 |

## 使用示例

完整的使用示例请参考项目中的 `scripts/dex_screener_examples.py` 文件，该文件展示了如何使用本模块的所有功能。

运行示例:

```bash
python scripts/dex_screener_examples.py
```

## 单元测试

使用以下命令运行单元测试:

```bash
python -m unittest tests/test_dex_screener_api.py
```

## 注意事项

- 请注意API的速率限制，避免频繁调用导致请求被拒绝
- 对于返回大量数据的API，考虑使用分页或过滤器
- 某些API调用可能需要几秒钟才能返回结果，请耐心等待 

# Digital Asset Standard (DAS) API

本模块实现了Digital Asset Standard (DAS) API的接口，用于获取Solana代币账户信息和持有者数据。

## 主要功能

- `get_token_accounts`: 获取特定铸币或所有者的所有代币账户信息
- `get_token_holders_count`: 获取代币持有者数量
- `get_token_holders_info`: 获取代币持有者详细信息，包括前10大持有者和占比

## 优化更新（2025/04/01）

本模块已进行优化，主要改进包括：

1. 优化了API响应解析，更准确地处理DAS API的返回结构
2. 添加了更全面的错误处理和日志记录
3. 新增了`get_token_holders_info`函数，可获取代币前10大持有者及占比
4. 改进了速率限制控制，防止API请求过频
5. 更好地支持代币持有者信息分页处理

## 使用示例

### 获取代币账户

```python
from src.api.das_api import get_token_accounts

# 使用铸币地址查询代币账户
response = get_token_accounts(mint="your_mint_address")

# 使用所有者地址查询代币账户
response = get_token_accounts(owner="your_owner_address")

# 显示零余额账户
response = get_token_accounts(
    owner="your_owner_address", 
    show_zero_balance=True
)

# 分页查询
response = get_token_accounts(
    mint="your_mint_address", 
    page=2, 
    limit=50
)
```

### 获取代币持有者数量

```python
from src.api.das_api import get_token_holders_count

# 获取代币持有者数量
count = get_token_holders_count("your_mint_address")
print(f"代币持有者数量: {count}")
```

### 获取详细持有者信息（新功能）

```python
from src.api.das_api import get_token_holders_info

# 获取代币持有者详细信息
count, top_holders = get_token_holders_info("your_mint_address")

# 输出前10大持有者信息
for i, holder in enumerate(top_holders):
    print(f"持有者 {i+1}: {holder['address']}")
    print(f"持有数量: {holder['amount']}")
    print(f"占比: {holder['percentage']}%")
```

## 配置

在使用此模块前，请确保在`.env`文件中配置了DAS API密钥：

```
DAS_API_KEY='your-api-key'
```

## 响应格式

成功响应示例：

```json
{
  "total": 2,
  "limit": 100,
  "cursor": "text",
  "token_accounts": [
    {
      "address": "text",
      "mint": "text",
      "owner": "text",
      "amount": 1,
      "delegated_amount": 1,
      "frozen": true
    }
  ]
}
```

错误响应示例：

```json
{
  "error": "错误信息"
}
```

## 测试工具

我们提供了一个命令行工具来测试DAS API功能:

```bash
python scripts/das_api_example.py --mint <mint_address>
python scripts/das_api_example.py --owner <owner_address> --show-zero-balance
python scripts/das_api_example.py --mint <mint_address> --test holders
```

可用选项:
- `--mint`: 代币铸币地址
- `--owner`: 所有者地址
- `--limit`: 返回结果的最大数量
- `--page`: 返回结果的页码
- `--show-zero-balance`: 显示余额为零的账户
- `--verbose`, `-v`: 显示详细信息
- `--test`: 指定测试功能 (accounts/count/holders/all)
- `--max-pages`: 获取持有者信息的最大页数

## 单元测试

运行单元测试：

```bash
python -m unittest tests.test_das_api
``` 