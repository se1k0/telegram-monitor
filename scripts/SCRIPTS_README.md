# 命令行脚本工具

本目录包含项目中所有命令行工具脚本，这些工具可以帮助您管理和维护Telegram监控系统。

## 代币数据管理工具

### token_data_updater.py

**统一的代币数据更新工具**

集成了市场数据、交易数据和交易量数据的更新功能，支持单个代币、批量代币和全量代币的更新，并提供定时任务功能。

```bash
# 更新单个代币的全部数据
python token_data_updater.py token SOL So11111111111111111111111111111111111111112

# 更新所有代币数据
python token_data_updater.py all --limit 100
```

详细文档请参考：[../documentation/README_TOKEN_UPDATER.md](../documentation/README_TOKEN_UPDATER.md)

### update_market_data.py

**代币市场数据更新工具**（已被token_data_updater.py集成）

更新代币的市值、流动性、价格等市场数据。

```bash
# 更新单个代币的市场数据
python update_market_data.py token SOL So11111111111111111111111111111111111111112

# 更新所有代币的市场数据
python update_market_data.py all --limit 100
```

### update_txn_data.py

**代币交易数据更新工具**（已被token_data_updater.py集成）

更新代币的1小时买入卖出交易数据。

```bash
# 更新单个代币的交易数据
python update_txn_data.py token SOL So11111111111111111111111111111111111111112

# 更新所有代币的交易数据
python update_txn_data.py all --limit 100
```

详细文档请参考：[../documentation/README_TXN_DATA_UPDATE.md](../documentation/README_TXN_DATA_UPDATE.md)

### update_volume_1h.py

**代币交易量更新工具**（已被token_data_updater.py集成）

更新所有代币的1小时交易量数据。

```bash
# 更新代币交易量数据（最多100个代币）
python update_volume_1h.py --limit 100
```

## 系统维护工具

### database_maintenance.py

**数据库维护工具**

检查、修复和维护数据库结构，不会修改数据库中的实际数据。

```bash
# 运行基本的数据库维护（检查并添加所有缺失的列）
python database_maintenance.py

# 初始化数据库（如果不存在）
python database_maintenance.py --init
```

详细文档请参考：[../documentation/README_DB_MAINTENANCE.md](../documentation/README_DB_MAINTENANCE.md)

## 频道和群组管理工具

### channel_manager_cli.py

**频道和群组管理工具**

管理系统监控的Telegram频道和群组，支持添加、移除和列出频道/群组。

```bash
# 列出所有频道和群组
python channel_manager_cli.py list

# 添加新频道
python channel_manager_cli.py add MomentumTrackerCN SOL
```

### discover_channels.py

**频道和群组发现工具**

自动发现新的Telegram频道和群组，并可选择性添加到监控列表。

```bash
# 发现频道和群组并显示列表
python discover_channels.py discover --min-members 500

# 自动添加发现的频道和群组
python discover_channels.py auto-add --min-members 1000 --max-channels 5
```

## 其他工具

### repair_database.py

**数据库修复工具**（已被database_maintenance.py集成）

修复数据库结构，添加缺失的列。

```bash
# 修复数据库结构
python repair_database.py
```

## 使用建议

1. 所有脚本应在项目根目录下运行，而不是在scripts目录中运行
2. 首次使用时，请先运行database_maintenance.py确保数据库结构正确
3. 对于定期数据更新，推荐使用token_data_updater.py代替旧的单功能更新脚本
4. 如需设置自动化任务，可将这些脚本加入系统的定时任务（crontab或任务计划程序） 