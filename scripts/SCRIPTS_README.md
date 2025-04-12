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

### hourly_update.py

**每小时自动更新代币数据脚本**

实现了代币数据的每小时自动更新，具有智能的API速率限制处理，随机化请求序列以避免被识别为机器人，以及完善的错误处理与重试机制。

```bash
# 运行每小时更新（限制处理500个代币）
python scripts/hourly_update.py --limit 500

# 测试模式，使用较小的批次和较长的延迟
python scripts/hourly_update.py --test --limit 10
```

此脚本已被集成到主程序中，会在每小时整点自动执行。

### update_community_reach.py

**代币社群覆盖数据更新工具**

实现了代币社群覆盖人数(community_reach)的自动计算与更新，基于电报频道和群组的成员数量动态计算覆盖范围。

```bash
# 更新特定代币的社群覆盖人数
python scripts/update_community_reach.py --token SOL

# 更新所有代币的社群覆盖人数
python scripts/update_community_reach.py
```

### force_update_all_stats.py

**强制更新所有代币统计数据工具**

绕过常规检查和限制，强制更新所有代币的统计数据，包括社群覆盖人数、交易数据和持有者数量。适用于需要全面刷新数据的场景。

```bash
# 更新所有代币的全部统计数据
python scripts/force_update_all_stats.py

# 更新特定类型的数据
python scripts/force_update_all_stats.py --skip-community --skip-holders
```

## 系统维护工具

### check_env.py

**环境变量检查工具**

检查.env文件加载和环境变量设置情况，帮助诊断配置问题。

```bash
# 检查环境变量配置
python scripts/check_env.py
```

### check_supabase.py

**Supabase数据库连接检查工具**

检查与Supabase数据库的连接状态和权限设置。

```bash
# 检查Supabase连接
python scripts/check_supabase.py
```

### auto_reconnect.py

**Telegram API自动重连工具**

当遇到API限流(FloodWaitError)或其他连接问题时，该脚本会自动等待指定时间后重试连接。

```bash
# 检查API连接状态
python scripts/auto_reconnect.py test

# 自动等待限流时间并重试连接
python scripts/auto_reconnect.py wait
```

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

### das_api_example.py

**DAS API测试工具**

用于测试和展示如何使用DAS API获取Solana代币持有者信息。

```bash
# 获取代币持有者数量
python scripts/das_api_example.py --mint So11111111111111111111111111111111111111112 --test count

# 获取代币前十大持有者
python scripts/das_api_example.py --mint So11111111111111111111111111111111111111112 --test holders
```

### setup_hourly_task.bat

**Windows下设置自动任务的批处理脚本**

在Windows系统中设置代币数据的每小时自动更新任务。

```
# 设置Windows计划任务
scripts\setup_hourly_task.bat
```

## 使用建议

1. 所有脚本应在项目根目录下运行，而不是在scripts目录中运行
2. 首次使用时，请先运行check_env.py确保环境配置正确
3. 对于定期数据更新，推荐使用token_data_updater.py和hourly_update.py
4. 如需设置自动化任务，可使用setup_hourly_task.bat或将脚本加入系统的定时任务 