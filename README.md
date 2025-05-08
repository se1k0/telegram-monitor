# Telegram 频道监控服务

## 数据库说明

本项目现在**只支持Supabase数据库**，不再支持SQLite或其他数据库。数据库配置需要在`.env`文件中设置：

```
DATABASE_URI=supabase://your_project_ref.supabase.co/your_anon_key
SUPABASE_URL=https://your_project_ref.supabase.co
SUPABASE_KEY=your_supabase_anon_key
SUPABASE_SERVICE_KEY=your_supabase_service_key
```

请确保在运行程序前正确配置以上参数。具体Supabase设置指南请参考`docs/supabase_setup_summary.md`。

## 功能特性

- 动态管理监控的Telegram频道
- 自动发现并添加新的Telegram频道
- 自动保存频道消息和媒体文件
- 提取和分析消息中的信息
- 将提取的信息保存到数据库中
- 支持同时监控频道和群组聊天
- 自动重连机制，处理API限流和连接问题
- 增强的代币提及记录系统

## 2025/04/02新增功能

### 全新的自动重连机制

- 实现了API限流(FloodWaitError)的智能处理和自动等待重连
- 添加了连接状态监控和自动恢复机制
- 记录限流信息到独立日志文件，便于追踪和分析
- 提供API连接诊断工具，快速定位问题
- 自动计算剩余等待时间，避免不必要的连接尝试

使用示例：

```bash
# 检查API连接状态
python scripts/auto_reconnect.py test

# 自动等待限流时间并重试连接
python scripts/auto_reconnect.py wait
```

### 增强的代币提及记录系统

- 新增了`tokens_mark`表专门记录代币被提及信息
- 通过新表结构实现更高效的代币提及查询和分析
- 提供代币提及数据完整性检查和自动修复工具
- 支持跨频道的代币提及追踪和统计
- 自动创建必要的数据库结构，确保系统兼容性

使用示例：

```bash
# 检查tokens_mark表是否存在并正常工作
python scripts/check_tokens_mark.py

# 创建tokens_mark表（如果不存在）
python scripts/create_tokens_mark_table.py
```

## 2025/04/01新增功能

### 增强的DAS API功能

- 优化了Digital Asset Standard (DAS) API模块，用于获取Solana代币账户和持有者信息
- 新增了`get_token_holders_info`函数，获取代币前10大持有者及占比分析
- 改进了API响应解析，更准确地处理DAS API的返回结构
- 添加了更全面的错误处理和日志记录功能
- 优化速率限制控制，防止API请求过频被拒绝

使用示例：

```python
# 获取代币持有者详细信息
from src.api.das_api import get_token_holders_info
count, top_holders = get_token_holders_info("your_mint_address")

# 输出前10大持有者信息
for i, holder in enumerate(top_holders):
    print(f"持有者 {i+1}: {holder['address']}")
    print(f"持有数量: {holder['amount']}")
    print(f"占比: {holder['percentage']}%")
```

### 每小时自动更新功能

- 新增了每小时自动更新代币数据的功能，实现实时数据监控
- 提供了Windows系统下的计划任务设置脚本，便于自动化部署
- 智能处理API速率限制，优化请求序列，避免被识别为机器人
- 实现了错误处理与重试机制，确保数据更新可靠性
- 新增详细的日志记录，便于故障排查和性能监控

使用示例：

```bash
# 手动执行更新脚本
python scripts/hourly_update.py --limit 500

# 设置计划任务（Windows系统）
scripts/setup_hourly_task.bat
```

### 新增代币社群覆盖数据自动更新工具

- 实现了代币社群覆盖人数(community_reach)的自动计算与更新
- 基于电报频道和群组的成员数量动态计算覆盖范围
- 支持单个代币或批量更新所有代币的社群覆盖数据
- 命令行工具简单易用，支持参数自定义

使用示例：

```bash
# 更新特定代币的社群覆盖人数
python scripts/update_community_reach.py --token SOL

# 更新所有代币的社群覆盖人数
python scripts/update_community_reach.py
```

### 代币标记表结构优化

- 新增了`tokens_mark`表专门记录代币被提及信息
- 优化了表结构设计，提高查询效率
- 自动创建和修复数据库结构，确保系统兼容性
- 改进了索引设计，加速常用查询操作

系统会自动检查并创建所需表结构，如需手动创建可执行：
```bash
python scripts/create_tokens_mark_table.py
```

### Web前端界面增强

- 优化了主页的代币列表显示，添加了社群覆盖人数和传播次数的显示列
- 新增了代币详情页中的市值变化图表，直观展示代币市值随时间的变化
- 改进了消息详情页，增加了相关代币列表和媒体文件预览功能
- 新增了代币在特定频道的提及详情页面，包含提及历史记录和市值变化图表
- 优化了页面响应速度和用户交互体验
- 增强了移动端适配，提供更好的移动设备访问体验

### API功能扩展

- 增强了token市场历史数据API，支持更灵活的时间范围查询和数据聚合
- 优化了API响应结构，提供更规范和一致的数据格式
- 增加了API缓存机制，减少重复请求，提高响应速度
- 实现了代币详情数据的综合API，一次请求即可获取代币的所有相关信息
- 添加了错误处理和参数验证，提高API稳定性和安全性

使用示例：

```
# 获取代币市场历史数据
GET /api/token_market_history/{chain}/{contract}

# 返回示例
{
  "token_symbol": "SOL",
  "contract": "So11111111111111111111111111111111111111112",
  "history": [
    {
      "date": "2025-03-25T12:00:00",
      "market_cap": 45000000000,
      "volume_24h": 1500000000,
      "buys_1h": 145,
      "sells_1h": 120
    },
    ...
  ]
}
```

## 2025/03/27新增功能

### 新增代币传播统计功能

- 在数据库中新增了`spread_count`和`community_reach`字段，分别记录代币传播次数和社群覆盖人数
- 代币传播次数：统计该代币在电报群共被提及的总次数
- 代币社群覆盖人数：统计该代币在所有电报群覆盖的总人数
- 智能处理重复统计问题：同一个群内多次提及同一代币时，不重复计算群成员数
- 动态调整覆盖人数：当群组成员数量发生变化时，自动更新社群覆盖人数统计
- 提供数据库升级脚本，自动处理历史数据的传播次数和覆盖人数统计

**首次使用注意**：在使用此功能前，需要更新数据库结构：
```bash
# 添加代币传播统计相关字段并初始化历史数据
python scripts/add_token_spread_columns.py
```

## 2025/03/25新增功能

### 新增DEX Screener API模块

- 实现了DEX Screener的所有API接口，用于获取代币和交易对信息
- 支持获取最新代币档案和推广代币信息
- 支持搜索交易对和查询代币流动池
- 支持查询多个代币地址的交易对信息
- 完整的错误处理和速率限制遵循
- 提供单元测试和使用示例

使用示例：

```python
# 搜索SOL/USDC交易对
from src.api.dex_screener_api import search_pairs
results = search_pairs("SOL/USDC")

# 获取SOL代币的流动池
from src.api.dex_screener_api import get_token_pools
pools = get_token_pools("solana", "So11111111111111111111111111111111111111112")
```

更多详情请查看 `src/api/README.md`。

### 新增代币1小时交易数据功能

- 在数据库中新增了`buys_1h`和`sells_1h`字段，记录代币1小时内的买入卖出交易数
- 通过DEX Screener API获取代币在所有交易对中的交易数据并汇总
- 提供了独立更新交易数据和综合更新市场+交易数据的多种方式
- 支持单个代币更新、批量更新和全量更新
- 新增命令行工具`update_txn_data.py`方便定期更新交易数据

**首次使用注意**：在使用此功能前，需要更新数据库结构：
```bash
# 自动修复数据库结构（添加新列）
python repair_database.py
```

使用示例：

```bash
# 更新单个代币的交易数据
python scripts/update_txn_data.py token SOL So11111111111111111111111111111111111111112

# 更新所有代币的交易数据
python scripts/update_txn_data.py all --limit 100
```

更多详情请查看 `documentation/txn_data_update.md`。

### 统一的代币数据更新工具

为了提高代币数据更新的便捷性和效率，我们整合了原有的多个更新脚本，创建了一个统一的代币数据更新工具：

- 集成了市场数据更新、交易数据更新和交易量数据更新功能
- 支持单个代币、批量代币和全量代币的更新
- 支持选择性更新特定类型的数据
- 支持定时任务和循环执行，便于监控数据变化
- 自动检查并修复数据库结构

使用示例：

```bash
# 更新单个代币的全部数据
python scripts/token_data_updater.py token SOL So11111111111111111111111111111111111111112

# 仅更新特定代币的市场数据
python scripts/token_data_updater.py token ETH 0x1f9840a85d5af5bf1d1762f925bdaddc4201f984 --type market

# 更新多个代币的全部数据
python scripts/token_data_updater.py symbols SOL ETH BTC

# 更新所有代币数据（最多100个）
python scripts/token_data_updater.py all --limit 100

# 使用循环执行功能监控数据，每30分钟更新一次，连续执行12小时
python scripts/token_data_updater.py all --limit 50 --repeat 24 --interval 30
```

详细使用说明请查看 `scripts/README_TOKEN_UPDATER.md`。

## 2025/03/01新增功能

### 更新了WEB前端页面

### 新增Telegram群组支持

- 现在系统可以监控普通群组，而不仅限于频道
- 系统会自动标记消息是来自频道还是群组
- 自动发现功能现在能识别并添加群组
- 从群组中提取的代币信息会被特别标记，方便分析群组和频道的信息差异
- 所有相关数据表增加了群组支持字段
- 针对不同类型的聊天实体（频道/群组）使用专门的处理逻辑

此功能允许系统更全面地监控Telegram生态中的信息流动，获取更广泛的代币相关信息。

## 2025/02/28新增功能

### 新增可靠性和错误处理

- 使用装饰器实现统一的错误捕获和处理
- 关键操作失败时自动重试，提高成功率
- 实时监控系统错误，生成详细报告
- 确保程序在关闭时能够正确清理资源
- 使用事务确保批处理操作的原子性


## 2025/02/27新增功能

### 增强的消息分析功能

新增了代币信息分析功能：(基础情感分析, 没有接入NLP)

- 自动分析消息中的情感倾向，识别积极/消极词汇
- 计算消息中的炒作程度，识别可能的虚假宣传
- 基于多种因素评估代币的风险等级
- 跟踪代币价格变化，分析市场趋势

## 使用情感分析

情感分析功能需要词典文件，请在 `data/sentiment` 目录下放置以下文件：

- `positive_words.txt`: 积极词汇列表，每行一个词
- `negative_words.txt`: 消极词汇列表，每行一个词
- `hype_words.txt`: 炒作词汇列表，每行一个词

如果这些文件不存在，系统会使用内置的基本词典。

## 2025/02/26新增功能

### 异步批处理

系统现在支持异步批处理消息和代币信息，提高了处理效率：

- 系统会将消息缓存并定期批量保存到数据库，减少数据库连接次数
- 代代币信息提取和更新采用批处理方式，提高处理效率
- 可以通过配置文件调整批处理大小和间隔


## 配置

项目的所有配置都集中在 `.env` 文件中。首次使用时，你可以复制 `.env.example` 文件并重命名为 `.env`，然后按需修改配置：

```bash
cp .env.example .env
```

## 主要配置项

### Telegram API 配置
```
TG_API_ID='your_api_id'
TG_API_HASH='your_api_hash'
```

### 数据库配置
```
DATABASE_URI=supabase://your_project_ref.supabase.co/your_anon_key
SUPABASE_URL=https://your_project_ref.supabase.co
SUPABASE_KEY=your_supabase_anon_key
SUPABASE_SERVICE_KEY=your_supabase_service_key
```

### 日志配置
```
LOG_LEVEL=DEBUG
LOG_MAX_SIZE=5242880  # 5MB in bytes
LOG_BACKUP_COUNT=3
```

### 自动发现频道配置
```
AUTO_CHANNEL_DISCOVERY=true
DISCOVERY_INTERVAL=3600
MIN_CHANNEL_MEMBERS=500
MAX_AUTO_CHANNELS=10
EXCLUDED_CHANNELS=my_test_channel,temporary_channel
```

### Web应用配置
```
FLASK_SECRET_KEY='secure_secret_key'
WEB_HOST=0.0.0.0
WEB_PORT=5000
WEB_DEBUG=false
```

完整的配置选项请参考 `.env.example` 文件。

## 安装

1. 克隆仓库
2. 安装依赖:
   ```
   pip install -r requirements.txt
   ```
3. 创建 `.env` 文件，填入必要的环境变量（可参考 `.env.example`）：
   ```
   TG_API_ID=你的Telegram API ID
   TG_API_HASH=你的Telegram API Hash
   DATABASE_URI=supabase://your_project_ref.supabase.co/your_anon_key
   LOG_LEVEL=INFO
   # 其他配置...
   ```

## 使用方法

### 启动监控服务

```bash
python main.py
```

### 启动Web界面

```bash
cd src/web
python web_app.py
```

### 频道和群组管理

使用命令行工具管理频道和群组：

```bash
# 列出所有频道和群组
python scripts/channel_manager_cli.py list

# 添加新频道
python scripts/channel_manager_cli.py add 频道用户名 链名称
# 例如: python scripts/channel_manager_cli.py add MomentumTrackerCN SOL

# 添加新群组（使用群组ID，因为群组没有用户名）
python scripts/channel_manager_cli.py add_group 群组ID 链名称 [群组名称]
# 例如: python scripts/channel_manager_cli.py add_group 1234567890 ETH ETH交易群

# 移除频道或群组
python scripts/channel_manager_cli.py remove 频道用户名或群组ID

# 更新所有频道和群组状态
python scripts/channel_manager_cli.py update
```

### 自动发现频道和群组

使用命令行工具发现和添加频道和群组：

```bash
# 发现频道和群组并显示列表
python scripts/discover_channels.py discover --min-members 500

# 发现频道和群组并自动添加
python scripts/discover_channels.py discover --min-members 500 --auto-add --max-channels 10

# 直接自动添加频道和群组
python scripts/discover_channels.py auto-add --min-members 1000 --max-channels 5

# 只发现群组（不包括频道）
python scripts/discover_channels.py discover --groups-only --min-members 300
```

### 配置自动发现频道和群组

在 `.env` 文件中添加以下配置：

```
# 自动发现频道和群组配置
# 是否启用自动发现功能 (true/false)
AUTO_CHANNEL_DISCOVERY=true
# 自动发现的间隔（秒）
DISCOVERY_INTERVAL=3600
# 自动添加的频道或群组最小成员数
MIN_CHANNEL_MEMBERS=1000
# 每次最多自动添加的数量
MAX_AUTO_CHANNELS=5
# 排除的频道列表（不会自动添加的频道，逗号分隔）
EXCLUDED_CHANNELS=my_test_channel,temporary_channel
# 是否优先添加群组（而非频道）
PREFER_GROUPS=false
# 是否只监控群组（不监控频道）
GROUPS_ONLY=false
# 是否只监控频道（不监控群组）
CHANNELS_ONLY=false
```

系统会自动识别频道或群组所属的区块链，支持的区块链包括：SOL、ETH、BTC、BCH、AVAX、BSC、MATIC、TRX、TON、ARB、OP、ZK、BASE、LINE、KLAY、FUSE、CELO、KCS、KSM、DOT、ADA、XRP、LINK、XLM、XMR、LTC等。识别规则基于频道或群组的标题、描述和用户名中包含的关键词。

如果需要添加新的区块链识别规则，可以在代码中调用：

```python
# 示例：添加新的区块链识别关键词
from src.core.channel_discovery import ChannelDiscovery

discovery = ChannelDiscovery(client, channel_manager)
discovery.add_chain_keywords('NEWCHAIN', ['new_chain', 'newchain', '新链'])
```

## 项目结构

- `main.py`: 主程序入口
- `src/`: 源代码目录
  - `core/`: 核心功能
    - `telegram_listener.py`: Telegram 监听服务
    - `channel_manager.py`: 频道管理模块
    - `channel_discovery.py`: 频道发现模块
  - `database/`: 数据库相关代码
    - `models.py`: 数据库模型
    - `db_handler.py`: 数据库操作处理
  - `analysis/`: 消息分析功能
    - `token_analyzer.py`: 代币信息分析模块
  - `api/`: API 集成模块
    - `get_token_meta_info.py`: 获取代币元数据接口
    - `get_token_holders_info.py`: 获取代币持有者信息接口
  - `utils/`: 工具函数
    - `logger.py`: 日志管理模块
    - `error_handler.py`: 错误处理装饰器和工具
    - `utils.py`: 通用工具函数
  - `web/`: Web应用
    - `web_app.py`: Flask Web应用入口
    - `templates/`: HTML模板
    - `static/`: 静态资源
- `scripts/`: 脚本工具
  - `channel_manager_cli.py`: 频道管理命令行工具
  - `discover_channels.py`: 频道发现命令行工具
  - `repair_database.py`: 数据库修复工具
- `tests/`: 测试目录
- `config/`: 配置文件
  - `settings.py`: 配置加载和管理模块
  - `sensitive_words.txt`: 敏感词过滤文件
- `logs/`: 日志目录
- `data/`: 数据存储目录
  - `sentiment/`: 情感分析词典目录
- `media/`: 媒体文件存储目录

## 注意事项

- 首次运行时，会自动创建必要的目录
- 默认会初始化两个频道：`MomentumTrackerCN` 和 `ETH_Momentum_Tracker_CN`
- 使用命令行工具管理频道时，需要有网络连接以验证频道信息
- 自动发现功能会定期检查你已加入的Telegram频道和群组，并根据配置自动添加符合条件的频道和群组


## 2025/02/28 功能优化

### 代币价格和市值监控增强

## 环境要求

- Python 3.8+
- Telethon 1.28.5+
- Supabase客户端库
- Flask 2.3.3+
- 其他依赖见 requirements.txt

### 外部API集成

本项目集成了多个外部API，用于获取和分析加密货币数据：

- **DEX Screener API**: 提供DEX交易对、代币信息和市场数据
- **DAS API**: 提供Solana代币账户和持有者信息

详细API文档和使用方法请参考[src/api/README.md](src/api/README.md)文件。

## 最近修复的问题

### 代币处理逻辑修复 (2025-04-13)

#### 问题描述
系统在处理包含合约地址的新消息时，会从DEX API获取代币信息，但如果数据库中不存在该代币，系统会直接报告错误并终止处理，而不是将新发现的代币添加到数据库中。

错误日志表现为：
```
从DEX API获取到代币符号: PPI
数据库中未找到代币 SOL/FfEeM9QKnDTqPNNqnVphpxbH2Tb5MATtFnFDqNU8VJNu
通过DEX API获取代币信息失败: 数据库中未找到该代币
```

#### 解决方案
修改了代币市场数据更新的逻辑，当系统发现数据库中不存在该代币时，现在会自动创建一个新的代币记录，而不是返回错误。

#### 实现细节
1. 修改了 `update_token_market_data_async` 函数，添加了新的代币创建逻辑：
   - 当数据库中找不到代币时，使用从DEX API获取的信息创建新的代币记录
   - 如果DEX API未提供代币符号，将使用合约地址的前8位作为临时符号
   - 添加了 "is_new" 标志到返回结果，以区分新创建的代币和更新的代币（注意：此标志仅用于API响应和日志记录，不存储在数据库中）

2. 更新了 `telegram_listener.py` 中的消息处理逻辑：
   - 移除了重复的代币保存逻辑，因为现在创建/更新代币的工作已经在 `update_token_market_data_async` 中完成
   - 调整了日志消息，区分创建新代币和更新现有代币的情况
   - 简化了代币标记保存逻辑

#### 效果
- 系统现在可以自动发现并添加新的代币到数据库中
- 避免了因为数据库中不存在代币而中断处理流程的问题
- 提高了数据收集的完整性，确保所有发现的代币都能被跟踪和分析

#### 未来改进
- 考虑添加更多验证逻辑，确保只有高质量的代币数据被添加到数据库
- 添加定期清理机制，对于那些只出现一次且没有后续活动的代币进行归档或移除
- 优化DEX API数据获取流程，减少API调用次数，提高系统效率

## 自动清理无效代币功能

系统现在具有自动清理无效或已废弃代币的功能。当系统尝试更新代币信息时，如果发现该代币在DEX上已不存在（DEX API返回空结果），系统会自动从数据库中删除该代币及其相关数据，包括标记记录和历史数据。

### 工作原理

1. **检测阶段**：在定期更新代币数据时，系统会调用DEX API获取代币的最新交易对信息
2. **验证阶段**：如果DEX API返回空结果，系统会执行二次验证，确认代币确实不存在
3. **删除阶段**：确认后，系统按以下顺序删除相关数据：
   - 首先删除代币标记数据（`tokens_mark`表）
   - 然后删除代币历史数据（`token_history`表）
   - 最后删除代币主记录（`tokens`表）
4. **日志记录**：整个过程会记录详细日志，包括删除前的代币信息、删除原因和结果

### 配置选项

删除功能内置在代币更新流程中，无需额外配置。每次运行`hourly_update.py`脚本时，系统会自动检测并清理无效代币。

### 统计信息

在每小时更新任务的结果中，会包含以下与删除相关的统计信息：

- 已删除代币数量
- 删除代币的符号和合约地址
- 删除原因

这些信息可以在日志中查看，也可以在Web界面上的统计数据中找到。

### 注意事项

- 系统只会删除在DEX上确实不存在的代币
- 删除操作会确保所有相关数据都被清理，避免数据库残留孤立记录
- 删除前会进行二次验证，降低误删风险
- 整个过程有详细的日志记录，便于追踪和审计