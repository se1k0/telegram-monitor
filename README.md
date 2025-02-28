# Telegram 频道监控服务

这是一个用于监控Telegram频道的服务，可以自动捕获消息并保存。

## 功能特性

- 动态管理监控的Telegram频道
- 自动发现并添加新的Telegram频道
- 自动保存频道消息和媒体文件
- 提取和分析消息中的信息
- 将提取的信息保存到数据库中
- 支持同时监控频道和群组聊天
- SQLite数据库性能优化，支持并发访问

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


## 配置说明

除了原有配置外，新增以下配置选项：

```yaml
# 批处理配置
batch_size: 10          # 批处理大小
batch_interval: 30      # 批处理间隔（秒）

# Web界面配置
web:
  host: 0.0.0.0         # Web服务器主机
  port: 5000            # Web服务器端口
  debug: false          # 是否启用调试模式

# 错误处理配置
error_handling:
  max_retries: 3        # 最大重试次数
  retry_delay: 1.0      # 重试延迟（秒）
  report_interval: 3600 # 错误报告间隔（秒）
```


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
   DATABASE_URI=sqlite:///data/telegram_data.db
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
  - `config.json`: 主配置文件
  - `settings.py`: 配置加载和管理模块
  - `sensitive_words.txt`: 敏感词过滤文件
- `logs/`: 日志目录
- `data/`: 数据存储目录
  - `sentiment/`: 情感分析词典目录
- `media/`: 媒体文件存储目录

## 注意事项

- 首次运行时，会自动创建数据库和必要的目录
- 默认会初始化两个频道：`MomentumTrackerCN` 和 `ETH_Momentum_Tracker_CN`
- 使用命令行工具管理频道时，需要有网络连接以验证频道信息
- 自动发现功能会定期检查你已加入的Telegram频道和群组，并根据配置自动添加符合条件的频道和群组
- 如果在升级到群组支持版本后遇到数据库错误，请运行修复脚本：
  ```bash
  python scripts/repair_database.py
  ```
  这将更新数据库结构，添加支持群组功能所需的字段


## 2025/02/28 功能优化

### SQLite 并发访问优化

为了解决 SQLite 在并发访问时出现的 "database is locked" 错误，以及提高整体性能，实施以下优化措施：

#### 1. WAL 模式

启用了 SQLite 的 Write-Ahead Logging (WAL) 模式
- 提高并发性：读取操作不会阻塞写入操作
- 更快的事务性能：写入操作更高效
- 更好的崩溃恢复能力

```python
connection.execute("PRAGMA journal_mode=WAL")
```

#### 2. 同步模式优化

将同步模式设置为 NORMAL，在保证数据安全的同时提高写入性能：

```python
connection.execute("PRAGMA synchronous=NORMAL")
```

#### 3. 缓存大小优化

增加 SQLite 的缓存大小，减少磁盘 I/O 操作：

```python
connection.execute("PRAGMA cache_size=-64000")  # 约64MB缓存
```

#### 4. 连接超时设置

增加连接超时时间，减少"database is locked"错误：

```python
connection.execute("PRAGMA busy_timeout=30000")  # 30秒
```

#### 5. 重试机制

为所有 SQLite 操作添加了自动重试机制，即使在高并发情况下也能保证操作最终成功：

```python
# 示例：带有重试的数据库操作
@retry_sqlite_operation(max_retries=5, delay=1)
def database_operation():
    # 执行数据库操作
```

#### 6. 批量操作优化

使用批量保存提高大量数据写入的效率：

```python
session.bulk_save_objects(objects, return_defaults=False)
```

## 环境要求

- Python 3.8+
- Telethon 1.28.5+
- SQLAlchemy 2.0.20+
- Flask 2.3.3+
- 其他依赖见 requirements.txt