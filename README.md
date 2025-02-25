# Telegram 频道监控服务

这是一个用于监控Telegram频道的服务，可以自动捕获消息并保存。

## 功能特性

- 动态管理监控的Telegram频道
- 自动发现并添加新的Telegram频道
- 自动保存频道消息和媒体文件
- 提取和分析消息中的信息
- 将提取的信息保存到数据库中

## 安装

1. 克隆仓库
2. 安装依赖:
   ```
   pip install -r requirements.txt
   ```
3. 创建 `.env` 文件，填入必要的环境变量：
   ```
   TG_API_ID=你的Telegram API ID
   TG_API_HASH=你的Telegram API Hash
   FLASK_SECRET_KEY=用于Web应用的密钥
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

### 频道管理

使用命令行工具管理频道：

```bash
# 列出所有频道
python scripts/channel_manager_cli.py list

# 添加新频道
python scripts/channel_manager_cli.py add 频道用户名 链名称
# 例如: python scripts/channel_manager_cli.py add MomentumTrackerCN SOL

# 移除频道
python scripts/channel_manager_cli.py remove 频道用户名

# 更新频道状态
python scripts/channel_manager_cli.py update
```

### 自动发现频道

使用新的命令行工具发现和添加频道：

```bash
# 发现频道并显示列表
python scripts/discover_channels.py discover --min-members 500

# 发现频道并自动添加
python scripts/discover_channels.py discover --min-members 500 --auto-add --max-channels 10

# 直接自动添加频道
python scripts/discover_channels.py auto-add --min-members 1000 --max-channels 5
```

### 配置自动发现频道

在 `.env` 文件中添加以下配置：

```
# 自动发现频道配置
# 是否启用自动发现频道功能 (true/false)
AUTO_CHANNEL_DISCOVERY=true
# 自动发现频道的间隔（秒）
DISCOVERY_INTERVAL=3600
# 自动添加的频道最小成员数
MIN_CHANNEL_MEMBERS=1000
# 每次最多自动添加的频道数
MAX_AUTO_CHANNELS=5
# 排除的频道列表（不会自动添加的频道，逗号分隔）
EXCLUDED_CHANNELS=my_test_channel,temporary_channel
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
  - `utils/`: 工具函数
  - `web/`: Web应用
    - `web_app.py`: Flask Web应用入口
    - `templates/`: HTML模板
    - `static/`: 静态资源
- `scripts/`: 脚本工具
  - `channel_manager_cli.py`: 频道管理命令行工具
  - `discover_channels.py`: 频道发现命令行工具
- `config/`: 配置文件
- `logs/`: 日志目录
- `data/`: 数据存储目录
- `media/`: 媒体文件存储目录

## 注意事项

- 首次运行时，会自动创建数据库和必要的目录
- 默认会初始化两个频道：`MomentumTrackerCN` 和 `ETH_Momentum_Tracker_CN`
- 使用命令行工具管理频道时，需要有网络连接以验证频道信息
- 自动发现功能会定期检查你已加入的Telegram频道，并根据配置自动添加符合条件的频道 