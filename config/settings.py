import os
from pathlib import Path
from dotenv import load_dotenv

# 修改BASE_DIR为项目根目录
BASE_DIR = Path(__file__).resolve().parent.parent

# 加载环境变量（自动从项目根目录查找.env文件）
load_dotenv(BASE_DIR / '.env')

# 优化群组配置（建议改为环境变量读取）
MONITOR_GROUPS = os.getenv('MONITOR_GROUPS', 'group_username1,group_username2').split(',')

# 数据库配置
DATABASE_URI = os.getenv('DATABASE_URI', f'sqlite:///{BASE_DIR}/data/telegram_data.db')
# PostgreSQL生产环境配置示例
# DATABASE_URI = 'postgresql+psycopg2://user:password@localhost/dbname'

# 关键词过滤文件路径
KEYWORDS_FILE = BASE_DIR / 'config/sensitive_words.txt'

# 日志配置
LOG_DIR = BASE_DIR / 'logs'
LOG_FILE = LOG_DIR / 'monitor.log'

# 增强的日志配置
LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': LOG_FILE,
            'maxBytes': 1024*1024*5,  # 5MB
            'backupCount': 3,
            'formatter': 'standard',
        },
    },
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'loggers': {
        '': {
            'handlers': ['file'],
            'level': os.getenv('LOG_LEVEL', 'INFO'),
            'propagate': True
        }
    }
}

# 自动发现频道配置（新增）
# 是否启用自动发现频道功能
auto_channel_discovery = os.getenv('AUTO_CHANNEL_DISCOVERY', 'true').lower() == 'true'
# 自动发现频道的间隔（秒）
discovery_interval = int(os.getenv('DISCOVERY_INTERVAL', '3600'))
# 自动添加的频道最小成员数
min_channel_members = int(os.getenv('MIN_CHANNEL_MEMBERS', '500'))
# 每次最多自动添加的频道数
max_auto_channels = int(os.getenv('MAX_AUTO_CHANNELS', '10'))
# 排除的频道列表（不会自动添加的频道）
excluded_channels = os.getenv('EXCLUDED_CHANNELS', '').split(',')
# 自动发现频道的链关键词（用于猜测频道所属的链）
chain_keywords = {
    'SOL': ['solana', 'sol', '索拉纳'],
    'ETH': ['ethereum', 'eth', '以太坊'],
    'BTC': ['bitcoin', 'btc', '比特币'],
    'AVAX': ['avalanche', 'avax', '雪崩'],
    'BSC': ['binance', 'bnb', 'bsc', '币安链'],
    'MATIC': ['polygon', 'matic', '多边形'],
}

# 增强EnvConfig类
class EnvConfig:
    def __init__(self):
        # 统一管理API配置
        self.API_ID = int(os.getenv('TG_API_ID', 0))
        self.API_HASH = os.getenv('TG_API_HASH', '')
        
        # 添加群组配置校验
        self.MONITOR_GROUPS = [g.strip() for g in MONITOR_GROUPS if g.strip()]
        if not self.MONITOR_GROUPS:
            raise ValueError("至少需要配置一个监控群组")

        # 添加路径校验
        sensitive_words_path = BASE_DIR / 'config/sensitive_words.txt'
        if not sensitive_words_path.exists():
            print("警告：未检测到关键词过滤文件，将创建空文件")
            sensitive_words_path.touch()

        # 新增媒体存储路径
        self.MEDIA_DIR = BASE_DIR / 'media'
        self.MEDIA_DIR.mkdir(exist_ok=True)
        
        # 新增自动发现频道配置
        self.AUTO_CHANNEL_DISCOVERY = auto_channel_discovery
        self.DISCOVERY_INTERVAL = discovery_interval
        self.MIN_CHANNEL_MEMBERS = min_channel_members
        self.MAX_AUTO_CHANNELS = max_auto_channels
        self.EXCLUDED_CHANNELS = [ch.strip() for ch in excluded_channels if ch.strip()]
        self.CHAIN_KEYWORDS = chain_keywords

# 实例化配置对象便于引用
env_config = EnvConfig()

# 增强日志配置
LOG_CONFIG['handlers']['console'] = {
    'class': 'logging.StreamHandler',
    'formatter': 'standard'
}
LOG_CONFIG['loggers']['']['handlers'] = ['file', 'console']  # 同时输出到文件和终端
