import os
import json
from pathlib import Path
from dotenv import load_dotenv

# 修改BASE_DIR为项目根目录
BASE_DIR = Path(__file__).resolve().parent.parent

# 配置文件路径
CONFIG_FILE = BASE_DIR / 'config/config.json'

# 加载环境变量（自动从项目根目录查找.env文件）
load_dotenv(BASE_DIR / '.env')

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

# 代币数据更新配置
token_update_limit = int(os.getenv('TOKEN_UPDATE_LIMIT', '500'))
token_update_min_delay = float(os.getenv('TOKEN_UPDATE_MIN_DELAY', '0.5'))
token_update_max_delay = float(os.getenv('TOKEN_UPDATE_MAX_DELAY', '2.0'))
token_update_batch_size = int(os.getenv('TOKEN_UPDATE_BATCH_SIZE', '50'))

# 自动发现频道的链关键词（用于猜测频道所属的链）
chain_keywords = {
    'SOL': ['solana', 'sol', 'Solana', 'SOL', '索拉纳'],
    'ETH': ['ethereum', 'eth', 'Ethereum', 'ETH', '以太坊'],
    'BTC': ['bitcoin', 'btc', 'Bitcoin', 'BTC', '比特币'],
    'BCH': ['bitcoin cash', 'bch', 'Bitcoin Cash', 'BCH', '比特币现金'],
    'AVAX': ['avalanche', 'avax', 'Avalanche', 'AVAX', '雪崩'],
    'BSC': ['binance', 'bnb', 'bsc', 'Binance', 'BNB', 'BSC', '币安链', '币安'],
    'MATIC': ['polygon', 'matic', 'Matic', 'MATIC', '多边形'],
    'TRX': ['tron', 'trx', 'Tron', 'TRX', '波场'],
    'TON': ['ton', 'Ton', 'TON', 'TON链'],
    'ARB': ['arbitrum', 'arb', 'Arbitrum', 'ARB', 'Arbitrum链'],
    'OP': ['optimism', 'op', 'Optimism', 'OP', 'Optimism链'],
    'ZK': ['zksync', 'zks', 'ZKSync', 'ZK', 'ZKSync链'],
    'BASE': ['base', 'Base', 'BASE', 'Base链'],
    'LINE': ['line', 'Line', 'LINE', 'Line链'],
    'KLAY': ['klaytn', 'klay', 'Klaytn', 'KLAY', 'Klaytn链'],
    'FUSE': ['fuse', 'Fuse', 'FUSE', 'Fuse链'],
    'CELO': ['celo', 'Celo', 'CELO', 'Celo链'],
    'KCS': ['kucoin', 'kcs', 'KCS', 'KCS链'],
    'KSM': ['kusama', 'ksm', 'Kusama', 'KSM', 'Kusama链'],
    'DOT': ['polkadot', 'dot', 'Polkadot', 'DOT', '波卡'],
    'ADA': ['cardano', 'ada', 'Cardano', 'ADA', '卡尔达诺'],
    'XRP': ['ripple', 'xrp', 'Ripple', 'XRP', '瑞波'],
    'LINK': ['chainlink', 'link', 'Chainlink', 'LINK', '链链'],
    'XLM': ['stellar', 'xlm', 'Stellar', 'XLM', '恒星'],
    'XMR': ['monero', 'xmr', 'Monero', 'XMR', '门罗'],
    'LTC': ['litecoin', 'ltc', 'Litecoin', 'LTC', '莱特币'],
}

# 增强EnvConfig类
class EnvConfig:
    def __init__(self):
        # 统一管理API配置
        self.API_ID = int(os.getenv('TG_API_ID', 0))
        self.API_HASH = os.getenv('TG_API_HASH', '')
        
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
        
        # 代币数据更新配置
        self.TOKEN_UPDATE_LIMIT = token_update_limit
        self.TOKEN_UPDATE_MIN_DELAY = token_update_min_delay
        self.TOKEN_UPDATE_MAX_DELAY = token_update_max_delay
        self.TOKEN_UPDATE_BATCH_SIZE = token_update_batch_size

# 实例化配置对象便于引用
env_config = EnvConfig()

# 增强日志配置
LOG_CONFIG['handlers']['console'] = {
    'class': 'logging.StreamHandler',
    'formatter': 'standard'
}
LOG_CONFIG['loggers']['']['handlers'] = ['file', 'console']  # 同时输出到文件和终端

# 加载配置函数
def load_config(config_file=CONFIG_FILE):
    """
    从配置文件加载配置
    
    Args:
        config_file: 配置文件路径，默认为CONFIG_FILE
        
    Returns:
        dict: 配置字典
    """
    try:
        if os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            return config
        else:
            print(f"警告：配置文件 {config_file} 不存在，将使用默认配置")
            # 创建默认配置
            default_config = {
                "telegram": {
                    "api_id": env_config.API_ID,
                    "api_hash": env_config.API_HASH
                },
                "database": {
                    "uri": DATABASE_URI
                },
                "web_server": {
                    "host": "0.0.0.0",
                    "port": 5000,
                    "debug": False
                },
                "discovery": {
                    "enabled": env_config.AUTO_CHANNEL_DISCOVERY,
                    "interval": env_config.DISCOVERY_INTERVAL,
                    "min_members": env_config.MIN_CHANNEL_MEMBERS,
                    "max_channels": env_config.MAX_AUTO_CHANNELS,
                    "excluded_channels": env_config.EXCLUDED_CHANNELS
                },
                "token_update": {
                    "limit": env_config.TOKEN_UPDATE_LIMIT,
                    "min_delay": env_config.TOKEN_UPDATE_MIN_DELAY,
                    "max_delay": env_config.TOKEN_UPDATE_MAX_DELAY,
                    "batch_size": env_config.TOKEN_UPDATE_BATCH_SIZE
                }
            }
            
            # 确保配置目录存在
            os.makedirs(os.path.dirname(config_file), exist_ok=True)
            
            # 写入默认配置文件
            try:
                with open(config_file, 'w', encoding='utf-8') as f:
                    json.dump(default_config, f, indent=4, ensure_ascii=False)
                print(f"已创建默认配置文件: {config_file}")
            except Exception as e:
                print(f"创建默认配置文件时出错: {str(e)}，将继续使用内存中的默认配置")
                
            return default_config
    except Exception as e:
        print(f"加载配置文件时出错: {str(e)}")
        raise
