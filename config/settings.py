import os
import json
from pathlib import Path
from dotenv import load_dotenv
import logging
import sys
from datetime import datetime

# 设置基本日志 - 只设置基础配置，不添加处理器
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
    # 去掉处理器配置，由setup_logger统一处理
)
logger = logging.getLogger(__name__)

# 导入 Supabase 客户端库
try:
    from supabase import create_client, Client
    HAS_SUPABASE = True
except ImportError:
    logger.warning("未安装 supabase 客户端库. 如需使用 Supabase, 请运行: pip install supabase")
    HAS_SUPABASE = False

# 修改BASE_DIR为项目根目录
BASE_DIR = Path(__file__).resolve().parent.parent

# 尝试多个可能的.env文件位置
ENV_PATHS = [
    BASE_DIR / '.env',  # 项目根目录
    BASE_DIR.parent / '.env',  # 上级目录
    Path.cwd() / '.env',  # 当前工作目录
    Path(os.path.expanduser('~')) / '.telegram-monitor.env'  # 用户主目录
]

# 加载环境变量
env_loaded = False
found_env_path = None  # 跟踪找到的.env文件路径

for env_path in ENV_PATHS:
    if os.path.exists(env_path):
        try:
            load_dotenv(env_path)
            logger.info(f"已加载环境变量文件: {env_path}")
            env_loaded = True
            found_env_path = env_path  # 记录找到的路径
            break
        except Exception as e:
            logger.error(f"加载环境变量文件 {env_path} 失败: {e}")

if not env_loaded:
    logger.warning(f"未找到任何环境变量文件, 将使用系统环境变量或默认值")
    # 尝试不指定文件路径加载.env（查找默认位置）
    try:
        load_dotenv()
        logger.info("已从默认位置加载环境变量")
    except Exception as e:
        logger.error(f"从默认位置加载环境变量失败: {e}")
    
    # 如果没有找到.env文件，使用默认文件路径
    found_env_path = ENV_PATHS[0]  # 使用项目根目录作为默认位置

# 获取并验证数据库配置
DATABASE_URI = os.getenv('DATABASE_URI', '')
if not DATABASE_URI:
    logger.error("未设置DATABASE_URI环境变量，请在.env文件中设置supabase://开头的数据库连接字符串")
elif not DATABASE_URI.startswith('supabase://'):
    logger.error(f"错误: DATABASE_URI必须以supabase://开头，当前值: {DATABASE_URI}")
else:
    logger.info(f"成功读取DATABASE_URI: {DATABASE_URI[:15]}...")

# PostgreSQL生产环境配置示例
# DATABASE_URI = 'postgresql+psycopg2://user:password@localhost/dbname'

# Supabase 配置
SUPABASE_URL = os.getenv('SUPABASE_URL', '')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', '')
SUPABASE_SERVICE_KEY = os.getenv('SUPABASE_SERVICE_KEY', '')

# 初始化 Supabase 客户端
supabase_client = None
if HAS_SUPABASE and SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print(f"Supabase 客户端初始化成功: {SUPABASE_URL}")
    except Exception as e:
        print(f"Supabase 客户端初始化失败: {str(e)}")

# 关键词过滤文件路径
KEYWORDS_FILE = BASE_DIR / 'config/sensitive_words.txt'

# 日志配置
LOG_DIR = BASE_DIR / 'logs'
os.makedirs(LOG_DIR, exist_ok=True)  # 确保日志目录存在
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_MAX_SIZE = int(os.getenv('LOG_MAX_SIZE', str(1024*1024*5)))  # 默认5MB
LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', '3'))

# 增强的日志配置
LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': str(LOG_DIR / f"{datetime.now().strftime('%Y-%m-%d')}_monitor.log"),  # 使用当天日期
            'maxBytes': LOG_MAX_SIZE,
            'backupCount': LOG_BACKUP_COUNT,
            'formatter': 'standard',
            'encoding': 'utf-8',
            'mode': 'a',  # 确保使用追加模式
        },
        'console': {
            'class': 'logging.StreamHandler',
            'stream': sys.stdout,
            'formatter': 'standard',
        }
    },
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s - %(message)s'
        },
    },
    'loggers': {
        '': {
            'handlers': ['file', 'console'],
            'level': LOG_LEVEL,
            'propagate': True
        }
    }
}

# 自动发现频道配置
AUTO_CHANNEL_DISCOVERY = os.getenv('AUTO_CHANNEL_DISCOVERY', 'true').lower() == 'true'
DISCOVERY_INTERVAL = int(os.getenv('DISCOVERY_INTERVAL', '3600'))
MIN_CHANNEL_MEMBERS = int(os.getenv('MIN_CHANNEL_MEMBERS', '500'))
MAX_AUTO_CHANNELS = int(os.getenv('MAX_AUTO_CHANNELS', '10'))
EXCLUDED_CHANNELS = os.getenv('EXCLUDED_CHANNELS', '').split(',')

# Web应用配置
WEB_HOST = os.getenv('WEB_HOST', '0.0.0.0')
WEB_PORT = int(os.getenv('WEB_PORT', '5000'))
WEB_DEBUG = os.getenv('WEB_DEBUG', 'false').lower() == 'true'
FLASK_SECRET_KEY = os.getenv('FLASK_SECRET_KEY', 'telegram-monitor-default-secret-key')

# 群组和频道优先级配置
PREFER_GROUPS = os.getenv('PREFER_GROUPS', 'false').lower() == 'true'
GROUPS_ONLY = os.getenv('GROUPS_ONLY', 'false').lower() == 'true'
CHANNELS_ONLY = os.getenv('CHANNELS_ONLY', 'false').lower() == 'true'

# 批处理配置
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '5'))
BATCH_INTERVAL = int(os.getenv('BATCH_INTERVAL', '15'))

# 错误处理配置
ERROR_MAX_RETRIES = int(os.getenv('ERROR_MAX_RETRIES', '3'))
ERROR_RETRY_DELAY = float(os.getenv('ERROR_RETRY_DELAY', '1.0'))
ERROR_REPORT_INTERVAL = int(os.getenv('ERROR_REPORT_INTERVAL', '3600'))

# DAS API 配置
DAS_API_KEY = os.getenv('DAS_API_KEY', '')

# 代币数据更新配置
TOKEN_UPDATE_LIMIT = int(os.getenv('TOKEN_UPDATE_LIMIT', '20'))
TOKEN_UPDATE_INTERVAL = int(os.getenv('TOKEN_UPDATE_INTERVAL', '999999'))  # 将默认值从2分钟改为999999分钟，实际上禁用更新
TOKEN_UPDATE_MIN_DELAY = float(os.getenv('TOKEN_UPDATE_MIN_DELAY', '0.5'))
TOKEN_UPDATE_MAX_DELAY = float(os.getenv('TOKEN_UPDATE_MAX_DELAY', '2.0'))
TOKEN_UPDATE_BATCH_SIZE = int(os.getenv('TOKEN_UPDATE_BATCH_SIZE', '50'))

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
        # Telegram API配置
        self.API_ID = int(os.getenv('TG_API_ID', '0'))
        self.API_HASH = os.getenv('TG_API_HASH', '')
        
        # 数据库配置
        self.DATABASE_URI = DATABASE_URI
        
        # Supabase配置
        self.SUPABASE_URL = SUPABASE_URL
        self.SUPABASE_KEY = SUPABASE_KEY
        self.SUPABASE_SERVICE_KEY = SUPABASE_SERVICE_KEY
        
        # 添加路径校验
        sensitive_words_path = BASE_DIR / 'config/sensitive_words.txt'
        if not sensitive_words_path.exists():
            print("警告：未检测到关键词过滤文件，将创建空文件")
            sensitive_words_path.touch()

        # 媒体存储路径
        self.MEDIA_DIR = BASE_DIR / 'media'
        self.MEDIA_DIR.mkdir(exist_ok=True)
        
        # 自动发现频道配置
        self.AUTO_CHANNEL_DISCOVERY = AUTO_CHANNEL_DISCOVERY
        self.DISCOVERY_INTERVAL = DISCOVERY_INTERVAL
        self.MIN_CHANNEL_MEMBERS = MIN_CHANNEL_MEMBERS
        self.MAX_AUTO_CHANNELS = MAX_AUTO_CHANNELS
        self.EXCLUDED_CHANNELS = [ch.strip() for ch in EXCLUDED_CHANNELS if ch.strip()]
        self.CHAIN_KEYWORDS = chain_keywords
        
        # Web应用配置
        self.WEB_HOST = WEB_HOST
        self.WEB_PORT = WEB_PORT
        self.WEB_DEBUG = WEB_DEBUG
        self.FLASK_SECRET_KEY = FLASK_SECRET_KEY
        
        # 群组和频道优先级配置
        self.PREFER_GROUPS = PREFER_GROUPS
        self.GROUPS_ONLY = GROUPS_ONLY
        self.CHANNELS_ONLY = CHANNELS_ONLY
        
        # 批处理配置
        self.BATCH_SIZE = BATCH_SIZE
        self.BATCH_INTERVAL = BATCH_INTERVAL
        
        # 错误处理配置
        self.ERROR_MAX_RETRIES = ERROR_MAX_RETRIES
        self.ERROR_RETRY_DELAY = ERROR_RETRY_DELAY
        self.ERROR_REPORT_INTERVAL = ERROR_REPORT_INTERVAL
        
        # DAS API配置
        self.DAS_API_KEY = DAS_API_KEY
        
        # 代币数据更新配置
        self.TOKEN_UPDATE_LIMIT = TOKEN_UPDATE_LIMIT
        self.TOKEN_UPDATE_INTERVAL = TOKEN_UPDATE_INTERVAL
        self.TOKEN_UPDATE_MIN_DELAY = TOKEN_UPDATE_MIN_DELAY
        self.TOKEN_UPDATE_MAX_DELAY = TOKEN_UPDATE_MAX_DELAY
        self.TOKEN_UPDATE_BATCH_SIZE = TOKEN_UPDATE_BATCH_SIZE

# 实例化配置对象便于引用
env_config = EnvConfig()

# 注释：这个配置将由setup_logger在需要时使用，不自动应用
# logging.config.dictConfig(LOG_CONFIG)  # 不直接应用配置

# 加载配置函数
def load_config(_=None):
    """
    从环境变量加载配置，兼容旧代码
    
    Args:
        _: 忽略参数，兼容旧代码（原来接收config_file参数）
        
    Returns:
        dict: 配置字典
    """
    try:
        # 从环境变量构建配置字典
        config = {
            "telegram": {
                "api_id": env_config.API_ID,
                "api_hash": env_config.API_HASH
            },
            "database": {
                "uri": DATABASE_URI
            },
            "DATABASE_URI": DATABASE_URI,
            "web_server": {
                "host": WEB_HOST,
                "port": WEB_PORT,
                "debug": WEB_DEBUG
            },
            "discovery": {
                "enabled": AUTO_CHANNEL_DISCOVERY,
                "interval": DISCOVERY_INTERVAL,
                "min_members": MIN_CHANNEL_MEMBERS,
                "max_channels": MAX_AUTO_CHANNELS,
                "excluded_channels": env_config.EXCLUDED_CHANNELS
            },
            "token_update": {
                "limit": TOKEN_UPDATE_LIMIT,
                "interval": TOKEN_UPDATE_INTERVAL,
                "min_delay": TOKEN_UPDATE_MIN_DELAY,
                "max_delay": TOKEN_UPDATE_MAX_DELAY,
                "batch_size": TOKEN_UPDATE_BATCH_SIZE
            }
        }
        
        # 日志信息
        logger.info(f"已从环境变量加载配置，DATABASE_URI: {DATABASE_URI[:15]}..." if DATABASE_URI else "已从环境变量加载配置，但DATABASE_URI未设置")
        
        return config
    except Exception as e:
        logger.error(f"加载配置时出错: {str(e)}")
        raise

# 为了兼容现有代码，定义CONFIG_FILE变量
# 使用找到的环境变量文件路径或默认路径
CONFIG_FILE = str(found_env_path)  # 使用找到的环境变量文件路径
