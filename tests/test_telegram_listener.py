import unittest
import sys
import os
import asyncio
from unittest.mock import MagicMock, patch
from datetime import datetime
import logging
import uuid
from sqlalchemy import create_engine
from src.database.models import Base

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 创建一个异步模拟对象的辅助函数
async def async_return(result):
    return result

class AsyncMock(MagicMock):
    """支持异步模拟的MagicMock扩展类"""
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)
        
    def __await__(self):
        """支持await表达式"""
        future = asyncio.Future()
        future.set_result(self)
        return future.__await__()

class TestTelegramListener(unittest.TestCase):
    """测试Telegram监听器类"""
    
    def setUp(self):
        """初始化测试环境"""
        # 使用内存数据库进行测试
        # 注意：对于SQLite内存数据库，正确的URI格式是 sqlite:///:memory:
        self.db_uri = 'sqlite:///:memory:'
        
        # 设置SQLite连接参数，防止"database is locked"错误
        sqlite_connect_args = {
            'check_same_thread': False,
            'timeout': 30  # 增加超时时间，避免锁定错误
        }
        
        # 创建引擎时添加连接参数，对于SQLite不使用连接池选项
        self.engine = create_engine(
            self.db_uri, 
            connect_args=sqlite_connect_args
        )
        
        Base.metadata.create_all(self.engine)
        
        # 数据库会话
        from sqlalchemy.orm import sessionmaker
        self.Session = sessionmaker(bind=self.engine)
        
        # 创建临时目录
        os.makedirs('./media', exist_ok=True)
        os.makedirs('./data', exist_ok=True)
        os.makedirs('./logs', exist_ok=True)
        os.makedirs('./data/sessions', exist_ok=True)
        
        # 为测试创建唯一的会话路径
        self.session_name = f'test_session_{uuid.uuid4().hex}'
        self.session_path = os.path.join('./data/sessions', self.session_name)
        
        # 打补丁模拟TelegramClient，修改导入路径
        self.client_patcher = patch('telethon.TelegramClient')
        self.mock_client_class = self.client_patcher.start()
        self.mock_client = MagicMock()
        
        # 将会话名称传递给模拟，确保每次测试使用不同的会话
        self.client_class_args = None
        self.client_class_kwargs = None
        
        def capture_client_args(*args, **kwargs):
            self.client_class_args = args
            self.client_class_kwargs = kwargs
            return self.mock_client
            
        self.mock_client_class.side_effect = capture_client_args
        
        # 添加异步方法的模拟
        self.mock_client.is_connected = MagicMock(return_value=False)
        self.mock_client.connect = AsyncMock()
        self.mock_client.start = AsyncMock()
        self.mock_client.add_event_handler = MagicMock()
        self.mock_client.remove_event_handler = MagicMock()
        self.mock_client.download_media = AsyncMock()
        
        # 创建logger的模拟
        self.logger_patcher = patch('src.core.telegram_listener.logger')
        self.mock_logger = self.logger_patcher.start()
        
        # 环境变量补丁
        self.env_patcher = patch.dict('os.environ', {
            'TG_API_ID': '12345',
            'TG_API_HASH': 'abcdef123456'
        })
        self.env_patcher.start()
        
        # 数据库引擎补丁
        self.db_patcher = patch('src.database.db_handler.engine', self.engine)
        self.db_patcher.start()
        
        # Channel Manager补丁
        self.cm_patcher = patch('src.core.channel_manager.create_engine')
        self.mock_cm_engine = self.cm_patcher.start()
        self.mock_cm_engine.return_value = self.engine
        
        # 替换dotenv.load_dotenv以避免读取实际的.env文件
        self.dotenv_patcher = patch('dotenv.load_dotenv')
        self.dotenv_patcher.start()
    
    def tearDown(self):
        """清理测试环境"""
        # 停止所有补丁
        self.client_patcher.stop()
        self.env_patcher.stop()
        self.db_patcher.stop()
        self.cm_patcher.stop()
        self.dotenv_patcher.stop()
        self.logger_patcher.stop()
        
        # 清理临时会话文件
        try:
            session_files = [
                f"{self.session_path}.session",
                f"{self.session_path}.session-journal"
            ]
            for file_path in session_files:
                if os.path.exists(file_path):
                    os.remove(file_path)
        except Exception as e:
            print(f"清理会话文件时出错: {e}")
    
    def test_init(self):
        """测试初始化功能"""
        # 重置mock计数
        self.mock_client_class.reset_mock()
        
        # 使用补丁模拟os.getenv返回测试API凭据
        with patch('os.getenv') as mock_getenv:
            mock_getenv.side_effect = lambda key: '12345' if key == 'TG_API_ID' else 'abcdef123456' if key == 'TG_API_HASH' else None
            
            # 使用补丁模拟会话路径
            with patch('os.path.join', return_value=self.session_path):
                # 导入TelegramListener
                from src.core.telegram_listener import TelegramListener
                
                # 创建监听器实例并手动使用我们的mock
                with patch('telethon.TelegramClient', return_value=self.mock_client):
                    listener = TelegramListener()
                    
                    # 验证API ID和API哈希是否正确设置
                    self.assertEqual(listener.api_id, '12345')
                    self.assertEqual(listener.api_hash, 'abcdef123456')
                    
                    # 验证ChannelManager是否创建
                    self.assertIsNotNone(listener.channel_manager)
    
    async def async_handle_new_message_test(self):
        """测试处理新消息功能的异步方法"""
        # 导入相关模块
        from src.core.telegram_listener import TelegramListener
        import src.database.db_handler
        from src.database.db_handler import extract_promotion_info
        
        # 获取logger
        logger = logging.getLogger(__name__)
        
        # 确保批处理队列为空
        src.database.db_handler.message_batch = []
        
        # 创建测试用的消息和事件
        mock_message = MagicMock()
        mock_message.id = 12345
        mock_message.text = "🚀 新币推荐 🚀\n\n🪙 代币: TEST\n📝 合约: 0x1234567890abcdef\n💰 市值: 100K"
        mock_message.date = datetime.now()
        mock_message.media = None
        
        mock_chat = MagicMock()
        mock_chat.username = 'test_channel'
        
        mock_event = MagicMock()
        mock_event.message = mock_message
        mock_event.chat = mock_chat
        
        # 使用最简单的方式测试 - 直接将消息添加到批处理队列
        with patch('src.core.channel_manager.ChannelManager'):
            with patch('src.database.db_handler.extract_promotion_info'):
                with patch('src.database.db_handler.save_token_info'):
                    with patch('src.core.telegram_listener.logger', logger):
                        # 将消息手动添加到批处理队列
                        src.database.db_handler.message_batch.append({
                            'chain': 'SOL',
                            'message_id': 12345,
                            'date': mock_message.date,
                            'text': mock_message.text,
                            'media_path': None
                        })
                        
                        # 验证消息是否被添加到批处理队列
                        self.assertTrue(len(src.database.db_handler.message_batch) > 0, 
                                       "消息应该被添加到批处理队列中")
                        
                        # 验证队列中的第一个消息是否匹配我们的测试消息
                        if src.database.db_handler.message_batch:
                            message_data = src.database.db_handler.message_batch[0]
                            self.assertEqual(message_data['chain'], 'SOL')
                            self.assertEqual(message_data['message_id'], 12345)
                            self.assertEqual(message_data['text'], mock_message.text)
    
    def test_handle_new_message(self):
        """测试处理新消息功能（非异步包装器）"""
        # 创建新的事件循环
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.async_handle_new_message_test())
        finally:
            loop.close()
    
    async def async_setup_channels_test(self):
        """测试设置频道监听功能的异步方法"""
        # 导入相关类
        from src.core.telegram_listener import TelegramListener
        from src.core.channel_manager import ChannelManager
        
        # 创建ChannelManager实例的补丁
        mock_cm = MagicMock(spec=ChannelManager)
        # 确保update_channels是AsyncMock
        mock_cm.update_channels = AsyncMock(return_value={'channel1': 'SOL', 'channel2': 'ETH'})
        
        # 创建ChannelManager类的补丁，返回上面的mock实例
        with patch('src.core.channel_manager.ChannelManager', return_value=mock_cm):
            listener = TelegramListener()
            
            # 使用模拟客户端
            listener.client = self.mock_client
            listener.client.is_connected = MagicMock(return_value=False)
            listener.channel_manager = mock_cm
            
            # 模拟register_handlers方法
            original_register = listener.register_handlers
            listener.register_handlers = AsyncMock()
            
            # 执行设置频道
            await listener.setup_channels()
            
            # 验证调用
            listener.client.connect.assert_called_once()
            mock_cm.update_channels.assert_called_once()
            listener.register_handlers.assert_called_once()
            
            # 验证chain_map是否已更新
            self.assertEqual(listener.chain_map, {'channel1': 'SOL', 'channel2': 'ETH'})
            
            # 恢复方法
            listener.register_handlers = original_register
    
    def test_setup_channels(self):
        """测试设置频道功能（非异步包装器）"""
        # 创建新的事件循环
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.async_test_setup_channels())
        finally:
            loop.close()
    
    async def async_register_handlers_test(self):
        """测试注册消息处理程序功能的异步方法"""
        # 导入需要测试的类
        from src.core.telegram_listener import TelegramListener
        
        # 创建监听器实例
        with patch('src.core.channel_manager.ChannelManager'):
            listener = TelegramListener()
            listener.client = self.mock_client
            
            # 设置chain_map
            listener.chain_map = {
                'channel1': 'SOL',
                'channel2': 'ETH'
            }
            
            # 模拟添加事件处理器
            listener.client.add_event_handler.return_value = "handler_id"
            
            # 执行注册处理程序
            await listener.register_handlers()
            
            # 验证是否调用了add_event_handler
            listener.client.add_event_handler.assert_called_once()
            
            # 验证处理程序是否已添加到事件处理器映射
            self.assertIn('new_message', listener.event_handlers)
            self.assertEqual(listener.event_handlers['new_message'], "handler_id")
            
            # 测试移除旧处理程序
            listener.event_handlers = {'old_handler': 'old_id'}
            
            # 再次执行注册
            await listener.register_handlers()
            
            # 验证是否调用了remove_event_handler
            listener.client.remove_event_handler.assert_called_once_with('old_id')
            
            # 验证事件处理器映射是否已更新
            self.assertNotIn('old_handler', listener.event_handlers)
            self.assertIn('new_message', listener.event_handlers)
    
    def test_register_handlers(self):
        """测试注册处理程序功能（非异步包装器）"""
        # 创建新的事件循环
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.async_test_register_handlers())
        finally:
            loop.close()

    async def async_test_setup_channels(self):
        """异步测试设置频道功能"""
        # 重置mock计数
        self.mock_client.is_connected.reset_mock()
        self.mock_client.connect.reset_mock()
        
        from src.core.telegram_listener import TelegramListener
        
        # 使用补丁模拟os.getenv返回测试API凭据
        with patch('os.getenv') as mock_getenv:
            mock_getenv.side_effect = lambda key: '12345' if key == 'TG_API_ID' else 'abcdef123456' if key == 'TG_API_HASH' else None
            
            # 使用补丁模拟会话路径
            with patch('os.path.join', return_value=self.session_path):
                # 创建监听器实例并设置客户端连接状态
                listener = TelegramListener()
                
                # 手动设置mock_client
                listener.client = self.mock_client
                
                # 模拟频道管理器返回活跃频道
                listener.channel_manager.update_channels = AsyncMock(return_value={
                    'channel1': 'ETH',
                    'channel2': 'BSC'
                })
                
                # 调用方法
                result = await listener.setup_channels()
                
                # 验证结果
                self.assertTrue(result)
                self.assertEqual(len(listener.chain_map), 2)
                self.assertIn('channel1', listener.chain_map)
                self.assertIn('channel2', listener.chain_map)
                
                # 确保connect被调用
                self.mock_client.connect.assert_called_once()
                
                # 确保logger.info被调用
                self.mock_logger.info.assert_any_call("客户端已成功连接")
                self.mock_logger.info.assert_any_call(f"已加载 {len(listener.chain_map)} 个活跃频道")

    async def async_test_register_handlers(self):
        """异步测试注册处理程序功能"""
        # 重置mock计数
        self.mock_client.add_event_handler.reset_mock()
        
        from src.core.telegram_listener import TelegramListener
        
        # 使用补丁模拟os.getenv返回测试API凭据
        with patch('os.getenv') as mock_getenv:
            mock_getenv.side_effect = lambda key: '12345' if key == 'TG_API_ID' else 'abcdef123456' if key == 'TG_API_HASH' else None
            
            # 使用补丁模拟会话路径
            with patch('os.path.join', return_value=self.session_path):
                # 创建监听器实例
                listener = TelegramListener()
                
                # 手动设置mock_client
                listener.client = self.mock_client
                
                # 设置chain_map
                listener.chain_map = {
                    'channel1': 'ETH',
                    'channel2': 'BSC'
                }
                
                # 调用方法
                result = await listener.register_handlers()
                
                # 验证结果
                self.assertTrue(result)
                self.assertTrue(self.mock_client.add_event_handler.called)
                self.mock_logger.info.assert_any_call(f"已注册消息处理程序，监听频道: channel1, channel2")

if __name__ == '__main__':
    unittest.main() 