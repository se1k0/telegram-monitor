import unittest
import sys
import os
import asyncio
from unittest.mock import MagicMock, patch
from datetime import datetime

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
        from sqlalchemy import create_engine
        from src.database.models import Base
        
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        
        # 创建临时目录
        os.makedirs('./media', exist_ok=True)
        os.makedirs('./data', exist_ok=True)
        os.makedirs('./logs', exist_ok=True)
        
        # 数据库会话
        from sqlalchemy.orm import sessionmaker
        self.Session = sessionmaker(bind=self.engine)
        
        # 打补丁模拟TelegramClient
        self.client_patcher = patch('src.core.telegram_listener.TelegramClient')
        self.mock_client_class = self.client_patcher.start()
        self.mock_client = MagicMock()
        
        # 设置客户端类返回模拟的客户端实例
        self.mock_client_class.return_value = self.mock_client
        
        # 添加异步方法的模拟
        self.mock_client.is_connected = MagicMock(return_value=False)
        self.mock_client.connect = AsyncMock()
        self.mock_client.start = AsyncMock()
        self.mock_client.add_event_handler = MagicMock()
        self.mock_client.remove_event_handler = MagicMock()
        self.mock_client.download_media = AsyncMock()
        
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
        self.client_patcher.stop()
        self.env_patcher.stop()
        self.db_patcher.stop()
        self.cm_patcher.stop()
        self.dotenv_patcher.stop()
    
    def test_init(self):
        """测试初始化功能"""
        # 模拟ChannelManager
        with patch('src.core.telegram_listener.ChannelManager') as mock_cm:
            # 导入需要测试的类
            from src.core.telegram_listener import TelegramListener
            
            # 创建监听器实例
            listener = TelegramListener()
            
            # 验证API认证信息
            self.assertEqual(listener.api_id, '12345')
            self.assertEqual(listener.api_hash, 'abcdef123456')
            
            # 验证是否创建了客户端
            self.assertIsNotNone(listener.client)
            
            # 验证是否创建了频道管理器
            self.assertIsNotNone(listener.channel_manager)
            
            # 验证事件处理器映射是否为空
            self.assertEqual(len(listener.event_handlers), 0)
    
    async def async_handle_new_message_test(self):
        """测试处理新消息功能的异步方法"""
        # 导入需要测试的类
        from src.core.telegram_listener import TelegramListener
        
        # 创建监听器实例，但用模拟替换依赖
        with patch('src.core.telegram_listener.ChannelManager'):
            listener = TelegramListener()
            
            # 使用模拟客户端
            listener.client = self.mock_client
            
            # 设置chain_map
            listener.chain_map = {
                'test_channel': 'SOL'
            }
            
            # 创建模拟事件
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
            
            # 模拟下载媒体文件和保存消息
            with patch('src.core.telegram_listener.save_telegram_message') as mock_save_message:
                # 设置save_message返回True表示成功保存
                mock_save_message.return_value = True
                
                # 模拟提取推广信息
                with patch('src.core.telegram_listener.extract_promotion_info') as mock_extract_info:
                    from src.database.models import PromotionInfo
                    mock_promo = PromotionInfo(
                        token_symbol='TEST',
                        contract_address='0x1234567890abcdef',
                        market_cap='100K',
                        promotion_count=1,
                        telegram_url=None,
                        twitter_url=None,
                        website_url=None,
                        first_trending_time=datetime.now(),
                        chain='SOL'
                    )
                    mock_extract_info.return_value = mock_promo
                    
                    # 模拟保存token信息
                    with patch('src.core.telegram_listener.save_token_info') as mock_save_token:
                        # 处理消息
                        await listener.handle_new_message(mock_event)
                        
                        # 验证是否调用了save_telegram_message
                        mock_save_message.assert_called_once()
                        args = mock_save_message.call_args[1]
                        self.assertEqual(args['chain'], 'SOL')
                        self.assertEqual(args['message_id'], 12345)
                        self.assertEqual(args['text'], mock_message.text)
                        
                        # 验证是否调用了extract_promotion_info
                        mock_extract_info.assert_called_once_with(mock_message.text, mock_message.date, 'SOL')
                        
                        # 验证是否调用了save_token_info
                        mock_save_token.assert_called_once()
                        token_data = mock_save_token.call_args[0][0]
                        self.assertEqual(token_data['chain'], 'SOL')
                        self.assertEqual(token_data['token_symbol'], 'TEST')
                        self.assertEqual(token_data['contract'], '0x1234567890abcdef')
    
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
        # 导入需要测试的类
        from src.core.telegram_listener import TelegramListener
        
        # 创建监听器实例
        with patch('src.core.telegram_listener.ChannelManager') as mock_cm_class:
            mock_cm = MagicMock()
            mock_cm_class.return_value = mock_cm
            
            listener = TelegramListener()
            listener.client = self.mock_client
            
            # 模拟客户端连接状态
            listener.client.is_connected = MagicMock(return_value=False)
            
            # 模拟更新频道
            mock_channels = {
                'channel1': 'SOL',
                'channel2': 'ETH'
            }
            mock_cm.update_channels = AsyncMock(return_value=mock_channels)
            
            # 模拟注册处理程序
            listener.register_handlers = AsyncMock()
            
            # 执行设置频道
            await listener.setup_channels()
            
            # 验证调用
            listener.client.connect.assert_called_once()
            mock_cm.update_channels.assert_called_once()
            listener.register_handlers.assert_called_once()
            
            # 验证chain_map是否已更新
            self.assertEqual(listener.chain_map, mock_channels)
    
    def test_setup_channels(self):
        """测试设置频道监听功能（非异步包装器）"""
        # 创建新的事件循环
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.async_setup_channels_test())
        finally:
            loop.close()
    
    async def async_register_handlers_test(self):
        """测试注册消息处理程序功能的异步方法"""
        # 导入需要测试的类
        from src.core.telegram_listener import TelegramListener
        
        # 创建监听器实例
        with patch('src.core.telegram_listener.ChannelManager'):
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
        """测试注册消息处理程序功能（非异步包装器）"""
        # 创建新的事件循环
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.async_register_handlers_test())
        finally:
            loop.close()

if __name__ == '__main__':
    unittest.main() 