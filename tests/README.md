# Telegram 监控系统测试套件

这个目录包含了对Telegram监控系统的单元测试和集成测试。

## 测试内容

测试套件包含以下主要测试模块：

1. `test_models.py` - 测试数据库模型和相关功能
2. `test_utils.py` - 测试工具函数
3. `test_channel_manager.py` - 测试频道管理器功能
4. `test_telegram_listener.py` - 测试Telegram监听器功能

## 运行测试

### 运行所有测试

```bash
python tests/run_tests.py
```

### 运行单个测试模块

```bash
python -m unittest tests/test_models.py
python -m unittest tests/test_utils.py
python -m unittest tests/test_channel_manager.py
python -m unittest tests/test_telegram_listener.py
```

### 运行特定测试用例

```bash
python -m unittest tests.test_models.TestDatabaseModels.test_telegram_channel
```

## 测试环境要求

在运行测试前，确保已安装所有依赖项：

```bash
pip install -r requirements.txt
```

测试运行期间会自动创建必要的目录结构。大多数测试使用内存数据库进行，不会影响实际数据库内容。

## 开发新的测试

开发新测试时，请遵循以下规则：

1. 为每个功能模块创建单独的测试文件，命名为`test_模块名.py`
2. 测试类应继承`unittest.TestCase`
3. 测试方法应以`test_`开头
4. 使用`setUp`和`tearDown`方法初始化和清理测试环境
5. 异步测试应使用`async def`定义，并提供一个同步包装器

## 注意事项

- 测试使用模拟对象替代外部依赖，如Telegram API
- 大多数测试不需要实际的网络连接
- 如果需要模拟复杂的Telegram事件，请参考`test_telegram_listener.py`中的示例 