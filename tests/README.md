# Telegram 监控系统测试套件

这个目录包含了对Telegram监控系统的单元测试和集成测试。

## 测试内容

测试套件包含以下主要测试模块：

1. `test_models.py` - 测试数据库模型和相关功能
2. `test_utils.py` - 测试工具函数和辅助功能
3. `test_channel_manager.py` - 测试频道管理器功能
4. `test_telegram_listener.py` - 测试Telegram监听器功能
5. `test_async_retry.py` - 测试异步重试机制
6. `test_channel_discovery.py` - 测试频道发现和递归搜索功能
7. `test_db_handler.py` - 测试数据库处理和存储功能
8. `test_error_handler.py` - 测试错误处理和日志记录功能

## 运行测试

### 运行所有测试

```bash
python tests/run_tests.py
```

### 运行带有覆盖率报告的测试

```bash
python tests/run_tests.py --coverage
```

### 运行特定测试模块

```bash
# 运行单个测试文件
python tests/run_tests.py test_models.py

# 运行多个测试文件
python tests/run_tests.py test_models.py test_utils.py

# 使用Python unittest模块直接运行
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

测试还需要以下依赖项，这些主要用于测试和代码质量：

```
pytest
coverage
pytest-asyncio
pytest-cov
```

测试运行期间会自动创建必要的目录结构（logs、data、media）。大多数测试使用内存数据库进行，不会影响实际数据库内容。

## 开发新的测试

开发新测试时，请遵循以下规则：

1. 为每个功能模块创建单独的测试文件，命名为`test_模块名.py`
2. 测试类应继承`unittest.TestCase`
3. 测试方法应以`test_`开头
4. 使用`setUp`和`tearDown`方法初始化和清理测试环境
5. 异步测试应使用`async def`定义，并提供一个同步包装器
6. 使用模拟对象替代外部依赖，如Telegram API
7. 尽可能使测试独立，避免依赖外部资源

## 测试文件结构

每个测试文件应包含：

- 导入必要的模块
- 一个或多个测试类（继承自`unittest.TestCase`）
- 测试方法（以`test_`开头）
- 必要的辅助方法和测试数据

## 注意事项

- 测试使用模拟对象替代外部依赖，如Telegram API和数据库连接
- 大多数测试不需要实际的网络连接
- 如果需要模拟复杂的Telegram事件，请参考`test_telegram_listener.py`中的示例
- 测试执行可能会创建临时文件，这些文件应在测试结束后清理
- 如果测试需要访问.env配置，请确保.env文件存在或使用模拟数据 