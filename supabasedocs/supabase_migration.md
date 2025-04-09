# 数据库迁移指南：从SQLite迁移到Supabase

本文档提供了将Telegram监控服务的数据库从本地SQLite迁移到Supabase云数据库的详细步骤。

## 目录

1. [前提条件](#前提条件)
2. [准备工作](#准备工作)
3. [在Supabase创建表结构](#在supabase创建表结构)
4. [迁移数据](#迁移数据)
5. [更新应用程序配置](#更新应用程序配置)
6. [验证迁移](#验证迁移)
7. [故障排除](#故障排除)

## 前提条件

在开始迁移之前，请确保满足以下条件：

- 已有Supabase账户并创建了项目
- 已获取Supabase项目URL和API密钥
- 已安装Python 3.7或更高版本
- 已安装Supabase Python客户端库
- 确保你的项目已正确备份

## 准备工作

1. 安装必要的Python库：

```bash
pip install supabase
```

2. 更新`.env`文件中的Supabase配置：

```
# Supabase配置
SUPABASE_URL=https://your-project-url.supabase.co
SUPABASE_KEY=your-supabase-api-key
```

## 在Supabase创建表结构

1. 登录Supabase控制台
2. 导航到SQL编辑器
3. 复制`scripts/create_supabase_tables.sql`中的内容
4. 在SQL编辑器中粘贴并运行SQL脚本

**重要提示**：在运行SQL脚本前，请确保已了解脚本内容并确认其符合您的需求。脚本将创建与SQLite数据库中相同的表结构。

## 迁移数据

我们提供了一个专门的迁移脚本，可以自动将数据从SQLite迁移到Supabase。

1. 运行迁移脚本：

```bash
python scripts/migrate_to_supabase.py
```

2. 脚本将执行以下操作：
   - 检查Supabase连接
   - 验证表结构
   - 批量迁移数据
   - 更新`.env`文件以使用Supabase

迁移过程可能需要一些时间，取决于数据量的大小。脚本将显示进度条和迁移状态信息。

## 更新应用程序配置

在迁移完成后，我们需要更新应用程序代码以使用Supabase。我们提供了一个自动化脚本来完成这项工作：

```bash
python scripts/update_db_usage.py
```

此脚本将：
- 扫描项目中使用数据库的Python文件
- 将直接使用SQLAlchemy的代码替换为使用数据库工厂
- 更新导入语句和数据库操作

**注意**：自动化脚本可能无法捕获所有需要修改的地方，特别是自定义SQL查询和复杂的数据库操作。请在运行后手动检查代码。

## 验证迁移

迁移完成后，请进行以下验证：

1. 检查Supabase中的表是否包含正确的数据
2. 运行应用程序并确保其能正常连接到Supabase
3. 测试主要功能以确保一切正常工作

验证命令：

```bash
python main.py --check-db
```

此命令将检查应用程序是否能够成功连接到Supabase并执行基本操作。

## 故障排除

如果在迁移过程中遇到问题，请检查以下几点：

### 连接问题

- 确保Supabase URL和API密钥正确
- 检查网络连接是否正常
- 验证Supabase项目是否活跃

### 表结构错误

- 检查是否已正确运行创建表的SQL脚本
- 确认表结构与SQLite数据库中的结构匹配

### 权限问题

- 确保Supabase API密钥具有足够的权限
- 检查RLS（行级安全）策略是否配置正确

### 数据类型不匹配

- SQLite和PostgreSQL（Supabase底层数据库）的数据类型有一些差异
- 检查日期、布尔值等类型的字段是否正确转换

## 附录：手动迁移

如果自动迁移脚本无法满足您的需求，您也可以按照以下步骤手动迁移：

1. 导出SQLite数据为CSV格式
2. 在Supabase控制台中导入CSV数据
3. 手动更新应用程序代码以使用Supabase客户端

## 结论

完成上述步骤后，您的Telegram监控服务应该已成功从本地SQLite数据库迁移到Supabase云数据库。这将为您的应用程序提供更好的可扩展性、可靠性和安全性。

如果您在迁移过程中遇到任何问题，请检查日志文件或联系技术支持获取帮助。 