# Telegram监控系统 Supabase数据库集成指南

本指南详细介绍了如何将Telegram监控系统与Supabase云数据库集成，包括初始设置、数据迁移、ID自增配置和安全设置。

## 目录

1. [Supabase配置](#1-supabase配置)
2. [数据库结构设置](#2-数据库结构设置)
3. [数据迁移](#3-数据迁移)
4. [配置ID自增长](#4-配置id自增长)
5. [安全配置](#5-安全配置)
6. [验证与测试](#6-验证与测试)
7. [故障排除](#7-故障排除)

## 1. Supabase配置

### 1.1 创建项目

1. 访问 [Supabase Dashboard](https://app.supabase.com/)
2. 点击 "New Project" 创建新项目
3. 填写项目名称和密码，选择适当的区域
4. 等待项目创建完成

### 1.2 获取密钥

1. 在项目控制台中，点击左侧导航栏中的 "Project Settings"
2. 选择 "API" 标签页
3. 记录以下信息：
   - **项目URL**: 格式为 `https://your-project-id.supabase.co`
   - **匿名密钥(anon key)**: 用于普通API访问和读取操作
   - **服务角色密钥(service role key)**: 用于管理操作，如数据迁移和写入

### 1.3 更新环境配置

1. 在项目的根目录中找到 `.env` 文件
2. 添加或更新以下配置：
   ```
   SUPABASE_URL=https://your-project-id.supabase.co
   SUPABASE_KEY=your-anon-key
   SUPABASE_SERVICE_KEY=your-service-role-key
   DATABASE_URI=supabase://your-project-id.supabase.co/your-anon-key
   ```

> **重要**: 服务角色密钥具有完全管理权限，请妥善保管，不要提交到版本控制系统！

## 2. 数据库结构设置

### 2.1 创建表结构

1. 在Supabase控制台中，点击 "SQL Editor"
2. 点击 "New Query" 创建新查询
3. 将 `scripts/create_supabase_tables.sql` 的内容复制到查询编辑器中
4. 点击 "Run" 执行脚本，创建所有必要的表

### 2.2 调整整数列类型

由于SQLite和PostgreSQL对整数类型的处理不同，需要修改整数列类型：

1. 在SQL编辑器中创建新查询
2. 将 `scripts/update_supabase_schema.sql` 的内容复制到查询编辑器中
3. 执行查询，将关键整数列类型修改为 `BIGINT`

## 3. 数据迁移

### 3.1 准备工作

安装必要的依赖：
```bash
pip install supabase tqdm python-dotenv
```

### 3.2 运行迁移脚本

执行修复版的迁移脚本：
```bash
python scripts/migrate_to_supabase_fixed.py
```

这个脚本会执行以下操作：
- 检查Supabase连接
- 选择适合的密钥（优先使用服务角色密钥）
- 验证表结构是否存在
- 将数据从SQLite迁移到Supabase
- 更新环境配置

### 3.3 监控迁移进度

迁移脚本会显示进度条并输出日志，包括：
- 每个表的迁移进度
- 成功迁移的记录数
- 可能出现的错误信息

## 4. 配置ID自增长

### 4.1 ID自增序列设置

要确保表的ID字段正确自增，需要设置序列：

1. 在SQL编辑器中创建新查询
2. 将 `scripts/update_supabase_id_columns.sql` 的内容复制到查询编辑器中
3. 执行查询，为所有表创建和配置ID自增序列

### 4.2 验证ID自增配置

验证ID自增配置是否生效：

1. 在Supabase控制台中，点击 "Table Editor"
2. 选择任意表（如 `telegram_channels`）
3. 点击 "Insert Row"，不填写ID字段
4. 填写其他必要字段，保存
5. 验证是否自动生成了ID值

## 5. 安全配置

### 5.1 行级安全策略(RLS)

Supabase默认启用行级安全策略(RLS)，您可以选择以下配置方式：

1. **推荐配置**:
   - 使用服务角色密钥进行写入操作
   - 使用匿名密钥进行读取操作
   - 保持严格的RLS策略，限制匿名用户只读访问

2. **备选配置**:
   - 如果没有服务角色密钥访问权限，可临时调整RLS策略
   - 执行 `scripts/check_supabase_rls.py` 检查RLS配置
   - 迁移完成后恢复更严格的RLS策略

### 5.2 验证RLS配置

检查当前RLS策略是否满足需求：
```bash
python scripts/check_supabase_rls.py
```

## 6. 验证与测试

### 6.1 检查数据库连接

运行以下命令检查数据库连接是否正常：
```bash
python main.py --check-db
```

### 6.2 验证数据完整性

1. 在Supabase控制台中检查各表的记录数是否与原始数据库一致
2. 抽样检查部分记录内容是否正确迁移
3. 验证关联关系是否保持完整

### 6.3 测试系统功能

1. 启动应用程序：`python main.py`
2. 测试Telegram监听功能，确认新消息能正确保存到Supabase
3. 测试Web界面能正确显示和操作数据

## 7. 故障排除

### 7.1 连接错误

如果遇到连接错误：
1. 确认`.env`文件中的URL和密钥是否正确
2. 检查网络连接是否正常
3. 验证Supabase项目是否活跃

### 7.2 ID自增问题

如果ID自增不正常：
1. 检查 `update_supabase_id_columns.sql` 是否已成功执行
2. 在SQL编辑器中查询序列状态：
   ```sql
   SELECT * FROM information_schema.sequences;
   ```
3. 重新执行ID序列配置脚本

### 7.3 RLS权限问题

如果遇到 "violates row-level security policy" 错误：
1. 检查应用程序是否正确使用服务角色密钥进行写入操作
2. 临时调整RLS策略允许匿名用户写入：
   ```sql
   ALTER POLICY "允许匿名用户只读访问tokens表" ON tokens
   USING (true) WITH CHECK (true);
   ```
3. 使用 `check_supabase_rls.py` 验证RLS配置

### 7.4 数据类型问题

如果遇到数据类型错误：
1. 检查 `update_supabase_schema.sql` 是否已成功执行
2. 确认所有需要的整数列已修改为 `BIGINT` 类型
3. 如果有日期/时间类型错误，确保格式兼容PostgreSQL

## 结论

完成本指南中的所有步骤后，您的Telegram监控系统应已成功集成Supabase云数据库，享受更好的可扩展性、可靠性和安全性。

如需进一步帮助，请参考[Supabase官方文档](https://supabase.com/docs)或联系技术支持团队。 