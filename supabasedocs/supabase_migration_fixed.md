# SQLite到Supabase数据库迁移指南（修复版）

## 背景

原迁移脚本由于Supabase中的整数类型范围问题导致迁移失败。错误信息通常类似于：

```
value '2383506717' is out of range for type integer
```

这是因为PostgreSQL（Supabase使用的数据库）的`integer`类型范围为-2,147,483,648到2,147,483,647，而SQLite允许更大的整数值。

## 解决方案

修复版迁移脚本解决了以下几个主要问题：

1. **表结构修改**：将Supabase中相关的整数列类型从`integer`改为`bigint`，以支持更大范围的整数
2. **密钥权限管理**：优先使用服务角色密钥(service role key)进行数据迁移，以绕过行级安全策略(RLS)的限制
3. **行级安全策略(RLS)配置**：提供更新RLS策略的可选方案，适用于只有匿名密钥的情况

## 迁移步骤

### 1. 准备工作

1. 确保安装了必要的依赖库：
   ```bash
   pip install supabase tqdm python-dotenv
   ```

2. 确保SQLite数据库文件存在于指定位置
3. 设置Supabase项目，并获取API URL和密钥

### 2. 配置Supabase密钥

1. 登录Supabase Dashboard创建一个新项目
2. 在"项目设置" > "API"中获取以下信息：
   - 项目URL（例如：`https://example.supabase.co`）
   - 匿名密钥（anon public）- 用于普通API访问
   - 服务角色密钥（service role）- 用于管理操作，如数据迁移
3. 在`.env`文件中配置Supabase连接信息：
   ```
   SUPABASE_URL=https://your-project-url.supabase.co
   SUPABASE_KEY=your-anon-key
   SUPABASE_SERVICE_KEY=your-service-role-key
   ```

> **重要提示**：服务角色密钥具有管理员权限，可以绕过所有安全策略，请妥善保管！

### 3. 执行表结构更新

在迁移数据之前，需要先更新Supabase中的表结构：

1. 在Supabase Dashboard中找到SQL编辑器
2. 创建新查询，粘贴`scripts/update_supabase_schema.sql`文件中的SQL语句
3. 执行SQL，将整数列类型更改为`bigint`

### 4. 运行修复版迁移脚本

执行以下命令运行迁移脚本：

```bash
python scripts/migrate_to_supabase_fixed.py
```

脚本将执行以下操作：

1. 检查Supabase连接
2. 自动选择适合的密钥（优先使用服务角色密钥）
2. 提示您确认表结构是否已更新
4. 检查必要的表是否存在于Supabase中
5. 将SQLite数据库中的数据迁移到Supabase
6. 更新`.env`文件，将数据库配置指向Supabase

### 5. 密钥选择策略

迁移脚本会根据您的配置自动选择合适的密钥：

- **推荐方式**：使用服务角色密钥(service role key)迁移数据
  - 可以绕过行级安全策略(RLS)
  - 无需修改RLS配置
  - 适合一次性的数据迁移操作
  
- **备选方式**：使用匿名密钥(anon key)迁移数据
  - 需要修改RLS策略允许数据写入（参见`docs/supabase_rls_update_guide.md`）
  - 迁移完成后应恢复更严格的RLS策略
  - 如果没有服务角色密钥的访问权限时使用

### 6. 验证迁移

1. 在Supabase Dashboard中检查数据是否成功迁移
2. 运行应用程序，确认其能够正常连接Supabase并访问数据
3. 运行检查命令确认连接正常：
   ```bash
   python main.py --check-db
   ```

## 改进特性

修复版迁移脚本包含以下改进：

1. **大整数处理**：通过修改Supabase表结构支持大整数
2. **双密钥支持**：区分普通API密钥和管理密钥，提高安全性
3. **错误恢复**：批量插入失败时自动尝试单条插入
4. **详细日志**：提供更完整的错误信息和迁移进度
5. **连接验证**：在迁移前验证Supabase连接
6. **表结构更新**：包含表结构更新脚本

## 常见问题

### 连接错误

如果遇到连接错误：
1. 检查`.env`文件中的Supabase URL和API密钥是否正确
2. 确保网络连接正常，可以访问Supabase服务
3. 验证API密钥没有过期，并具有适当的权限

### 表结构错误

如果遇到表结构错误：
1. 确保已在Supabase中执行了`update_supabase_schema.sql`中的语句
2. 检查表是否已正确创建，列名和类型是否正确

### 权限错误

如果遇到"violates row-level security policy"错误：
1. 确认是否使用了服务角色密钥(service role key)，如果没有，请获取并使用
2. 如果只能使用匿名密钥(anon key)，请参考`docs/supabase_rls_update_guide.md`修改RLS策略
3. 验证RLS策略是否已正确配置，允许匿名用户执行写入操作

## 联系支持

如果在迁移过程中遇到任何问题，请联系技术支持团队。 