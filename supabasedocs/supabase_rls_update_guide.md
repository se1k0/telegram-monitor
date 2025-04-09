# Supabase行级安全策略(RLS)更新指南

## 背景

使用修复版的迁移脚本(`migrate_to_supabase_fixed.py`)进行数据迁移时，如果使用匿名密钥(anon key)，可能会遇到以下错误：

```
new row violates row-level security policy for table "tokens"
```

这是因为Supabase默认的行级安全策略只允许匿名用户读取数据，不允许写入操作。

## 首选解决方案：使用服务角色密钥(Service Role Key)

**强烈推荐**使用服务角色密钥(service role key)进行数据迁移，而不是修改RLS策略。

### 优势：
- 不需要修改RLS策略
- 服务角色密钥可以绕过所有RLS限制
- 无需在迁移后恢复RLS设置
- 符合最佳安全实践

### 步骤：
1. 登录Supabase Dashboard
2. 导航到：**项目设置** > **API**
3. 找到**service_role secret**（服务角色密钥）并复制
4. 在`.env`文件中添加：
   ```
   SUPABASE_SERVICE_KEY=your_service_role_key
   ```
5. 重新运行迁移脚本

## 备选方案：修改RLS策略（仅当无法获取服务角色密钥时使用）

如果您无法获取服务角色密钥，可以临时修改RLS策略以允许匿名用户执行写入操作。

> **⚠️ 安全警告**: 此方法会临时降低数据库安全性，只应在开发环境或受控环境中使用，完成迁移后应立即恢复更严格的策略。

### 方法一：使用SQL编辑器

1. 登录Supabase Dashboard
2. 导航到SQL编辑器
3. 创建新查询，并粘贴以下SQL语句：

```sql
-- 删除原有的只读政策
DROP POLICY IF EXISTS "允许匿名用户读取 messages" ON public.messages;
DROP POLICY IF EXISTS "允许匿名用户读取 tokens" ON public.tokens;
DROP POLICY IF EXISTS "允许匿名用户读取 tokens_mark" ON public.tokens_mark;
DROP POLICY IF EXISTS "允许匿名用户读取 promotion_channels" ON public.promotion_channels;
DROP POLICY IF EXISTS "允许匿名用户读取 hidden_tokens" ON public.hidden_tokens;
DROP POLICY IF EXISTS "允许匿名用户读取 telegram_channels" ON public.telegram_channels;

-- 创建允许所有操作的政策
CREATE POLICY "允许匿名用户完全访问 messages" ON public.messages
    FOR ALL
    TO anon
    USING (true)
    WITH CHECK (true);

CREATE POLICY "允许匿名用户完全访问 tokens" ON public.tokens
    FOR ALL
    TO anon
    USING (true)
    WITH CHECK (true);

CREATE POLICY "允许匿名用户完全访问 tokens_mark" ON public.tokens_mark
    FOR ALL
    TO anon
    USING (true)
    WITH CHECK (true);

CREATE POLICY "允许匿名用户完全访问 promotion_channels" ON public.promotion_channels
    FOR ALL
    TO anon
    USING (true)
    WITH CHECK (true);

CREATE POLICY "允许匿名用户完全访问 hidden_tokens" ON public.hidden_tokens
    FOR ALL
    TO anon
    USING (true)
    WITH CHECK (true);

CREATE POLICY "允许匿名用户完全访问 telegram_channels" ON public.telegram_channels
    FOR ALL
    TO anon
    USING (true)
    WITH CHECK (true);

-- 验证策略是否已更新
SELECT
    schemaname,
    tablename,
    policyname,
    roles,
    cmd,
    permissive
FROM
    pg_policies
WHERE
    schemaname = 'public';
```

4. 执行SQL语句
5. 查看输出，确保政策已正确更新

### 方法二：使用Supabase界面

您也可以通过图形界面更新每个表的RLS策略：

1. 登录Supabase Dashboard
2. 导航到：**表编辑器**
3. 对于每个表（messages, tokens, tokens_mark等）：
   - 选择表
   - 点击**政策**标签
   - 删除现有的只读政策
   - 点击**添加政策**
   - 选择**自定义政策**
   - 设置目标角色为**anon**
   - 允许的操作选择**ALL**
   - USING表达式填写`true`
   - WITH CHECK表达式填写`true`
   - 保存策略

## 迁移后恢复更严格的RLS策略

完成迁移后，建议恢复更严格的RLS策略：

```sql
-- 删除临时的完全访问政策
DROP POLICY IF EXISTS "允许匿名用户完全访问 messages" ON public.messages;
DROP POLICY IF EXISTS "允许匿名用户完全访问 tokens" ON public.tokens;
DROP POLICY IF EXISTS "允许匿名用户完全访问 tokens_mark" ON public.tokens_mark;
DROP POLICY IF EXISTS "允许匿名用户完全访问 promotion_channels" ON public.promotion_channels;
DROP POLICY IF EXISTS "允许匿名用户完全访问 hidden_tokens" ON public.hidden_tokens;
DROP POLICY IF EXISTS "允许匿名用户完全访问 telegram_channels" ON public.telegram_channels;

-- 创建只读政策
CREATE POLICY "允许匿名用户读取 messages" ON public.messages
    FOR SELECT
    TO anon
    USING (true);

CREATE POLICY "允许匿名用户读取 tokens" ON public.tokens
    FOR SELECT
    TO anon
    USING (true);

CREATE POLICY "允许匿名用户读取 tokens_mark" ON public.tokens_mark
    FOR SELECT
    TO anon
    USING (true);

CREATE POLICY "允许匿名用户读取 promotion_channels" ON public.promotion_channels
    FOR SELECT
    TO anon
    USING (true);

CREATE POLICY "允许匿名用户读取 hidden_tokens" ON public.hidden_tokens
    FOR SELECT
    TO anon
    USING (true);

CREATE POLICY "允许匿名用户读取 telegram_channels" ON public.telegram_channels
    FOR SELECT
    TO anon
    USING (true);
```

## 验证RLS策略已更新

运行迁移脚本前，您可以使用以下工具检查RLS策略是否正确配置：

```bash
python scripts/check_supabase_rls.py
```

这个脚本会执行以下操作：
1. 检查所有必要表的RLS策略配置
2. 尝试执行测试写入操作
3. 报告RLS策略是否正确配置

## 常见问题

### 我不想修改RLS策略，有其他方法吗？

是的，使用服务角色密钥(service role key)是最佳选择。服务角色密钥可以绕过所有RLS限制，无需修改任何政策。

### 修改RLS策略有什么风险？

修改RLS策略允许匿名用户写入数据会降低安全性，可能导致未授权的数据修改。这应该仅用于受控环境，且在完成迁移后立即恢复更严格的策略。

### 迁移后应该使用哪个密钥？

- 对于日常API访问，使用匿名密钥(anon key)
- 对于管理操作(如迁移)，使用服务角色密钥(service role key)

> **重要提示**：服务角色密钥拥有管理员权限，切勿在客户端应用中使用它 