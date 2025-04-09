# Supabase ID字段自增长设置指南

## 背景

在从SQLite迁移到Supabase的过程中，我们需要确保所有表的`id`字段都设置为自动增长，这样在插入新记录时就不需要手动提供ID值。

## 问题描述

目前，Supabase中的表结构可能没有将`id`字段设置为自动增长序列，这会导致以下问题：

1. 插入数据时需要显式提供`id`值
2. 可能出现ID冲突或重复
3. 程序代码需要自行管理ID生成逻辑

## 解决方案

我们提供了一个SQL脚本`update_supabase_id_columns.sql`来修改Supabase中的表结构，确保所有表的`id`字段都使用BIGSERIAL类型（即自增长的BIGINT类型）。

### 执行步骤

1. 登录Supabase管理控制台：https://app.supabase.com/
2. 找到你的项目并点击进入
3. 在左侧导航栏中选择"SQL Editor"
4. 点击"New Query"创建一个新的SQL查询
5. 将`scripts/update_supabase_id_columns.sql`文件中的内容复制粘贴到查询编辑器中
6. 点击"Run"执行SQL脚本

### 脚本说明

这个脚本会对以下表进行修改：

- `telegram_channels`：Telegram频道数据表
- `tokens_mark`：代币标记数据表
- `messages`：消息数据表
- `tokens`：代币数据表

对每个表，脚本会执行以下操作：

1. 将`id`列的类型修改为BIGINT（大整数类型）
2. 添加一个新的BIGSERIAL序列（如果不存在）
3. 设置`id`列的默认值为该序列的下一个值
4. 如果表中已有数据，更新序列的起始值为当前最大ID值+1

## 验证步骤

执行完脚本后，请按照以下步骤验证修改是否生效：

1. 在Supabase控制台左侧导航栏中选择"Table Editor"
2. 选择任意一个修改过的表（如`telegram_channels`）
3. 点击"Insert Row"，然后留空`id`字段
4. 填写其他必要字段后点击"Save"
5. 观察新插入的记录是否自动生成了一个ID值

## 代码适配

我们已经修改了程序中的Supabase适配器代码，确保在插入新记录时不提供`id`字段，让数据库自动生成。主要修改包括：

1. 在`execute_query`方法中，当查询类型为'insert'或'upsert'时，移除数据中的`id`字段（如果存在且为`None`）
2. 在各个`save_*`方法中，确保插入新记录时不包含`id`字段，更新现有记录时保留原始ID

## 注意事项

1. 执行此脚本前，建议先备份你的数据库
2. 如果执行过程中遇到错误，请检查错误信息并根据情况修改脚本
3. 这个脚本设计为可以重复执行，不会对已经设置好的表造成重复修改

如果你在执行过程中遇到任何问题，请参考Supabase文档或联系技术支持。 