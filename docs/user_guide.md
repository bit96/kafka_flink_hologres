# Kafka-Flink-Hologres 工具使用指南

## 目录

- [1. 简介](#1-简介)
- [2. 安装与配置](#2-安装与配置)
- [3. 快速开始](#3-快速开始)
- [4. 命令详解](#4-命令详解)
  - [4.1 generate 命令](#41-generate-命令)
  - [4.2 deploy 命令](#42-deploy-命令)
  - [4.3 start 命令](#43-start-命令)
  - [4.4 status 命令](#44-status-命令)
  - [4.5 fetch 命令](#45-fetch-命令)
- [5. 阿里云 Flink 集成](#5-阿里云-flink-集成)
- [6. 高级用法](#6-高级用法)
- [7. 故障排查](#7-故障排查)
- [8. 最佳实践](#8-最佳实践)
- [9. 常见问题](#9-常见问题)

---

## 1. 简介

Kafka-Flink-Hologres 自动化工具是一个命令行工具，帮助您：

- 自动从 Kafka Topic 采样数据
- 智能推断字段类型
- 生成 Hologres DDL 建表语句
- 生成 Flink SQL 作业
- **一键部署到阿里云 Flink 平台**

本工具基于 Python 开发，使用 Pydantic 进行数据验证，Click 构建命令行界面。

### 核心特性

✅ **自动化**：从数据采样到类型推断，再到 SQL 生成，全自动化流程

✅ **智能化**：80% 以上匹配度自动推断为时间戳类型

✅ **兼容性**：SQL 关键字自动加引号，避免冲突

✅ **可部署**：支持直接部署到阿里云 Flink

✅ **可追踪**：完整记录作业生命周期

---

## 2. 安装与配置

### 2.1 环境要求

- Python >= 3.11
- Kafka 集群
- Hologres 数据库实例
- 阿里云 Flink 实例（可选，用于部署功能）

### 2.2 安装依赖

```bash
# 使用 pip 安装
pip install pydantic pyyaml kafka-python psycopg2-binary click \
  alibabacloud-flink-open20221130 alibabacloud-tea-openapi alibabacloud-tea-util

# 或使用 uv（推荐）
uv pip install pydantic pyyaml kafka-python psycopg2-binary click \
  alibabacloud-flink-open20221130 alibabacloud-tea-openapi alibabacloud-tea-util
```

### 2.3 数据库初始化

在 Hologres 中执行初始化脚本，创建必要的表结构：

```bash
# 执行初始化脚本
psql -h <hologres_host> -p <port> -U <user> -d <database> \
  -f scripts/init_database.sql
```

**重要**：除了原有的表外，还需要创建 `aliyun_flink_jobs` 表用于追踪阿里云 Flink 作业：

```sql
CREATE TABLE IF NOT EXISTS aliyun_flink_jobs (
    id SERIAL PRIMARY KEY,
    sql_record_id INTEGER NOT NULL REFERENCES flink_sql_record(id),
    deployment_id TEXT NOT NULL,
    job_id TEXT,
    status TEXT NOT NULL DEFAULT 'CREATED',
    workspace_id TEXT,
    namespace TEXT,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    error_message TEXT,
    flink_config JSONB
);
```

### 2.4 配置管理

复制示例配置：

```bash
cp config.yaml.example config.yaml
```

编辑 `config.yaml` 文件：

```yaml
# Hologres 配置（必需）
hologres:
  host: "hologres-cn-hangzhou.aliyuncs.com"
  port: 80
  database: "my_database"
  user: "my_user"
  password: "my_password"

# 阿里云 Flink 配置（可选，用于 deploy 功能）
aliyun_flink:
  workspace_id: "your_workspace_id"
  namespace: "your_namespace"
  access_key_id: "your_access_key_id"
  access_key_secret: "your_access_key_secret"
  region: "cn-hangzhou"
  endpoint: "https://flink-xxx.cn-hangzhou.aliyuncs.com"
```

### 2.5 阿里云配置说明

| 字段 | 说明 | 获取方式 |
|------|------|----------|
| workspace_id | 工作空间 ID | 阿里云 Flink 控制台 -> 配置管理 |
| namespace | 命名空间 | 通常为 default 或自定义名称 |
| access_key_id | 访问密钥 ID | 阿里云控制台 -> 访问密钥管理 |
| access_key_secret | 访问密钥密码 | 阿里云控制台 -> 访问密钥管理 |
| region | 地域 | 如：cn-hangzhou、cn-beijing |
| endpoint | Flink API 端点 | 阿里云 Flink 控制台 -> OpenAPI 文档 |

---

## 3. 快速开始

### 3.1 仅生成 SQL（本地测试）

如果您只想生成 Flink SQL 用于查看或测试，使用 `generate` 命令：

```bash
# 最简单的用法
./scripts/run.sh generate --topic-name my_topic

# 输出示例
[SUCCESS] 生成成功！Record ID: 123
```

生成的 SQL 会保存在 `flink_sql_record` 表中，您可以在 Hologres 中查询。

### 3.2 端到端部署（推荐）

如果您想直接部署到阿里云 Flink，使用 `deploy` 命令：

```bash
# 端到端部署
./scripts/run.sh deploy --topic-name user_behavior

# 输出示例
[SUCCESS] 部署成功！
Deployment ID: deployment_abc123
Job ID: job_def456
状态: RUNNING
阿里云作业记录 ID: 123
```

整个过程大约需要 1-3 分钟，包括：
1. 生成 Flink SQL
2. 创建作业草稿
3. 部署作业
4. 启动作业
5. 保存记录

---

## 4. 命令详解

### 4.1 generate 命令

**功能**：仅生成 Flink SQL，不进行部署。

**使用场景**：
- 查看生成的 SQL 内容
- 进行 SQL 审阅
- 本地测试

**命令格式**：

```bash
./scripts/run.sh generate [OPTIONS]
```

**参数选项**：

| 选项 | 必需 | 默认值 | 说明 |
|------|------|--------|------|
| --topic-name | 是 | - | Kafka Topic 名称 |
| --sink-table | 否 | 自动生成 | Hologres Sink 表名 |
| --demo-file | 否 | - | Demo 数据文件路径（从文件加载数据而非 Kafka） |
| --config | 否 | config.yaml | 配置文件路径 |

**使用示例**：

```bash
# 基本用法
./scripts/run.sh generate --topic-name user_event

# 指定自定义表名
./scripts/run.sh generate --topic-name user_event --sink-table stg_user_event_daily

# 使用自定义配置文件
./scripts/run.sh generate --topic-name user_event \
  --config /path/to/config.yaml

# 从文件加载数据（调试用）
./scripts/run.sh generate --topic-name user_event \
  --demo-file demo/sample_data.json
```

**注意事项**：
- 如果 Sink 表已存在，会报错
- 至少需要获取到 1 条消息
- 支持从文件加载 JSON 数据进行调试

### 4.2 deploy 命令

**功能**：端到端部署，生成 SQL 并部署到阿里云 Flink。

**使用场景**：
- 生产环境一键部署
- 自动化作业创建
- 快速数据同步

**命令格式**：

```bash
./scripts/run.sh deploy [OPTIONS]
```

**参数选项**：

| 选项 | 必需 | 默认值 | 说明 |
|------|------|--------|------|
| --topic-name | 是 | - | Kafka Topic 名称 |
| --sink-table | 否 | 自动生成 | Hologres Sink 表名 |
| --demo-file | 否 | - | Demo 数据文件路径 |
| --config | 否 | config.yaml | 配置文件路径 |

**使用示例**：

```bash
# 基本部署
./scripts/run.sh deploy --topic-name user_behavior

# 指定自定义表名
./scripts/run.sh deploy --topic-name user_behavior \
  --sink-table stg_kafka_behavior_rt

# 从文件部署（用于调试）
./scripts/run.sh deploy --topic-name user_behavior \
  --demo-file demo/sample_data.json
```

**输出示例**：

```
[SUCCESS] 部署成功！
Deployment ID: deployment_abc123
Job ID: job_def456
状态: RUNNING
阿里云作业记录 ID: 123
```

**详细流程**：

1. **Step 1**：生成 Flink SQL
   - 连接 Kafka 采样数据
   - 推断字段类型
   - 生成 DDL 和 INSERT 语句

2. **Step 2**：创建作业草稿
   - 调用 CreateDeploymentDraft API
   - 轮询等待草稿创建完成（超时 60 秒）

3. **Step 3**：部署作业
   - 调用 DeployDeploymentDraft API
   - 轮询等待部署完成（超时 300 秒）

4. **Step 4**：启动作业
   - 调用 StartJobWithParams API
   - 轮询等待作业运行（超时 300 秒）

5. **Step 5**：保存记录
   - 在 `aliyun_flink_jobs` 表中记录作业信息

**注意事项**：
- 需要完整配置 `aliyun_flink` 字段
- 整个过程约 1-3 分钟，请耐心等待
- 部署失败时会抛出异常，可查看日志定位问题

### 4.3 start 命令

**功能**：启动已部署但未运行的作业。

**使用场景**：
- 作业部署后未自动启动
- 手动停止后重新启动
- 批量启动作业

**命令格式**：

```bash
./scripts/run.sh start [OPTIONS]
```

**参数选项**：

| 选项 | 必需 | 默认值 | 说明 |
|------|------|--------|------|
| --deployment-id | 是 | - | 部署 ID |
| --config | 否 | config.yaml | 配置文件路径 |

**使用示例**：

```bash
# 启动作业
./scripts/run.sh start --deployment-id deployment_abc123

# 输出示例
[SUCCESS] 作业启动成功！
Job ID: job_def456
状态: RUNNING
```

### 4.4 status 命令

**功能**：查询作业的运行状态。

**使用场景**：
- 监控作业运行状态
- 排查作业问题
- 获取作业详情

**命令格式**：

```bash
./scripts/run.sh status [OPTIONS]
```

**参数选项**：

| 选项 | 必需 | 默认值 | 说明 |
|------|------|--------|------|
| --job-id | 是 | - | 作业实例 ID |
| --config | 否 | config.yaml | 配置文件路径 |

**使用示例**：

```bash
# 查询作业状态
./scripts/run.sh status --job-id job_def456

# 输出示例
作业 ID: job_def456
当前状态: RUNNING
```

**状态说明**：

| 状态 | 描述 | 建议操作 |
|------|------|----------|
| CREATED | 已创建作业草稿 | 使用 `deploy` 或 `start` 命令 |
| STARTING | 正在启动 | 等待，60 秒内应变为 RUNNING |
| RUNNING | 正常运行 | 正常，无需操作 |
| FAILED | 运行失败 | 检查日志，排查错误原因 |
| STOPPED | 已停止 | 使用 `start` 命令重新启动 |

### 4.5 fetch 命令

**功能**：从 Kafka 拉取数据或从文件加载数据。

**使用场景**：
- 数据调试
- 数据预览
- 生成 Demo 数据

**命令格式**：

```bash
./scripts/run.sh fetch [OPTIONS]
```

**参数选项**：

| 选项 | 必需 | 默认值 | 说明 |
|------|------|--------|------|
| --topic-name | 否 | - | Kafka Topic 名称 |
| --count | 否 | 10 | 拉取消息数量 |
| --demo-file | 否 | - | Demo 数据文件路径 |
| --config | 否 | config.yaml | 配置文件路径 |

**使用示例**：

```bash
# 从 Kafka 拉取数据
./scripts/run.sh fetch --topic-name user_behavior --count 20

# 从文件加载数据
./scripts/run.sh fetch --demo-file demo/sample_data.json --count 5
```

**输出示例**：

```
成功拉取 3 条消息:

--- 消息 1 ---
{
  "user_id": 123,
  "action": "click",
  "timestamp": "2024-01-01T10:00:00"
}

--- 消息 2 ---
{
  "user_id": 456,
  "action": "view",
  "timestamp": "2024-01-01T10:01:00"
}

--- 消息 3 ---
{
  "user_id": 789,
  "action": "purchase",
  "timestamp": "2024-01-01T10:02:00"
}
```

---

## 5. 阿里云 Flink 集成

### 5.1 集成原理

本工具通过调用阿里云 Flink 的 OpenAPI，实现作业的自动化部署和管理。

**核心 API 调用链**：

```mermaid
graph LR
    A[CreateDeploymentDraft] --> B[GetDeploymentDraftResult]
    B --> C[DeployDeploymentDraft]
    C --> D[GetDeployment]
    D --> E[StartJobWithParams]
    E --> F[GetJob]
```

**异步处理机制**：
- 阿里云 Flink 的草稿创建、部署和启动都是异步操作
- 工具采用轮询机制，定期检查状态直到完成
- 支持可配置的超时时间

### 5.2 轮询机制

| 操作 | 默认超时 | 轮询间隔 | 最大轮询次数 |
|------|----------|----------|--------------|
| 草稿创建 | 60 秒 | 2 秒 | 30 次 |
| 部署作业 | 300 秒 | 5 秒 | 60 次 |
| 启动作业 | 300 秒 | 5 秒 | 60 次 |

**轮询逻辑**：

```python
def wait_for_deployment_draft(draft_id: str, timeout: int = 60) -> bool:
    start_time = time.time()
    while time.time() - start_time < timeout:
        result = get_deployment_draft_result(draft_id)
        status = result.get('status')

        if status == 'SUCCESS':
            return True
        elif status == 'FAILED':
            raise RuntimeError(f"草稿创建失败: {result.get('message')}")

        time.sleep(2)
    return False  # 超时
```

### 5.3 错误处理

**常见错误类型**：

1. **配置错误**
   - 症状：`配置缺失或无效`
   - 解决：检查 `config.yaml` 中的 `aliyun_flink` 配置

2. **权限错误**
   - 症状：`Access Denied` 或权限不足
   - 解决：检查 AccessKey 权限，确保有 Flink 操作权限

3. **网络错误**
   - 症状：`连接超时` 或 `网络不可达`
   - 解决：检查网络连接，确认 endpoint 正确

4. **SQL 语法错误**
   - 症状：`草稿创建失败: SQL 语法错误`
   - 解决：检查生成的 SQL，或提供 demo_file 进行调试

5. **资源不足**
   - 症状：`部署失败: 资源不足`
   - 解决：检查阿里云 Flink 实例的资源配额

### 5.4 监控与日志

**日志文件**：
- 位置：`logs/app.log`
- 包含：完整的 API 调用日志、错误堆栈、性能指标

**日志级别**：
- INFO：正常操作日志
- WARNING：非关键异常（如网络波动）
- ERROR：关键错误，需要关注

**查看日志**：

```bash
# 实时查看日志
tail -f logs/app.log

# 搜索关键词
grep "部署" logs/app.log

# 查看错误日志
grep "ERROR" logs/app.log
```

---

## 6. 高级用法

### 6.1 自定义表名生成

**默认规则**：

```
topic_name: "user-behavior-log"
→ 自动生成表名: "stg_kafka_user_behavior_log_rt"
```

**自定义规则**：

```bash
# 指定自定义表名
./scripts/run.sh deploy --topic-name user-behavior \
  --sink-table stg_kafka_user_action_daily
```

**命名建议**：
- 使用下划线分隔单词
- 以 `stg_` 开头表示暂存表
- 以 `_rt` 结尾表示实时表

### 6.2 使用 Demo 数据调试

当无法连接到 Kafka 或需要调试时，可以使用 Demo 数据：

```bash
# 创建 Demo 文件
cat > demo/sample.json << 'EOF'
[
  {
    "user_id": 123,
    "action": "click",
    "amount": 99.9,
    "timestamp": "2024-01-01T10:00:00"
  },
  {
    "user_id": 456,
    "action": "view",
    "amount": 0,
    "timestamp": "2024-01-01T10:01:00"
  }
]
EOF

# 使用 Demo 数据生成 SQL
./scripts/run.sh generate --topic-name user_behavior \
  --demo-file demo/sample.json

# 使用 Demo 数据部署（用于测试）
./scripts/run.sh deploy --topic-name user_behavior \
  --demo-file demo/sample.json
```

### 6.3 批量部署

**Bash 脚本示例**：

```bash
#!/bin/bash
# batch_deploy.sh

TOPICS=(
    "user_behavior"
    "order_event"
    "payment_event"
    "log_event"
)

for topic in "${TOPICS[@]}"; do
    echo "开始部署: $topic"
    ./scripts/run.sh deploy --topic-name "$topic"
    echo "完成部署: $topic"
    echo "---"
done
```

### 6.4 多环境配置

创建多环境配置文件：

```bash
# 开发环境
cp config.yaml config.dev.yaml
# 配置开发环境的数据库和 Flink

# 生产环境
cp config.yaml config.prod.yaml
# 配置生产环境的数据库和 Flink

# 使用指定配置
./scripts/run.sh deploy --topic-name user_behavior \
  --config config.prod.yaml
```

---

## 7. 故障排查

### 7.1 常见问题

#### 问题 1：无法连接到 Kafka

**症状**：
```
[ERROR] 连接 Kafka 失败: Connection refused
```

**排查步骤**：
1. 检查 Kafka 服务是否正常运行
2. 检查 `kafka_brokers` 配置是否正确
3. 检查网络连通性：`telnet kafka_host 9092`

**解决方案**：
- 确保 Kafka 集群可用
- 使用 `fetch` 命令测试连接
- 检查防火墙设置

#### 问题 2：表已存在错误

**症状**：
```
[ERROR] 表已存在: stg_kafka_my_topic_rt，请使用不同的表名
```

**解决方案**：
```bash
# 使用自定义表名
./scripts/run.sh deploy --topic-name my_topic \
  --sink-table stg_kafka_my_topic_v2_rt

# 或者删除已存在的表
DROP TABLE IF EXISTS stg_kafka_my_topic_rt;
```

#### 问题 3：部署超时

**症状**：
```
[WARNING] 部署超时: deployment_abc123
```

**排查步骤**：
1. 查看日志：`grep "部署超时" logs/app.log`
2. 在阿里云控制台手动检查部署状态
3. 检查资源配额

**解决方案**：
- 增大超时时间（修改源码）
- 释放不必要的资源
- 检查网络延迟

#### 问题 4：权限不足

**症状**：
```
[ERROR] 启动作业失败: Access Denied
```

**解决方案**：
1. 检查 AccessKey 是否正确
2. 确认 AccessKey 有 Flink 相关权限
3. 尝试使用子账号的 AccessKey

### 7.2 调试技巧

**启用详细日志**：

```python
# 在 config.yaml 中配置
logging:
  level: DEBUG
  file: logs/app.log
```

**使用 fetch 命令预览数据**：

```bash
# 先查看数据格式
./scripts/run.sh fetch --topic-name my_topic --count 5

# 根据数据格式调整 SQL 或字段类型
```

**分段调试**：

```bash
# Step 1: 仅生成 SQL，不部署
./scripts/run.sh generate --topic-name my_topic

# Step 2: 查看生成的 SQL（在 Hologres 中查询）
SELECT full_sql FROM flink_sql_record WHERE topic_name = 'my_topic';

# Step 3: 使用 demo_file 调试
./scripts/run.sh deploy --topic-name my_topic \
  --demo-file demo/sample.json
```

### 7.3 日志分析

**关键日志模式**：

```bash
# 查找部署流程日志
grep -E "(Step [1-5]:|Deployment ID|Job ID)" logs/app.log

# 查找错误日志
grep "ERROR" logs/app.log | tail -20

# 查找 API 调用日志
grep "发起 API 请求" logs/app.log

# 查找轮询日志
grep "等待" logs/app.log
```

**性能分析**：

```bash
# 统计各步骤耗时
grep -E "开始.*流程|完成.*流程" logs/app.log

# 分析部署时间
grep -E "开始部署|部署完成" logs/app.log
```

---

## 8. 最佳实践

### 8.1 命名规范

**Topic 名称**：
- 使用小写字母和下划线
- 避免特殊字符
- 示例：`user_behavior`, `order_event`

**表名称**：
- 以 `stg_` 开头（暂存表）
- 包含 `kafka` 标识
- 以 `_rt` 结尾（实时）
- 示例：`stg_kafka_user_behavior_rt`

**字段名称**：
- 使用小写字母和下划线
- 避免使用 SQL 关键字（如 `order`、`user`）
- 示例：`user_id`, `created_at`, `event_type`

### 8.2 类型推断建议

**自动推断规则**：
- int/long → BIGINT
- float/double → DOUBLE
- str → TEXT
- bool → BOOLEAN
- 时间戳格式 → TIMESTAMPTZ（需 80% 匹配）

**手动调整**：
如果类型推断不准确，可以：
1. 修改 demo 数据的字段值
2. 使用更准确的示例数据
3. 部署后手动调整字段类型

### 8.3 部署策略

**开发环境**：
- 使用 `generate` 命令测试 SQL
- 使用 `demo_file` 调试
- 小数据量测试

**生产环境**：
- 使用 `deploy` 命令一键部署
- 部署前验证数据质量
- 监控作业状态

**批处理部署**：
- 使用脚本批量部署
- 设置合理的时间间隔
- 监控部署成功率

### 8.4 监控建议

**关键指标**：
- 部署成功率
- 部署耗时
- 作业运行时间
- 错误率

**监控方案**：
- 定期查询 `aliyun_flink_jobs` 表
- 收集部署日志
- 设置告警（如状态为 FAILED）

**查询脚本示例**：

```sql
-- 查看最近部署的作业
SELECT id, topic_name, status, create_time, update_time
FROM aliyun_flink_jobs
ORDER BY create_time DESC
LIMIT 10;

-- 查看失败作业
SELECT id, topic_name, error_message, update_time
FROM aliyun_flink_jobs
WHERE status = 'FAILED'
ORDER BY update_time DESC;
```

### 8.5 安全建议

**AccessKey 管理**：
- 不要将 AccessKey 提交到代码仓库
- 使用环境变量或密钥管理系统
- 定期轮换 AccessKey
- 使用最小权限原则

**配置管理**：
- 不同环境使用不同配置文件
- 敏感信息使用加密存储
- 定期检查配置安全性

**网络安全**：
- 限制网络访问范围
- 使用 VPN 或专线
- 启用访问日志

---

## 9. 常见问题

### Q1：是否支持 Avro 或 Protobuf 格式？

**A**：目前仅支持 JSON 格式的 Kafka 消息。Avro 和 Protobuf 格式将在后续版本中支持。

### Q2：能否修改已部署的作业？

**A**：不能直接修改已部署的作业。建议：
1. 停止当前作业
2. 删除部署
3. 重新部署新的 SQL

### Q3：如何实现数据的增量同步？

**A**：Flink SQL 中的 `scan.startup.mode` 参数控制启动模式：
- `earliest-offset`：从最早位置开始
- `latest-offset`：从最新位置开始（默认）
- `group-offsets`：从指定 group 的 offset 开始

可以通过修改生成的 SQL 来调整此参数。

### Q4：支持多表 join 吗？

**A**：当前版本生成的是单表同步 SQL。如需多表 join，建议：
1. 先生成各表的 SQL
2. 手动编写包含 join 的复杂 SQL
3. 使用 `deploy` 命令部署

### Q5：如何查看生成的完整 SQL？

**A**：
```bash
# 在 Hologres 中查询
SELECT full_sql FROM flink_sql_record WHERE topic_name = 'your_topic';
```

### Q6：部署失败后如何重试？

**A**：直接重新运行 `deploy` 命令即可。工具会自动创建新的部署实例。

### Q7：如何备份作业配置？

**A**：作业配置保存在 `aliyun_flink_jobs` 和 `flink_sql_record` 表中，可以定期导出：

```sql
-- 导出 SQL 记录
COPY (SELECT * FROM flink_sql_record) TO '/tmp/flink_sql_record.csv' CSV HEADER;

-- 导出作业记录
COPY (SELECT * FROM aliyun_flink_jobs) TO '/tmp/aliyun_flink_jobs.csv' CSV HEADER;
```

### Q8：工具支持并发部署吗？

**A**：是的，工具支持多进程并发执行。但建议：
- 避免同时部署到同一个 Topic
- 控制并发数量（建议不超过 5 个）
- 监控资源使用情况

---

## 附录

### A. 完整配置文件示例

```yaml
# config.yaml
hologres:
  host: "hologres-cn-hangzhou.aliyuncs.com"
  port: 80
  database: "my_database"
  user: "my_user"
  password: "my_password"

aliyun_flink:
  workspace_id: "your_workspace_id"
  namespace: "default"
  access_key_id: "your_access_key_id"
  access_key_secret: "your_access_key_secret"
  region: "cn-hangzhou"
  endpoint: "https://flink-xxx.cn-hangzhou.aliyuncs.com"

logging:
  level: INFO
  file: logs/app.log
```

### B. CLI 帮助信息

```bash
# 查看所有命令
./scripts/run.sh --help

# 查看特定命令帮助
./scripts/run.sh generate --help
./scripts/run.sh deploy --help
./scripts/run.sh start --help
./scripts/run.sh status --help
./scripts/run.sh fetch --help
```

### C. API 文档参考

- [阿里云 Flink OpenAPI 文档](https://help.aliyun.com/zh/flink)
- [阿里云访问密钥管理](https://ram.console.aliyun.com/manage/ak)
- [Flink SQL 参考](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/overview/)

---

**文档版本**：v1.0.0

**最后更新**：2024-11-22

**联系方式**：如有问题，请提交 Issue 到项目仓库。
