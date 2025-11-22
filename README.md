# Kafka-Flink-Hologres 自动化工具

自动从 Kafka Topic 采样数据，推断数据类型，生成 Hologres DDL 和 Flink SQL 的命令行工具，并支持一键部署到阿里云 Flink 平台。

## 功能特性

- 自动连接 Kafka 并采样消息数据
- 智能推断字段类型（支持 BIGINT、DOUBLE、TEXT、BOOLEAN、TIMESTAMPTZ）
- 自动生成 Hologres 建表 DDL
- 自动生成 Flink SQL（Source DDL、Sink DDL、INSERT 语句）
- 支持 SQL 关键字冲突处理（字段名自动加引号）
- **阿里云 Flink 集成**：一键部署 Flink 作业到阿里云平台
- **完整作业生命周期管理**：创建草稿、部署、启动、状态监控
- 完整的日志记录

## 环境要求

- Python >= 3.11
- Kafka 集群
- Hologres 数据库
- 阿里云 Flink 实例（可选，用于部署功能）
- 阿里云访问密钥（AccessKey ID/Secret，用于部署功能）

## 快速开始

### 1. 安装依赖

```bash
pip install pydantic pyyaml kafka-python psycopg2-binary click alibabacloud-flink-open20221130 alibabacloud-tea-openapi alibabacloud-tea-util
```

### 2. 配置数据库

在 Hologres 中执行初始化脚本：

```bash
psql -h <hologres_host> -p <port> -U <user> -d <database> -f scripts/init_database.sql
```

### 3. 创建配置文件

复制示例配置并修改：

```bash
cp config.yaml.example config.yaml
```

编辑 `config.yaml`：

```yaml
hologres:
  host: "hologres-cn-hangzhou.aliyuncs.com"
  port: 80
  database: "my_database"
  user: "my_user"
  password: "my_password"

# 阿里云 Flink 配置（可选，用于部署功能）
aliyun_flink:
  workspace_id: "your_workspace_id"
  namespace: "your_namespace"
  access_key_id: "your_access_key_id"
  access_key_secret: "your_access_key_secret"
  region: "cn-hangzhou"
  endpoint: "https://flink-xxx.cn-hangzhou.aliyuncs.com"
```

### 4. 运行工具

```bash
# 查看帮助
./scripts/run.sh --help

# === 仅生成 Flink SQL（本地操作） ===
# 生成 Flink SQL
./scripts/run.sh generate --topic-name my_topic

# 指定 Sink 表名
./scripts/run.sh generate --topic-name my_topic --sink-table my_custom_table

# 使用自定义配置文件
./scripts/run.sh generate --topic-name my_topic --config /path/to/config.yaml

# === 阿里云 Flink 集成（端到端部署） ===
# 端到端：生成 SQL 并部署到阿里云 Flink
./scripts/run.sh deploy --topic-name my_topic

# 启动已部署的作业
./scripts/run.sh start --deployment-id <deployment_id>

# 查询作业状态
./scripts/run.sh status --job-id <job_id>

# === 数据拉取（调试辅助） ===
# 从 Kafka 拉取数据
./scripts/run.sh fetch --topic-name my_topic --count 20

# 从文件加载数据
./scripts/run.sh fetch --demo-file /path/to/data.json --count 10
```

## 项目结构

```
kafka_flink_hologres/
├── README.md                          # 项目说明
├── pyproject.toml                     # 项目配置
├── config.yaml.example                # 配置示例
├── scripts/
│   ├── run.sh                         # 运行脚本
│   └── init_database.sql              # 数据库初始化
├── logs/                              # 日志目录
├── src/kafka_flink_tool/              # 源代码
│   ├── __init__.py                    # 包初始化
│   ├── __main__.py                    # CLI 入口
│   ├── cli.py                         # CLI 实现
│   ├── config.py                      # 配置管理
│   ├── models.py                      # 数据模型
│   ├── logger.py                      # 日志配置
│   ├── database.py                    # 数据库访问
│   ├── kafka_client.py                # Kafka 客户端
│   ├── flink_client.py                # 阿里云 Flink API 客户端 ⭐ 新增
│   ├── type_inference.py              # 类型推断
│   ├── ddl_generator.py               # DDL 生成
│   ├── sql_generator.py               # Flink SQL 生成
│   └── service.py                     # 业务服务
└── tests/                             # 测试代码
    ├── test_flink_client.py           # Flink 客户端测试 ⭐ 新增
    └── ...
```

## 类型映射

| Kafka 数据类型 | Hologres 类型 | Flink 类型 |
|--------------|--------------|------------|
| int          | BIGINT       | BIGINT     |
| float        | DOUBLE       | DOUBLE     |
| str          | TEXT         | STRING     |
| bool         | BOOLEAN      | BOOLEAN    |
| 时间戳字符串   | TIMESTAMPTZ  | TIMESTAMP(3) |

## 输出示例

运行工具后，将生成以下内容：

### Hologres DDL

```sql
CREATE TABLE IF NOT EXISTS stg_kafka_my_topic_rt (
    "id" BIGINT,
    "name" TEXT,
    "amount" DOUBLE,
    "created_at" TIMESTAMPTZ
);

COMMENT ON TABLE stg_kafka_my_topic_rt IS '从 Kafka 同步的数据表';
```

### Flink SQL

```sql
-- Source DDL
CREATE TABLE kafka_source_my_topic (
    `id` BIGINT,
    `name` STRING,
    `amount` DOUBLE,
    `created_at` TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'my_topic',
    'properties.bootstrap.servers' = 'broker1:9092',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
);

-- Sink DDL
CREATE TABLE hologres_sink_stg_kafka_my_topic_rt (
    `id` BIGINT,
    `name` STRING,
    `amount` DOUBLE,
    `created_at` TIMESTAMP(3)
) WITH (
    'connector' = 'hologres',
    'dbname' = 'my_database',
    'tablename' = 'stg_kafka_my_topic_rt',
    'username' = 'my_user',
    'password' = '***',
    'endpoint' = 'hologres-cn-hangzhou.aliyuncs.com:80'
);

-- INSERT SQL
INSERT INTO hologres_sink_stg_kafka_my_topic_rt
SELECT `id`, `name`, `amount`, `created_at`
FROM kafka_source_my_topic;
```

## 阿里云 Flink 集成

本工具支持将生成的 Flink SQL 一键部署到阿里云 Flink 平台，实现端到端的自动化。

### 端到端部署流程

当使用 `deploy` 命令时，系统会自动执行以下步骤：

```
1. 生成 Flink SQL
   ├── 采样 Kafka 数据
   ├── 推断字段类型
   └── 生成 DDL 和 INSERT 语句

2. 创建作业草稿 (CreateDeploymentDraft)
   └── 等待草稿创建完成 (GetDeploymentDraftResult)

3. 部署作业 (DeployDeploymentDraft)
   └── 等待部署完成 (GetDeployment)

4. 启动作业 (StartJobWithParams)
   └── 等待作业运行 (GetJob)

5. 保存作业记录
   └── 记录到 aliyun_flink_jobs 表
```

### 部署示例

```bash
# 端到端部署，从生成到运行
$ ./scripts/run.sh deploy --topic-name user_behavior

[SUCCESS] 部署成功！
Deployment ID: deployment_abc123
Job ID: job_def456
状态: RUNNING
阿里云作业记录 ID: 123

# 启动已部署的作业
$ ./scripts/run.sh start --deployment-id deployment_abc123

[SUCCESS] 作业启动成功！
Job ID: job_def456
状态: RUNNING

# 查询作业状态
$ ./scripts/run.sh status --job-id job_def456

作业 ID: job_def456
当前状态: RUNNING
```

### 作业生命周期状态

阿里云 Flink 作业具有以下状态：

| 状态 | 描述 |
|------|------|
| CREATED | 已创建作业草稿 |
| DEPLOYING | 正在部署 |
| RUNNING | 运行中 |
| FAILED | 运行失败 |
| STOPPED | 已停止 |

### 数据库记录

工具会自动在 `aliyun_flink_jobs` 表中记录作业的完整生命周期：

```sql
SELECT id, deployment_id, job_id, status, create_time, update_time
FROM aliyun_flink_jobs
WHERE id = 123;

 id | deployment_id  |  job_id  | status  |      create_time       |      update_time
----+---------------+----------+---------+------------------------+------------------------
123 | deployment_abc | job_def  | RUNNING | 2024-01-01 10:00:00   | 2024-01-01 10:05:00
```

## 日志

日志文件保存在 `logs/app.log`，包含详细的运行信息。

## 注意事项

### 通用注意事项

1. **采样数量**：默认采样 10 条消息，如果 Topic 中消息不足会报错
2. **JSON 格式**：目前只支持 JSON 格式的 Kafka 消息
3. **表名冲突**：如果 Sink 表已存在，需要指定不同的表名
4. **时间戳检测**：需要 80% 以上的字符串值符合时间戳格式才会判定为 TIMESTAMPTZ

### 阿里云 Flink 部署注意事项

5. **配置完整性**：部署功能需要完整配置 `aliyun_flink` 字段，包括 workspace_id、namespace、access_key_id 等
6. **网络连通性**：确保运行环境可以访问阿里云 Flink API endpoint
7. **权限要求**：使用的 AccessKey 需要有 Flink 相关的操作权限
8. **异步操作**：部署过程涉及多个异步 API，会自动轮询等待，但请耐心等待（约 1-3 分钟）
9. **超时配置**：
   - 草稿创建：默认超时 60 秒
   - 部署作业：默认超时 300 秒
   - 启动作业：默认超时 300 秒
10. **作业失败**：如果部署或启动失败，可使用 `status` 命令查看具体错误信息
11. **数据库表**：首次使用需要在 Hologres 中创建 `aliyun_flink_jobs` 表（参考 `scripts/init_database.sql`）

## 许可证

MIT License
