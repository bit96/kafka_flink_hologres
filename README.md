# Kafka-Flink-Hologres 自动化工具

自动从 Kafka Topic 采样数据，推断数据类型，生成 Hologres DDL 和 Flink SQL 的命令行工具。

## 功能特性

- 自动连接 Kafka 并采样消息数据
- 智能推断字段类型（支持 BIGINT、DOUBLE、TEXT、BOOLEAN、TIMESTAMPTZ）
- 自动生成 Hologres 建表 DDL
- 自动生成 Flink SQL（Source DDL、Sink DDL、INSERT 语句）
- 支持 SQL 关键字冲突处理（字段名自动加引号）
- 完整的日志记录

## 环境要求

- Python >= 3.11
- Kafka 集群
- Hologres 数据库

## 快速开始

### 1. 安装依赖

```bash
pip install pydantic pyyaml kafka-python psycopg2-binary click
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
```

### 4. 运行工具

```bash
# 查看帮助
./scripts/run.sh --help

# 生成 Flink SQL
./scripts/run.sh generate --topic-name my_topic

# 指定 Sink 表名
./scripts/run.sh generate --topic-name my_topic --sink-table my_custom_table

# 使用自定义配置文件
./scripts/run.sh generate --topic-name my_topic --config /path/to/config.yaml
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
│   ├── type_inference.py              # 类型推断
│   ├── ddl_generator.py               # DDL 生成
│   ├── sql_generator.py               # Flink SQL 生成
│   └── service.py                     # 业务服务
└── tests/                             # 测试代码
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

## 日志

日志文件保存在 `logs/app.log`，包含详细的运行信息。

## 注意事项

1. **采样数量**：默认采样 10 条消息，如果 Topic 中消息不足会报错
2. **JSON 格式**：目前只支持 JSON 格式的 Kafka 消息
3. **表名冲突**：如果 Sink 表已存在，需要指定不同的表名
4. **时间戳检测**：需要 80% 以上的字符串值符合时间戳格式才会判定为 TIMESTAMPTZ

## 许可证

MIT License
