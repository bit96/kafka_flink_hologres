-- 创建 Kafka 配置表
CREATE TABLE IF NOT EXISTS kafka_topic_config (
    id BIGSERIAL PRIMARY KEY,
    topic_name TEXT NOT NULL UNIQUE,
    kafka_brokers TEXT NOT NULL,
    data_format TEXT NOT NULL DEFAULT 'json',
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_kafka_topic_config_topic_name ON kafka_topic_config(topic_name);
CREATE INDEX idx_kafka_topic_config_is_active ON kafka_topic_config(is_active);

COMMENT ON TABLE kafka_topic_config IS 'Kafka Topic 配置信息表';
COMMENT ON COLUMN kafka_topic_config.topic_name IS 'Kafka Topic 名称';
COMMENT ON COLUMN kafka_topic_config.kafka_brokers IS 'Kafka Broker 地址列表，格式：host1:port1,host2:port2';

-- 创建 Flink SQL 记录表
CREATE TABLE IF NOT EXISTS flink_sql_record (
    id BIGSERIAL PRIMARY KEY,
    topic_id BIGINT NOT NULL,
    topic_name TEXT NOT NULL,
    sink_table_name TEXT NOT NULL,
    source_ddl TEXT NOT NULL,
    sink_ddl TEXT NOT NULL,
    insert_sql TEXT NOT NULL,
    full_sql TEXT NOT NULL,
    inferred_schema JSONB,
    sample_count INTEGER NOT NULL DEFAULT 10,
    status TEXT NOT NULL DEFAULT 'generated',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deployed_at TIMESTAMPTZ,
    deprecated_at TIMESTAMPTZ
);

CREATE INDEX idx_flink_sql_record_topic_name ON flink_sql_record(topic_name);
CREATE INDEX idx_flink_sql_record_status ON flink_sql_record(status);
