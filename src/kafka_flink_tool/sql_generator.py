from .models import InferredSchema
from .config import HologresConfig


class FlinkSQLGenerator:
    # Hologres 类型到 Flink 类型的映射
    TYPE_MAPPING = {
        'BIGINT': 'BIGINT',
        'DOUBLE PRECISION': 'DOUBLE',
        'TEXT': 'STRING',
        'BOOLEAN': 'BOOLEAN',
        'TIMESTAMPTZ': 'TIMESTAMP(3)'
    }

    def generate_full_sql(
        self,
        topic_name: str,
        sink_table: str,
        schema: InferredSchema,
        kafka_brokers: str,
        hologres_config: HologresConfig
    ) -> tuple[str, str, str, str]:

        source_ddl = self._generate_source_ddl(topic_name, schema, kafka_brokers)
        sink_ddl = self._generate_sink_ddl(sink_table, schema, hologres_config)
        insert_sql = self._generate_insert_sql(topic_name, sink_table, schema)
        full_sql = f"{source_ddl}\n\n{sink_ddl}\n\n{insert_sql}"

        return source_ddl, sink_ddl, insert_sql, full_sql

    def _generate_source_ddl(self, topic_name: str, schema: InferredSchema, brokers: str) -> str:
        source_table = f"kafka_source_{topic_name}"

        fields = ["    `key_col` STRING"]
        for field in schema.fields:
            flink_type = self.TYPE_MAPPING.get(field.type, 'STRING')
            fields.append(f"    `value_{field.name}` {flink_type}")

        fields_str = ",\n".join(fields)

        return f"""CREATE TEMPORARY TABLE {source_table} (
{fields_str}
) WITH (
    'connector' = 'kafka',
    'topic' = '{topic_name}',
    'properties.bootstrap.servers' = '{brokers}',
    'properties.group.id' = 'flink_{topic_name}',
    'key.fields' = 'key_col',
    'key.fields-prefix' = 'key_',
    'key.format' = 'raw',
    'value.fields-include' = 'EXCEPT_KEY',
    'value.format' = 'json',
    'value.fields-prefix' = 'value_',
    'scan.startup.mode' = 'latest-offset'
);"""

    def _generate_sink_ddl(self, sink_table: str, schema: InferredSchema, config: HologresConfig) -> str:
        sink_table_name = f"hologres_sink_{sink_table}"

        fields = ["`etl_time` TIMESTAMP(3)", "`key_col` STRING"]
        for field in schema.fields:
            flink_type = self.TYPE_MAPPING.get(field.type, 'STRING')
            fields.append(f"    `{field.name}` {flink_type}")

        fields_str = ",\n".join(fields)

        return f"""CREATE TEMPORARY TABLE {sink_table_name} (
{fields_str}
) WITH (
    'connector' = 'hologres',
    'dbname' = '{config.database}',
    'tablename' = '{sink_table}',
    'username' = '{config.user}',
    'password' = '{config.password}',
    'endpoint' = '{config.vpc_host}:{config.port}',
    'ignoredelete' = 'true',
    'mutatetype' = 'insertOrReplace',
    'sdkMode' = 'jdbc',
    'connectionSize' = '3',
    'jdbcWriteBatchSize' = '256',
    'jdbcWriteBatchByteSize' = '2097152',
    'jdbcWriteFlushInterval' = '10000',
    'connectionPoolName' = 'flink-{sink_table}'
);"""

    def _generate_insert_sql(self, topic_name: str, sink_table: str, schema: InferredSchema) -> str:
        source_table = f"kafka_source_{topic_name}"
        sink_table_name = f"hologres_sink_{sink_table}"

        select_fields = ["cast(now() as timestamp) as etl_time", "`key_col`"]

        for field in schema.fields:
            source_field = f"`value_{field.name}`"
            target_field = field.name

            # 根据 Hologres 类型生成对应的 CAST 语句
            if field.type == 'TIMESTAMPTZ':
                select_fields.append(f"cast({source_field} as timestamp) as {target_field}")
            elif field.type == 'DOUBLE PRECISION':
                select_fields.append(f"cast({source_field} as decimal(20,2)) as {target_field}")
            elif field.type == 'BIGINT':
                # 如果原始数据是字符串，需要转换
                select_fields.append(f"cast({source_field} as bigint) as {target_field}")
            else:
                # TEXT, BOOLEAN 等直接映射
                select_fields.append(f"{source_field} as {target_field}")

        fields_str = "\n    ,".join(select_fields)

        return f"""INSERT INTO {sink_table_name}
SELECT {fields_str}
FROM {source_table};"""
