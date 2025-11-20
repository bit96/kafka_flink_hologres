from .models import InferredSchema
from .config import HologresConfig


class FlinkSQLGenerator:
    # Hologres 类型到 Flink 类型的映射
    TYPE_MAPPING = {
        'BIGINT': 'BIGINT',
        'DOUBLE': 'DOUBLE',
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

        fields = []
        for field in schema.fields:
            flink_type = self.TYPE_MAPPING.get(field.type, 'STRING')
            # Flink 字段名也加反引号，避免关键字冲突
            fields.append(f"    `{field.name}` {flink_type}")

        fields_str = ",\n".join(fields)

        return f"""CREATE TABLE {source_table} (
{fields_str}
) WITH (
    'connector' = 'kafka',
    'topic' = '{topic_name}',
    'properties.bootstrap.servers' = '{brokers}',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
);"""

    def _generate_sink_ddl(self, sink_table: str, schema: InferredSchema, config: HologresConfig) -> str:
        sink_table_name = f"hologres_sink_{sink_table}"

        fields = []
        for field in schema.fields:
            flink_type = self.TYPE_MAPPING.get(field.type, 'STRING')
            fields.append(f"    `{field.name}` {flink_type}")

        fields_str = ",\n".join(fields)

        return f"""CREATE TABLE {sink_table_name} (
{fields_str}
) WITH (
    'connector' = 'hologres',
    'dbname' = '{config.database}',
    'tablename' = '{sink_table}',
    'username' = '{config.user}',
    'password' = '{config.password}',
    'endpoint' = '{config.host}:{config.port}'
);"""

    def _generate_insert_sql(self, topic_name: str, sink_table: str, schema: InferredSchema) -> str:
        source_table = f"kafka_source_{topic_name}"
        sink_table_name = f"hologres_sink_{sink_table}"

        field_names = [f"`{field.name}`" for field in schema.fields]
        fields_str = ", ".join(field_names)

        return f"""INSERT INTO {sink_table_name}
SELECT {fields_str}
FROM {source_table};"""
