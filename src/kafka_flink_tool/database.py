import psycopg2
import json
from typing import Optional
from .config import HologresConfig
from .models import KafkaTopicConfig, FlinkSQLRecord


class HologresDAO:
    def __init__(self, config: HologresConfig):
        self.config = config
        self._conn = None

    def _get_connection(self):
        """获取数据库连接，带健康检查"""
        if not self._conn or self._conn.closed:
            self._conn = self._create_connection()
        else:
            # 测试连接是否可用
            try:
                with self._conn.cursor() as cur:
                    cur.execute("SELECT 1")
            except Exception:
                self._conn = self._create_connection()
        return self._conn

    def _create_connection(self):
        """创建新的数据库连接"""
        return psycopg2.connect(
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            user=self.config.user,
            password=self.config.password
        )

    def get_topic_config_by_name(self, topic_name: str) -> Optional[KafkaTopicConfig]:
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, topic_name, kafka_brokers, data_format, description, is_active "
                "FROM kafka_topic_config WHERE topic_name = %s AND is_active = true",
                (topic_name,)
            )
            row = cur.fetchone()
            if row:
                return KafkaTopicConfig(
                    id=row[0],
                    topic_name=row[1],
                    kafka_brokers=row[2],
                    data_format=row[3],
                    description=row[4],
                    is_active=row[5]
                )
        return None

    def table_exists(self, table_name: str) -> bool:
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_name = %s)",
                (table_name,)
            )
            return cur.fetchone()[0]

    def create_table(self, ddl: str) -> None:
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()

    def save_flink_sql_record(self, record: FlinkSQLRecord) -> int:
        """保存 Flink SQL 记录

        修正点：将 inferred_schema dict 转为 JSON 字符串
        """
        conn = self._get_connection()
        with conn.cursor() as cur:
            # 将 dict 转为 JSON 字符串
            inferred_schema_json = json.dumps(record.inferred_schema) if record.inferred_schema else None

            cur.execute(
                """
                INSERT INTO flink_sql_record (
                    topic_id, topic_name, sink_table_name,
                    source_ddl, sink_ddl, insert_sql, full_sql,
                    inferred_schema, sample_count, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                RETURNING id
                """,
                (
                    record.topic_id, record.topic_name, record.sink_table_name,
                    record.source_ddl, record.sink_ddl, record.insert_sql, record.full_sql,
                    inferred_schema_json, record.sample_count, record.status
                )
            )
            record_id = cur.fetchone()[0]
        conn.commit()
        return record_id

    def close(self):
        if self._conn and not self._conn.closed:
            self._conn.close()
