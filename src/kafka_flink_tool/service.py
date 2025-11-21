import json
from typing import Optional
from .config import ConfigManager
from .database import HologresDAO
from .kafka_client import KafkaClient
from .type_inference import TypeInferencer
from .ddl_generator import DDLGenerator
from .sql_generator import FlinkSQLGenerator
from .models import FlinkSQLRecord
from .logger import get_logger

logger = get_logger(__name__)


class GeneratorService:
    def __init__(self, config_path: str = "config.yaml"):
        self.config_manager = ConfigManager(config_path)
        self.hologres_config = self.config_manager.get_hologres_config()
        self.dao = HologresDAO(self.hologres_config)

    def generate(self, topic_name: str, sink_table: Optional[str] = None, demo_file: Optional[str] = None) -> int:
        # 1. 获取 Topic 配置
        logger.info(f"查询 Topic 配置: {topic_name}")
        topic_config = self.dao.get_topic_config_by_name(topic_name)
        if not topic_config:
            raise ValueError(f"Topic 配置不存在: {topic_name}")

        # 2. 采样数据
        if demo_file:
            logger.info(f"从文件加载数据: {demo_file}")
            messages = KafkaClient.load_from_file(demo_file, count=10)
            logger.info(f"加载完成，共 {len(messages)} 条数据")
        else:
            logger.info(f"连接 Kafka: {topic_config.kafka_brokers}")
            kafka_client = KafkaClient(topic_config.kafka_brokers, topic_name)
            logger.info(f"采样 Topic: {topic_name} (最多 10 条)")
            messages = kafka_client.sample_messages(count=10)
            logger.info(f"采样完成，共 {len(messages)} 条数据")

        # 检查是否有数据（至少 1 条）
        if not messages:
            raise ValueError("没有获取到任何数据，无法进行类型推断")

        # 3. 推断类型
        logger.info("推断数据类型...")
        inferencer = TypeInferencer()
        schema = inferencer.infer_schema(messages)
        logger.info(f"推断完成，共 {len(schema.fields)} 个字段")

        # 4. 确定 sink 表名
        if not sink_table:
            sink_table = f"stg_kafka_{topic_name}_rt"

        # 5. 生成 DDL
        logger.info("生成 DDL...")
        ddl_gen = DDLGenerator()
        hologres_ddl = ddl_gen.generate_hologres_ddl(sink_table, schema)

        # 6. 生成 Flink SQL
        logger.info("生成 Flink SQL...")
        sql_gen = FlinkSQLGenerator()
        source_ddl, sink_ddl, insert_sql, full_sql = sql_gen.generate_full_sql(
            topic_name, sink_table, schema,
            topic_config.kafka_brokers, self.hologres_config
        )

        # 7. 检查表是否存在
        logger.info(f"检查 Sink 表: {sink_table}")
        if self.dao.table_exists(sink_table):
            logger.warning(f"表已存在: {sink_table}")
            raise ValueError(f"表已存在: {sink_table}，请使用不同的表名")

        # 8. 创建表
        logger.info("创建表...")
        self.dao.create_table(hologres_ddl)
        logger.info("创建表成功")

        # 9. 保存 SQL 记录
        logger.info("保存 SQL 记录...")
        record = FlinkSQLRecord(
            topic_id=topic_config.id,
            topic_name=topic_name,
            sink_table_name=sink_table,
            source_ddl=source_ddl,
            sink_ddl=sink_ddl,
            insert_sql=insert_sql,
            full_sql=full_sql,
            inferred_schema=json.loads(schema.model_dump_json()),
            sample_count=len(messages),
            status="generated"
        )
        record_id = self.dao.save_flink_sql_record(record)
        logger.info(f"保存成功，Record ID: {record_id}")

        return record_id

    def __del__(self):
        if hasattr(self, 'dao'):
            self.dao.close()
