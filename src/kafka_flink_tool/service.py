import json
from typing import Optional
from .config import ConfigManager, AliyunFlinkConfig
from .database import HologresDAO
from .kafka_client import KafkaClient
from .type_inference import TypeInferencer
from .ddl_generator import DDLGenerator
from .sql_generator import FlinkSQLGenerator
from .models import FlinkSQLRecord, AliyunFlinkJob
from .flink_client import AliyunFlinkClient
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
            # 将 topic 名称中的连字符和点替换为下划线，生成合法表名
            safe_topic_name = topic_name.replace('-', '_').replace('.', '_')
            sink_table = f"stg_kafka_{safe_topic_name}_rt"

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


class AliyunFlinkService:
    """阿里云 Flink 业务服务"""

    def __init__(self, config_path: str = "config.yaml"):
        self.config_manager = ConfigManager(config_path)
        self.hologres_config = self.config_manager.get_hologres_config()
        self.flink_config = self.config_manager.get_aliyun_flink_config()
        self.dao = HologresDAO(self.hologres_config)
        self.flink_client = AliyunFlinkClient(self.flink_config)

    def generate_and_deploy(self, topic_name: str, sink_table: Optional[str] = None,
                           demo_file: Optional[str] = None) -> dict:
        """端到端：生成 SQL 并部署到阿里云 Flink

        Args:
            topic_name: Kafka Topic 名称
            sink_table: Hologres 表名（可选）
            demo_file: 演示数据文件（可选）

        Returns:
            dict: 包含 deployment_id 和 job_id 的字典

        Raises:
            RuntimeError: 部署流程失败
        """
        logger.info("开始端到端生成并部署流程")

        try:
            # Step 1: 生成 Flink SQL
            logger.info("Step 1: 生成 Flink SQL")
            generator = GeneratorService()
            record_id = generator.generate(topic_name, sink_table, demo_file)
            record = self.dao.save_flink_sql_record.__wrapped__(generator.dao, record_id) if hasattr(generator.dao, '_get_connection') else None

            # 从数据库获取完整记录
            with self.dao._get_connection() as conn:
                import psycopg2
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT * FROM flink_sql_record WHERE id = %s",
                        (record_id,)
                    )
                    row = cur.fetchone()
                    if row:
                        sql_content = row[6]  # full_sql 字段
                    else:
                        raise RuntimeError("无法获取 SQL 记录")

            # Step 2: 创建作业草稿
            logger.info("Step 2: 创建作业草稿")
            draft_id = self.flink_client.create_deployment_draft(sql_content)
            if not self.flink_client.wait_for_deployment_draft(draft_id):
                raise RuntimeError("草稿创建超时")

            # Step 3: 部署作业
            logger.info("Step 3: 部署作业")
            deployment_id = self.flink_client.deploy_deployment_draft(draft_id)
            if not self.flink_client.wait_for_deployment(deployment_id):
                raise RuntimeError("部署超时")

            # Step 4: 启动作业
            logger.info("Step 4: 启动作业")
            job_id = self.flink_client.start_job_with_params(deployment_id)
            if not self.flink_client.wait_for_job(job_id):
                raise RuntimeError("作业启动超时")

            # Step 5: 创建阿里云Flink作业记录
            logger.info("Step 5: 创建阿里云Flink作业记录")
            aliyun_job = AliyunFlinkJob(
                sql_record_id=record_id,
                deployment_id=deployment_id,
                job_id=job_id,
                status='RUNNING',
                workspace_id=self.flink_config.workspace_id,
                namespace=self.flink_config.namespace
            )
            aliyun_job_id = self.dao.create_aliyun_flink_job(aliyun_job)

            logger.info(f"部署完成！Deployment ID: {deployment_id}, Job ID: {job_id}")

            return {
                'success': True,
                'deployment_id': deployment_id,
                'job_id': job_id,
                'aliyun_job_id': aliyun_job_id,
                'status': 'RUNNING'
            }

        except Exception as e:
            logger.error(f"部署失败: {e}")
            raise RuntimeError(f"部署流程失败: {e}")

    def start_job(self, deployment_id: str) -> dict:
        """启动已部署的作业

        Args:
            deployment_id: 部署ID

        Returns:
            dict: 包含 job_id 和状态
        """
        logger.info(f"启动作业: {deployment_id}")

        try:
            job_id = self.flink_client.start_job_with_params(deployment_id)
            if not self.flink_client.wait_for_job(job_id):
                raise RuntimeError("作业启动超时")

            logger.info(f"作业启动成功: {job_id}")

            return {
                'success': True,
                'job_id': job_id,
                'status': 'RUNNING'
            }

        except Exception as e:
            logger.error(f"启动作业失败: {e}")
            raise

    def get_job_status(self, job_id: str) -> dict:
        """查询作业状态

        Args:
            job_id: 作业实例ID

        Returns:
            dict: 包含状态信息
        """
        logger.info(f"查询作业状态: {job_id}")

        try:
            status = self.flink_client.get_job_status(job_id)

            return {
                'success': True,
                'job_id': job_id,
                'status': status
            }

        except Exception as e:
            logger.error(f"查询作业状态失败: {e}")
            raise

    def __del__(self):
        if hasattr(self, 'dao'):
            self.dao.close()
        if hasattr(self, 'flink_client'):
            self.flink_client.close()
