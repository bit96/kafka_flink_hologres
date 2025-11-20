import click
import json
from .service import GeneratorService
from .config import ConfigManager
from .database import HologresDAO
from .kafka_client import KafkaClient
from .logger import get_logger

logger = get_logger(__name__)


@click.group()
def cli():
    """Kafka-Flink-Hologres 自动化工具"""
    pass


@cli.command()
@click.option('--topic-name', required=True, help='Kafka Topic 名称')
@click.option('--sink-table', default=None, help='Hologres Sink 表名')
@click.option('--config', default='config.yaml', help='配置文件路径')
def generate(topic_name: str, sink_table: str, config: str):
    """生成 Flink SQL"""
    try:
        service = GeneratorService(config)
        record_id = service.generate(topic_name, sink_table)
        click.echo(f"[SUCCESS] 生成成功！Record ID: {record_id}")
    except Exception as e:
        logger.error(f"生成失败: {e}")
        click.echo(f"[ERROR] {e}", err=True)
        raise click.Abort()


@cli.command()
@click.option('--topic-name', required=True, help='Kafka Topic 名称')
@click.option('--count', default=10, help='拉取消息数量')
@click.option('--config', default='config.yaml', help='配置文件路径')
def fetch(topic_name: str, count: int, config: str):
    """从 Kafka 拉取数据并打印"""
    try:
        config_manager = ConfigManager(config)
        dao = HologresDAO(config_manager.get_hologres_config())

        topic_config = dao.get_topic_config_by_name(topic_name)
        if not topic_config:
            raise ValueError(f"Topic 配置不存在: {topic_name}")

        kafka_client = KafkaClient(topic_config.kafka_brokers, topic_name)
        messages = kafka_client.sample_messages(count=count)

        click.echo(f"成功拉取 {len(messages)} 条消息:\n")
        for i, msg in enumerate(messages, 1):
            click.echo(f"--- 消息 {i} ---")
            click.echo(json.dumps(msg, ensure_ascii=False, indent=2))

        dao.close()
    except Exception as e:
        logger.error(f"拉取失败: {e}")
        click.echo(f"[ERROR] {e}", err=True)
        raise click.Abort()


if __name__ == '__main__':
    cli()
