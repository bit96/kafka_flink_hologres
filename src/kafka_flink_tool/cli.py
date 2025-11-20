import click
from .service import GeneratorService
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


if __name__ == '__main__':
    cli()
