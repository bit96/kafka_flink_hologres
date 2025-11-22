from typing import Optional, List
from pydantic import BaseModel


class KafkaTopicConfig(BaseModel):
    id: int
    topic_name: str
    kafka_brokers: str
    data_format: str
    description: Optional[str] = None
    is_active: bool


class FieldSchema(BaseModel):
    name: str
    type: str  # BIGINT, DOUBLE, TEXT, BOOLEAN, TIMESTAMPTZ
    nullable: bool = True


class InferredSchema(BaseModel):
    fields: List[FieldSchema]
    sample_data_count: int


class FlinkSQLRecord(BaseModel):
    id: Optional[int] = None
    topic_id: int
    topic_name: str
    sink_table_name: str
    source_ddl: str
    sink_ddl: str
    insert_sql: str
    full_sql: str
    inferred_schema: Optional[dict] = None
    sample_count: int = 10
    status: str = "generated"


class AliyunFlinkJob(BaseModel):
    """阿里云 Flink 作业记录模型"""
    id: Optional[int] = None
    sql_record_id: int
    deployment_id: str
    job_id: Optional[str] = None
    status: str = "CREATED"
    workspace_id: Optional[str] = None
    namespace: Optional[str] = None
    create_time: Optional[str] = None
    update_time: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    error_message: Optional[str] = None
    flink_config: Optional[dict] = None
