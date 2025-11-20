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
