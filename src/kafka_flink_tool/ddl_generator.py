from .models import InferredSchema


class DDLGenerator:
    def generate_hologres_ddl(self, table_name: str, schema: InferredSchema) -> str:
        """生成 Hologres 表 DDL

        修正点：字段名添加双引号，避免 SQL 关键字冲突
        """
        lines = [f"CREATE TABLE IF NOT EXISTS {table_name} ("]

        field_defs = [
            '    "etl_time" TIMESTAMPTZ',
            '    "key_col" TEXT'
        ]
        for field in schema.fields:
            nullable = "" if field.nullable else "NOT NULL"
            # 字段名添加双引号
            field_defs.append(f'    "{field.name}" {field.type} {nullable}'.strip())

        lines.append(",\n".join(field_defs))
        lines.append(");")

        ddl = "\n".join(lines)

        # 添加注释
        comments = [
            f"\nCOMMENT ON TABLE {table_name} IS '从 Kafka 同步的数据表';",
            f'COMMENT ON COLUMN {table_name}."etl_time" IS \'ETL 时间戳\';',
            f'COMMENT ON COLUMN {table_name}."key_col" IS \'Kafka Key\';'
        ]
        for field in schema.fields:
            comments.append(
                f'COMMENT ON COLUMN {table_name}."{field.name}" IS \'{field.name} 字段\';'
            )

        return ddl + "\n" + "\n".join(comments)
