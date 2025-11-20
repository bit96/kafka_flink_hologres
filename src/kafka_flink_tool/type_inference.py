from typing import List, Dict, Any
from collections import defaultdict
from datetime import datetime
from .models import FieldSchema, InferredSchema


class TypeInferencer:
    def infer_schema(self, messages: List[Dict[str, Any]]) -> InferredSchema:
        # 1. 收集所有字段的值
        field_values = defaultdict(list)
        for msg in messages:
            for key, value in msg.items():
                field_values[key].append(value)

        # 2. 推断每个字段的类型
        fields = []
        for field_name, values in field_values.items():
            field_type = self._infer_field_type(values)
            fields.append(FieldSchema(
                name=field_name,
                type=field_type,
                nullable=True
            ))

        # 3. 按第一条消息的字段顺序排序，保证字段顺序一致性
        if messages:
            first_message_keys = list(messages[0].keys())
            fields.sort(key=lambda f: first_message_keys.index(f.name)
                       if f.name in first_message_keys else 999)

        return InferredSchema(
            fields=fields,
            sample_data_count=len(messages)
        )

    def _infer_field_type(self, values: List[Any]) -> str:
        """推断字段类型

        修正点：时间戳检测更严格，需要 80% 以上的值是时间戳
        修正点2: 正确处理 bool/int 类型混淆 (bool 是 int 的子类)
        """
        # 收集非 None 值的类型
        types = set()
        for v in values:
            if v is not None:
                types.add(type(v).__name__)

        # 应用宽松类型策略
        # 先检查 bool，因为 bool 是 int 的子类
        if 'bool' in types and len(types) == 1:
            return 'BOOLEAN'
        elif 'float' in types:
            return 'DOUBLE'
        elif 'int' in types and 'float' not in types:
            # 如果同时有 bool 和 int，说明有非布尔的整数
            if 'bool' in types:
                return 'TEXT'  # bool + int 混合，降级为 TEXT
            return 'BIGINT'
        elif 'str' in types:
            # 检查是否大部分值都是时间戳格式
            timestamp_count = sum(
                1 for v in values
                if isinstance(v, str) and self._is_timestamp(v)
            )
            total_str_count = sum(1 for v in values if isinstance(v, str))

            # 如果 80% 以上的字符串值是时间戳，则判定为 TIMESTAMPTZ
            if total_str_count > 0 and timestamp_count >= total_str_count * 0.8:
                return 'TIMESTAMPTZ'
            return 'TEXT'
        else:
            return 'TEXT'

    def _is_timestamp(self, value: str) -> bool:
        """检查字符串是否是时间戳格式"""
        timestamp_formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d',
        ]
        for fmt in timestamp_formats:
            try:
                datetime.strptime(value, fmt)
                return True
            except (ValueError, TypeError):
                continue
        return False
