import json
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path
from kafka import KafkaConsumer

logger = logging.getLogger(__name__)


class KafkaClient:
    def __init__(self, brokers: str, topic_name: str):
        self.brokers = brokers.split(',')
        self.topic_name = topic_name

    def _safe_json_deserializer(self, m: bytes) -> Optional[Dict[str, Any]]:
        """安全的 JSON 反序列化器，处理解析失败的情况"""
        try:
            return json.loads(m.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"JSON 解析失败，跳过该消息: {e}")
            return None

    def sample_messages(self, count: int = 10) -> List[Dict[str, Any]]:
        """采样消息

        修正点：
        1. 使用 earliest 而非 latest，避免无新消息时无限等待
        2. 添加 consumer_timeout_ms 超时设置
        3. 添加 group_id，指定为 topic_name + _stg 后缀
        4. 处理 JSON 解析失败的情况
        """
        consumer = KafkaConsumer(
            self.topic_name,
            bootstrap_servers=self.brokers,
            group_id=f'{self.topic_name}_stg',
            auto_offset_reset='earliest',  # 从最早的消息开始
            enable_auto_commit=False,
            consumer_timeout_ms=30000,  # 30 秒超时
            value_deserializer=self._safe_json_deserializer
        )

        messages = []
        try:
            for message in consumer:
                # 过滤掉解析失败的消息（None）
                if message.value is not None:
                    messages.append(message.value)
                    if len(messages) >= count:
                        break
        except StopIteration:
            # 超时或没有更多消息
            pass
        finally:
            consumer.close()

        if len(messages) < count:
            raise ValueError(f"采样数据不足，期望 {count} 条，实际 {len(messages)} 条")

        return messages

    @staticmethod
    def load_from_file(file_path: str, count: int = 10) -> List[Dict[str, Any]]:
        """从文件加载 demo 数据"""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")

        messages = []
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if line.startswith('key:'):
                    value_start = line.find('value:')
                    if value_start != -1:
                        json_str = line[value_start + 6:]
                        try:
                            messages.append(json.loads(json_str))
                            if len(messages) >= count:
                                break
                        except json.JSONDecodeError as e:
                            logger.warning(f"JSON 解析失败: {e}")

        if not messages:
            raise ValueError(f"文件中没有有效数据: {file_path}")

        return messages
