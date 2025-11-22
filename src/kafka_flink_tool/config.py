import yaml
from pydantic import BaseModel


class HologresConfig(BaseModel):
    host: str
    vpc_host: str
    port: int = 80
    database: str
    user: str
    password: str


class AliyunFlinkConfig(BaseModel):
    """阿里云 Flink 配置"""
    workspace_id: str
    namespace: str
    access_key_id: str
    access_key_secret: str
    region: str = "cn-hangzhou"
    endpoint: str


class ConfigManager:
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self._config = None

    def get_hologres_config(self) -> HologresConfig:
        if not self._config:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
        return HologresConfig(**self._config['hologres'])

    def get_aliyun_flink_config(self) -> AliyunFlinkConfig:
        """获取阿里云 Flink 配置"""
        if not self._config:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
        return AliyunFlinkConfig(**self._config.get('aliyun_flink', {}))
