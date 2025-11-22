import time
import logging
from typing import Optional

# 阿里云 SDK 导入 - 允许在文档生成或测试时跳过
try:
    from alibabacloud_ververica20220718.client import Client as FlinkClient
    from alibabacloud_ververica20220718 import models as FinkModels
except ImportError:
    try:
        from alibabacloud_flink_open20221130.client import Client as FlinkClient
        from alibabacloud_flink_open20221130 import models as FinkModels
    except ImportError:
        # 保留类定义但标记为不可用
        FlinkClient = None
        FinkModels = None
        logging.warning("阿里云 Flink SDK 未安装，部分功能将不可用")

from alibabacloud_tea_openapi import models as OpenApiModels
from alibabacloud_tea_util import models as UtilModels
from alibabacloud_tea_util.client import Client as UtilClient

from .config import AliyunFlinkConfig

logger = logging.getLogger(__name__)


class AliyunFlinkClient:
    """阿里云 Flink API 客户端"""

    def __init__(self, config: AliyunFlinkConfig):
        self.config = config
        self._client = None
        self._init_client()

    def _init_client(self):
        """初始化阿里云 Flink 客户端"""
        try:
            # 创建 OpenAPI 配置
            openapi_config = OpenApiModels.OpenApiConfig(
                endpoint=self.config.endpoint,
                region_id=self.config.region
            )

            # 创建 Flink 客户端
            self._client = FlinkClient(openapi_config)

        except Exception as e:
            logger.error(f"初始化阿里云 Flink 客户端失败: {e}")
            raise

    def _make_request(self, action: str, params: dict) -> dict:
        """发起 API 请求"""
        try:
            request = FinkModels.OpenAPIRequest(
                action=f"Stream{action}",
                version="2022-11-30"
            )

            # 设置请求头
            headers = {
                'X-Tea-TraceId': UtilClient.get_uuid(),
                'Content-Type': 'application/json'
            }

            # 设置请求参数
            body = {
                'workspace': self.config.workspace_id,
                'namespace': self.config.namespace,
                **params
            }

            logger.info(f"发起 API 请求: {action}, 参数: {body}")

            # 发起请求
            runtime = UtilModels.RuntimeOptions(
                connect_timeout=10000,
                read_timeout=10000,
                max_attempts=3
            )

            response = self._client.stream_xxx(
                action=action,
                request=request,
                headers=headers,
                body=body,
                runtime=runtime
            )

            logger.info(f"API 响应: {response.body}")

            return response.body

        except Exception as e:
            logger.error(f"API 请求失败: {action}, 错误: {e}")
            raise

    def create_deployment_draft(self, sql_content: str) -> str:
        """Step 1: 创建作业草稿

        Args:
            sql_content: Flink SQL 内容

        Returns:
            draft_id: 草稿ID

        Raises:
            RuntimeError: 创建失败
        """
        try:
            body = {
                'sql_content': sql_content,
                'kind': 'Deployment'
            }

            response = self._make_request('CreateDeploymentDraft', body)

            if response.get('success'):
                return response.get('data', {}).get('id')
            else:
                error_msg = response.get('message', '创建草稿失败')
                raise RuntimeError(f"创建作业草稿失败: {error_msg}")

        except Exception as e:
            logger.error(f"创建作业草稿异常: {e}")
            raise

    def get_deployment_draft_result(self, draft_id: str) -> dict:
        """Step 2: 获取作业草稿创建结果

        Args:
            draft_id: 草稿ID

        Returns:
            dict: 包含 status 和 message

        Raises:
            RuntimeError: 查询失败
        """
        try:
            body = {
                'id': draft_id
            }

            response = self._make_request('GetDeploymentDraftResult', body)

            if response.get('success'):
                data = response.get('data', {})
                return {
                    'status': data.get('status'),
                    'message': data.get('message'),
                    'id': draft_id
                }
            else:
                error_msg = response.get('message', '查询失败')
                raise RuntimeError(f"获取草稿创建结果失败: {error_msg}")

        except Exception as e:
            logger.error(f"获取草稿创建结果异常: {e}")
            raise

    def deploy_deployment_draft(self, draft_id: str) -> str:
        """Step 3: 部署作业草稿（异步）

        Args:
            draft_id: 草稿ID

        Returns:
            deployment_id: 部署ID

        Raises:
            RuntimeError: 部署失败
        """
        try:
            body = {
                'id': draft_id
            }

            response = self._make_request('DeployDeploymentDraft', body)

            if response.get('success'):
                return response.get('data', {}).get('id')
            else:
                error_msg = response.get('message', '部署失败')
                raise RuntimeError(f"部署作业草稿失败: {error_msg}")

        except Exception as e:
            logger.error(f"部署作业草稿异常: {e}")
            raise

    def get_deployment_status(self, deployment_id: str) -> str:
        """Step 4: 获取已部署作业状态

        Args:
            deployment_id: 部署ID

        Returns:
            str: 作业状态 (CREATED/DEPLOYING/RUNNING/FAILED)

        Raises:
            RuntimeError: 查询失败
        """
        try:
            body = {
                'id': deployment_id
            }

            response = self._make_request('GetDeployment', body)

            if response.get('success'):
                data = response.get('data', {})
                return data.get('status', 'UNKNOWN')
            else:
                error_msg = response.get('message', '查询失败')
                raise RuntimeError(f"获取部署状态失败: {error_msg}")

        except Exception as e:
            logger.error(f"获取部署状态异常: {e}")
            raise

    def start_job_with_params(self, deployment_id: str, params: Optional[dict] = None) -> str:
        """Step 5: 启动作业实例

        Args:
            deployment_id: 部署ID
            params: 启动参数（可选）

        Returns:
            job_id: 作业实例ID

        Raises:
            RuntimeError: 启动失败
        """
        try:
            body = {
                'deployment_id': deployment_id
            }

            if params:
                body.update(params)

            response = self._make_request('StartJob', body)

            if response.get('success'):
                return response.get('data', {}).get('id')
            else:
                error_msg = response.get('message', '启动失败')
                raise RuntimeError(f"启动作业失败: {error_msg}")

        except Exception as e:
            logger.error(f"启动作业异常: {e}")
            raise

    def get_job_status(self, job_id: str) -> str:
        """Step 6: 获取作业运行状态

        Args:
            job_id: 作业实例ID

        Returns:
            str: 作业状态 (CREATED/STARTING/RUNNING/FAILED/STOPPED)

        Raises:
            RuntimeError: 查询失败
        """
        try:
            body = {
                'id': job_id
            }

            response = self._make_request('GetJob', body)

            if response.get('success'):
                data = response.get('data', {})
                return data.get('status', 'UNKNOWN')
            else:
                error_msg = response.get('message', '查询失败')
                raise RuntimeError(f"获取作业状态失败: {error_msg}")

        except Exception as e:
            logger.error(f"获取作业状态异常: {e}")
            raise

    # ==================== 异步轮询机制 ====================

    def wait_for_deployment_draft(self, draft_id: str, timeout: int = 60) -> bool:
        """等待草稿创建完成

        Args:
            draft_id: 草稿ID
            timeout: 超时时间（秒）

        Returns:
            bool: True 成功，False 超时

        Raises:
            RuntimeError: 创建失败
        """
        logger.info(f"等待草稿创建完成: {draft_id}")
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                result = self.get_deployment_draft_result(draft_id)
                status = result.get('status')

                if status == 'SUCCESS':
                    logger.info(f"草稿创建成功: {draft_id}")
                    return True
                elif status == 'FAILED':
                    error_msg = result.get('message', '草稿创建失败')
                    raise RuntimeError(f"草稿创建失败: {error_msg}")

                time.sleep(2)

            except Exception as e:
                logger.warning(f"检查草稿状态异常: {e}")
                time.sleep(2)

        logger.warning(f"草稿创建超时: {draft_id}")
        return False

    def wait_for_deployment(self, deployment_id: str, timeout: int = 300) -> bool:
        """等待部署完成

        Args:
            deployment_id: 部署ID
            timeout: 超时时间（秒）

        Returns:
            bool: True 成功，False 超时

        Raises:
            RuntimeError: 部署失败
        """
        logger.info(f"等待部署完成: {deployment_id}")
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                status = self.get_deployment_status(deployment_id)

                if status == 'RUNNING':
                    logger.info(f"部署完成: {deployment_id}")
                    return True
                elif status == 'FAILED':
                    raise RuntimeError(f"部署失败: {deployment_id}")

                time.sleep(5)

            except Exception as e:
                logger.warning(f"检查部署状态异常: {e}")
                time.sleep(5)

        logger.warning(f"部署超时: {deployment_id}")
        return False

    def wait_for_job(self, job_id: str, timeout: int = 300) -> bool:
        """等待作业启动完成

        Args:
            job_id: 作业实例ID
            timeout: 超时时间（秒）

        Returns:
            bool: True 成功，False 超时

        Raises:
            RuntimeError: 作业启动失败
        """
        logger.info(f"等待作业启动完成: {job_id}")
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                status = self.get_job_status(job_id)

                if status == 'RUNNING':
                    logger.info(f"作业启动完成: {job_id}")
                    return True
                elif status == 'FAILED':
                    raise RuntimeError(f"作业启动失败: {job_id}")

                time.sleep(5)

            except Exception as e:
                logger.warning(f"检查作业状态异常: {e}")
                time.sleep(5)

        logger.warning(f"作业启动超时: {job_id}")
        return False

    def close(self):
        """关闭客户端"""
        if self._client:
            self._client = None
