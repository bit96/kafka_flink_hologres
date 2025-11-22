import pytest
from unittest.mock import Mock, patch
from kafka_flink_tool.flink_client import AliyunFlinkClient
from kafka_flink_tool.config import AliyunFlinkConfig


class TestAliyunFlinkClient:
    """阿里云 Flink API 客户端测试"""

    @pytest.fixture
    def config(self):
        """测试配置"""
        return AliyunFlinkConfig(
            workspace_id="test-workspace",
            namespace="test-namespace",
            access_key_id="test-key-id",
            access_key_secret="test-key-secret",
            region="cn-hangzhou",
            endpoint="https://flink-test.cn-hangzhou.aliyuncs.com"
        )

    @pytest.fixture
    def client(self, config):
        """测试客户端（使用 mock）"""
        with patch('kafka_flink_tool.flink_client.FlinkClient'):
            client = AliyunFlinkClient(config)
            client._client = Mock()
            return client

    def test_create_deployment_draft_success(self, client):
        """测试创建作业草稿 - 成功"""
        # Mock 响应
        mock_response = {
            'success': True,
            'data': {'id': 'draft-123'}
        }
        client._make_request = Mock(return_value=mock_response)

        # 执行
        draft_id = client.create_deployment_draft("SELECT * FROM test")

        # 断言
        assert draft_id == 'draft-123'
        client._make_request.assert_called_once_with(
            'CreateDeploymentDraft',
            {
                'sql_content': 'SELECT * FROM test',
                'kind': 'Deployment'
            }
        )

    def test_create_deployment_draft_failure(self, client):
        """测试创建作业草稿 - 失败"""
        # Mock 响应
        mock_response = {
            'success': False,
            'message': 'SQL 语法错误'
        }
        client._make_request = Mock(return_value=mock_response)

        # 执行并断言异常
        with pytest.raises(RuntimeError, match="创建作业草稿失败"):
            client.create_deployment_draft("INVALID SQL")

    def test_get_deployment_draft_result(self, client):
        """测试获取作业草稿创建结果"""
        # Mock 响应
        mock_response = {
            'success': True,
            'data': {
                'status': 'SUCCESS',
                'message': '草稿创建成功'
            }
        }
        client._make_request = Mock(return_value=mock_response)

        # 执行
        result = client.get_deployment_draft_result('draft-123')

        # 断言
        assert result['status'] == 'SUCCESS'
        assert result['id'] == 'draft-123'

    def test_deploy_deployment_draft(self, client):
        """测试部署作业草稿"""
        # Mock 响应
        mock_response = {
            'success': True,
            'data': {'id': 'deployment-456'}
        }
        client._make_request = Mock(return_value=mock_response)

        # 执行
        deployment_id = client.deploy_deployment_draft('draft-123')

        # 断言
        assert deployment_id == 'deployment-456'

    def test_get_deployment_status(self, client):
        """测试获取部署状态"""
        # Mock 响应
        mock_response = {
            'success': True,
            'data': {'status': 'RUNNING'}
        }
        client._make_request = Mock(return_value=mock_response)

        # 执行
        status = client.get_deployment_status('deployment-456')

        # 断言
        assert status == 'RUNNING'

    def test_start_job_with_params(self, client):
        """测试启动作业"""
        # Mock 响应
        mock_response = {
            'success': True,
            'data': {'id': 'job-789'}
        }
        client._make_request = Mock(return_value=mock_response)

        # 执行
        job_id = client.start_job_with_params('deployment-456')

        # 断言
        assert job_id == 'job-789'

    def test_get_job_status(self, client):
        """测试获取作业状态"""
        # Mock 响应
        mock_response = {
            'success': True,
            'data': {'status': 'RUNNING'}
        }
        client._make_request = Mock(return_value=mock_response)

        # 执行
        status = client.get_job_status('job-789')

        # 断言
        assert status == 'RUNNING'

    @patch('time.sleep')
    @patch('kafka_flink_tool.flink_client.time.time')
    def test_wait_for_deployment_draft_success(self, mock_time, mock_sleep, client):
        """测试等待草稿创建完成 - 成功"""
        # Mock 时间函数
        mock_time.side_effect = [0, 0, 2, 2, 4, 4, 6, 6]  # 模拟 6 秒

        # Mock 响应 - 第一次失败，第二次成功
        mock_responses = [
            {'status': 'PENDING'},
            {'status': 'SUCCESS'}
        ]
        client.get_deployment_draft_result = Mock(side_effect=mock_responses)

        # 执行
        result = client.wait_for_deployment_draft('draft-123', timeout=60)

        # 断言
        assert result is True
        assert client.get_deployment_draft_result.call_count == 2

    @patch('time.sleep')
    @patch('kafka_flink_tool.flink_client.time.time')
    def test_wait_for_deployment_draft_timeout(self, mock_time, mock_sleep, client):
        """测试等待草稿创建完成 - 超时"""
        # Mock 时间函数 - 永远不超时
        mock_time.side_effect = [0, 0, 2, 2, 4, 4, 6, 6, 8, 8, 10, 10]

        # Mock 响应 - 一直PENDING
        client.get_deployment_draft_result = Mock(return_value={'status': 'PENDING'})

        # 执行
        result = client.wait_for_deployment_draft('draft-123', timeout=10)

        # 断言
        assert result is False

    @patch('time.sleep')
    @patch('kafka_flink_tool.flink_client.time.time')
    def test_wait_for_deployment_draft_failure(self, mock_time, mock_sleep, client):
        """测试等待草稿创建完成 - 失败"""
        # Mock 时间函数
        mock_time.side_effect = [0, 0, 2, 2]

        # Mock 响应 - 失败
        client.get_deployment_draft_result = Mock(return_value={'status': 'FAILED'})

        # 执行并断言异常
        with pytest.raises(RuntimeError, match="草稿创建失败"):
            client.wait_for_deployment_draft('draft-123', timeout=60)

    @patch('time.sleep')
    @patch('kafka_flink_tool.flink_client.time')
    def test_wait_for_deployment_success(self, mock_time, mock_sleep, client):
        """测试等待部署完成 - 成功"""
        # Mock 时间函数
        mock_time.time.side_effect = [0, 0, 5, 5, 10, 10, 15, 15]

        # Mock 响应
        client.get_deployment_status = Mock(side_effect=['DEPLOYING', 'RUNNING'])

        # 执行
        result = client.wait_for_deployment('deployment-456', timeout=300)

        # 断言
        assert result is True

    @patch('time.sleep')
    @patch('kafka_flink_tool.flink_client.time')
    def test_wait_for_job_success(self, mock_time, mock_sleep, client):
        """测试等待作业启动完成 - 成功"""
        # Mock 时间函数
        mock_time.time.side_effect = [0, 0, 5, 5, 10, 10, 15, 15]

        # Mock 响应
        client.get_job_status = Mock(side_effect=['STARTING', 'RUNNING'])

        # 执行
        result = client.wait_for_job('job-789', timeout=300)

        # 断言
        assert result is True


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
