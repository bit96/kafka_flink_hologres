# 阿里云 Flink 平台集成实现计划

## 文档信息
- **创建日期**: 2025-11-21
- **项目**: kafka_flink_hologres
- **目标**: 实现将 Flink SQL 提交到阿里云 Flink 平台执行
- **状态**: 待实施

## 一、项目背景

基于已实现的 Kafka 数据采集和 Flink SQL 生成功能，现需要将生成的 Flink SQL 部署到阿里云实时计算 Flink 平台，实现端到端的数据流处理。

### 当前状态
- ✅ 已实现 Kafka 数据采集
- ✅ 已实现 Flink SQL 生成
- ✅ 已实现 Hologres 数据写入
- ❌ **待完成**: 阿里云 Flink 平台集成

## 二、核心流程设计

### 2.1 完整的 API 调用链

```
Step 1: CreateDeploymentDraft (创建作业草稿)
  ↓
Step 2: GetDeployDeploymentDraftResult (获取作业草稿创建结果)
  ↓
Step 3: DeployDeploymentDraftAsync (部署作业草稿)
  ↓
Step 4: GetDeployment (获取已部署作业状态)
  ↓
Step 5: StartJobWithParams (启动作业实例)
  ↓
Step 6: GetJob (获取作业运行状态)
  ↓
Step 7: 返回最终状态
```

### 2.2 核心 API 清单

1. **CreateDeploymentDraft** - 创建作业草稿
2. **GetDeployDeploymentDraftResult** - 获取作业草稿创建结果
3. **DeployDeploymentDraftAsync** - 部署作业草稿
4. **GetDeployment** - 查询 Deployment 状态
5. **StartJobWithParams** - 启动作业实例
6. **GetJob** - 查询 Job 状态

## 三、模块架构设计

### 3.1 新增模块列表

#### 模块 1: 阿里云 Flink API 客户端
- **文件路径**: `src/kafka_flink_tool/flink_client.py`
- **代码量**: ~200 行
- **核心类**: `AliyunFlinkClient`
- **主要方法**:
  - `create_deployment_draft()` - 创建草稿
  - `deploy_deployment_draft()` - 部署草稿
  - `start_job_with_params()` - 启动作业
  - `get_deployment_status()` - 查询部署状态
  - `get_job_status()` - 查询作业状态

#### 模块 2: 配置扩展
- **文件路径**: `src/kafka_flink_tool/config.py`
- **代码量**: ~30 行
- **新增配置**:
  - workspace_id
  - namespace
  - access_key_id
  - access_key_secret
  - region
  - endpoint

#### 模块 3: 数据模型和数据库扩展
- **文件路径**: `src/kafka_flink_tool/models.py` 和 `database.py`
- **代码量**: ~60 行
- **新增模型**:
  - `AliyunFlinkJob` - 阿里云Flink作业记录模型
- **数据库变更**:
  ```sql
  -- 创建阿里云Flink作业记录表
  CREATE TABLE aliyun_flink_jobs (
      id SERIAL PRIMARY KEY,
      sql_record_id INTEGER NOT NULL REFERENCES flink_sql_record(id),
      deployment_id TEXT NOT NULL,
      job_id TEXT,
      status TEXT NOT NULL DEFAULT 'CREATED',
      workspace_id TEXT,
      namespace TEXT,
      create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      start_time TIMESTAMP,
      end_time TIMESTAMP,
      error_message TEXT,
      flink_config JSONB
  );
  ```

#### 模块 4: 业务服务扩展
- **文件路径**: `src/kafka_flink_tool/service.py`
- **代码量**: ~80 行
- **新增方法**:
  - `generate_and_deploy()` - 端到端生成并部署
  - `start_job()` - 启动作业
  - `get_job_status()` - 查询作业状态

#### 模块 5: CLI 扩展
- **文件路径**: `src/kafka_flink_tool/cli.py`
- **代码量**: ~60 行
- **新增命令**:
  - `deploy` - 生成 SQL 并部署到 Flink
  - `start` - 启动已部署的作业
  - `stop` - 停止运行中的作业
  - `status` - 查询作业状态

## 四、关键技术点

### 4.1 阿里云 API 签名
- 使用 `aliyun-python-sdk-core` 库
- 确保 AK/SK 从环境变量读取

### 4.2 异步轮询机制
```python
import time

def wait_for_status(check_func, check_id: str, expected_status: str, timeout: int = 300):
    """通用轮询函数"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        status = check_func(check_id)
        if status == expected_status:
            return True
        elif status == "FAILED":
            raise RuntimeError(f"{check_id} 失败")
        time.sleep(5)
    return False
```

### 4.3 错误处理
- API 调用失败自动重试（最多 3 次）
- 部署失败自动回滚（删除 Deployment）
- 清晰的错误提示

### 4.4 业务方法示例
```python
def generate_and_deploy(self, topic: str, hologres_table: str):
    """
    端到端生成并部署 Flink 作业

    Args:
        topic: Kafka Topic 名称
        hologres_table: Hologres 表名

    Returns:
        dict: 包含 deployment_id 和 job_id 的字典
    """
    try:
        # Step 1: 生成 Flink SQL
        sql_record = self.database.create_sql_record(topic, hologres_table)
        sql = sql_record.flink_sql

        # Step 2: 创建作业草稿
        draft_id = self.flink_client.create_deployment_draft(sql)
        if not self.flink_client.wait_for_deployment_draft(draft_id):
            raise RuntimeError("草稿创建超时")

        # Step 3: 部署作业
        deployment_id = self.flink_client.deploy_deployment_draft(draft_id)
        if not self.flink_client.wait_for_deployment(deployment_id):
            raise RuntimeError("部署超时")

        # Step 4: 启动作业
        job_id = self.flink_client.start_job_with_params(deployment_id)
        if not self.flink_client.wait_for_job(job_id):
            raise RuntimeError("作业启动超时")

        # Step 5: 创建阿里云Flink作业记录
        aliyun_job = self.database.create_aliyun_flink_job(
            sql_record_id=sql_record.id,
            deployment_id=deployment_id,
            job_id=job_id,
            status='RUNNING',
            workspace_id=self.config.workspace_id,
            namespace=self.config.namespace
        )

        return {
            'success': True,
            'deployment_id': deployment_id,
            'job_id': job_id,
            'status': 'RUNNING',
            'aliyun_job_id': aliyun_job.id
        }

    except Exception as e:
        logger.error(f"部署失败: {e}")
        raise
```

### 4.5 资源配置
创建草稿时需要指定：
- 并行度 (parallelism)
- CPU 和内存资源
- Checkpoint 配置
- 重启策略

**注意**: 具体配置参数在实际实现时根据阿里云 Flink API 文档调整

### 4.6 环境准备
在开始开发前，需要准备以下环境：

#### 4.6.1 阿里云账号准备
- 注册阿里云账号并开通实时计算 Flink 版服务
- 获取 Access Key ID 和 Access Key Secret
- 创建工作空间（workspace）和命名空间（namespace）

#### 4.6.2 环境变量配置
在 `.env` 文件中配置：
```bash
# 阿里云 Flink 配置
ALIYUN_FLINK_WORKSPACE_ID=your_workspace_id
ALIYUN_FLINK_NAMESPACE=your_namespace
ALIYUN_ACCESS_KEY_ID=your_access_key_id
ALIYUN_ACCESS_KEY_SECRET=your_access_key_secret
ALIYUN_REGION=cn-hangzhou
ALIYUN_ENDPOINT=https://flink-xxx.cn-hangzhou.aliyuncs.com
```

#### 4.6.3 数据库迁移
执行迁移脚本创建新表：
```bash
# 升级
python -m kafka_flink_tool database upgrade
```

## 五、依赖管理

### 5.1 新增依赖
```toml
[project]
dependencies = [
    "aliyun-python-sdk-core>=2.13.0",
    "requests>=2.28.0",
]
```

## 六、实施计划

### 阶段 1: 阿里云 Flink API 客户端（1 天）
- [ ] 实现 flink_client.py
- [ ] 实现 6 个核心 API 调用
- [ ] 实现异步轮询机制
- [ ] 单元测试

### 阶段 2: 配置和数据模型扩展（0.5 天）
- [ ] 扩展 config.py
- [ ] 扩展 models.py
- [ ] 扩展 database.py
- [ ] 数据库迁移

### 阶段 3: 业务服务扩展（1 天）
- [ ] 实现 generate_and_deploy 方法
- [ ] 实现 start_job 方法
- [ ] 实现 get_job_status 方法
- [ ] 集成测试

### 阶段 4: CLI 扩展（0.5 天）
- [ ] 新增 CLI 命令
- [ ] 测试所有命令
- [ ] 端到端测试

### 阶段 5: 文档和交付（0.5 天）
- [ ] 更新 README
- [ ] 编写使用文档
- [ ] 最终测试

**总计: 3.5 天**

## 七、风险和注意事项

### 7.1 权限管理
确保 AK/SK 有足够权限：
- CreateDeploymentDraft
- GetDeployDeploymentDraftResult
- DeployDeployment
- StartJob
- GetDeployment
- GetJob

### 7.2 成本控制
- Flink 作业运行会产生费用
- 提供资源配置选项

### 7.3 安全性
- AK/SK 不能硬编码
- 配置文件使用环境变量
- 添加到 .gitignore

## 八、验收标准

### 8.1 功能完整性
- ✅ 可以生成并部署 Flink SQL 到阿里云
- ✅ 可以启动/停止作业
- ✅ 可以查询作业状态
- ✅ 可以通过 CLI 调用所有功能

### 8.2 稳定性
- ✅ API 调用失败自动重试
- ✅ 部署失败自动回滚
- ✅ 异常情况有清晰的错误提示

### 8.3 易用性
- ✅ CLI 命令简洁明了
- ✅ 文档完善

## 九、后续优化方向

1. **作业监控**: 集成作业指标监控
2. **告警通知**: 作业失败自动告警
3. **批量部署**: 支持批量创建和部署
4. **版本管理**: 支持作业版本回滚
5. **资源优化**: 自动调整并行度和资源配置
6. **MCP 集成**: 添加 MCP 工具支持

## 十、遗留问题

需要进一步确认的问题：
1. 阿里云 Flink 平台的具体 API 认证方式
2. 各 API 的详细参数和返回格式
3. 阿里云账户的额度限制和费用标准
4. 具体的工作空间和命名空间配置

## 十一、决策记录

- **决策 1**: 采用同步 + 轮询的方式处理异步操作，简化实现复杂度
- **决策 2**: 将所有 Flink 相关的配置集中在 FlinkConfig 类中
- **决策 3**: 先实现核心功能，MCP 集成列为后续优化项
- **决策 4**: 保留原有代码结构，通过扩展而非重构的方式添加新功能

---
**文档维护人**: 开发团队
**文档版本**: v2.0 (正式版)
**最后更新**: 2025-11-22

