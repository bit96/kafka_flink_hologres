#!/bin/bash
# scripts/run.sh - 运行脚本

# 获取脚本所在目录的父目录（项目根目录）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# 设置 PYTHONPATH
export PYTHONPATH="$PROJECT_ROOT/src:$PYTHONPATH"

# 运行工具
python -m kafka_flink_tool "$@"
