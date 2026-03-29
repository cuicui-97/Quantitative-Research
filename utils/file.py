"""
文件操作工具函数
"""
from pathlib import Path


def ensure_directory(path):
    """
    确保目录存在

    Args:
        path: 目录路径（str 或 Path）
    """
    Path(path).mkdir(parents=True, exist_ok=True)
