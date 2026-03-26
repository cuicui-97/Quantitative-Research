"""
矩阵 IO 工具

提供矩阵的保存、加载、统计等基础功能
"""
import logging
import pandas as pd
from pathlib import Path
from typing import Union

logger = logging.getLogger(__name__)


def save_matrix(
    matrix: pd.DataFrame,
    output_file: Union[str, Path],
    compress: bool = False
) -> Path:
    """
    保存矩阵到文件

    Args:
        matrix: 矩阵 DataFrame
        output_file: 输出文件路径
        compress: 是否使用 gzip 压缩

    Returns:
        Path: 保存的文件路径
    """
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    if compress:
        if not str(output_file).endswith('.gz'):
            output_file = Path(str(output_file) + '.gz')
        matrix.to_csv(output_file, compression='gzip')
    else:
        matrix.to_csv(output_file)

    logger.info(f"矩阵已保存: {output_file}")
    return output_file


def load_matrix(file_path: Union[str, Path]) -> pd.DataFrame:
    """
    从文件加载矩阵

    Args:
        file_path: 文件路径

    Returns:
        DataFrame: 矩阵
    """
    file_path = Path(file_path)

    if not file_path.exists():
        logger.error(f"文件不存在: {file_path}")
        return pd.DataFrame()

    if str(file_path).endswith('.gz'):
        matrix = pd.read_csv(file_path, compression='gzip', index_col=0)
    else:
        matrix = pd.read_csv(file_path, index_col=0)

    logger.info(f"矩阵已加载: {file_path} ({matrix.shape[0]} × {matrix.shape[1]})")
    return matrix


def matrix_statistics(matrix: pd.DataFrame, name: str = "矩阵") -> dict:
    """
    计算矩阵统计信息

    Args:
        matrix: 矩阵 DataFrame
        name: 矩阵名称

    Returns:
        dict: 统计信息
    """
    if matrix.empty:
        logger.warning(f"{name}为空")
        return {}

    stats = {
        'name': name,
        'shape': matrix.shape,
        'rows': matrix.shape[0],
        'cols': matrix.shape[1],
        'total_cells': matrix.shape[0] * matrix.shape[1],
    }

    # 数值统计
    if matrix.dtypes[0] in ['int8', 'int16', 'int32', 'int64', 'float32', 'float64']:
        stats['mean'] = matrix.mean().mean()
        stats['std'] = matrix.std().mean()
        stats['min'] = matrix.min().min()
        stats['max'] = matrix.max().max()
        stats['null_ratio'] = matrix.isna().mean().mean()

        # 如果是二值矩阵（只有0和1）
        unique_values = set()
        for col in matrix.columns[:10]:  # 采样前10列
            unique_values.update(matrix[col].dropna().unique())

        if unique_values.issubset({0, 1, 0.0, 1.0}):
            stats['ones_count'] = (matrix == 1).sum().sum()
            stats['ones_ratio'] = stats['ones_count'] / stats['total_cells']

    # 日期范围
    if hasattr(matrix.index, 'min'):
        stats['date_range'] = (str(matrix.index.min()), str(matrix.index.max()))

    # 打印统计信息
    logger.info(f"\n{name}统计信息:")
    logger.info(f"  维度: {stats['rows']} 行 × {stats['cols']} 列")

    if 'mean' in stats:
        logger.info(f"  平均值: {stats['mean']:.6f}")
    if 'null_ratio' in stats:
        logger.info(f"  缺失值比例: {stats['null_ratio']:.2%}")
    if 'ones_ratio' in stats:
        logger.info(f"  值为1的比例: {stats['ones_ratio']:.2%}")
    if 'date_range' in stats:
        logger.info(f"  日期范围: {stats['date_range'][0]} ~ {stats['date_range'][1]}")

    return stats
