#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
构建微盘股矩阵

微盘股定义：总市值(TotalMV)小于50亿（500000万元）
矩阵格式：行=交易日，列=股票，值为1表示是微盘股，0表示不是
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from config.config import Config
from data_engine.processors.matrix_io import load_matrix, save_matrix
from utils import setup_logger


def build_microcap_matrix(threshold: float = 500000.0, logger=None) -> pd.DataFrame:
    """
    构建微盘股矩阵

    Args:
        threshold: 微盘股阈值（单位：万元），默认50亿=500000万元
        logger: 日志对象

    Returns:
        DataFrame: 微盘股矩阵（1=是微盘股，0=不是微盘股）
    """
    if logger:
        logger.info(f"构建微盘股矩阵（总市值 < {threshold}万元 = {threshold/10000:.0f}亿）...")

    # 加载总市值矩阵
    total_mv_file = Config.MATRIX_DATA_DIR / 'total_mv_matrix.csv'
    if not total_mv_file.exists():
        raise FileNotFoundError(f"总市值矩阵不存在: {total_mv_file}")

    total_mv = load_matrix(total_mv_file)
    if logger:
        logger.info(f"加载总市值矩阵: {total_mv.shape}")

    # 构建微盘股矩阵：总市值 < 阈值 的位置设为1
    microcap_matrix = (total_mv < threshold).astype(np.int8)

    # 统计
    total_elements = microcap_matrix.size
    microcap_count = microcap_matrix.sum().sum()
    microcap_ratio = microcap_count / total_elements

    if logger:
        logger.info(f"微盘股比例: {microcap_ratio:.2%}")
        logger.info(f"微盘股矩阵构建完成: {microcap_matrix.shape}")

    return microcap_matrix


def main():
    logger = setup_logger(prefix="microcap_matrix")
    logger.info("="*60)
    logger.info("开始构建微盘股矩阵")
    logger.info("="*60)

    try:
        # 构建微盘股矩阵（30亿阈值）
        microcap_matrix = build_microcap_matrix(threshold=300000.0, logger=logger)

        # 保存矩阵
        output_file = Config.MATRIX_DATA_DIR / 'microcap_matrix.csv'
        save_matrix(microcap_matrix, output_file)
        logger.info(f"微盘股矩阵已保存: {output_file}")

        # 输出一些统计信息
        logger.info("\n微盘股统计:")
        logger.info(f"  总交易日数: {len(microcap_matrix)}")
        logger.info(f"  总股票数: {len(microcap_matrix.columns)}")

        # 随机选几个日期看看微盘股数量
        sample_dates = microcap_matrix.index[::len(microcap_matrix)//5][:5]
        for date in sample_dates:
            count = microcap_matrix.loc[date].sum()
            ratio = count / len(microcap_matrix.columns)
            logger.info(f"  {date}: {count}只 ({ratio:.1%})")

        logger.info("\n构建完成!")

    except Exception as e:
        logger.error(f"构建微盘股矩阵失败: {e}")
        raise


if __name__ == '__main__':
    main()
