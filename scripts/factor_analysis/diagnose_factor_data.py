#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
因子数据质量诊断脚本

检查PB因子、市值因子、收益率矩阵的数据质量
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config.config import Config
from data_engine.processors.matrix_io import load_matrix
from data_engine.utils import setup_logger
import pandas as pd
import numpy as np


def check_matrix_quality(matrix: pd.DataFrame, name: str, logger):
    """
    检查矩阵的数据质量

    Args:
        matrix: 数据矩阵
        name: 矩阵名称
        logger: 日志记录器
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"{name} 数据质量检查")
    logger.info(f"{'='*80}")

    # 基本信息
    logger.info(f"维度: {matrix.shape[0]} 天 × {matrix.shape[1]} 只股票")
    logger.info(f"日期范围: {matrix.index[0]} ~ {matrix.index[-1]}")

    # 缺失值统计
    total_cells = matrix.size
    nan_count = matrix.isna().sum().sum()
    nan_ratio = (nan_count / total_cells) * 100
    logger.info(f"缺失值: {nan_count:,} ({nan_ratio:.2f}%)")

    # 有效值统计
    valid_values = matrix.values[~np.isnan(matrix.values)]

    if len(valid_values) > 0:
        logger.info(f"\n有效值统计:")
        logger.info(f"  数量: {len(valid_values):,}")
        logger.info(f"  最小值: {valid_values.min():.6f}")
        logger.info(f"  1%分位: {np.percentile(valid_values, 1):.6f}")
        logger.info(f"  5%分位: {np.percentile(valid_values, 5):.6f}")
        logger.info(f"  25%分位: {np.percentile(valid_values, 25):.6f}")
        logger.info(f"  中位数: {np.median(valid_values):.6f}")
        logger.info(f"  75%分位: {np.percentile(valid_values, 75):.6f}")
        logger.info(f"  95%分位: {np.percentile(valid_values, 95):.6f}")
        logger.info(f"  99%分位: {np.percentile(valid_values, 99):.6f}")
        logger.info(f"  最大值: {valid_values.max():.6f}")
        logger.info(f"  均值: {valid_values.mean():.6f}")
        logger.info(f"  标准差: {valid_values.std():.6f}")

        # 异常值检查
        logger.info(f"\n异常值检查:")

        # 负值
        negative_count = (valid_values < 0).sum()
        if negative_count > 0:
            logger.warning(f"  ⚠️  负值数量: {negative_count} ({negative_count/len(valid_values)*100:.2f}%)")
        else:
            logger.info(f"  ✓ 无负值")

        # 零值
        zero_count = (valid_values == 0).sum()
        if zero_count > 0:
            logger.info(f"  零值数量: {zero_count} ({zero_count/len(valid_values)*100:.2f}%)")

        # 极端值（>99.9%分位或<0.1%分位）
        p999 = np.percentile(valid_values, 99.9)
        p001 = np.percentile(valid_values, 0.1)
        extreme_high = (valid_values > p999).sum()
        extreme_low = (valid_values < p001).sum()
        logger.info(f"  极高值(>99.9%分位={p999:.2f}): {extreme_high}")
        logger.info(f"  极低值(<0.1%分位={p001:.2f}): {extreme_low}")

    else:
        logger.warning(f"  ⚠️  矩阵全部为NaN!")

    # 每日有效股票数统计
    daily_valid_counts = matrix.notna().sum(axis=1)
    logger.info(f"\n每日有效股票数:")
    logger.info(f"  最小: {daily_valid_counts.min()}")
    logger.info(f"  平均: {daily_valid_counts.mean():.0f}")
    logger.info(f"  最大: {daily_valid_counts.max()}")

    return {
        'shape': matrix.shape,
        'date_range': (str(matrix.index[0]), str(matrix.index[-1])),
        'nan_ratio': nan_ratio,
        'valid_count': len(valid_values),
        'min': valid_values.min() if len(valid_values) > 0 else np.nan,
        'max': valid_values.max() if len(valid_values) > 0 else np.nan,
        'mean': valid_values.mean() if len(valid_values) > 0 else np.nan,
        'std': valid_values.std() if len(valid_values) > 0 else np.nan,
        'negative_count': negative_count if len(valid_values) > 0 else 0,
        'zero_count': zero_count if len(valid_values) > 0 else 0,
    }


def check_time_alignment(matrices: dict, logger):
    """
    检查矩阵之间的时间对齐

    Args:
        matrices: 矩阵字典
        logger: 日志记录器
    """
    logger.info(f"\n{'='*80}")
    logger.info("时间对齐检查")
    logger.info(f"{'='*80}")

    # 检查日期索引
    indices = {name: set(mat.index) for name, mat in matrices.items()}

    # 找出共同日期
    common_dates = set.intersection(*indices.values())
    logger.info(f"共同日期数: {len(common_dates)}")

    # 检查每对矩阵的对齐情况
    matrix_names = list(matrices.keys())
    for i in range(len(matrix_names)):
        for j in range(i+1, len(matrix_names)):
            name1, name2 = matrix_names[i], matrix_names[j]
            dates1, dates2 = indices[name1], indices[name2]

            only_in_1 = len(dates1 - dates2)
            only_in_2 = len(dates2 - dates1)
            common = len(dates1 & dates2)

            logger.info(f"\n{name1} vs {name2}:")
            logger.info(f"  共同日期: {common}")
            logger.info(f"  仅在{name1}: {only_in_1}")
            logger.info(f"  仅在{name2}: {only_in_2}")

            if only_in_1 > 0 or only_in_2 > 0:
                logger.warning(f"  ⚠️  时间范围不完全一致!")


def analyze_factor_groups(factor_matrix, tradability_matrix, factor_name, n_groups, logger):
    """
    分析因子分组的详细情况

    Args:
        factor_matrix: 因子矩阵
        tradability_matrix: 可交易矩阵
        factor_name: 因子名称
        n_groups: 分组数
        logger: 日志记录器
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"{factor_name}因子分组分析")
    logger.info(f"{'='*80}")

    # 对齐日期
    common_dates = factor_matrix.index.intersection(tradability_matrix.index)
    factor_aligned = factor_matrix.loc[common_dates]
    tradable_aligned = tradability_matrix.loc[common_dates]

    # shift(-1) 获取T+1日可交易状态
    tradable_next_day = tradable_aligned.shift(-1)

    # 统计每日可分组的股票数
    daily_tradable_counts = []
    daily_group_sizes = {i: [] for i in range(1, n_groups+1)}

    sample_dates = common_dates[:5]  # 只分析前5天作为样本

    for date in sample_dates:
        factor_values = factor_aligned.loc[date]
        tradable_status = tradable_next_day.loc[date]

        # 可交易且因子值非空
        tradable_mask = (tradable_status == 0) & factor_values.notna()
        tradable_factors = factor_values[tradable_mask]

        daily_tradable_counts.append(len(tradable_factors))

        if len(tradable_factors) >= n_groups:
            try:
                # 分组
                groups = pd.qcut(tradable_factors, q=n_groups, labels=False, duplicates='drop') + 1

                # 统计每组的股票数和因子值范围
                logger.info(f"\n日期: {date}")
                logger.info(f"  可交易股票数: {len(tradable_factors)}")

                for g in range(1, n_groups+1):
                    group_stocks = tradable_factors[groups == g]
                    if len(group_stocks) > 0:
                        daily_group_sizes[g].append(len(group_stocks))
                        logger.info(f"  第{g}组: {len(group_stocks)}只股票, "
                                  f"{factor_name}范围=[{group_stocks.min():.2f}, {group_stocks.max():.2f}], "
                                  f"均值={group_stocks.mean():.2f}")
            except ValueError as e:
                logger.warning(f"  ⚠️  分组失败: {e}")

    # 总体统计
    logger.info(f"\n总体统计（基于{len(sample_dates)}个样本日期）:")
    logger.info(f"  每日可交易股票数: {np.mean(daily_tradable_counts):.0f} ± {np.std(daily_tradable_counts):.0f}")

    for g in range(1, n_groups+1):
        if daily_group_sizes[g]:
            logger.info(f"  第{g}组平均股票数: {np.mean(daily_group_sizes[g]):.0f}")


def main():
    """主函数"""
    logger = setup_logger()

    logger.info("="*80)
    logger.info("因子数据质量诊断")
    logger.info("="*80)

    # 1. 加载矩阵
    logger.info("\n加载矩阵数据...")
    pb_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'pb_matrix.csv')
    mv_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'circ_mv_matrix.csv')
    return_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'open_return_matrix.csv')
    tradability_matrix = load_matrix(Config.MATRIX_DATA_DIR / 'tradability_matrix.csv')

    logger.info("✓ 矩阵加载完成")

    # 2. 数据质量检查
    matrices = {
        'PB矩阵': pb_matrix,
        '流通市值矩阵': mv_matrix,
        '开盘收益率矩阵': return_matrix,
        '可交易矩阵': tradability_matrix
    }

    quality_reports = {}
    for name, matrix in matrices.items():
        quality_reports[name] = check_matrix_quality(matrix, name, logger)

    # 3. 时间对齐检查
    check_time_alignment(matrices, logger)

    # 4. 因子分组分析
    analyze_factor_groups(pb_matrix, tradability_matrix, 'PB', 10, logger)
    analyze_factor_groups(mv_matrix, tradability_matrix, '市值', 10, logger)

    # 5. 生成总结报告
    logger.info(f"\n{'='*80}")
    logger.info("诊断总结")
    logger.info(f"{'='*80}")

    logger.info("\n数据时间范围:")
    for name, report in quality_reports.items():
        logger.info(f"  {name}: {report['date_range'][0]} ~ {report['date_range'][1]} ({report['shape'][0]}天)")

    logger.info("\n数据完整性:")
    for name, report in quality_reports.items():
        logger.info(f"  {name}: 缺失率 {report['nan_ratio']:.2f}%")

    logger.info("\n关键发现:")

    # 检查时间范围差异
    pb_days = quality_reports['PB矩阵']['shape'][0]
    return_days = quality_reports['开盘收益率矩阵']['shape'][0]
    if pb_days < return_days:
        logger.warning(f"  ⚠️  PB矩阵时间范围({pb_days}天)短于收益率矩阵({return_days}天)")
        logger.warning(f"      这意味着回测只覆盖{pb_days}天，可能存在时间段特殊性偏差")

    # 检查异常值
    for name in ['PB矩阵', '流通市值矩阵']:
        if quality_reports[name]['negative_count'] > 0:
            logger.warning(f"  ⚠️  {name}存在{quality_reports[name]['negative_count']}个负值")

    logger.info("\n✓ 诊断完成")
    logger.info("="*80)


if __name__ == '__main__':
    main()
