#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
因子构建性能对比测试

对比：
1. 旧架构 (FactorMatrixBuilder) - 逐股票循环
2. v3.0 架构 (UnifiedFactorBuilder) - 矩阵向量化
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import time
import logging

import pandas as pd
import numpy as np

logging.basicConfig(
    level=logging.WARNING,  # 减少日志干扰
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def benchmark_v3():
    """测试 v3.0 架构性能"""
    from data_engine.core import MomentumFactorBuilder

    logger.info("\n" + "=" * 70)
    logger.info("v3.0 架构测试")
    logger.info("=" * 70)

    builder = MomentumFactorBuilder(start_date='20240101')

    t0 = time.time()
    results = builder.build(save=False)
    elapsed = time.time() - t0

    cache_stats = builder.computation_graph.get_cache_stats()

    logger.info(f"构建 {len(results)} 个因子")
    logger.info(f"总耗时: {elapsed:.2f}s")
    logger.info(f"缓存: 命中 {cache_stats['hits']}, 未命中 {cache_stats['misses']}")

    return elapsed, results


def benchmark_old():
    """测试旧架构性能"""
    from data_engine.processors.factor_matrix_builder import FactorMatrixBuilder
    from data_engine.processors.data_loader import get_all_trading_dates, get_all_stocks

    logger.info("\n" + "=" * 70)
    logger.info("旧架构测试（逐股票循环）")
    logger.info("=" * 70)

    class OldMomentumBuilder(FactorMatrixBuilder):
        def get_factor_definitions(self):
            return [
                ('reversal_5d', '5日反转'),
                ('momentum_20d', '20日动量'),
                ('momentum_60d', '60日动量'),
                ('volatility_20d', '20日波动率'),
            ]

        def calculate_factors_for_stock(self, ts_code, df):
            close = df['close']
            return {
                'reversal_5d': -close.pct_change(5),
                'momentum_20d': close.pct_change(20),
                'momentum_60d': close.pct_change(60),
                'volatility_20d': close.pct_change().rolling(20).std(),
            }

    # 限制日期范围以加快测试
    dates = get_all_trading_dates()
    dates = dates[dates >= '20240101']

    builder = OldMomentumBuilder(dates=dates)

    t0 = time.time()

    # 只执行构建，不保存
    raw_data = builder.load_raw_data()
    matrices = builder.build_factor_matrices(raw_data)

    elapsed = time.time() - t0

    logger.info(f"构建 {len(matrices)} 个因子")
    logger.info(f"总耗时: {elapsed:.2f}s")

    return elapsed, matrices


def verify_results(v3_results, old_results):
    """验证两个架构结果是否一致"""
    logger.info("\n" + "=" * 70)
    logger.info("结果验证")
    logger.info("=" * 70)

    name_map = {
        'reversal_5d': 'reversal_5d',
        'momentum_20d': 'momentum_20d',
        'momentum_60d': 'momentum_60d',
        'volatility_20d': 'volatility_20d',
    }

    for v3_name, old_name in name_map.items():
        if v3_name not in v3_results or old_name not in old_results:
            continue

        v3_df = v3_results[v3_name]
        old_df = old_results[old_name]

        # 对齐索引
        common_idx = v3_df.index.intersection(old_df.index)
        common_cols = v3_df.columns.intersection(old_df.columns)

        v3_aligned = v3_df.loc[common_idx, common_cols]
        old_aligned = old_df.loc[common_idx, common_cols]

        # 计算差异
        diff = (v3_aligned - old_aligned).abs()
        max_diff = diff.max().max()
        mean_diff = diff.mean().mean()

        status = "✓" if max_diff < 1e-6 else "✗"
        logger.info(f"{status} {v3_name}: 最大差异={max_diff:.2e}, 平均差异={mean_diff:.2e}")


def main():
    """主测试"""
    logger.info("\n" + "=" * 70)
    logger.info("因子构建框架性能对比")
    logger.info("=" * 70)
    logger.info("测试说明：构建4个动量因子，日期范围 2024-01-01 至今")
    logger.info("=" * 70)

    # 测试 v3.0
    v3_time, v3_results = benchmark_v3()

    # 测试旧架构（可选，较慢）
    try:
        old_time, old_results = benchmark_old()

        # 对比
        logger.info("\n" + "=" * 70)
        logger.info("性能对比")
        logger.info("=" * 70)
        logger.info(f"旧架构耗时: {old_time:.2f}s")
        logger.info(f"v3.0 耗时:  {v3_time:.2f}s")
        logger.info(f"加速比:     {old_time/v3_time:.1f}x")

        # 验证结果一致性
        verify_results(v3_results, old_results)

    except Exception as e:
        logger.warning(f"旧架构测试失败: {e}")
        logger.info("v3.0 测试完成")

    logger.info("\n" + "=" * 70)
    logger.info("测试完成！")
    logger.info("=" * 70)


if __name__ == '__main__':
    main()
