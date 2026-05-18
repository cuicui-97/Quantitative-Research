#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
FactorMatrix 使用示例

演示如何用新的数据结构改进因子回测流程
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import numpy as np
import pandas as pd

from data_engine.core.factor_matrix import FactorMatrix
from data_engine.processors.matrix_io import load_matrix
from config.config import Config


def demo_basic_usage():
    """基础用法演示"""
    print("=" * 60)
    print("基础用法演示")
    print("=" * 60)

    # 1. 从 CSV 加载现有因子矩阵
    factor_file = Config.MATRIX_DATA_DIR / 'net_profit_yoy_matrix.csv'
    if factor_file.exists():
        fm = FactorMatrix.load_csv(factor_file, name='net_profit_yoy')
        print(f"\n加载因子矩阵: {fm}")
        print(f"  日期范围: {fm.info()['dates']}")
        print(f"  缺失值比例: {fm.info()['null_ratio']:.2%}")
    else:
        print(f"文件不存在: {factor_file}")
        return

    # 2. 截面排名（去极值）
    print("\n截面排名...")
    fm_ranked = fm.rank(axis=1)
    print(f"  排名后: {fm_ranked}")

    # 3. 截面标准化
    print("\n截面标准化 (Z-Score)...")
    fm_zscore = fm.zscore(axis=1)
    print(f"  标准化后均值: {fm_zscore.mean():.4f}")
    print(f"  标准化后标准差: {fm_zscore.std():.4f}")

    # 4. 日期切片
    print("\n日期切片 (2020-2023)...")
    fm_2020s = fm.slice_dates(start=20200101, end=20231231)
    print(f"  切片后: {fm_2020s}")


def demo_backtest_integration():
    """
    回测集成示例

    展示如何用 FactorMatrix 改进回测流程
    """
    print("\n" + "=" * 60)
    print("回测集成演示")
    print("=" * 60)

    # 加载多个矩阵
    print("\n加载矩阵...")

    factor = FactorMatrix.load_csv(
        Config.MATRIX_DATA_DIR / 'net_profit_yoy_matrix.csv',
        name='net_profit_yoy'
    )
    mv = FactorMatrix.load_csv(
        Config.MATRIX_DATA_DIR / 'circ_mv_matrix.csv',
        name='circ_mv'
    )
    # 其他矩阵可能在不同目录
    returns_path = Config.MATRIX_DATA_DIR / 'open_return_matrix.csv'
    if not returns_path.exists():
        returns_path = Config.DATA_DIR / 'matrices' / 'open_return_matrix.csv'
    returns = FactorMatrix.load_csv(returns_path, name='returns') if returns_path.exists() else None

    tradable_path = Config.MATRIX_DATA_DIR / 'tradability_matrix.csv'
    if not tradable_path.exists():
        tradable_path = Config.DATA_DIR / 'matrices' / 'tradability_matrix.csv'
    tradable = FactorMatrix.load_csv(tradable_path, name='tradable') if tradable_path.exists() else None

    print(f"  因子: {factor}")
    print(f"  市值: {mv}")
    print(f"  收益: {returns}")
    print(f"  可交易: {tradable}")

    # 对齐所有矩阵
    print("\n对齐矩阵...")
    factor_aligned, mv_aligned = factor.align(mv)
    returns_aligned, _ = returns.align(factor_aligned)
    tradable_aligned, _ = tradable.align(factor_aligned)

    print(f"  对齐后: {factor_aligned.shape}")

    # 因子预处理：去极值 + 标准化
    print("\n因子预处理...")
    factor_processed = factor_aligned.zscore(axis=1)  # 标准化
    print(f"  预处理后: {factor_processed}")

    # 保存为 NPZ（下次加载更快）
    print("\n保存为 NPZ 格式...")
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        npz_path = Path(tmpdir) / 'factor_processed.npz'
        factor_processed.save_npz(npz_path)

        # 快速加载
        loaded = FactorMatrix.load_npz(npz_path)
        print(f"  快速加载: {loaded}")


def demo_factor_combination():
    """
    因子合成示例

    展示如何合成多个因子
    """
    print("\n" + "=" * 60)
    print("因子合成演示")
    print("=" * 60)

    # 假设有两个因子
    n_dates, n_stocks = 100, 500
    dates = np.arange(20200101, 20200101 + n_dates)
    stocks = np.array([f'{i:06d}.SZ' for i in range(n_stocks)])

    # 创建模拟因子
    np.random.seed(42)
    fm1 = FactorMatrix(
        name='momentum',
        values=np.random.randn(n_dates, n_stocks).astype(np.float32),
        dates=dates,
        stocks=stocks
    )
    fm2 = FactorMatrix(
        name='value',
        values=np.random.randn(n_dates, n_stocks).astype(np.float32),
        dates=dates,
        stocks=stocks
    )

    print(f"\n因子1: {fm1}")
    print(f"因子2: {fm2}")

    # 等权合成
    print("\n等权合成...")
    combined_equal = (fm1.rank() + fm2.rank()) / 2
    combined_equal.name = "combined_equal"
    print(f"  合成因子: {combined_equal}")

    # 市值加权合成（市值大的因子权重更高）
    print("\n市值加权合成...")
    mv_values = np.random.lognormal(10, 1, (n_dates, n_stocks)).astype(np.float32)
    mv = FactorMatrix(name='mv', values=mv_values, dates=dates, stocks=stocks)

    # 计算权重
    w1 = mv.values / (mv.values + 1)  # 简单权重
    w2 = 1 - w1

    combined_weighted = FactorMatrix(
        name='combined_weighted',
        values=fm1.values * w1 + fm2.values * w2,
        dates=dates,
        stocks=stocks
    )
    print(f"  加权合成因子: {combined_weighted}")


def demo_performance_comparison():
    """
    性能对比：FactorMatrix vs 纯 pandas
    """
    print("\n" + "=" * 60)
    print("性能对比")
    print("=" * 60)

    import time

    # 创建测试数据
    n_dates, n_stocks = 2000, 3000
    dates = np.arange(20150101, 20150101 + n_dates)
    stocks = np.array([f'{i:06d}.SZ' for i in range(n_stocks)])
    values = np.random.randn(n_dates, n_stocks).astype(np.float32)

    fm = FactorMatrix(name='test', values=values, dates=dates, stocks=stocks)
    df = fm.to_pandas()

    print(f"\n数据规模: {n_dates} 天 × {n_stocks} 股票")

    # 测试截面排名
    t1 = time.time()
    fm_ranked = fm.rank(axis=1)
    t_fm = (time.time() - t1) * 1000

    t1 = time.time()
    df_ranked = df.rank(axis=1, pct=True)
    t_df = (time.time() - t1) * 1000

    print(f"\n截面排名:")
    print(f"  FactorMatrix: {t_fm:.1f} ms")
    print(f"  Pandas:       {t_df:.1f} ms")
    print(f"  开销:         {(t_fm/t_df - 1)*100:.0f}%")

    # 测试运算
    t1 = time.time()
    result = fm + fm * 0.5 - fm.shift(1)
    t_fm = (time.time() - t1) * 1000

    t1 = time.time()
    result_df = df + df * 0.5 - df.shift(1)
    t_df = (time.time() - t1) * 1000

    print(f"\n复杂运算 (加减移):")
    print(f"  FactorMatrix: {t_fm:.1f} ms")
    print(f"  Pandas:       {t_df:.1f} ms")
    print(f"  开销:         {(t_fm/t_df - 1)*100:.0f}%")

    print("\n结论: FactorMatrix 有少量转换开销，但提供了更好的结构")


if __name__ == '__main__':
    demo_basic_usage()
    demo_backtest_integration()
    demo_factor_combination()
    demo_performance_comparison()

    print("\n" + "=" * 60)
    print("演示完成!")
    print("=" * 60)
