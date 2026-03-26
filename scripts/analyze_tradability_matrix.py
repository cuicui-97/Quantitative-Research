#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
可交易矩阵分析脚本
提供各种统计分析和查询功能
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from config.config import Config


def load_matrix():
    """加载可交易矩阵"""
    matrix_file = Config.MATRIX_DATA_DIR / 'tradability_matrix.csv'
    if not matrix_file.exists():
        print(f"错误: 矩阵文件不存在: {matrix_file}")
        print("请先运行: python scripts/build_tradability_matrix.py")
        return None

    df = pd.read_csv(matrix_file, index_col=0)
    return df


def basic_stats(df):
    """基础统计信息"""
    print("\n" + "=" * 60)
    print("1. 基础统计信息")
    print("=" * 60)

    print(f"矩阵维度: {df.shape[0]} 个交易日 × {df.shape[1]} 只股票")
    print(f"数据类型: {df.values.dtype}")
    print(f"内存占用: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

    total_cells = df.shape[0] * df.shape[1]
    tradable_cells = df.sum().sum()
    tradable_rate = tradable_cells / total_cells

    print(f"\n可交易单元格数: {tradable_cells:,} / {total_cells:,}")
    print(f"可交易比例: {tradable_rate:.2%}")


def date_stats(df):
    """按日期统计"""
    print("\n" + "=" * 60)
    print("2. 按日期统计")
    print("=" * 60)

    # 每日可交易股票数
    daily_tradable = df.sum(axis=1)

    print(f"平均每日可交易股票数: {daily_tradable.mean():.0f}")
    print(f"最多可交易股票数: {daily_tradable.max():.0f} (日期: {daily_tradable.idxmax()})")
    print(f"最少可交易股票数: {daily_tradable.min():.0f} (日期: {daily_tradable.idxmin()})")

    # 显示最近 5 天
    print(f"\n最近 5 个交易日可交易股票数:")
    for date, count in daily_tradable.tail(5).items():
        print(f"  {date}: {count:.0f} 只")


def stock_stats(df):
    """按股票统计"""
    print("\n" + "=" * 60)
    print("3. 按股票统计")
    print("=" * 60)

    # 每只股票的可交易天数
    stock_tradable = df.sum(axis=0)
    total_days = df.shape[0]

    print(f"平均可交易天数: {stock_tradable.mean():.0f} / {total_days}")
    print(f"最多可交易天数: {stock_tradable.max():.0f} (股票: {stock_tradable.idxmax()})")
    print(f"最少可交易天数: {stock_tradable.min():.0f} (股票: {stock_tradable.idxmin()})")

    # 可交易率分布
    tradable_rate = stock_tradable / total_days
    print(f"\n可交易率分布:")
    print(f"  > 90%: {(tradable_rate > 0.9).sum()} 只")
    print(f"  70-90%: {((tradable_rate >= 0.7) & (tradable_rate <= 0.9)).sum()} 只")
    print(f"  50-70%: {((tradable_rate >= 0.5) & (tradable_rate < 0.7)).sum()} 只")
    print(f"  < 50%: {(tradable_rate < 0.5).sum()} 只")

    # 显示最不活跃的股票
    print(f"\n最不活跃的 5 只股票（可交易率）:")
    for stock, rate in tradable_rate.nsmallest(5).items():
        print(f"  {stock}: {rate:.2%}")


def query_examples(df):
    """查询示例"""
    print("\n" + "=" * 60)
    print("4. 查询示例")
    print("=" * 60)

    # 示例 1: 查询某股票某日是否可交易
    sample_date = df.index[-1]  # 最近一个交易日
    sample_stock = df.columns[0]  # 第一只股票
    is_tradable = df.loc[sample_date, sample_stock]

    print(f"\n示例 1: 查询 {sample_stock} 在 {sample_date} 是否可交易")
    print(f"  结果: {'可交易' if is_tradable == 1 else '不可交易'}")

    # 示例 2: 获取某日可交易股票列表
    tradable_stocks = df.loc[sample_date][df.loc[sample_date] == 1].index.tolist()
    print(f"\n示例 2: {sample_date} 可交易股票")
    print(f"  总数: {len(tradable_stocks)} 只")
    print(f"  示例: {tradable_stocks[:5]}...")

    # 示例 3: 筛选连续 N 天可交易的股票
    n_days = min(10, df.shape[0])
    recent_dates = df.index[-n_days:]
    always_tradable = df.loc[recent_dates].all(axis=0)
    stable_stocks = always_tradable[always_tradable == 1].index.tolist()

    print(f"\n示例 3: 连续 {n_days} 天可交易的股票")
    print(f"  总数: {len(stable_stocks)} 只")
    print(f"  示例: {stable_stocks[:5]}...")


def untradable_reasons(df):
    """不可交易原因分析（需要补充数据）"""
    print("\n" + "=" * 60)
    print("5. 不可交易原因分析")
    print("=" * 60)

    # 统计不可交易的单元格数
    untradable_cells = (df == 0).sum().sum()
    total_cells = df.shape[0] * df.shape[1]

    print(f"不可交易单元格数: {untradable_cells:,} / {total_cells:,}")
    print(f"不可交易比例: {untradable_cells / total_cells:.2%}")

    print(f"\n注意: 详细原因分析需要结合以下数据:")
    print(f"  - 基础数据（上市日期）")
    print(f"  - ST 状态数据")
    print(f"  - 停牌数据")
    print(f"  - 日线数据（涨跌停）")


def export_summary(df):
    """导出摘要统计"""
    print("\n" + "=" * 60)
    print("6. 导出摘要统计")
    print("=" * 60)

    output_dir = Config.MATRIX_DATA_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # 每日可交易股票数
    daily_summary = pd.DataFrame({
        'date': df.index,
        'tradable_count': df.sum(axis=1).values
    })
    daily_file = output_dir / 'tradability_daily_summary.csv'
    daily_summary.to_csv(daily_file, index=False)
    print(f"✓ 每日摘要已保存: {daily_file}")

    # 每只股票可交易天数
    stock_summary = pd.DataFrame({
        'ts_code': df.columns,
        'tradable_days': df.sum(axis=0).values,
        'tradable_rate': (df.sum(axis=0) / df.shape[0]).values
    })
    stock_file = output_dir / 'tradability_stock_summary.csv'
    stock_summary.to_csv(stock_file, index=False)
    print(f"✓ 股票摘要已保存: {stock_file}")


def main():
    """主函数"""
    print("=" * 60)
    print("可交易矩阵分析工具")
    print("=" * 60)

    # 加载矩阵
    df = load_matrix()
    if df is None:
        return 1

    # 执行分析
    basic_stats(df)
    date_stats(df)
    stock_stats(df)
    query_examples(df)
    untradable_reasons(df)
    export_summary(df)

    print("\n" + "=" * 60)
    print("分析完成！")
    print("=" * 60)

    return 0


if __name__ == '__main__':
    sys.exit(main())
