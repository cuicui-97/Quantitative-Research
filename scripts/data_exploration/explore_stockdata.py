#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
StockData 数据探查工具

全面分析 stockdata 目录下的数据情况，包括：
- 目录结构和文件统计
- 数据覆盖范围（日期、股票）
- 数据质量检查
- 可视化展示
"""
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from config.config import Config


def print_section(title):
    """打印章节标题"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def explore_directory_structure():
    """探查目录结构"""
    print_section("目录结构")

    data_dir = Config.DATA_DIR

    for subdir in sorted(data_dir.iterdir()):
        if subdir.is_dir():
            files = list(subdir.glob('*'))
            csv_files = [f for f in files if f.suffix in ['.csv', '.gz']]
            total_size = sum(f.stat().st_size for f in files if f.is_file()) / (1024**2)

            print(f"\n{subdir.name}/")
            print(f"  文件数: {len(files)}")
            print(f"  数据文件: {len(csv_files)}")
            print(f"  总大小: {total_size:.1f} MB")


def explore_basic_data():
    """探查基础数据"""
    print_section("基础数据 (basic/)")

    basic_file = Config.BASIC_DATA_DIR / 'all_companies_info.csv'
    if not basic_file.exists():
        print("  文件不存在")
        return

    df = pd.read_csv(basic_file)
    print(f"\n总股票数: {len(df)}")

    if 'list_date' in df.columns:
        df['list_date'] = pd.to_datetime(df['list_date'], format='%Y%m%d')
        print(f"\n上市时间分布:")
        print(f"  最早: {df['list_date'].min().strftime('%Y-%m-%d')}")
        print(f"  最晚: {df['list_date'].max().strftime('%Y-%m-%d')}")

        # 按年统计
        df['list_year'] = df['list_date'].dt.year
        yearly = df['list_year'].value_counts().sort_index()
        print(f"\n每年上市数量 (最近5年):")
        for year, count in yearly.tail(5).items():
            print(f"  {year}: {count} 只")


def explore_daily_data():
    """探查日线数据"""
    print_section("日线数据 (daily/)")

    daily_dir = Config.DAILY_DATA_DIR
    csv_files = list(daily_dir.glob('*.csv'))

    print(f"\n股票文件数: {len(csv_files)}")

    if not csv_files:
        return

    # 采样检查
    sample_files = csv_files[:5]
    print(f"\n抽样检查 ({len(sample_files)} 个文件):")

    for f in sample_files:
        try:
            df = pd.read_csv(f, nrows=3)
            print(f"  {f.stem}: {list(df.columns)}")
        except Exception as e:
            print(f"  {f.stem}: 读取失败 - {e}")


def explore_supplementary_data():
    """探查辅助数据"""
    print_section("辅助数据 (supplementary/)")

    supp_dir = Config.SUPPLEMENTARY_DATA_DIR
    csv_files = [f for f in supp_dir.glob('*.csv') if f.is_file()]

    print(f"\n数据文件:")
    for f in sorted(csv_files):
        size_mb = f.stat().st_size / (1024**2)
        print(f"  {f.name}: {size_mb:.1f} MB")

    # 交易日历
    calendar_file = supp_dir / 'trade_calendar.csv'
    if calendar_file.exists():
        cal = pd.read_csv(calendar_file)
        print(f"\n交易日历:")
        print(f"  总天数: {len(cal)}")
        print(f"  交易日: {cal['is_open'].sum()} 天")
        print(f"  日期范围: {cal['cal_date'].min()} ~ {cal['cal_date'].max()}")


def explore_factor_matrices():
    """探查因子矩阵"""
    print_section("因子矩阵 (factor/)")

    factor_dir = Config.MATRIX_DATA_DIR
    csv_files = [f for f in factor_dir.glob('*_matrix.csv') if f.is_file()]

    print(f"\n矩阵文件数: {len(csv_files)}")
    print(f"\n各矩阵信息:")

    for f in sorted(csv_files):
        try:
            # 只读取前几行获取维度信息
            df = pd.read_csv(f, index_col=0, nrows=5)

            # 获取完整维度（不读取全部数据）
            with open(f, 'r') as fp:
                n_rows = sum(1 for _ in fp) - 1  # 减1是表头
            n_cols = len(df.columns)

            size_mb = f.stat().st_size / (1024**2)

            print(f"\n  {f.name}:")
            print(f"    维度: {n_rows} × {n_cols}")
            print(f"    大小: {size_mb:.1f} MB")
            print(f"    日期: {df.index[0]} ~ {df.index[-1]}")

        except Exception as e:
            print(f"  {f.name}: 读取失败 - {e}")


def check_data_quality():
    """数据质量检查"""
    print_section("数据质量检查")

    # 检查收益矩阵
    returns_file = Config.MATRIX_DATA_DIR / 'open_return_matrix.csv'
    if returns_file.exists():
        print("\n收益矩阵 (open_return_matrix.csv):")
        df = pd.read_csv(returns_file, index_col=0)

        null_ratio = df.isna().mean().mean()
        print(f"  缺失值比例: {null_ratio:.2%}")

        # 极端收益率检查
        extreme_high = (df > 0.2).sum().sum()
        extreme_low = (df < -0.2).sum().sum()
        print(f"  极端正收益 (>20%): {extreme_high} 个")
        print(f"  极端负收益 (<-20%): {extreme_low} 个")


def generate_summary():
    """生成数据摘要"""
    print_section("数据摘要")

    summary = {
        '数据根目录': str(Config.DATA_DIR),
        '总大小(MB)': sum(f.stat().st_size for f in Config.DATA_DIR.rglob('*') if f.is_file()) / (1024**2),
    }

    for key, value in summary.items():
        print(f"  {key}: {value}")


def main():
    """主函数"""
    print("\n" + "=" * 70)
    print("  StockData 数据探查报告")
    print(f"  生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    explore_directory_structure()
    explore_basic_data()
    explore_daily_data()
    explore_supplementary_data()
    explore_factor_matrices()
    check_data_quality()
    generate_summary()

    print("\n" + "=" * 70)
    print("  探查完成")
    print("=" * 70)


if __name__ == '__main__':
    main()
