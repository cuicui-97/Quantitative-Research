#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Daily 数据 CSV → Parquet 转换工具

优势:
- 文件体积减少 50-80%
- 读取速度提升 5-10 倍
- 支持列式存储（只读需要的列）
- 自动保存数据类型

使用方法:
    python convert_daily_to_parquet.py --mode convert    # 执行转换
    python convert_daily_to_parquet.py --mode compare    # 对比测试
"""
import sys
from pathlib import Path
import argparse

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from config.config import Config
import time
import shutil


def convert_single_file(csv_path: Path, parquet_dir: Path, compression='snappy') -> bool:
    """
    转换单个 CSV 文件为 Parquet

    Args:
        csv_path: CSV 文件路径
        parquet_dir: 输出目录
        compression: 压缩算法 ('zstd', 'snappy', 'gzip')

    Returns:
        是否成功
    """
    try:
        # 读取 CSV
        df = pd.read_csv(csv_path)

        # 优化数据类型（减少内存）
        # 价格数据用 float32
        price_cols = [c for c in df.columns if any(x in c for x in ['open', 'high', 'low', 'close', 'change'])]
        for col in price_cols:
            if col in df.columns:
                df[col] = df[col].astype('float32')

        # 成交量/成交额用 float32
        vol_cols = [c for c in df.columns if 'vol' in c or 'amount' in c]
        for col in vol_cols:
            if col in df.columns:
                df[col] = df[col].astype('float32')

        # 日期列用 datetime（更高效）
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')

        # 保存为 Parquet
        output_path = parquet_dir / f"{csv_path.stem}.parquet"
        df.to_parquet(
            output_path,
            engine='pyarrow',
            compression=compression,
            index=False
        )

        return True

    except Exception as e:
        print(f"  转换失败 {csv_path.name}: {e}")
        return False


def convert_all_files(compression='zstd', limit=None):
    """转换所有文件"""
    csv_dir = Config.DAILY_DATA_DIR
    parquet_dir = Config.DATA_DIR / 'daily_parquet'
    parquet_dir.mkdir(parents=True, exist_ok=True)

    csv_files = list(csv_dir.glob('*.csv'))
    if limit:
        csv_files = csv_files[:limit]

    print(f"开始转换 {len(csv_files)} 个文件...")
    print(f"压缩算法: {compression}")
    print(f"输出目录: {parquet_dir}")
    print()

    success_count = 0
    total_csv_size = 0
    total_parquet_size = 0

    for i, csv_file in enumerate(csv_files, 1):
        if i % 500 == 0:
            print(f"  进度: {i}/{len(csv_files)} ({i/len(csv_files)*100:.1f}%)")

        csv_size = csv_file.stat().st_size
        total_csv_size += csv_size

        if convert_single_file(csv_file, parquet_dir, compression):
            success_count += 1
            parquet_file = parquet_dir / f"{csv_file.stem}.parquet"
            if parquet_file.exists():
                total_parquet_size += parquet_file.stat().st_size

    print(f"\n转换完成!")
    print(f"  成功: {success_count}/{len(csv_files)}")
    print(f"  CSV 总大小: {total_csv_size / 1024**2:.1f} MB")
    print(f"  Parquet 总大小: {total_parquet_size / 1024**2:.1f} MB")
    print(f"  压缩率: {(1 - total_parquet_size/total_csv_size)*100:.1f}%")


def compare_performance():
    """对比 CSV 和 Parquet 的性能"""
    csv_dir = Config.DAILY_DATA_DIR
    parquet_dir = Config.DATA_DIR / 'daily_parquet'

    # 找一个中等大小的文件测试
    test_files = list(csv_dir.glob('*.csv'))[:10]

    print("=" * 60)
    print("性能对比测试 (10个文件平均)")
    print("=" * 60)

    # CSV 读取测试
    csv_times = []
    for csv_file in test_files:
        t1 = time.time()
        df = pd.read_csv(csv_file)
        csv_times.append(time.time() - t1)
    avg_csv_time = np.mean(csv_times) * 1000  # 转毫秒

    # Parquet 读取测试
    parquet_times = []
    for csv_file in test_files:
        parquet_file = parquet_dir / f"{csv_file.stem}.parquet"
        if parquet_file.exists():
            t1 = time.time()
            df = pd.read_parquet(parquet_file)
            parquet_times.append(time.time() - t1)

    if parquet_times:
        avg_parquet_time = np.mean(parquet_times) * 1000
        print(f"\n读取速度:")
        print(f"  CSV:     {avg_csv_time:.2f} ms/文件")
        print(f"  Parquet: {avg_parquet_time:.2f} ms/文件")
        print(f"  加速比:  {avg_csv_time/avg_parquet_time:.1f}x")

    # 文件大小对比
    csv_sizes = [f.stat().st_size for f in test_files]
    parquet_sizes = []
    for csv_file in test_files:
        parquet_file = parquet_dir / f"{csv_file.stem}.parquet"
        if parquet_file.exists():
            parquet_sizes.append(parquet_file.stat().st_size)

    if parquet_sizes:
        avg_csv_size = np.mean(csv_sizes) / 1024  # KB
        avg_parquet_size = np.mean(parquet_sizes) / 1024  # KB
        print(f"\n文件大小:")
        print(f"  CSV:     {avg_csv_size:.1f} KB/文件")
        print(f"  Parquet: {avg_parquet_size:.1f} KB/文件")
        print(f"  压缩率:  {(1 - np.mean(parquet_sizes)/np.mean(csv_sizes))*100:.1f}%")

    # 只读特定列测试（Parquet 优势）
    test_file = test_files[0]
    parquet_file = parquet_dir / f"{test_file.stem}.parquet"

    if parquet_file.exists():
        print(f"\n只读特定列 ('close', 'vol') 测试:")

        t1 = time.time()
        df = pd.read_csv(test_file, usecols=['close', 'vol'])
        csv_partial_time = (time.time() - t1) * 1000

        t1 = time.time()
        df = pd.read_parquet(parquet_file, columns=['close', 'vol'])
        parquet_partial_time = (time.time() - t1) * 1000

        print(f"  CSV (usecols):     {csv_partial_time:.2f} ms")
        print(f"  Parquet (columns): {parquet_partial_time:.2f} ms")
        print(f"  加速比:            {csv_partial_time/parquet_partial_time:.1f}x")


def main():
    parser = argparse.ArgumentParser(description='Daily 数据 CSV → Parquet 转换')
    parser.add_argument('--mode', choices=['convert', 'compare', 'both'], default='both',
                       help='运行模式: convert=仅转换, compare=仅对比, both=转换+对比')
    parser.add_argument('--compression', choices=['zstd', 'snappy', 'gzip'], default='snappy',
                       help='压缩算法 (默认: zstd，推荐)')
    parser.add_argument('--limit', type=int, default=None,
                       help='限制转换文件数（用于测试）')

    args = parser.parse_args()

    if args.mode in ['convert', 'both']:
        convert_all_files(args.compression, args.limit)

    if args.mode in ['compare', 'both']:
        print()
        compare_performance()


if __name__ == '__main__':
    main()
