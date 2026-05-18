#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
预计算公告日最新报告期

为每个股票的每个公告日计算对应的最新报告期，
保存到 supplementary/ann_date_latest_end.csv，供后续因子构建使用。

使用方法:
    python build_ann_date_latest_end.py
    python build_ann_date_latest_end.py --field n_income_attr_p  # 指定字段（可选）
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import argparse
import logging
from typing import Optional

import pandas as pd
import numpy as np

from config.config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def build_ann_date_latest_end(
    data_file: Optional[Path] = None,
    output_file: Optional[Path] = None,
    field: Optional[str] = None
) -> pd.DataFrame:
    """
    构建公告日-最新报告期映射表

    Args:
        data_file: 财务数据文件路径，默认 Config.INCOME_DATA_FILE
        output_file: 输出文件路径，默认 Config.SUPPLEMENTARY_DATA_DIR / 'ann_date_latest_end.csv'
        field: 指定字段（用于过滤有效数据），默认 None 表示不过滤

    Returns:
        DataFrame: ts_code, ann_date, latest_end_date, report_type 列
    """
    data_file = data_file or Config.INCOME_DATA_FILE
    output_file = output_file or Config.SUPPLEMENTARY_DATA_DIR / 'ann_date_latest_end.csv'

    logger.info("=" * 70)
    logger.info("构建公告日-最新报告期映射表")
    logger.info("=" * 70)
    logger.info(f"数据源: {data_file}")

    # 加载数据
    df = pd.read_csv(
        data_file,
        dtype={
            'ann_date': str,
            'f_ann_date': str,
            'end_date': str,
            'report_type': str,
            'ts_code': str
        }
    )

    logger.info(f"原始记录: {len(df)} 条")

    # 过滤有效数据
    df = df[df['report_type'].isin(['1', '2', '3', '4'])]
    df = df[df['ann_date'].notna() & (df['ann_date'] != '')]

    # 如果指定了字段，只保留该字段有值的记录
    if field:
        df = df.dropna(subset=[field])
        logger.info(f"字段 {field} 有效记录: {len(df)} 条")

    # 类型优先级去重（同一 ann_date + end_date 保留优先级最高的）
    TYPE_PRIORITY = {'4': 0, '1': 1, '3': 2, '2': 3}
    df['_p'] = df['report_type'].map(TYPE_PRIORITY)
    df = (df.sort_values(['ts_code', 'ann_date', 'end_date', '_p'])
            .drop_duplicates(subset=['ts_code', 'ann_date', 'end_date'], keep='first')
            .drop(columns='_p'))

    logger.info(f"去重后记录: {len(df)} 条")

    # 核心：按 ts_code + ann_date 分组，取最大 end_date
    logger.info("计算每个公告日的最新报告期...")
    latest_end = df.groupby(['ts_code', 'ann_date']).agg({
        'end_date': 'max',
        'report_type': 'first'  # 保留报告类型信息
    }).reset_index()

    # 重命名列
    latest_end.columns = ['ts_code', 'ann_date', 'latest_end_date', 'report_type']

    # 排序
    latest_end = latest_end.sort_values(['ts_code', 'ann_date'])

    logger.info(f"结果记录: {len(latest_end)} 条")
    logger.info(f"股票数量: {latest_end['ts_code'].nunique()}")
    logger.info(f"平均每家公告次数: {len(latest_end) / latest_end['ts_code'].nunique():.1f}")

    # 保存
    output_file.parent.mkdir(parents=True, exist_ok=True)
    latest_end.to_csv(output_file, index=False)
    logger.info(f"已保存: {output_file}")

    # 统计信息
    logger.info("\n统计信息:")
    logger.info(f"  报告期分布:")
    logger.info(latest_end['latest_end_date'].value_counts().head(10).to_string())

    logger.info(f"\n  报告类型分布:")
    logger.info(latest_end['report_type'].value_counts().to_string())

    return latest_end


def verify_output(output_file: Optional[Path] = None):
    """验证输出文件"""
    output_file = output_file or Config.SUPPLEMENTARY_DATA_DIR / 'ann_date_latest_end.csv'

    if not output_file.exists():
        logger.error(f"文件不存在: {output_file}")
        return

    df = pd.read_csv(output_file, dtype=str)

    logger.info("\n" + "=" * 70)
    logger.info("验证输出文件")
    logger.info("=" * 70)
    logger.info(f"总行数: {len(df)}")
    logger.info(f"股票数: {df['ts_code'].nunique()}")
    logger.info(f"\n前5行:")
    print(df.head().to_string())

    # 验证特定股票
    sample_stock = df['ts_code'].iloc[0]
    logger.info(f"\n示例股票 {sample_stock} 的记录:")
    print(df[df['ts_code'] == sample_stock].head(10).to_string())


def main():
    parser = argparse.ArgumentParser(
        description='构建公告日-最新报告期映射表',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 构建全量映射表
  python build_ann_date_latest_end.py

  # 指定字段（用于过滤有效数据）
  python build_ann_date_latest_end.py --field n_income_attr_p

  # 验证输出
  python build_ann_date_latest_end.py --verify
        """
    )

    parser.add_argument('--field', default=None, help='指定字段名（可选，用于过滤）')
    parser.add_argument('--verify', action='store_true', help='验证输出文件')

    args = parser.parse_args()

    if args.verify:
        verify_output()
    else:
        build_ann_date_latest_end(field=args.field)
        verify_output()


if __name__ == '__main__':
    main()
