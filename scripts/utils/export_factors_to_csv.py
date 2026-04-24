#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
导出所有因子为长格式CSV

格式: ticker, date, factor1, factor2, ...

使用方法:
    python scripts/utils/export_factors_to_csv.py
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from config.config import Config
from data_engine.processors.matrix_io import load_matrix
from utils import setup_logger


def main():
    logger = setup_logger(prefix="export_factors")
    logger.info("="*60)
    logger.info("导出所有因子为长格式CSV")
    logger.info("="*60)

    # 定义要导出的因子列表
    factors = {
        # 概念因子
        'concept_price_boom': '概念价格景气',
        'concept_sentiment': '概念情绪景气',
        'concept_flow_boom': '概念资金景气',

        # 财务因子 - YoY同比增速
        'net_profit_yoy': '归母净利润YoY',
        'n_income_attr_p_yoy': '净利润YoY',
        'n_income_yoy': '总利润YoY',
        'revenue_yoy': '营业收入YoY',
        'total_revenue_yoy': '营业总收入YoY',
        'operate_profit_yoy': '营业利润YoY',
        'total_profit_yoy': '利润总额YoY',
        'ebitda_yoy': 'EBITDAYoY',
        'admin_exp_yoy': '管理费用YoY',
        'sell_exp_yoy': '销售费用YoY',
        'fin_exp_yoy': '财务费用YoY',

        # 财务因子 - 单季值
        'n_income_attr_p_sq': '净利润单季',
        'ebitda_sq': 'EBITDA单季',

        # 估值因子
        'pb': '市净率PB',
        'circ_mv': '流通市值',
        'total_mv': '总市值',
        'microcap': '微市值',

        # 量价技术指标
        'momentum_20d': '20日动量',
        'momentum_60d': '60日动量',
        'reversal_5d': '5日反转',
        'turnover_20d': '20日换手率',
        'turnover_vol_20d': '20日换手波动',
        'amihud_20d': 'Amihud非流动性',
        'vp_corr_20d': '量价相关系数',

        # 状态因子
        'listing_days': '上市天数',
        'st': 'ST状态',
        'tradability': '可交易性',
    }

    # 加载所有因子矩阵
    logger.info("\n[1/3] 加载因子矩阵...")

    factor_matrices = {}
    all_dates = None
    all_stocks = None

    for factor_name in factors.keys():
        matrix_file = Config.MATRIX_DATA_DIR / f'{factor_name}_matrix.csv'
        if matrix_file.exists():
            try:
                matrix = load_matrix(matrix_file)
                factor_matrices[factor_name] = matrix
                logger.info(f"  已加载: {factor_name} ({matrix.shape})")

                # 更新共同的日期和股票
                if all_dates is None:
                    all_dates = matrix.index
                    all_stocks = matrix.columns
                else:
                    all_dates = all_dates.intersection(matrix.index)
                    all_stocks = all_stocks.intersection(matrix.columns)
            except Exception as e:
                logger.warning(f"  加载失败: {factor_name} - {e}")
        else:
            logger.warning(f"  文件不存在: {matrix_file}")

    logger.info(f"\n对齐后: {len(all_dates)} 个交易日, {len(all_stocks)} 只股票")

    # 筛选2015年后的数据
    logger.info("\n[2/3] 筛选2015年后的数据...")
    all_dates_dt = pd.to_datetime(all_dates, format='%Y%m%d')
    date_mask = all_dates_dt >= '2015-01-01'
    filtered_dates = all_dates[date_mask]
    logger.info(f"  筛选后: {len(filtered_dates)} 个交易日 (2015年至今)")

    # 对齐所有因子矩阵
    logger.info("\n[3/3] 合并因子数据...")

    # 为每个因子创建长格式数据
    long_data_list = []

    for factor_name, factor_cn in factors.items():
        if factor_name not in factor_matrices:
            continue

        matrix = factor_matrices[factor_name]

        # 对齐到共同的日期和股票
        matrix_aligned = matrix.reindex(index=filtered_dates, columns=all_stocks)

        # 转换为长格式
        # 使用stack，保留非NA值
        long_series = matrix_aligned.stack(dropna=False)
        long_series.index.names = ['date', 'ticker']
        long_series.name = factor_name

        long_data_list.append(long_series)

        logger.info(f"  已处理: {factor_name} ({factor_cn})")

    # 合并所有因子
    logger.info("\n合并所有因子列...")
    combined_df = pd.concat(long_data_list, axis=1)

    # 重置索引，得到ticker, date作为列
    combined_df = combined_df.reset_index()

    # 调整ticker格式：去掉.SZ/.SH等后缀，并保持为字符串
    combined_df['ticker'] = combined_df['ticker'].astype(str).str.replace(r'\.\w+$', '', regex=True)

    # 重命名列为中文（可选）
    column_mapping = {'ticker': 'ticker', 'date': 'date'}
    column_mapping.update(factors)
    combined_df.columns = [column_mapping.get(c, c) for c in combined_df.columns]

    # 保存为CSV到wondertrade文件夹
    wondertrade_dir = Path('/Users/cuicui/Documents/wondertrade')
    wondertrade_dir.mkdir(parents=True, exist_ok=True)
    output_file = wondertrade_dir / 'all_factors_long_format.csv'
    logger.info(f"\n保存到: {output_file}")

    combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')

    # 统计信息
    logger.info(f"\n导出完成!")
    logger.info(f"  总行数: {len(combined_df):,}")
    logger.info(f"  总列数: {len(combined_df.columns)}")
    logger.info(f"  股票数: {combined_df['ticker'].nunique()}")
    logger.info(f"  日期数: {combined_df['date'].nunique()}")
    logger.info(f"  文件大小: {output_file.stat().st_size / 1024 / 1024:.1f} MB")
    logger.info(f"  ticker格式: 已去掉后缀（如000001.SZ→000001）")

    # 显示每列的非NA比例
    logger.info(f"\n各因子覆盖率:")
    for col in combined_df.columns:
        if col not in ['ticker', 'date']:
            coverage = combined_df[col].notna().mean()
            logger.info(f"  {col}: {coverage:.1%}")

    logger.info("\n" + "="*60)
    logger.info("CSV文件导出完成!")
    logger.info("="*60)
    logger.info(f"\n文件路径: {output_file}")
    logger.info(f"\n格式示例:")
    logger.info("  ticker, date, concept_price_boom, concept_sentiment, net_profit_yoy, pb, ...")
    logger.info("  000001.SZ, 20240102, 0.0012, 0.2341, 0.1567, 1.2345, ...")


if __name__ == '__main__':
    main()
