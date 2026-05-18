#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
将所有因子矩阵合并为一个长格式CSV文件（优化版，分批处理）

输出格式:
    trade_date, ts_code, factor_name, factor_value

使用方法:
    python scripts/matrix_building/merge_factors_to_long.py
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from config.config import Config
from utils import setup_logger


def main():
    logger = setup_logger(prefix="merge_factors")
    logger.info("=" * 60)
    logger.info("合并所有因子为长格式（优化版，分批追加）")
    logger.info("=" * 60)

    matrix_dir = Config.MATRIX_DATA_DIR

    # 定义要包含的因子（排除原始价格/成交量数据）
    factor_files = {
        # 动量/反转因子
        'reversal_5d': 'reversal_5d_matrix.csv',
        'momentum_20d': 'momentum_20d_matrix.csv',
        'momentum_60d': 'momentum_60d_matrix.csv',
        'volatility_20d': 'volatility_20d_matrix.csv',

        # 流动性因子
        'amihud_20d': 'amihud_20d_matrix.csv',
        'turnover_20d': 'turnover_20d_matrix.csv',
        'turnover_vol_20d': 'turnover_vol_20d_matrix.csv',
        'vp_corr_20d': 'vp_corr_20d_matrix.csv',

        # 估值因子
        'pb': 'pb_matrix.csv',
        'microcap': 'microcap_matrix.csv',

        # 财务因子 (YoY)
        'net_profit_yoy': 'net_profit_yoy_matrix.csv',
        'n_income_attr_p_yoy': 'n_income_attr_p_yoy_matrix.csv',
        'revenue_yoy': 'revenue_yoy_matrix.csv',
        'total_revenue_yoy': 'total_revenue_yoy_matrix.csv',
        'operate_profit_yoy': 'operate_profit_yoy_matrix.csv',
        'total_profit_yoy': 'total_profit_yoy_matrix.csv',
        'admin_exp_yoy': 'admin_exp_yoy_matrix.csv',
        'sell_exp_yoy': 'sell_exp_yoy_matrix.csv',
        'fin_exp_yoy': 'fin_exp_yoy_matrix.csv',
        'ebitda_yoy': 'ebitda_yoy_matrix.csv',

        # 状态/辅助因子
        'st': 'st_matrix.csv',
        'listing_days': 'listing_days_matrix.csv',
        'tradability': 'tradability_matrix.csv',
        'limit': 'limit_matrix.csv',

        # 市值
        'circ_mv': 'circ_mv_matrix.csv',
        'total_mv': 'total_mv_matrix.csv',

        # 收益率（用于分析）
        'open_return': 'open_return_matrix.csv',
    }

    output_file = Config.DATA_DIR / 'all_factors_long_format.csv'

    # 写入表头
    pd.DataFrame(columns=['trade_date', 'ts_code', 'factor_name', 'factor_value']).to_csv(
        output_file, index=False, encoding='utf-8-sig'
    )
    logger.info(f"创建输出文件: {output_file}")

    total_records = 0
    processed_factors = 0

    for factor_name, filename in factor_files.items():
        matrix_path = matrix_dir / filename
        logger.info(f"[{processed_factors+1}/{len(factor_files)}] 处理因子: {factor_name}")

        if not matrix_path.exists():
            logger.warning(f"  跳过: 文件不存在 {filename}")
            continue

        try:
            # 读取矩阵
            df = pd.read_csv(matrix_path, index_col=0)
            df.index = pd.to_datetime(df.index)

            # 分批处理列（股票），避免内存溢出
            chunk_size = 500  # 每批处理500只股票
            stocks = df.columns.tolist()

            for i in range(0, len(stocks), chunk_size):
                chunk_stocks = stocks[i:i+chunk_size]
                df_chunk = df[chunk_stocks].copy()

                # 转换为长格式
                df_long = df_chunk.reset_index().melt(
                    id_vars=[df.index.name or 'index'],
                    var_name='ts_code',
                    value_name='factor_value'
                )
                df_long.rename(columns={df_long.columns[0]: 'trade_date'}, inplace=True)
                df_long['factor_name'] = factor_name

                # 只保留非空值
                df_long = df_long.dropna(subset=['factor_value'])

                if not df_long.empty:
                    # 转换日期格式
                    df_long['trade_date'] = pd.to_datetime(df_long['trade_date']).dt.strftime('%Y%m%d')

                    # 追加到文件
                    df_long[['trade_date', 'ts_code', 'factor_name', 'factor_value']].to_csv(
                        output_file, mode='a', header=False, index=False, encoding='utf-8-sig'
                    )
                    total_records += len(df_long)

                del df_long

            processed_factors += 1
            logger.info(f"  完成，当前总记录数: {total_records:,}")
            del df

        except Exception as e:
            logger.error(f"  处理失败: {e}")
            continue

    logger.info(f"\n{'=' * 60}")
    logger.info(f"完成!")
    logger.info(f"输出文件: {output_file}")
    logger.info(f"总记录数: {total_records:,}")
    logger.info(f"成功处理因子: {processed_factors}/{len(factor_files)}")
    logger.info(f"{'=' * 60}")


if __name__ == '__main__':
    main()
