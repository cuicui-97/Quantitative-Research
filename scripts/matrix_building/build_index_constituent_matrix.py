#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
通用指数成分股矩阵构建脚本

支持任意指数（中证1000、沪深300、上证50等）
数据来源：Tushare pro.index_weight

使用方法：
    python build_index_constituent_matrix.py --index 000852.SH --name 中证1000
    python build_index_constituent_matrix.py --index 000300.SH --name 沪深300
    python build_index_constituent_matrix.py --index 000016.SH --name 上证50
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from config.config import Config
from data_engine.api.tushare_api import TushareAPI
from data_engine.processors.matrix_io import save_matrix
from utils import setup_logger


# 预定义指数配置
PREDEFINED_INDICES = {
    'zz1000': {'code': '000852.SH', 'name': '中证1000'},
    'hs300': {'code': '000300.SH', 'name': '沪深300'},
    'sz50': {'code': '000016.SH', 'name': '上证50'},
    'zz500': {'code': '000905.SH', 'name': '中证500'},
}


def fetch_index_constituents(api: TushareAPI, index_code: str, start_date: str, end_date: str, logger=None):
    """
    获取指数成分股历史数据

    Args:
        api: TushareAPI实例
        index_code: 指数代码（如 000852.SH）
        start_date: 开始日期（YYYYMMDD）
        end_date: 结束日期（YYYYMMDD）
        logger: 日志对象

    Returns:
        DataFrame: [trade_date, con_code, weight]
    """
    if logger:
        logger.info(f"  获取 {index_code} 成分股 ({start_date} ~ {end_date})...")

    try:
        df = api.pro.index_weight(index_code=index_code, start_date=start_date, end_date=end_date)

        if df is None or len(df) == 0:
            if logger:
                logger.warning(f"    未获取到数据")
            return None

        # 转换日期格式
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')

        if logger:
            logger.info(f"    获取到 {len(df)} 条记录")
            logger.info(f"    日期范围: {df['trade_date'].min().strftime('%Y-%m-%d')} ~ {df['trade_date'].max().strftime('%Y-%m-%d')}")

        return df[['trade_date', 'con_code', 'weight']]

    except Exception as e:
        if logger:
            logger.error(f"    获取失败: {e}")
        return None


def build_index_matrix(index_code: str, index_name: str, logger=None):
    """
    构建指数成分股时变矩阵

    Args:
        index_code: 指数代码
        index_name: 指数名称（用于日志和文件名）
        logger: 日志对象

    Returns:
        DataFrame: 指数成分股矩阵（日期 × 股票）
    """
    if logger:
        logger.info("="*60)
        logger.info(f"构建 {index_name} ({index_code}) 成分股矩阵")
        logger.info("="*60)

    # 初始化API
    api = TushareAPI()

    # 加载交易日历
    trade_calendar = pd.read_csv(Config.SUPPLEMENTARY_DATA_DIR / 'trade_calendar.csv')
    trade_calendar = trade_calendar[trade_calendar['is_open'] == 1]
    all_dates = pd.DatetimeIndex(pd.to_datetime(trade_calendar['cal_date'].astype(str), format='%Y%m%d'))

    # 确定数据获取范围（从2015年开始）
    start_date = '20150101'
    end_date = datetime.now().strftime('%Y%m%d')

    # 分批获取（每年一个批次，避免API限制）
    if logger:
        logger.info("\n[1/3] 获取成分股历史数据...")

    all_constituents = []
    current_year = 2015

    while current_year <= datetime.now().year:
        year_start = f"{current_year}0101"
        year_end = f"{current_year}1231"

        df = fetch_index_constituents(api, index_code, year_start, year_end, logger=logger)

        if df is not None and len(df) > 0:
            all_constituents.append(df)

        current_year += 1

    if len(all_constituents) == 0:
        if logger:
            logger.error("未获取到任何成分股数据")
        return None

    # 合并所有数据
    constituents_df = pd.concat(all_constituents, ignore_index=True)
    constituents_df = constituents_df.sort_values('trade_date')

    if logger:
        logger.info(f"\n  总共获取: {len(constituents_df)} 条记录")
        logger.info(f"  时间跨度: {constituents_df['trade_date'].min().strftime('%Y-%m-%d')} ~ {constituents_df['trade_date'].max().strftime('%Y-%m-%d')}")

    # 获取所有股票列表
    all_stocks = sorted(constituents_df['con_code'].unique())

    if logger:
        logger.info(f"  涉及股票数: {len(all_stocks)}")

    # 构建矩阵
    if logger:
        logger.info("\n[2/3] 构建时变成分股矩阵...")

    # 初始化矩阵（0/1）
    matrix = pd.DataFrame(
        0,
        index=all_dates.strftime('%Y%m%d'),
        columns=all_stocks,
        dtype=np.int8
    )

    # 对于每个成分股记录日，将该股票标记为1（从该日到下一个记录日）
    record_dates = sorted(constituents_df['trade_date'].unique())

    for i, record_date in enumerate(record_dates):
        # 获取该日期的成分股
        stocks_on_date = constituents_df[constituents_df['trade_date'] == record_date]['con_code'].tolist()

        # 确定这个成分股列表的有效期（从record_date到下一个record_date的前一天）
        start_str = record_date.strftime('%Y%m%d')

        if i + 1 < len(record_dates):
            end_date = record_dates[i + 1] - timedelta(days=1)
            end_str = end_date.strftime('%Y%m%d')
        else:
            end_str = all_dates[-1].strftime('%Y%m%d')

        # 在有效期内标记为1
        valid_dates = matrix.index[(matrix.index >= start_str) & (matrix.index <= end_str)]

        for stock in stocks_on_date:
            if stock in matrix.columns:
                matrix.loc[valid_dates, stock] = 1

    if logger:
        coverage = matrix.sum().sum() / matrix.size
        logger.info(f"  矩阵非零比例: {coverage:.2%}")
        logger.info(f"  平均每日成分股数: {matrix.sum(axis=1).mean():.0f}")

    # 显示样本
    if logger:
        logger.info("\n[3/3] 样本数据:")
        sample_dates = [matrix.index[0], matrix.index[len(matrix)//2], matrix.index[-1]]
        for d in sample_dates:
            count = matrix.loc[d].sum()
            logger.info(f"  {d}: {count} 只成分股")

    return matrix


def main():
    parser = argparse.ArgumentParser(description='构建指数成分股时变矩阵')
    parser.add_argument('--index', type=str, required=True,
                       help='指数代码（如 000852.SH）或预定义名称（zz1000, hs300, sz500等）')
    parser.add_argument('--name', type=str, default=None,
                       help='指数名称（用于文件名，可选）')
    parser.add_argument('--output', type=str, default=None,
                       help='输出文件名（可选，默认 {index_name}_matrix.csv）')

    args = parser.parse_args()

    # 解析指数代码和名称
    if args.index in PREDEFINED_INDICES:
        index_code = PREDEFINED_INDICES[args.index]['code']
        index_name = args.name or PREDEFINED_INDICES[args.index]['name']
    else:
        index_code = args.index
        index_name = args.name or index_code.replace('.', '_')

    # 设置日志
    logger = setup_logger(prefix=f"{index_name.lower()}_matrix")

    # 构建矩阵
    matrix = build_index_matrix(index_code, index_name, logger=logger)

    if matrix is None:
        logger.error("构建失败")
        return

    # 保存矩阵
    output_name = args.output or f"{index_name.lower().replace(' ', '_')}_matrix.csv"
    output_file = Config.MATRIX_DATA_DIR / output_name
    save_matrix(matrix, output_file)

    logger.info(f"\n{'='*60}")
    logger.info("构建完成!")
    logger.info(f"{'='*60}")
    logger.info(f"矩阵已保存: {output_file}")
    logger.info(f"矩阵维度: {matrix.shape}")

    # 保存最新成分股列表
    latest_date = matrix.index[-1]
    latest_stocks = matrix.columns[matrix.loc[latest_date] == 1].tolist()
    list_file = Config.SUPPLEMENTARY_DATA_DIR / f'{index_name.lower().replace(" ", "_")}_constituents_latest.csv'
    pd.DataFrame({'ts_code': latest_stocks, 'date': latest_date}).to_csv(list_file, index=False)
    logger.info(f"最新成分股列表: {list_file}")


if __name__ == '__main__':
    main()
