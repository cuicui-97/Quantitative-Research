#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
获取并合并完整的 ST 状态数据

整合了三个数据源：
1. stock_st API（2016年后的官方数据）
2. namechange 名称变更历史（全时段，从名称中提取ST）
3. 合并去重，得到最完整的 ST 数据

输出：
- st_status_merged.csv: 合并后的完整 ST 数据（只包含 trade_date, ts_code）
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import logging
from datetime import datetime
from tqdm import tqdm

from config.config import Config
from data_engine.api.tushare_api import TushareAPI
from data_engine.utils import setup_logger
from data_engine.utils.st_utils import is_st_name, extract_st_periods, expand_st_to_daily


# ==================== 第一部分：获取名称变更历史 ====================

def fetch_namechange_history(api: TushareAPI, logger) -> pd.DataFrame:
    """
    获取所有股票的名称变更历史

    Returns:
        DataFrame: 名称变更历史
    """
    logger.info("=" * 80)
    logger.info("第一步：获取名称变更历史")
    logger.info("=" * 80)

    # 检查是否已有数据
    output_file = Config.SUPPLEMENTARY_DATA_DIR / 'stock_namechange_history.csv'

    if output_file.exists():
        logger.info(f"发现已有名称变更历史: {output_file}")
        df_existing = pd.read_csv(output_file, dtype={'ts_code': str})
        logger.info(f"已有 {len(df_existing)} 条记录，涉及 {df_existing['ts_code'].nunique()} 只股票")
        return df_existing

    logger.info("未找到名称变更历史，需要重新获取")
    logger.info("请先运行 fetch_namechange_history.py")
    return pd.DataFrame()


# ==================== 第二部分：从名称变更中提取 ST ====================

def extract_st_from_namechange(df_all_changes: pd.DataFrame, api: TushareAPI, logger) -> pd.DataFrame:
    """
    从名称变更历史中提取 ST 状态

    Args:
        df_all_changes: 所有股票的名称变更记录
        api: TushareAPI 实例
        logger: 日志记录器

    Returns:
        DataFrame: 每日ST状态 (trade_date, ts_code)
    """
    logger.info("=" * 80)
    logger.info("第二步：从名称变更中提取 ST 状态")
    logger.info("=" * 80)

    if df_all_changes.empty:
        logger.warning("名称变更历史为空，无法提取ST状态")
        return pd.DataFrame(columns=['trade_date', 'ts_code'])

    logger.info(f"共 {len(df_all_changes)} 条记录，涉及 {df_all_changes['ts_code'].nunique()} 只股票")

    # 获取交易日历
    logger.info("获取交易日历...")
    df_cal = api.fetch_trade_cal(start_date='20090101', end_date=datetime.now().strftime('%Y%m%d'))
    trade_dates = df_cal[df_cal['is_open'] == 1]['cal_date'].tolist()
    logger.info(f"交易日: {len(trade_dates)} 天 ({trade_dates[0]} ~ {trade_dates[-1]})")

    # 提取ST状态时间段
    logger.info("提取ST状态时间段...")
    all_st_periods = []

    for ts_code, group in tqdm(df_all_changes.groupby('ts_code'), desc="提取ST时间段"):
        st_periods = extract_st_periods(group)
        if not st_periods.empty:
            all_st_periods.append(st_periods)

    if not all_st_periods:
        logger.warning("未找到任何ST记录")
        return pd.DataFrame(columns=['trade_date', 'ts_code'])

    df_st_periods = pd.concat(all_st_periods, ignore_index=True)

    # 去重（可能有重复的时间段）
    df_st_periods = df_st_periods.drop_duplicates(
        subset=['ts_code', 'entry_dt', 'remove_dt'],
        keep='first'
    )

    logger.info(f"找到 {len(df_st_periods)} 条ST时间段记录")
    logger.info(f"涉及 {df_st_periods['ts_code'].nunique()} 只股票")

    # 展开为每日数据
    logger.info("展开为每日数据...")
    df_daily = expand_st_to_daily(df_st_periods, trade_dates)

    # 只保留核心列（st_utils 返回的包含 name 和 st_type）
    df_daily = df_daily[['trade_date', 'ts_code']].copy()

    # 去重：同一只股票在同一天可能有多条ST记录（时间段重叠）
    df_daily = df_daily.drop_duplicates(subset=['trade_date', 'ts_code'], keep='first')

    logger.info(f"去重后共 {len(df_daily)} 条每日记录")

    return df_daily


# ==================== 第三部分：获取 stock_st API 数据 ====================

def fetch_st_from_api(logger) -> pd.DataFrame:
    """
    读取已有的 stock_st API 数据

    Returns:
        DataFrame: ST状态数据 (trade_date, ts_code)
    """
    logger.info("=" * 80)
    logger.info("第三步：读取 stock_st API 数据")
    logger.info("=" * 80)

    api_file = Config.SUPPLEMENTARY_DATA_DIR / 'st_status.csv'

    if not api_file.exists():
        logger.warning(f"未找到 stock_st API 数据: {api_file}")
        return pd.DataFrame(columns=['trade_date', 'ts_code'])

    df_api = pd.read_csv(api_file, dtype={'ts_code': str, 'trade_date': str})

    # 只保留核心列
    df_api = df_api[['trade_date', 'ts_code']].copy()

    logger.info(f"读取到 {len(df_api)} 条记录")
    logger.info(f"涉及 {df_api['ts_code'].nunique()} 只股票")
    logger.info(f"日期范围: {df_api['trade_date'].min()} ~ {df_api['trade_date'].max()}")

    return df_api


# ==================== 第四部分：合并数据 ====================

def merge_st_data(df_namechange: pd.DataFrame, df_api: pd.DataFrame, logger) -> pd.DataFrame:
    """
    合并两个数据源的 ST 数据（取并集）

    Args:
        df_namechange: 从 namechange 提取的 ST 数据
        df_api: 从 stock_st API 获取的 ST 数据
        logger: 日志记录器

    Returns:
        DataFrame: 合并后的 ST 数据 (trade_date, ts_code)
    """
    logger.info("=" * 80)
    logger.info("第四步：合并数据（取并集）")
    logger.info("=" * 80)

    # 合并
    df_merged = pd.concat([df_namechange, df_api], ignore_index=True)
    logger.info(f"合并前: {len(df_merged)} 条")

    # 去重
    df_merged = df_merged.drop_duplicates(subset=['trade_date', 'ts_code'])
    logger.info(f"去重后: {len(df_merged)} 条")

    # 排序
    df_merged = df_merged.sort_values(['trade_date', 'ts_code']).reset_index(drop=True)

    # 统计
    logger.info(f"日期范围: {df_merged['trade_date'].min()} ~ {df_merged['trade_date'].max()}")
    logger.info(f"涉及股票: {df_merged['ts_code'].nunique()} 只")

    # 按年份统计
    df_merged['year'] = df_merged['trade_date'].str[:4]
    year_stats = df_merged.groupby('year').agg({
        'ts_code': 'nunique',
        'trade_date': 'count'
    }).rename(columns={'ts_code': 'stocks', 'trade_date': 'records'})

    logger.info("\n按年份统计:")
    for year, row in year_stats.iterrows():
        logger.info(f"  {year}: {row['stocks']} 只股票, {row['records']} 条记录")

    # 删除临时列
    df_merged = df_merged.drop(columns=['year'])

    return df_merged


# ==================== 主函数 ====================

def main():
    """主函数"""
    logger = setup_logger()
    logger.info("=" * 80)
    logger.info("获取并合并完整的 ST 状态数据")
    logger.info("=" * 80)

    try:
        # 初始化 API
        api = TushareAPI()

        # 第一步：获取名称变更历史
        df_namechange_history = fetch_namechange_history(api, logger)

        if df_namechange_history.empty:
            logger.error("名称变更历史为空，请先运行 fetch_namechange_history.py")
            return 1

        # 第二步：从名称变更中提取 ST
        df_st_from_namechange = extract_st_from_namechange(df_namechange_history, api, logger)

        # 第三步：读取 stock_st API 数据
        df_st_from_api = fetch_st_from_api(logger)

        # 第四步：合并数据
        df_merged = merge_st_data(df_st_from_namechange, df_st_from_api, logger)

        # 保存
        output_file = Config.SUPPLEMENTARY_DATA_DIR / 'st_status_merged.csv'
        df_merged.to_csv(output_file, index=False, encoding='utf-8-sig')

        logger.info("=" * 80)
        logger.info("✓ 完成")
        logger.info("=" * 80)
        logger.info(f"输出文件: {output_file}")
        logger.info(f"总记录数: {len(df_merged)} 条")
        logger.info(f"涉及股票: {df_merged['ts_code'].nunique()} 只")
        logger.info("=" * 80)

        return 0

    except Exception as e:
        logger.error(f"执行失败: {str(e)}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
