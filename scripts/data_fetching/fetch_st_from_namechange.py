#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
从 namechange 接口获取历史 ST 状态数据

使用 namechange 接口获取所有股票的名称变更历史，
从中提取 ST 状态信息，补充 stock_st 接口缺失的历史数据
"""
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from datetime import datetime
from tqdm import tqdm

from config.config import Config
from data_engine.api.tushare_api import TushareAPI
from data_engine.utils import setup_logger
from data_engine.utils.st_utils import is_st_name, extract_st_periods, expand_st_to_daily


def main():
    """主函数"""
    logger = setup_logger()
    logger.info("=" * 80)
    logger.info("从 namechange 接口获取历史 ST 状态数据")
    logger.info("=" * 80)

    try:
        # 1. 初始化 API
        logger.info("初始化 Tushare API...")
        api = TushareAPI()

        # 2. 获取所有股票列表
        logger.info("获取股票列表...")
        df_stocks = api.fetch_stock_basic(list_status='L')
        logger.info(f"获取到 {len(df_stocks)} 只股票")

        # 3. 获取交易日历
        logger.info("获取交易日历...")
        df_cal = api.fetch_trade_cal(start_date='20090101', end_date=datetime.now().strftime('%Y%m%d'))
        trade_dates = df_cal[df_cal['is_open'] == 1]['cal_date'].tolist()
        logger.info(f"交易日: {len(trade_dates)} 天 ({trade_dates[0]} ~ {trade_dates[-1]})")

        # 4. 逐只股票获取名称变更历史
        logger.info("=" * 80)
        logger.info("开始获取名称变更历史...")
        logger.info("=" * 80)

        all_st_periods = []
        all_name_changes = []  # 保存所有名称变更记录
        failed_stocks = []

        for idx, row in tqdm(df_stocks.iterrows(), total=len(df_stocks), desc="获取名称变更"):
            ts_code = row['ts_code']

            try:
                # 获取该股票的名称变更历史
                df_changes = api.fetch_namechange(ts_code=ts_code)

                if not df_changes.empty:
                    # 保存所有名称变更记录
                    all_name_changes.append(df_changes)

                    # 提取ST状态时间段
                    st_periods = extract_st_periods(df_changes)

                    if not st_periods.empty:
                        all_st_periods.append(st_periods)
                        logger.debug(f"{ts_code}: 找到 {len(st_periods)} 条ST记录")

            except Exception as e:
                logger.warning(f"{ts_code}: 获取失败 - {e}")
                failed_stocks.append(ts_code)
                continue

        # 5. 保存所有名称变更记录
        if all_name_changes:
            df_all_name_changes = pd.concat(all_name_changes, ignore_index=True)
            logger.info(f"共获取 {len(df_all_name_changes)} 条名称变更记录")
            logger.info(f"涉及 {df_all_name_changes['ts_code'].nunique()} 只股票")

            # 保存完整的名称变更历史
            name_changes_file = Config.SUPPLEMENTARY_DATA_DIR / 'stock_namechange_history.csv'
            df_all_name_changes.to_csv(name_changes_file, index=False, encoding='utf-8-sig')
            logger.info(f"名称变更历史已保存: {name_changes_file}")
        else:
            logger.warning("未获取到任何名称变更记录")

        # 6. 合并所有ST时间段
        if all_st_periods:
            df_all_st_periods = pd.concat(all_st_periods, ignore_index=True)
            logger.info(f"共找到 {len(df_all_st_periods)} 条ST时间段记录")
            logger.info(f"涉及 {df_all_st_periods['ts_code'].nunique()} 只股票")

            # 保存ST时间段
            periods_file = Config.SUPPLEMENTARY_DATA_DIR / 'st_periods_from_namechange.csv'
            df_all_st_periods.to_csv(periods_file, index=False, encoding='utf-8-sig')
            logger.info(f"ST时间段已保存: {periods_file}")

            # 7. 展开为每日数据
            logger.info("=" * 80)
            logger.info("展开为每日数据...")
            logger.info("=" * 80)

            df_daily = expand_st_to_daily(df_all_st_periods, trade_dates)
            logger.info(f"展开后共 {len(df_daily)} 条每日记录")

            # 8. 保存每日数据
            daily_file = Config.SUPPLEMENTARY_DATA_DIR / 'st_status_from_namechange.csv'
            df_daily.to_csv(daily_file, index=False, encoding='utf-8-sig')
            logger.info(f"每日ST状态已保存: {daily_file}")

            # 9. 统计信息
            logger.info("=" * 80)
            logger.info("统计信息")
            logger.info("=" * 80)
            logger.info(f"日期范围: {df_daily['trade_date'].min()} ~ {df_daily['trade_date'].max()}")
            logger.info(f"涉及股票: {df_daily['ts_code'].nunique()} 只")
            logger.info(f"总记录数: {len(df_daily)} 条")

            # ST类型分布
            st_type_counts = df_daily['st_type'].value_counts()
            logger.info("\nST类型分布:")
            for st_type, count in st_type_counts.items():
                logger.info(f"  {st_type}: {count} 条")

            # 按年份统计
            df_daily['year'] = df_daily['trade_date'].str[:4]
            year_counts = df_daily.groupby('year')['ts_code'].nunique()
            logger.info("\n按年份统计（ST股票数）:")
            for year, count in year_counts.items():
                logger.info(f"  {year}: {count} 只")

        else:
            logger.warning("未找到任何ST记录")

        # 10. 失败统计
        if failed_stocks:
            logger.warning(f"\n获取失败的股票: {len(failed_stocks)} 只")
            logger.warning(f"失败股票: {', '.join(failed_stocks[:10])}" +
                         (f" ... (还有{len(failed_stocks)-10}只)" if len(failed_stocks) > 10 else ""))

        logger.info("=" * 80)
        logger.info("✓ 完成")
        logger.info("=" * 80)

        return 0

    except KeyboardInterrupt:
        logger.warning("\n用户中断操作")
        return 1
    except Exception as e:
        logger.error(f"执行失败: {str(e)}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
