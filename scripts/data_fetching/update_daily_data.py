#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日线数据增量更新脚本

功能：
- 检查现有文件的最后日期
- 只获取缺失的最新数据
- 追加到现有文件

使用方法：
    python scripts/data_fetching/update_daily_data.py
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from datetime import datetime, timedelta
from config.config import Config
from data_engine.api.tushare_api import TushareAPI
from data_engine.fetchers.daily_fetcher import DailyDataFetcher
from utils import setup_logger


def get_last_date_from_file(file_path):
    """从CSV文件获取最后日期"""
    try:
        df = pd.read_csv(file_path, dtype={'trade_date': str})
        if df.empty:
            return None
        return df['trade_date'].max()
    except:
        return None


def update_daily_data(logger=None):
    """增量更新日线数据"""
    if logger is None:
        logger = setup_logger(prefix="update_daily")

    # 初始化
    api = TushareAPI()
    fetcher = DailyDataFetcher(api)

    # 获取股票列表
    basic_info = pd.read_csv(Config.BASIC_DATA_DIR / 'all_companies_info.csv')
    all_stocks = basic_info[['ts_code', 'list_date']].copy()

    # 获取今天的日期
    today = datetime.now().strftime('%Y%m%d')

    logger.info("=" * 60)
    logger.info(f"开始增量更新日线数据（目标日期: {today}）")
    logger.info("=" * 60)

    success_count = 0
    skip_count = 0
    fail_count = 0
    no_update_count = 0

    total = len(all_stocks)

    for idx, row in enumerate(all_stocks.itertuples(), start=1):
        ts_code = row.ts_code
        file_path = Path(Config.DAILY_DATA_DIR) / f"{ts_code}.csv"

        if idx % 500 == 0:
            logger.info(f"进度: {idx}/{total} ({idx/total:.1%})")

        # 检查现有文件
        if file_path.exists():
            last_date = get_last_date_from_file(file_path)

            if last_date and last_date >= today:
                skip_count += 1
                continue

            # 需要更新，从最后日期的下一天开始
            start_date = (datetime.strptime(last_date, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')

            try:
                # 获取增量数据
                df_new = fetcher.fetch_daily_all_adj(ts_code, start_date=start_date, end_date=today)

                if df_new is not None and len(df_new) > 0:
                    # 读取旧数据
                    df_old = pd.read_csv(file_path, dtype={'trade_date': str})
                    # 合并并保存
                    df_combined = pd.concat([df_old, df_new], ignore_index=True)
                    df_combined.to_csv(file_path, index=False, encoding='utf-8-sig')
                    success_count += 1
                    logger.info(f"{ts_code}: 更新 {len(df_new)} 条数据 ({start_date} ~ {today})")
                else:
                    no_update_count += 1

            except Exception as e:
                logger.error(f"{ts_code}: 更新失败 - {e}")
                fail_count += 1
        else:
            # 新股票，获取全部数据
            try:
                df = fetcher.fetch_daily_all_adj(ts_code, start_date=row.list_date, end_date=today)
                if df is not None and len(df) > 0:
                    df.to_csv(file_path, index=False, encoding='utf-8-sig')
                    success_count += 1
                    logger.info(f"{ts_code}: 新建文件 ({len(df)} 条数据)")
                else:
                    no_update_count += 1
            except Exception as e:
                logger.error(f"{ts_code}: 新建失败 - {e}")
                fail_count += 1

    logger.info("=" * 60)
    logger.info("增量更新完成")
    logger.info(f"成功更新: {success_count} 只股票")
    logger.info(f"跳过(已最新): {skip_count} 只股票")
    logger.info(f"无需更新: {no_update_count} 只股票")
    logger.info(f"失败: {fail_count} 只股票")
    logger.info("=" * 60)


if __name__ == '__main__':
    logger = setup_logger(prefix="update_daily")
    update_daily_data(logger)
