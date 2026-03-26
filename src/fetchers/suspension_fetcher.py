"""
停牌信息抓取器
负责获取股票停牌信息
"""
import logging
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import Dict, Set

from config.config import Config
from src.api.tushare_api import TushareAPI
from src.trade_calendar import TradeCalendar
from src.utils import retry_on_error

logger = logging.getLogger(__name__)


class SuspensionFetcher:
    """停牌信息抓取器"""

    def __init__(self, api: TushareAPI):
        self.api = api
        self.output_dir = Config.SUPPLEMENTARY_DATA_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.trade_calendar = TradeCalendar(api)

    def fetch_suspension_status(self, start_date: str, end_date: str, force_refresh: bool = False) -> pd.DataFrame:
        """
        使用 suspend_d API 获取停牌状态历史数据

        通过遍历交易日获取每日处于停牌状态的股票（类似 stock_st API）
        智能识别缺失的日期范围并补充数据

        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            force_refresh: 是否强制刷新（忽略已有文件，重新获取所有数据）

        Returns:
            DataFrame: 包含 ts_code, trade_date, suspend_timing, suspend_type 等字段
        """
        output_file = self.output_dir / 'suspension_status.csv'

        # 如果文件存在且不强制刷新，智能增量更新
        if output_file.exists() and not force_refresh:
            logger.info(f"停牌数据已存在，从文件读取: {output_file}")
            existing_df = pd.read_csv(output_file, dtype={'trade_date': str})

            if len(existing_df) > 0:
                existing_min_date = existing_df['trade_date'].min()
                existing_max_date = existing_df['trade_date'].max()
                logger.info(f"现有数据日期范围: {existing_min_date} ~ {existing_max_date}")

                # 检查是否需要补充数据
                missing_ranges = []

                # 1. 检查是否需要补充历史数据（start_date < existing_min_date）
                if start_date < existing_min_date:
                    missing_ranges.append((start_date, existing_min_date))
                    logger.info(f"需要补充历史数据: {start_date} ~ {existing_min_date}")

                # 2. 检查是否需要补充最新数据（end_date > existing_max_date）
                if end_date > existing_max_date:
                    missing_ranges.append((existing_max_date, end_date))
                    logger.info(f"需要补充最新数据: {existing_max_date} ~ {end_date}")

                # 如果有缺失范围，进行增量更新
                if missing_ranges:
                    logger.info(f"开始补充缺失数据（共 {len(missing_ranges)} 个范围）")
                    new_dfs = [existing_df]

                    for i, (range_start, range_end) in enumerate(missing_ranges, 1):
                        logger.info(f"[{i}/{len(missing_ranges)}] 获取 {range_start} ~ {range_end} 的数据")
                        new_df = self._fetch_suspension_by_date_range(range_start, range_end)
                        if len(new_df) > 0:
                            new_dfs.append(new_df)

                    # 合并所有数据并去重
                    combined_df = pd.concat(new_dfs, ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last')
                    combined_df = combined_df.sort_values('trade_date').reset_index(drop=True)

                    # 保存更新后的数据
                    combined_df.to_csv(output_file, index=False)
                    logger.info(f"增量更新完成，总记录数: {len(combined_df)}")
                    logger.info(f"更新后日期范围: {combined_df['trade_date'].min()} ~ {combined_df['trade_date'].max()}")
                    return combined_df
                else:
                    logger.info(f"现有数据已覆盖请求范围 ({start_date} ~ {end_date})，无需更新")
                    return existing_df

            return existing_df

        # 全量获取
        logger.info(f"开始获取停牌数据 ({start_date} ~ {end_date})")
        df = self._fetch_suspension_by_date_range(start_date, end_date)

        # 保存到文件
        df.to_csv(output_file, index=False)
        logger.info(f"停牌数据已保存: {output_file}, 共 {len(df)} 条记录")

        return df

    def _fetch_suspension_by_date_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        遍历日期范围获取停牌状态

        支持断点续传：如果中途失败，已获取的数据会被保存

        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            DataFrame: 所有日期的停牌状态记录
        """
        # 使用 trade_cal API 获取交易日历
        trade_dates = self.trade_calendar.get_trade_dates(start_date, end_date)

        # 从最新日期往前取（倒序）
        trade_dates_reversed = sorted(trade_dates, reverse=True)

        logger.info(f"需要查询 {len(trade_dates_reversed)} 个交易日（从最新日期开始）")

        # 检查是否有临时文件（断点续传）
        temp_file = self.output_dir / 'suspension_status_temp.csv'
        processed_dates = set()

        if temp_file.exists():
            logger.info(f"发现临时文件，加载已有数据: {temp_file}")
            df_existing = pd.read_csv(temp_file, dtype={'trade_date': str})
            if len(df_existing) > 0:
                processed_dates = set(df_existing['trade_date'].unique())
                logger.info(f"已处理 {len(processed_dates)} 个交易日的数据")
            df_list = [df_existing]
        else:
            df_list = []

        # 遍历交易日
        count = 0
        total = len(trade_dates_reversed)

        try:
            for date_str in trade_dates_reversed:
                # 跳过已处理的日期
                if date_str in processed_dates:
                    logger.debug(f"跳过已处理日期: {date_str}")
                    continue

                count += 1
                logger.info(f"[{count}/{total}] 获取 {date_str} 的停牌数据")

                # 使用 retry_on_error 包装 API 调用
                df_date = self._fetch_single_date_with_retry(date_str)

                if df_date is not None and len(df_date) > 0:
                    df_list.append(df_date)
                    logger.info(f"  获取到 {len(df_date)} 只停牌股票")
                else:
                    logger.debug(f"  {date_str} 无停牌股票")

                # 每处理 50 个交易日，保存一次临时文件
                if count % 50 == 0:
                    df_temp = pd.concat(df_list, ignore_index=True)
                    df_temp = df_temp.drop_duplicates(subset=['ts_code', 'trade_date'])
                    df_temp.to_csv(temp_file, index=False)
                    logger.info(f"  临时保存: 已处理 {len(processed_dates) + count} 个交易日")

                # API 限流
                time.sleep(0.3)

        except KeyboardInterrupt:
            logger.warning("用户中断操作，保存已获取的数据...")
            if df_list:
                df_partial = pd.concat(df_list, ignore_index=True)
                df_partial = df_partial.drop_duplicates(subset=['ts_code', 'trade_date'])
                df_partial.to_csv(temp_file, index=False)
                logger.info(f"部分数据已保存到临时文件: {temp_file}")
                logger.info(f"已获取 {len(df_partial)} 条停牌记录")
            raise

        except Exception as e:
            logger.error(f"获取数据时发生错误: {e}")
            # 保存已获取的数据
            if df_list:
                df_partial = pd.concat(df_list, ignore_index=True)
                df_partial = df_partial.drop_duplicates(subset=['ts_code', 'trade_date'])
                df_partial.to_csv(temp_file, index=False)
                logger.info(f"部分数据已保存到临时文件: {temp_file}")
                logger.info(f"已获取 {len(df_partial)} 条停牌记录")
                logger.info("下次运行时将从断点继续")
                # 返回已获取的数据，而不是抛出异常
                return df_partial
            raise

        if not df_list:
            logger.warning("未获取到停牌数据")
            return pd.DataFrame()

        # 合并所有记录并去重
        df = pd.concat(df_list, ignore_index=True)
        df = df.drop_duplicates(subset=['ts_code', 'trade_date'])

        # 删除临时文件
        if temp_file.exists():
            temp_file.unlink()
            logger.info("临时文件已删除")

        return df

    @retry_on_error(max_retries=3, delay=1.0)
    def _fetch_single_date_with_retry(self, date_str: str) -> pd.DataFrame:
        """
        获取单个交易日的停牌数据（带重试机制）

        Args:
            date_str: 交易日期 YYYYMMDD

        Returns:
            DataFrame: 停牌记录
        """
        return self.api.fetch_suspend_d(trade_date=date_str, suspend_type='S')

