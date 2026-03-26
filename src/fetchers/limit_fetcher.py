#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
涨跌停价格数据获取器

获取全市场股票的涨跌停价格数据（使用 stk_limit 接口）
"""
import logging
import pandas as pd
from pathlib import Path
from typing import List, Optional
from tqdm import tqdm
from config.config import Config
from src.api.tushare_api import TushareAPI

logger = logging.getLogger(__name__)


class LimitFetcher:
    """涨跌停价格数据获取器"""

    def __init__(self, api: TushareAPI):
        """
        初始化获取器

        Args:
            api: TushareAPI 实例
        """
        self.api = api
        self.output_file = Config.SUPPLEMENTARY_DATA_DIR / 'limit_prices.csv'

    def fetch_limit_prices_by_date(self, trade_date: str) -> pd.DataFrame:
        """
        获取单个交易日的涨跌停价格数据

        Args:
            trade_date: 交易日期 YYYYMMDD

        Returns:
            DataFrame: 涨跌停价格数据
        """
        return self.api.fetch_stk_limit(trade_date=trade_date)

    def fetch_limit_prices_range(
        self,
        trade_dates: List[str],
        start_date: str = None,
        end_date: str = None,
        save_interval: int = 50
    ) -> pd.DataFrame:
        """
        批量获取涨跌停价格数据（按交易日循环）

        Args:
            trade_dates: 交易日期列表
            start_date: 开始日期 YYYYMMDD（可选，用于断点续传）
            end_date: 结束日期 YYYYMMDD（可选，用于断点续传）
            save_interval: 保存间隔（每 N 个交易日保存一次）

        Returns:
            DataFrame: 合并后的涨跌停价格数据
        """
        # 过滤日期范围
        if start_date:
            trade_dates = [d for d in trade_dates if d >= start_date]
        if end_date:
            trade_dates = [d for d in trade_dates if d <= end_date]

        if not trade_dates:
            logger.warning("没有需要获取的交易日期")
            return pd.DataFrame()

        logger.info(f"开始获取涨跌停价格数据: {len(trade_dates)} 个交易日")
        logger.info(f"日期范围: {trade_dates[0]} ~ {trade_dates[-1]}")

        all_data = []
        for i, trade_date in enumerate(tqdm(trade_dates, desc="获取涨跌停价格")):
            try:
                df = self.fetch_limit_prices_by_date(trade_date)
                if not df.empty:
                    all_data.append(df)

                # 定期保存（断点续传）
                if (i + 1) % save_interval == 0:
                    temp_df = pd.concat(all_data, ignore_index=True)
                    self._save_data(temp_df, is_temp=True)
                    logger.info(f"进度保存: 已获取 {i + 1}/{len(trade_dates)} 个交易日")

            except Exception as e:
                logger.error(f"获取 {trade_date} 涨跌停价格失败: {e}")
                continue

        if not all_data:
            logger.warning("未获取到任何涨跌停价格数据")
            return pd.DataFrame()

        # 合并所有数据
        result_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"获取完成: 共 {len(result_df)} 条记录")

        return result_df

    def fetch_and_save(
        self,
        trade_dates: List[str],
        start_date: str = None,
        end_date: str = None,
        save_interval: int = 50
    ) -> Path:
        """
        获取涨跌停价格数据并保存

        Args:
            trade_dates: 交易日期列表
            start_date: 开始日期 YYYYMMDD（可选）
            end_date: 结束日期 YYYYMMDD（可选）
            save_interval: 保存间隔

        Returns:
            Path: 保存的文件路径
        """
        # 检查是否已有数据（支持增量更新）
        existing_df = self._load_existing_data()
        missing_dates = self._identify_missing_dates(
            trade_dates, existing_df, start_date, end_date
        )

        if not missing_dates:
            logger.info("所有数据已存在，无需重新获取")
            return self.output_file

        # 获取缺失的数据
        new_df = self.fetch_limit_prices_range(
            trade_dates=missing_dates,
            save_interval=save_interval
        )

        if new_df.empty:
            logger.warning("未获取到新数据")
            return self.output_file

        # 合并新旧数据
        if not existing_df.empty:
            logger.info("合并新旧数据...")
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            # 去重（按 ts_code 和 trade_date）
            combined_df = combined_df.drop_duplicates(
                subset=['ts_code', 'trade_date'],
                keep='last'
            )
            combined_df = combined_df.sort_values(['trade_date', 'ts_code']).reset_index(drop=True)
            logger.info(f"合并后共 {len(combined_df)} 条记录")
        else:
            combined_df = new_df

        # 保存数据
        return self._save_data(combined_df, is_temp=False)

    def _load_existing_data(self) -> pd.DataFrame:
        """加载已有的涨跌停价格数据"""
        if not self.output_file.exists():
            logger.info("未找到已有数据文件")
            return pd.DataFrame()

        logger.info(f"加载已有数据: {self.output_file}")
        df = pd.read_csv(self.output_file, dtype={'trade_date': str})
        logger.info(f"已有数据: {len(df)} 条记录，日期范围: {df['trade_date'].min()} ~ {df['trade_date'].max()}")
        return df

    def _identify_missing_dates(
        self,
        all_dates: List[str],
        existing_df: pd.DataFrame,
        start_date: str = None,
        end_date: str = None
    ) -> List[str]:
        """
        识别缺失的日期范围

        Args:
            all_dates: 所有交易日期
            existing_df: 已有数据
            start_date: 用户指定的开始日期
            end_date: 用户指定的结束日期

        Returns:
            List[str]: 需要获取的日期列表
        """
        if existing_df.empty:
            # 没有已有数据，返回用户指定的日期范围
            dates = all_dates
            if start_date:
                dates = [d for d in dates if d >= start_date]
            if end_date:
                dates = [d for d in dates if d <= end_date]
            logger.info(f"无已有数据，需要获取全部日期: {len(dates)} 个")
            return dates

        # 已有数据的日期范围
        existing_min = existing_df['trade_date'].min()
        existing_max = existing_df['trade_date'].max()

        # 用户指定的日期范围
        user_start = start_date or all_dates[0]
        user_end = end_date or all_dates[-1]

        missing_dates = []

        # 1. 补充历史数据（user_start < existing_min）
        if user_start < existing_min:
            history_dates = [d for d in all_dates if user_start <= d < existing_min]
            missing_dates.extend(history_dates)
            logger.info(f"需要补充历史数据: {len(history_dates)} 个交易日 ({user_start} ~ {existing_min})")

        # 2. 补充最新数据（user_end > existing_max）
        if user_end > existing_max:
            latest_dates = [d for d in all_dates if existing_max < d <= user_end]
            missing_dates.extend(latest_dates)
            logger.info(f"需要补充最新数据: {len(latest_dates)} 个交易日 ({existing_max} ~ {user_end})")

        return missing_dates

    def _save_data(self, df: pd.DataFrame, is_temp: bool = False) -> Path:
        """
        保存数据到文件

        Args:
            df: 数据
            is_temp: 是否为临时保存

        Returns:
            Path: 保存的文件路径
        """
        output_file = self.output_file
        if is_temp:
            output_file = self.output_file.with_suffix('.tmp.csv')

        # 确保目录存在
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # 保存数据
        df.to_csv(output_file, index=False)

        if not is_temp:
            logger.info(f"涨跌停价格数据已保存: {output_file}")
            logger.info(f"数据统计: {len(df)} 条记录")
            logger.info(f"日期范围: {df['trade_date'].min()} ~ {df['trade_date'].max()}")
            logger.info(f"股票数量: {df['ts_code'].nunique()} 只")

        return output_file
