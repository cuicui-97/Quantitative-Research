"""
利润表数据抓取器

PIT（Point-in-Time）设计：保留每次公告的历史快照，
同一报告期可能存在多行（不同 ann_date 的修订版本），
确保回测时不使用未来数据。
"""
import time
import pandas as pd
from datetime import datetime

from data_engine.api.tushare_api import TushareAPI
from data_engine.fetchers.base_fetcher import BaseFetcher
from utils import retry_on_error
from config.config import Config


class IncomeFetcher(BaseFetcher):
    """利润表数据抓取器（PIT 设计）"""

    # 利润表文件路径
    OUTPUT_FILE = 'income.csv'
    # 断点续传临时文件
    CHECKPOINT_FILE = 'income_checkpoint.txt'

    def __init__(self, api: TushareAPI):
        super().__init__(api, use_output_dir=True)
        self.output_file = self.output_dir / self.OUTPUT_FILE
        self.checkpoint_file = self.output_dir / self.CHECKPOINT_FILE

    def _generate_periods(self, start_year: int, end_year: int = None) -> list:
        """
        生成所有季度末日期列表

        Args:
            start_year: 起始年份
            end_year: 结束年份（默认当前年）

        Returns:
            list: 季度末日期列表，如 ['20050331', '20050630', ...]
        """
        if end_year is None:
            end_year = datetime.now().year

        periods = []
        for year in range(start_year, end_year + 1):
            for quarter_end in ['0331', '0630', '0930', '1231']:
                period = f'{year}{quarter_end}'
                # 不包含未来的报告期
                if period <= datetime.now().strftime('%Y%m%d'):
                    periods.append(period)
        return periods

    def _load_existing(self) -> pd.DataFrame:
        """加载已有数据"""
        if self.output_file.exists():
            df = pd.read_csv(
                self.output_file,
                dtype={'ann_date': str, 'f_ann_date': str, 'end_date': str}
            )
            self.logger.info(f"已有数据: {len(df)} 条记录")
            return df
        return pd.DataFrame()

    def _load_checkpoint(self) -> set:
        """加载已完成的 period 列表"""
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, 'r') as f:
                done = set(line.strip() for line in f if line.strip())
            self.logger.info(f"从断点恢复，已完成 {len(done)} 个报告期")
            return done
        return set()

    def _save_checkpoint(self, period: str):
        """记录已完成的 period"""
        with open(self.checkpoint_file, 'a') as f:
            f.write(f'{period}\n')

    def _merge_and_save(self, existing_df: pd.DataFrame, new_df: pd.DataFrame):
        """
        合并新旧数据并保存

        PIT 关键：按 (ts_code, ann_date, end_date) 去重，保留所有历史版本
        """
        if len(new_df) == 0:
            return existing_df

        combined = pd.concat([existing_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(
            subset=['ts_code', 'ann_date', 'end_date', 'report_type'],
            keep='last'
        )
        combined = combined.sort_values(['ts_code', 'end_date', 'ann_date']).reset_index(drop=True)
        combined.to_csv(self.output_file, index=False)
        return combined

    @retry_on_error(max_retries=3, delay=2.0)
    def _fetch_single_period_type(self, period: str, report_type: str) -> pd.DataFrame:
        """获取单个报告期、单个report_type的数据（带重试）"""
        return self.api.fetch_income(period=period, report_type=report_type)

    def _fetch_single_period(self, period: str) -> pd.DataFrame:
        """获取单个报告期的所有report_type数据（1-5）"""
        dfs = []
        for rt in ['1', '2', '3', '4', '5']:
            df = self._fetch_single_period_type(period, rt)
            if len(df) > 0:
                dfs.append(df)
            time.sleep(0.3)
        if not dfs:
            return pd.DataFrame()
        result = pd.concat(dfs, ignore_index=True)
        self.logger.info(f"  {period}: 共 {len(result)} 条, report_type分布={result['report_type'].value_counts().to_dict()}")
        return result

    def fetch_all(self, start_year: int = 2005) -> pd.DataFrame:
        """
        全量获取利润表数据

        按季度循环拉取，支持断点续传。

        Args:
            start_year: 起始年份（默认2005）

        Returns:
            DataFrame: 完整利润表数据
        """
        self.logger.info(f"开始获取利润表数据（从 {start_year} 年至今）")

        periods = self._generate_periods(start_year)
        done_periods = self._load_checkpoint()
        existing_df = self._load_existing()

        remaining = [p for p in periods if p not in done_periods]
        self.logger.info(f"共 {len(periods)} 个报告期，已完成 {len(done_periods)} 个，剩余 {len(remaining)} 个")

        new_records = []

        try:
            for i, period in enumerate(remaining, 1):
                self.logger.info(f"[{i}/{len(remaining)}] 获取 {period} 的数据")

                df = self._fetch_single_period(period)

                if len(df) > 0:
                    new_records.append(df)
                    self.logger.info(f"  获取到 {len(df)} 条记录")
                else:
                    self.logger.warning(f"  {period} 无数据")

                self._save_checkpoint(period)

                # 每 20 个报告期保存一次
                if i % 20 == 0 and new_records:
                    new_df = pd.concat(new_records, ignore_index=True)
                    existing_df = self._merge_and_save(existing_df, new_df)
                    new_records = []
                    self.logger.info(f"  中间保存完成，当前总记录数: {len(existing_df)}")

                time.sleep(0.5)

        except KeyboardInterrupt:
            self.logger.warning("用户中断，保存已获取数据...")

        # 保存剩余数据
        if new_records:
            new_df = pd.concat(new_records, ignore_index=True)
            existing_df = self._merge_and_save(existing_df, new_df)

        # 完成后清理断点文件
        if self.checkpoint_file.exists() and len(remaining) > 0:
            done_now = self._load_checkpoint()
            if set(periods).issubset(done_now):
                self.checkpoint_file.unlink()
                self.logger.info("所有报告期获取完毕，断点文件已清理")

        self.logger.info(f"利润表数据获取完成，共 {len(existing_df)} 条记录")
        return existing_df

    def update(self, n_quarters: int = 2) -> pd.DataFrame:
        """
        增量更新：拉取最近 N 个季度

        覆盖延迟公告和追溯调整的情况。

        Args:
            n_quarters: 回溯季度数（默认2，覆盖延迟公告）

        Returns:
            DataFrame: 更新后的完整数据
        """
        self.logger.info(f"增量更新利润表数据（最近 {n_quarters} 个季度）")

        all_periods = self._generate_periods(2005)
        recent_periods = all_periods[-n_quarters:]

        existing_df = self._load_existing()
        new_records = []

        for period in recent_periods:
            self.logger.info(f"更新 {period} 的数据")
            df = self._fetch_single_period(period)
            if len(df) > 0:
                new_records.append(df)
            time.sleep(0.5)

        if new_records:
            new_df = pd.concat(new_records, ignore_index=True)
            existing_df = self._merge_and_save(existing_df, new_df)
            self.logger.info(f"增量更新完成，总记录数: {len(existing_df)}")

        return existing_df
