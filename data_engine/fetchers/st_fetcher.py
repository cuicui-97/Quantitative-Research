"""
ST 状态数据抓取器
使用 stock_st API 获取股票 ST 状态历史数据
"""
import pandas as pd

from data_engine.api.tushare_api import TushareAPI
from data_engine.fetchers.base_fetcher import BaseFetcher
from data_engine.fetchers.fetcher_utils import incremental_update, with_checkpoint
from data_engine.utils import retry_on_error


class STFetcher(BaseFetcher):
    """ST 状态数据抓取器（使用 stock_st API）"""

    def __init__(self, api: TushareAPI):
        super().__init__(api, use_output_dir=True)

    @incremental_update('st_status.csv', unique_keys=['ts_code', 'trade_date'])
    def fetch_st_status(self, start_date: str, end_date: str, force_refresh: bool = False) -> pd.DataFrame:
        """
        使用 stock_st API 获取 ST 状态历史数据

        通过遍历交易日获取每日的 ST 状态快照
        智能识别缺失的日期范围并补充数据

        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            force_refresh: 是否强制刷新（忽略已有文件，重新获取所有数据）

        Returns:
            DataFrame: 包含 ts_code, name, trade_date, type, type_name 等字段
        """
        return self._fetch_st_by_date_range(start_date, end_date)

    @with_checkpoint('st_status_temp.csv')
    def _fetch_st_by_date_range(self, start_date: str, end_date: str) -> tuple:
        """
        遍历日期范围获取 ST 状态

        支持断点续传：如果中途失败，已获取的数据会被保存

        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            tuple: (dates_list, fetch_single_func)
        """
        # 使用 trade_cal API 获取交易日历
        trade_dates = self.trade_calendar.get_trade_dates(start_date, end_date)

        # 从最新日期往前取（倒序）
        trade_dates_reversed = sorted(trade_dates, reverse=True)

        self.logger.info(f"需要查询 {len(trade_dates_reversed)} 个交易日（从最新日期开始）")

        # 返回日期列表和单日期获取函数
        return trade_dates_reversed, self._fetch_single_date_with_retry

    @retry_on_error(max_retries=3, delay=1.0)
    def _fetch_single_date_with_retry(self, date_str: str) -> pd.DataFrame:
        """
        获取单个日期的 ST 状态（带重试机制）

        Args:
            date_str: 日期 YYYYMMDD

        Returns:
            DataFrame: ST 状态记录
        """
        return self.api.fetch_stock_st(trade_date=date_str)


