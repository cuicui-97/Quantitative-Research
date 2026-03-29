"""
指数日线数据抓取器
使用 index_daily API 获取指数日线数据
"""
import pandas as pd

from data_engine.api.tushare_api import TushareAPI
from data_engine.fetchers.base_fetcher import BaseFetcher
from data_engine.fetchers.fetcher_utils import incremental_update
from utils import retry_on_error


class IndexDataFetcher(BaseFetcher):
    """指数日线数据抓取器（使用 index_daily API）"""

    def __init__(self, api: TushareAPI):
        super().__init__(api, use_output_dir=True)

    def fetch_index_daily(
        self,
        ts_code: str = '000300.SH',
        start_date: str = '20050101',
        end_date: str = '20261231',
        force_refresh: bool = False
    ) -> pd.DataFrame:
        """
        获取指数日线数据

        支持智能增量更新：检测已有数据的日期范围，只补充缺失部分

        Args:
            ts_code: 指数代码（默认 '000300.SH' 沪深300）
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            force_refresh: 是否强制刷新（忽略已有文件，重新获取所有数据）

        Returns:
            DataFrame: 包含 ts_code, trade_date, open, high, low, close, vol, amount 等字段
        """
        # 动态创建装饰器（因为文件名依赖 ts_code）
        @incremental_update(f'index_daily_{ts_code}.csv', unique_keys=['trade_date'])
        def _fetch(self, start_date, end_date, force_refresh=False):
            return self._fetch_index_with_retry(ts_code, start_date, end_date)

        return _fetch(self, start_date, end_date, force_refresh)

    @retry_on_error(max_retries=3, delay=1.0)
    def _fetch_index_with_retry(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取指数日线数据（带重试机制）

        Args:
            ts_code: 指数代码
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            DataFrame: 指数日线数据
        """
        df = self.api.fetch_index_daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date
        )

        if df is None or len(df) == 0:
            self.logger.warning(f"未获取到指数日线数据: {ts_code} ({start_date} ~ {end_date})")
            return pd.DataFrame()

        # 按日期排序
        df = df.sort_values('trade_date').reset_index(drop=True)
        return df
