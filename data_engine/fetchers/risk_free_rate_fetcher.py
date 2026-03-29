"""
无风险利率数据抓取器
使用 shibor API 获取 Shibor 利率数据
"""
import pandas as pd

from data_engine.api.tushare_api import TushareAPI
from data_engine.fetchers.base_fetcher import BaseFetcher
from data_engine.fetchers.fetcher_utils import incremental_update
from utils import retry_on_error


class RiskFreeRateFetcher(BaseFetcher):
    """无风险利率数据抓取器（使用 shibor API）"""

    def __init__(self, api: TushareAPI):
        super().__init__(api, use_output_dir=True)

    @incremental_update('shibor.csv', date_column='date', unique_keys=['date'])
    def fetch_shibor(
        self,
        start_date: str = '20050101',
        end_date: str = '20261231',
        force_refresh: bool = False
    ) -> pd.DataFrame:
        """
        获取 Shibor 利率数据

        支持智能增量更新：检测已有数据的日期范围，只补充缺失部分

        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            force_refresh: 是否强制刷新（忽略已有文件，重新获取所有数据）

        Returns:
            DataFrame: 包含 date, rate1w, rate2w, rate1m, rate3m, rate6m, rate9m, rate1y 等字段
        """
        return self._fetch_shibor_with_retry(start_date, end_date)

    @retry_on_error(max_retries=3, delay=1.0)
    def _fetch_shibor_with_retry(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取 Shibor 利率数据（带重试机制）

        Args:
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            DataFrame: Shibor 利率数据
        """
        df = self.api.fetch_shibor(
            start_date=start_date,
            end_date=end_date
        )

        if df is None or len(df) == 0:
            self.logger.warning(f"未获取到 Shibor 利率数据: {start_date} ~ {end_date}")
            return pd.DataFrame()

        # 按日期排序
        df = df.sort_values('date').reset_index(drop=True)
        return df
