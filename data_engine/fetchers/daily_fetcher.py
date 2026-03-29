"""
日线数据抓取器
负责抓取股票日线行情数据
"""
import logging
import pandas as pd
from pathlib import Path
from config.config import Config
from data_engine.api.tushare_api import TushareAPI
from data_engine.fetchers.base_fetcher import BaseFetcher
from utils import retry_on_error


class DailyDataFetcher(BaseFetcher):
    """
    日线行情数据抓取器

    负责批量抓取股票日线行情数据（支持三种复权类型）
    """

    def __init__(self, api: TushareAPI, basic_info_df: pd.DataFrame = None):
        """
        初始化日线数据抓取器

        Args:
            api: TushareAPI 实例
            basic_info_df: 股票基础信息 DataFrame（包含 ts_code, list_date）
        """
        super().__init__(api, use_output_dir=False)
        self.basic_info_df = basic_info_df

    @retry_on_error()
    def fetch_daily_hfq(self, ts_code, start_date, end_date='20991231'):
        """
        获取单只股票的后复权日线数据

        Args:
            ts_code: 股票代码
            start_date: 开始日期（YYYYMMDD）
            end_date: 结束日期（YYYYMMDD，默认到未来）

        Returns:
            pandas.DataFrame: 日线数据（按日期升序）
        """
        return self.api.fetch_daily_bar(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            adj='hfq'
        )

    def _get_stock_info(self, ts_code: str) -> dict:
        """
        获取股票基础信息

        Args:
            ts_code: 股票代码

        Returns:
            dict: 包含 list_date 等信息
        """
        if self.basic_info_df is not None:
            stock_info = self.basic_info_df[self.basic_info_df['ts_code'] == ts_code]
            if not stock_info.empty:
                return stock_info.iloc[0].to_dict()
        return {'list_date': '19900101'}

    @retry_on_error()
    def fetch_daily_all_adj(self, ts_code: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取包含三种复权类型的日线数据

        Args:
            ts_code: 股票代码
            start_date: 开始日期 YYYYMMDD（默认从上市日期开始）
            end_date: 结束日期 YYYYMMDD（默认到未来）

        Returns:
            DataFrame: 包含三种复权类型的日线数据

        字段说明：
            - 不复权：open_raw, high_raw, low_raw, close_raw, pre_close_raw
            - 前复权：open_qfq, high_qfq, low_qfq, close_qfq, pre_close_qfq
            - 后复权：open_hfq, high_hfq, low_hfq, close_hfq, pre_close_hfq
            - 别名字段：open, high, low, close, pre_close（指向后复权）
            - 通用字段：trade_date, vol, amount, pct_chg, change
        """
        # 获取上市日期
        if start_date is None:
            stock_info = self._get_stock_info(ts_code)
            start_date = stock_info.get('list_date', '19900101')

        if end_date is None:
            end_date = '20991231'

        self.logger.info(f"获取 {ts_code} 的日线数据（三种复权类型）: {start_date} ~ {end_date}")

        # 获取三种复权类型的数据
        adj_types = [
            (None, 'raw'),   # 不复权
            ('qfq', 'qfq'),  # 前复权
            ('hfq', 'hfq'),  # 后复权
        ]

        dfs = []

        for adj_param, suffix in adj_types:
            try:
                self.logger.debug(f"  获取 {ts_code} 的 {suffix} 数据...")

                # 调用 API 层（统一速率控制）
                df = self.api.fetch_daily_bar(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    adj=adj_param
                )

                if df is None or len(df) == 0:
                    self.logger.warning(f"  {ts_code} 的 {suffix} 数据为空")
                    continue

                # 重命名价格字段
                price_columns = ['open', 'high', 'low', 'close', 'pre_close']
                rename_map = {col: f"{col}_{suffix}" for col in price_columns if col in df.columns}
                df = df.rename(columns=rename_map)

                dfs.append(df)

            except Exception as e:
                self.logger.error(f"  获取 {ts_code} 的 {suffix} 数据失败: {e}")
                continue

        if not dfs:
            self.logger.error(f"{ts_code} 所有复权类型的数据都获取失败")
            return pd.DataFrame()

        # 合并数据（基于 trade_date）
        df_merged = dfs[0].copy()

        for df in dfs[1:]:
            # 只保留价格字段，其他字段（vol, amount等）使用第一个DataFrame的
            price_cols = [col for col in df.columns if '_raw' in col or '_qfq' in col or '_hfq' in col]
            df_price = df[['trade_date'] + price_cols]

            df_merged = df_merged.merge(df_price, on='trade_date', how='left')

        # 按日期排序
        df_merged = df_merged.sort_values('trade_date').reset_index(drop=True)

        # 添加别名字段（指向后复权，保持向后兼容）
        if 'open_hfq' in df_merged.columns:
            df_merged['open'] = df_merged['open_hfq']
            df_merged['high'] = df_merged['high_hfq']
            df_merged['low'] = df_merged['low_hfq']
            df_merged['close'] = df_merged['close_hfq']
            df_merged['pre_close'] = df_merged['pre_close_hfq']

        self.logger.info(f"  合并完成: {len(df_merged)} 条记录")

        return df_merged

    def fetch_all_stocks(
        self,
        stock_list_df,
        data_dir=None,
        skip_existing=None,
        start_index=0,
        batch_size=None
    ):
        """
        批量获取所有股票的日线数据

        Args:
            stock_list_df: 股票列表 DataFrame（必须包含 ts_code 和 list_date 列）
            data_dir: 数据保存目录（默认使用配置）
            skip_existing: 是否跳过已存在的文件（默认使用配置）
            start_index: 起始索引（用于分批抓取）
            batch_size: 批次大小（None 表示全部抓取）

        Returns:
            dict: 统计信息（success_count, skip_count, fail_count）
        """
        # 使用默认配置
        if data_dir is None:
            data_dir = Config.DAILY_DATA_DIR
        if skip_existing is None:
            skip_existing = Config.SKIP_EXISTING

        # 确保目录存在
        Path(data_dir).mkdir(parents=True, exist_ok=True)

        # 计算抓取范围
        end_index = start_index + batch_size if batch_size else len(stock_list_df)
        stock_subset = stock_list_df.iloc[start_index:end_index]
        total = len(stock_subset)

        # 统计信息
        success_count = 0
        skip_count = 0
        fail_count = 0
        fail_list = []

        self.logger.info("=" * 60)
        self.logger.info(
            f"开始批量抓取日线数据：第 {start_index + 1} ~ {start_index + total} 只股票 "
            f"(共 {len(stock_list_df)} 只)"
        )
        self.logger.info(f"数据保存目录: {data_dir}")
        self.logger.info(f"断点续传: {'开启' if skip_existing else '关闭'}")
        self.logger.info("=" * 60)

        # 遍历股票列表
        for idx, row in enumerate(stock_subset.itertuples(), start=1):
            ts_code = row.ts_code
            list_date = row.list_date

            # 生成文件路径
            file_path = Path(data_dir) / f"{ts_code}.csv"

            # 断点续传：检查文件是否已存在
            if skip_existing and file_path.exists():
                self.logger.info(
                    f"[{idx}/{total}] {ts_code} 已存在，跳过"
                )
                skip_count += 1
                continue

            try:
                # 获取数据（三种复权类型）
                df = self.fetch_daily_all_adj(ts_code, start_date=list_date)

                if df is not None and len(df) > 0:
                    # 保存到文件
                    df.to_csv(file_path, index=False, encoding='utf-8-sig')
                    self.logger.info(
                        f"[{idx}/{total}] {ts_code} 保存成功 ({len(df)} 条数据)"
                    )
                    success_count += 1
                else:
                    self.logger.warning(
                        f"[{idx}/{total}] {ts_code} 无数据"
                    )
                    fail_count += 1
                    fail_list.append((ts_code, "无数据"))

            except Exception as e:
                self.logger.error(
                    f"[{idx}/{total}] {ts_code} 失败: {e}"
                )
                fail_count += 1
                fail_list.append((ts_code, str(e)))

        # 输出统计信息
        self.logger.info("=" * 60)
        self.logger.info("批量抓取完成")
        self.logger.info(f"成功: {success_count}")
        self.logger.info(f"跳过: {skip_count}")
        self.logger.info(f"失败: {fail_count}")

        if fail_list:
            self.logger.warning("失败列表:")
            for ts_code, reason in fail_list:
                self.logger.warning(f"  - {ts_code}: {reason}")

        self.logger.info("=" * 60)

        return {
            'success_count': success_count,
            'skip_count': skip_count,
            'fail_count': fail_count,
            'fail_list': fail_list
        }
