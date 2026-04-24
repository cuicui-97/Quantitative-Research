import time
import pandas as pd

from data_engine.api.tushare_api import TushareAPI
from data_engine.fetchers.base_fetcher import BaseFetcher
from utils import retry_on_error
from config.config import Config


class IndustryFetcher(BaseFetcher):
    """申万行业成分数据抓取器"""

    OUTPUT_FILE = 'industry_members.csv'

    def __init__(self, api: TushareAPI):
        super().__init__(api, use_output_dir=True)
        self.output_file = self.output_dir / self.OUTPUT_FILE

    @retry_on_error(max_retries=3, delay=2.0)
    def _fetch_l1_members(self, l1_code: str) -> pd.DataFrame:
        """获取单个一级行业的全部成分（含历史变更）"""
        return self.api.fetch_index_member_all(l1_code=l1_code)

    def fetch_all(self) -> pd.DataFrame:
        """
        获取全市场申万行业成分数据（含历史变更记录）

        流程：
        1. 获取申万一级行业分类列表
        2. 逐个一级行业拉取成分（含 in_date / out_date）
        3. 合并后保存到 industry_members.csv

        Returns:
            DataFrame: 列包含 ts_code, l1_code, l1_name, l2_code, l2_name, in_date, out_date
        """
        self.logger.info("获取申万一级行业分类列表...")
        classify_df = self.api.fetch_index_classify(level='L1', src='SW2021')

        if classify_df.empty:
            self.logger.error("获取行业分类失败，返回空数据")
            return pd.DataFrame()

        l1_codes = classify_df['index_code'].tolist()
        self.logger.info(f"共 {len(l1_codes)} 个一级行业，开始逐个拉取成分...")

        all_records = []
        for i, l1_code in enumerate(l1_codes, 1):
            l1_name = classify_df.loc[classify_df['index_code'] == l1_code, 'industry_name'].iloc[0]
            self.logger.info(f"  [{i}/{len(l1_codes)}] {l1_name} ({l1_code})")

            try:
                df = self._fetch_l1_members(l1_code)
                if not df.empty:
                    all_records.append(df)
                    self.logger.info(f"    获取到 {len(df)} 条记录")
            except Exception as e:
                self.logger.error(f"    获取 {l1_code} 失败: {e}")
                continue

            time.sleep(0.3)

        if not all_records:
            self.logger.error("未获取到任何行业成分数据")
            return pd.DataFrame()

        result = pd.concat(all_records, ignore_index=True)

        # 只保留关键字段
        keep_cols = [c for c in ['ts_code', 'name', 'l1_code', 'l1_name', 'l2_code', 'l2_name', 'in_date', 'out_date', 'is_new'] if c in result.columns]
        result = result[keep_cols]

        # in_date / out_date 统一为字符串
        for col in ['in_date', 'out_date']:
            if col in result.columns:
                result[col] = result[col].astype(str).replace('nan', '')

        result.to_csv(self.output_file, index=False)
        self.logger.info(f"行业成分数据已保存: {self.output_file}（共 {len(result)} 条）")

        return result
