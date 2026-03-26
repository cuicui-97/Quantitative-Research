"""
基础数据抓取器
负责抓取股票基础信息
"""
import logging
import pandas as pd
from config.config import Config
from src.api.tushare_api import TushareAPI
from src.utils import retry_on_error


class BasicDataFetcher:
    """
    基础数据抓取器

    负责抓取并合并股票基础信息，包括：
    - stock_basic: 核心基础信息
    - stock_company: 公司详细信息
    - new_share: IPO 信息
    """

    def __init__(self, api: TushareAPI):
        """
        初始化基础数据抓取器

        Args:
            api: TushareAPI 实例
        """
        self.api = api
        self.logger = logging.getLogger(__name__)

    @retry_on_error()
    def fetch_stock_basic(self):
        """
        获取股票基础信息（包括已退市公司）

        Returns:
            pandas.DataFrame: 股票基础信息
        """
        self.logger.info("开始获取 stock_basic 数据（包括退市公司）")

        # 获取所有状态的股票（上市 + 退市 + 暂停上市）
        df_list = []

        # L=上市, D=退市, P=暂停上市
        for status in ['L', 'D', 'P']:
            status_name = {'L': '上市', 'D': '退市', 'P': '暂停上市'}[status]
            self.logger.info(f"正在获取 {status_name} 股票...")

            df = self.api.fetch_stock_basic(
                list_status=status,
                fields='ts_code,symbol,name,area,industry,market,list_date,delist_date'
            )

            if df is not None and len(df) > 0:
                df['list_status'] = status  # 添加状态标识
                df_list.append(df)
                self.logger.info(f"获取到 {len(df)} 条 {status_name} 股票数据")

        # 合并所有数据
        if df_list:
            df_all = pd.concat(df_list, ignore_index=True)
            self.logger.info(
                f"获取到 {len(df_all)} 条 stock_basic 数据 "
                f"(上市: {len(df_all[df_all['list_status']=='L'])}, "
                f"退市: {len(df_all[df_all['list_status']=='D'])}, "
                f"暂停: {len(df_all[df_all['list_status']=='P'])})"
            )
            return df_all
        else:
            self.logger.warning("未获取到任何 stock_basic 数据")
            return pd.DataFrame()

    @retry_on_error()
    def fetch_stock_company(self):
        """
        获取公司详细信息

        注意: stock_company API 单次最多返回4500条，需要按交易所分批提取

        Returns:
            pandas.DataFrame: 公司详细信息
        """
        self.logger.info("开始获取 stock_company 数据")

        df_list = []
        # 按交易所分批提取: SSE=上交所, SZSE=深交所, BSE=北交所
        for exchange in ['SSE', 'SZSE', 'BSE']:
            self.logger.info(f"正在获取 {exchange} 交易所的公司信息...")

            df = self.api.fetch_stock_company(
                exchange=exchange,
                fields='ts_code,chairman,manager,secretary,reg_capital,setup_date,'
                       'province,city,website,email,employees,main_business,business_scope'
            )

            if df is not None and len(df) > 0:
                df_list.append(df)
                self.logger.info(f"获取到 {len(df)} 条 {exchange} 公司数据")

        # 合并所有交易所数据
        if df_list:
            df_all = pd.concat(df_list, ignore_index=True)
            self.logger.info(f"获取到 {len(df_all)} 条 stock_company 数据")
            return df_all
        else:
            self.logger.warning("未获取到任何 stock_company 数据")
            return pd.DataFrame()

    @retry_on_error()
    def fetch_new_share(self):
        """
        获取 IPO 信息

        注意: new_share API 的日期参数是指"网上发行日期"，不指定日期则获取所有历史数据
        单次最多返回2000条，可能需要分批获取

        Returns:
            pandas.DataFrame: IPO 信息
        """
        self.logger.info("开始获取 new_share 数据")

        try:
            # 不指定日期范围，获取所有历史IPO数据
            df = self.api.fetch_new_share()

            if df is not None and len(df) > 0:
                self.logger.info(f"获取到 {len(df)} 条 new_share 数据")
                self.logger.info(f"new_share 数据列: {df.columns.tolist()}")
                self.logger.info(f"new_share 数据日期范围: {df['ipo_date'].min()} ~ {df['ipo_date'].max()}")
                self.logger.info(f"new_share 前5条数据:\n{df.head(5)}")
            else:
                self.logger.warning("new_share API 返回空数据")

            return df
        except Exception as e:
            self.logger.error(f"获取 new_share 数据失败: {e}")
            return None

    def merge_all_data(self):
        """
        合并所有基础数据

        Returns:
            pandas.DataFrame: 合并后的基础数据
        """
        self.logger.info("=" * 60)
        self.logger.info("开始合并基础数据")

        # 获取各部分数据
        df_basic = self.fetch_stock_basic()
        self.logger.info(f"df_basic shape: {df_basic.shape}, columns: {list(df_basic.columns)}")

        df_company = self.fetch_stock_company()
        self.logger.info(f"df_company shape: {df_company.shape}, columns: {list(df_company.columns)}")

        df_new = self.fetch_new_share()
        if df_new is not None and len(df_new) > 0:
            self.logger.info(f"df_new shape: {df_new.shape}, columns: {list(df_new.columns)}")
        else:
            self.logger.warning("new_share 数据为空，将跳过 IPO 信息合并")
            # 创建空 DataFrame，只包含 ts_code 列
            df_new = pd.DataFrame(columns=['ts_code'])

        # 左连接合并（以 df_basic 为主）
        self.logger.info("正在合并 stock_basic 和 stock_company")
        df_merged = df_basic.merge(
            df_company,
            on='ts_code',
            how='left',
            suffixes=('', '_dup')
        )
        self.logger.info(f"合并后 shape: {df_merged.shape}")

        self.logger.info("正在合并 new_share 数据")
        df_merged = df_merged.merge(
            df_new,
            on='ts_code',
            how='left',
            suffixes=('', '_dup')
        )
        self.logger.info(f"合并后 shape: {df_merged.shape}")

        # 删除重复列（后缀为 _dup 的列）
        dup_cols = [col for col in df_merged.columns if col.endswith('_dup')]
        if dup_cols:
            df_merged = df_merged.drop(columns=dup_cols)
            self.logger.info(f"删除了 {len(dup_cols)} 个重复列: {dup_cols}")

        self.logger.info(
            f"合并完成：共 {len(df_merged)} 条记录，{len(df_merged.columns)} 个字段"
        )
        self.logger.info(f"字段列表: {', '.join(df_merged.columns.tolist())}")
        self.logger.info("=" * 60)

        return df_merged
