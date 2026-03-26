"""
矩阵处理器

统一的矩阵处理器，封装所有矩阵构建的业务逻辑：
1. 补充数据矩阵（ST、停牌）
2. 交易可用性相关矩阵（上市天数、缺失、涨跌停、交易可用性）
3. 收益率矩阵

矩阵值统一标准：
- 1 = 不可交易
- 0 = 可交易
- 涨跌停矩阵特殊：1=涨停，-1=跌停，0=正常
"""
import logging
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional, List

from config.config import Config
from src.processors.matrix_builder import MatrixBuilder
from src.processors.matrix_io import load_matrix, save_matrix

logger = logging.getLogger(__name__)


class MatrixProcessor:
    """统一矩阵处理器"""

    def __init__(
        self,
        basic_info: Optional[pd.DataFrame] = None,
        st_matrix: Optional[pd.DataFrame] = None,
        suspension_matrix: Optional[pd.DataFrame] = None,
        limit_prices: Optional[pd.DataFrame] = None
    ):
        """
        初始化矩阵处理器

        Args:
            basic_info: 基础数据 DataFrame（包含 ts_code 和 list_date）
            st_matrix: ST 状态矩阵（可选，如果为 None 则从文件加载）
            suspension_matrix: 停牌状态矩阵（可选，如果为 None 则从文件加载）
            limit_prices: 涨跌停价格数据（可选，如果为 None 则从文件加载）
        """
        self.basic_info = basic_info.set_index('ts_code') if basic_info is not None else None

        # 加载 ST 状态矩阵
        if st_matrix is None:
            st_matrix_file = Config.MATRIX_DATA_DIR / 'st_matrix.csv'
            if st_matrix_file.exists():
                logger.info(f"加载 ST 状态矩阵: {st_matrix_file}")
                self.st_matrix = load_matrix(st_matrix_file)
            else:
                logger.warning(f"ST 状态矩阵不存在: {st_matrix_file}")
                self.st_matrix = None
        else:
            self.st_matrix = st_matrix

        # 加载停牌状态矩阵
        if suspension_matrix is None:
            suspension_matrix_file = Config.MATRIX_DATA_DIR / 'suspension_matrix.csv'
            if suspension_matrix_file.exists():
                logger.info(f"加载停牌状态矩阵: {suspension_matrix_file}")
                self.suspension_matrix = load_matrix(suspension_matrix_file)
            else:
                logger.warning(f"停牌状态矩阵不存在: {suspension_matrix_file}")
                self.suspension_matrix = None
        else:
            self.suspension_matrix = suspension_matrix

        # 加载涨跌停价格数据
        if limit_prices is None:
            limit_prices_file = Config.SUPPLEMENTARY_DATA_DIR / 'limit_prices.csv'
            if limit_prices_file.exists():
                logger.info(f"加载涨跌停价格数据: {limit_prices_file}")
                self.limit_prices = pd.read_csv(limit_prices_file, dtype={'trade_date': str})
            else:
                logger.warning(f"涨跌停价格数据不存在: {limit_prices_file}")
                self.limit_prices = None
        else:
            self.limit_prices = limit_prices

    # ==================== 补充数据矩阵 ====================

    def build_st_matrix(
        self,
        st_file: Optional[Path] = None,
        all_stocks: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        构建 ST 状态矩阵

        Args:
            st_file: ST 状态数据文件路径
            all_stocks: 所有股票代码列表

        Returns:
            DataFrame: ST 状态矩阵（1=ST 不可交易，0=非ST 可交易）
        """
        logger.info("构建 ST 状态矩阵...")

        # 读取数据
        if st_file is None:
            st_file = Config.SUPPLEMENTARY_DATA_DIR / 'st_status.csv'

        if not st_file.exists():
            logger.error(f"ST 状态数据文件不存在: {st_file}")
            return pd.DataFrame()

        st_df = pd.read_csv(st_file, dtype={'trade_date': str})
        logger.info(f"读取到 {len(st_df)} 条 ST 状态记录")

        # 使用通用构建器（default_value=0 表示默认可交易）
        matrix = MatrixBuilder.from_long_format(
            df=st_df,
            all_stocks=all_stocks,
            default_value=0  # 非ST股票默认为0（可交易）
        )

        logger.info(f"ST 状态矩阵构建完成（不可交易比例: {matrix.mean().mean():.2%}）")
        return matrix

    def build_suspension_matrix(
        self,
        suspension_file: Optional[Path] = None,
        all_stocks: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        构建停牌状态矩阵

        Args:
            suspension_file: 停牌状态数据文件路径
            all_stocks: 所有股票代码列表

        Returns:
            DataFrame: 停牌状态矩阵（1=停牌 不可交易，0=正常 可交易）
        """
        logger.info("构建停牌状态矩阵...")

        # 读取数据
        if suspension_file is None:
            suspension_file = Config.SUPPLEMENTARY_DATA_DIR / 'suspension_status.csv'

        if not suspension_file.exists():
            logger.error(f"停牌状态数据文件不存在: {suspension_file}")
            return pd.DataFrame()

        suspension_df = pd.read_csv(suspension_file, dtype={'trade_date': str})
        logger.info(f"读取到 {len(suspension_df)} 条停牌记录")

        # 使用通用构建器（default_value=0 表示默认可交易）
        matrix = MatrixBuilder.from_long_format(
            df=suspension_df,
            all_stocks=all_stocks,
            default_value=0  # 未停牌默认为0（可交易）
        )

        logger.info(f"停牌状态矩阵构建完成（不可交易比例: {matrix.mean().mean():.2%}）")
        return matrix

    # ==================== 交易可用性相关矩阵 ====================

    def build_listing_days_matrix(
        self,
        dates: pd.DatetimeIndex,
        stocks: List[str],
        min_listing_days: int = 180,
        n_jobs: int = 4
    ) -> pd.DataFrame:
        """
        构建上市天数矩阵

        Args:
            dates: 全局日期索引
            stocks: 股票代码列表
            min_listing_days: 最小上市天数

        Returns:
            DataFrame: 上市天数矩阵（1=不满足 不可交易，0=满足 可交易）
        """
        logger.info(f"构建上市天数矩阵（最小 {min_listing_days} 天）...")

        def condition_func(stock_info: pd.Series, dates: pd.DatetimeIndex) -> np.ndarray:
            """检查是否满足上市天数和退市状态（返回1表示不可交易）"""
            result = np.zeros(len(dates), dtype=np.int8)

            # 检查上市日期
            list_date_str = stock_info.get('list_date')
            if pd.notna(list_date_str):
                list_date = pd.to_datetime(list_date_str, format='%Y%m%d')
                days_since_listing = (dates - list_date).days
                # 上市不足 min_listing_days 天的设为1（不可交易）
                result = (days_since_listing < min_listing_days).astype(np.int8)

            # 检查退市日期
            delist_date_str = stock_info.get('delist_date')
            if pd.notna(delist_date_str):
                delist_date = pd.to_datetime(delist_date_str, format='%Y%m%d')
                # 退市后的日期设为1（不可交易）
                result = result | (dates >= delist_date).astype(np.int8)

            return result

        matrix = MatrixBuilder.from_basic_info(
            basic_info=self.basic_info,
            dates=dates,
            stocks=stocks, 
            condition_func=condition_func,
            n_jobs=n_jobs
        )

        logger.info(f"不可交易比例: {matrix.mean().mean():.2%}")
        return matrix

    def build_missing_data_matrix(
        self,
        dates: pd.DatetimeIndex,
        stocks: List[str],
        n_jobs: int = 4
    ) -> pd.DataFrame:
        """
        构建数据缺失矩阵

        Args:
            dates: 全局日期索引
            stocks: 股票代码列表

        Returns:
            DataFrame: 数据缺失矩阵（1=缺失 不可交易，0=有数据 可交易）
        """
        logger.info("构建数据缺失矩阵...")

        def extractor_func(ts_code: str, dates: pd.DatetimeIndex) -> np.ndarray:
            """提取数据可用性（返回1表示不可交易）"""
            daily_file = Config.DAILY_DATA_DIR / f'{ts_code}.csv'
            if not daily_file.exists():
                return np.ones(len(dates), dtype=np.int8)  # 缺失数据，不可交易

            try:
                df_daily = pd.read_csv(daily_file, dtype={'trade_date': str})
                if df_daily.empty or 'close' not in df_daily.columns:
                    return np.ones(len(dates), dtype=np.int8)

                df_daily['trade_date'] = pd.to_datetime(df_daily['trade_date'], format='%Y%m%d')
                df_daily = df_daily.set_index('trade_date').reindex(dates)

                # 缺失数据返回1（不可交易）
                return df_daily['close'].isna().astype(np.int8).values
            except Exception:
                return np.ones(len(dates), dtype=np.int8)

        matrix = MatrixBuilder.from_daily_files(
            dates=dates,
            stocks=stocks,
            extractor_func=extractor_func,
            desc="检查数据完整性",
            n_jobs=n_jobs
        )

        logger.info(f"数据缺失比例: {matrix.mean().mean():.2%}")
        return matrix

    def build_limit_matrix(
        self,
        dates: pd.DatetimeIndex,
        stocks: List[str],
        n_jobs: int = 4
    ) -> pd.DataFrame:
        """
        构建涨跌停矩阵（使用涨跌停价格数据 - 矩阵化版本）

        Args:
            dates: 全局日期索引
            stocks: 股票代码列表
            n_jobs: 并行线程数（本方法不使用，保持接口一致）

        Returns:
            DataFrame: 涨跌停矩阵（1=涨停，-1=跌停，0=正常）
        """
        logger.info("构建涨跌停矩阵（使用涨跌停价格数据 - 矩阵化版本）...")

        # 检查涨跌停价格数据
        if self.limit_prices is None or self.limit_prices.empty:
            logger.error("涨跌停价格数据不存在，请先运行: python scripts/fetch_limit_prices.py")
            return pd.DataFrame()

        # 1. 构建涨停价格矩阵
        logger.info("构建涨停价格矩阵...")
        up_limit_matrix = MatrixBuilder.from_long_format(
            df=self.limit_prices,
            value_col='up_limit',
            all_stocks=stocks,
            all_dates=dates.strftime('%Y%m%d').tolist(),
            default_value=np.nan  # 没有数据的位置用 NaN
        )

        # 2. 构建跌停价格矩阵
        logger.info("构建跌停价格矩阵...")
        down_limit_matrix = MatrixBuilder.from_long_format(
            df=self.limit_prices,
            value_col='down_limit',
            all_stocks=stocks,
            all_dates=dates.strftime('%Y%m%d').tolist(),
            default_value=np.nan
        )

        # 3. 构建开盘价矩阵
        logger.info("构建开盘价矩阵...")
        def extractor_func(ts_code: str, dates: pd.DatetimeIndex) -> np.ndarray:
            """提取开盘价"""
            daily_file = Config.DAILY_DATA_DIR / f'{ts_code}.csv'
            if not daily_file.exists():
                return np.full(len(dates), np.nan, dtype=np.float32)

            try:
                df_daily = pd.read_csv(daily_file, dtype={'trade_date': str})
                if df_daily.empty or 'trade_date' not in df_daily.columns:
                    return np.full(len(dates), np.nan, dtype=np.float32)

                df_daily['trade_date'] = pd.to_datetime(df_daily['trade_date'], format='%Y%m%d')
                df_daily = df_daily.set_index('trade_date').reindex(dates)

                # 获取开盘价
                if 'open_raw' in df_daily.columns:
                    open_price = df_daily['open_raw']
                elif 'open' in df_daily.columns:
                    open_price = df_daily['open']
                else:
                    return np.full(len(dates), np.nan, dtype=np.float32)

                return open_price.values.astype(np.float32)
            except Exception as e:
                logger.debug(f"处理 {ts_code} 失败: {e}")
                return np.full(len(dates), np.nan, dtype=np.float32)

        open_price_matrix = MatrixBuilder.from_daily_files(
            dates=dates,
            stocks=stocks,
            extractor_func=extractor_func,
            desc="读取开盘价",
            n_jobs=n_jobs
        )

        # 4. 矩阵运算判断涨跌停
        logger.info("矩阵运算判断涨跌停...")

        # 初始化结果矩阵（全0，表示正常）
        result_matrix = pd.DataFrame(
            np.zeros_like(open_price_matrix.values, dtype=np.int8),
            index=open_price_matrix.index,
            columns=open_price_matrix.columns
        )

        # 判断涨停（容忍 0.01 元误差）
        is_limit_up = open_price_matrix >= (up_limit_matrix - 0.01)
        result_matrix[is_limit_up] = 1

        # 判断跌停（容忍 0.01 元误差）
        is_limit_down = open_price_matrix <= (down_limit_matrix + 0.01)
        result_matrix[is_limit_down] = -1

        # 统计
        total_elements = result_matrix.size
        limit_up_count = (result_matrix == 1).sum().sum()
        limit_down_count = (result_matrix == -1).sum().sum()
        normal_count = (result_matrix == 0).sum().sum()

        logger.info(f"涨停比例: {limit_up_count / total_elements:.2%}")
        logger.info(f"跌停比例: {limit_down_count / total_elements:.2%}")
        logger.info(f"正常比例: {normal_count / total_elements:.2%}")

        return result_matrix

    def build_tradability_matrix(
        self,
        dates: pd.DatetimeIndex,
        stocks: List[str],
        save_intermediate: bool = False,
        n_jobs: int = 4
    ) -> pd.DataFrame:
        """
        构建交易可用性矩阵

        综合判断：上市天数、ST、数据缺失、停牌、涨跌停
        任一条件不满足（值为1或非0）则不可交易

        Args:
            dates: 全局日期索引
            stocks: 股票代码列表
            save_intermediate: 是否保存中间矩阵
            n_jobs: 并行线程数

        Returns:
            DataFrame: 交易可用性矩阵（1=不可交易，0=可交易）
        """
        logger.info("构建交易可用性矩阵...")

        # 1. 上市天数矩阵
        logger.info("[1/5] 构建上市天数矩阵...")
        listing_days_matrix = self.build_listing_days_matrix(dates, stocks, n_jobs=n_jobs)
        if save_intermediate:
            save_matrix(listing_days_matrix, Config.MATRIX_DATA_DIR / 'listing_days_matrix.csv')

        # 2. ST 状态矩阵
        logger.info("[2/5] 加载 ST 状态矩阵...")
        if self.st_matrix is None:
            logger.error("ST 状态矩阵不存在")
            return pd.DataFrame()
        st_matrix_aligned = MatrixBuilder.align_matrix(
            self.st_matrix, dates.strftime('%Y%m%d'), stocks, fill_value=0
        )

        # 3. 数据缺失矩阵
        logger.info("[3/5] 构建数据缺失矩阵...")
        missing_data_matrix = self.build_missing_data_matrix(dates, stocks, n_jobs=n_jobs)
        if save_intermediate:
            save_matrix(missing_data_matrix, Config.MATRIX_DATA_DIR / 'missing_data_matrix.csv')

        # 4. 停牌状态矩阵
        logger.info("[4/5] 加载停牌状态矩阵...")
        if self.suspension_matrix is None:
            logger.error("停牌状态矩阵不存在")
            return pd.DataFrame()
        suspension_matrix_aligned = MatrixBuilder.align_matrix(
            self.suspension_matrix, dates.strftime('%Y%m%d'), stocks, fill_value=0
        )

        # 5. 涨跌停矩阵
        logger.info("[5/5] 构建涨跌停矩阵...")
        limit_matrix = self.build_limit_matrix(dates, stocks, n_jobs=n_jobs)
        if save_intermediate:
            save_matrix(limit_matrix, Config.MATRIX_DATA_DIR / 'limit_matrix.csv')

        # 转换涨跌停矩阵：涨停或跌停都设为1（不可交易）
        limit_binary = (limit_matrix != 0).astype(np.int8)

        # 合并所有条件（任一为1则不可交易）
        logger.info("合并所有条件...")
        tradability_matrix = (
            listing_days_matrix |
            st_matrix_aligned |
            missing_data_matrix |
            suspension_matrix_aligned |
            limit_binary
        ).astype(np.int8)

        # 保存
        output_file = Config.MATRIX_DATA_DIR / 'tradability_matrix.csv'
        save_matrix(tradability_matrix, output_file)

        # 统计
        tradable_ratio = 1 - tradability_matrix.mean().mean()
        logger.info(f"可交易比例: {tradable_ratio:.2%}")

        return tradability_matrix

    def build_return_matrix(
        self,
        dates: pd.DatetimeIndex,
        stocks: List[str],
        n_jobs: int = 4
    ) -> pd.DataFrame:
        """
        构建开盘收益率矩阵

        Args:
            dates: 全局日期索引
            stocks: 股票代码列表

        Returns:
            DataFrame: 开盘收益率矩阵（Return_t = (Open_t - Open_{t-1}) / Open_{t-1}）
        """
        logger.info("构建开盘收益率矩阵...")

        def extractor_func(ts_code: str, dates: pd.DatetimeIndex) -> np.ndarray:
            """提取开盘收益率"""
            daily_file = Config.DAILY_DATA_DIR / f'{ts_code}.csv'
            if not daily_file.exists():
                return np.full(len(dates), np.nan, dtype=np.float32)

            try:
                df_daily = pd.read_csv(daily_file, dtype={'trade_date': str})
                if df_daily.empty or 'trade_date' not in df_daily.columns:
                    return np.full(len(dates), np.nan, dtype=np.float32)

                df_daily['trade_date'] = pd.to_datetime(df_daily['trade_date'], format='%Y%m%d')
                df_daily = df_daily.set_index('trade_date').reindex(dates)

                # 使用后复权开盘价
                if 'open' in df_daily.columns:
                    open_price = df_daily['open']
                else:
                    return np.full(len(dates), np.nan, dtype=np.float32)

                # 计算收益率
                returns = open_price.pct_change().values
                return returns.astype(np.float32)
            except Exception:
                return np.full(len(dates), np.nan, dtype=np.float32)

        matrix = MatrixBuilder.from_daily_files(
            dates=dates,
            stocks=stocks,
            extractor_func=extractor_func,
            desc="计算开盘收益率",
            n_jobs=n_jobs
        )

        # 保存
        output_file = Config.MATRIX_DATA_DIR / 'open_return_matrix.csv'
        save_matrix(matrix, output_file)

        logger.info(f"开盘收益率矩阵构建完成")
        return matrix
