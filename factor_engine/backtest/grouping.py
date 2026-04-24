import pandas as pd
import numpy as np
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class FactorGrouper:
    """
    因子分组器

    核心功能：
    1. 过滤T+1日可交易的股票
    2. 在可交易股票中按因子值分组（十分位数）
    3. 返回分组矩阵
    """

    def __init__(self, n_groups: int = 10):
        self.n_groups = n_groups

    def group_by_factor(
        self,
        factor_matrix: pd.DataFrame,
        tradability_matrix: pd.DataFrame,
        logger: Optional[logging.Logger] = None
    ) -> pd.DataFrame:
        """
        按因子值分组（向量化实现）

        核心逻辑：
        1. shift(-1) 获取T+1日可交易状态
        2. 将不可交易或因子缺失的位置置为 NaN
        3. 按行（截面）做百分位排名，映射到 1~n_groups

        Args:
            factor_matrix: 因子矩阵 (dates × stocks)
            tradability_matrix: 可交易矩阵（0=可交易，1=不可交易）
            logger: 日志记录器

        Returns:
            group_matrix: 分组矩阵，值为1-n（NaN表示不参与分组）
        """
        if logger:
            logger.info(f"开始因子分组（先过滤T+1日可交易，再分成{self.n_groups}组）...")

        # T+1日可交易状态前移到T日
        tradable_next_day = tradability_matrix.shift(-1)

        # 不可交易或因子缺失的位置置 NaN
        masked = factor_matrix.where((tradable_next_day == 0) & factor_matrix.notna())

        # 截面百分位排名（0~1），NaN 自动忽略
        pct_rank = masked.rank(axis=1, pct=True, na_option='keep')

        # 映射到 1~n_groups
        # rank 返回 (0, 1]，乘以 n_groups 后 ceil 得到 1~n_groups
        group_matrix = np.ceil(pct_rank * self.n_groups).clip(1, self.n_groups)

        # 日期样本数不足 n_groups 的行置 NaN
        valid_count = masked.notna().sum(axis=1)
        insufficient = valid_count < self.n_groups
        group_matrix.loc[insufficient] = np.nan

        if logger:
            skipped = int(insufficient.sum())
            total = len(factor_matrix)
            total_grouped = int(group_matrix.notna().sum().sum())
            logger.info(f"  跳过 {skipped} 个日期（可交易样本不足）")
            logger.info(f"  有效日期: {total - skipped}")
            logger.info(f"  成功分组样本数: {total_grouped:,}")

        return group_matrix
