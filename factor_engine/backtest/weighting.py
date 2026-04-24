import pandas as pd
import numpy as np
from typing import Optional, TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from .transaction_cost import TransactionCostCalculator

logger = logging.getLogger(__name__)


class WeightCalculator:
    """
    收益加权计算器

    支持两种加权方式：
    1. 等权（equal）: 组内股票等权平均
    2. 市值加权（market_cap）: 按流通市值加权
    """

    @staticmethod
    def calculate_group_returns(
        group_matrix: pd.DataFrame,
        return_matrix: pd.DataFrame,
        mv_matrix: Optional[pd.DataFrame] = None,
        weighting: str = 'equal',
        transaction_cost: Optional['TransactionCostCalculator'] = None,
        logger: Optional[logging.Logger] = None
    ) -> pd.DataFrame:
        """
        计算各组收益（向量化实现）

        核心逻辑：
        - T日分组 → T+1日收益（shift(-1)）
        - 等权：对每个组号，用 group_matrix == g 做 mask，行均值即为组收益
        - 市值加权：同上，但先对市值归一化再加权

        Args:
            group_matrix: 分组矩阵（T日），值为 1~n_groups 或 NaN
            return_matrix: 收益率矩阵
            mv_matrix: 市值矩阵（市值加权时需要）
            weighting: 'equal' 或 'market_cap'
            transaction_cost: 交易成本计算器（可选）
            logger: 日志记录器

        Returns:
            group_returns: DataFrame (dates × groups)
        """
        if logger:
            logger.info(f"计算各组收益（{weighting} 加权）...")

        if weighting == 'market_cap' and mv_matrix is None:
            raise ValueError("市值加权需要提供 mv_matrix 参数")

        # T+1日收益前移到T日
        returns_next_day = return_matrix.shift(-1)

        n_groups = int(group_matrix.max().max())
        group_returns = pd.DataFrame(
            index=group_matrix.index,
            columns=range(1, n_groups + 1),
            dtype=float
        )

        if weighting == 'equal':
            for g in range(1, n_groups + 1):
                mask = group_matrix == g  # (dates × stocks) bool
                # 将非本组位置置 NaN，再按行取均值
                group_returns[g] = returns_next_day.where(mask).mean(axis=1)

        elif weighting == 'market_cap':
            for g in range(1, n_groups + 1):
                mask = group_matrix == g
                # 本组收益与市值，非本组置 NaN
                r = returns_next_day.where(mask)
                w = mv_matrix.where(mask & mv_matrix.notna() & (mv_matrix > 0))
                # 行内归一化权重
                w_sum = w.sum(axis=1)
                # 加权收益 = sum(r * w) / sum(w)，逐行广播
                group_returns[g] = (r * w).sum(axis=1) / w_sum.replace(0, np.nan)

        if logger:
            logger.info(f"  分组收益矩阵: {group_returns.shape}")
            logger.info(f"  有效数据点: {group_returns.notna().sum().sum():,}")

        if transaction_cost is not None:
            if logger:
                logger.info("  扣除交易成本...")
            group_returns = transaction_cost.calculate_cost(
                group_returns,
                group_matrix,
                mv_matrix
            )
            if logger:
                logger.info("  交易成本扣除完成")

        return group_returns
