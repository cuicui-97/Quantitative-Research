"""
收益加权模块

负责计算不同加权方式下的组合收益
"""
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
        计算各组收益

        核心逻辑：
        1. shift(-1) 将T+1日的收益前移到T日
        2. 对每个日期、每个组，找到该组的股票
        3. 根据加权方式计算该组的平均收益
        4. 如果启用交易成本，扣除换手成本

        Args:
            group_matrix: 分组矩阵（T日）
            return_matrix: 收益率矩阵
            mv_matrix: 市值矩阵（市值加权时需要）
            weighting: 加权方式，'equal'=等权，'market_cap'=市值加权
            transaction_cost: 交易成本计算器（可选）
            logger: 日志记录器

        Returns:
            group_returns: DataFrame (dates × groups)，每组每日的平均收益
        """
        if logger:
            logger.info(f"计算各组收益（{weighting} 加权）...")

        # shift(-1) 将T+1日的收益前移到T日
        # 这样可以实现：T日分组 → T+1日收益
        returns_next_day = return_matrix.shift(-1)

        # 如果使用市值加权，准备市值矩阵
        if weighting == 'market_cap':
            if mv_matrix is None:
                raise ValueError("市值加权需要提供 mv_matrix 参数")
            # 使用T日的市值作为权重
            mv_for_weighting = mv_matrix.copy()

        # 初始化结果矩阵
        n_groups = int(group_matrix.max().max())
        group_returns = pd.DataFrame(
            index=group_matrix.index,
            columns=range(1, n_groups + 1),
            dtype=float
        )

        # 逐日计算每组收益
        for date in group_matrix.index:
            for group in range(1, n_groups + 1):
                # 找到该日该组的股票
                group_mask = (group_matrix.loc[date] == group)
                group_stocks = group_mask[group_mask].index

                if len(group_stocks) == 0:
                    continue

                if weighting == 'equal':
                    # 等权平均：简单算术平均
                    group_returns.loc[date, group] = returns_next_day.loc[date, group_stocks].mean()

                elif weighting == 'market_cap':
                    # 市值加权平均
                    # 1. 获取该组股票的T日市值
                    mv_weights = mv_for_weighting.loc[date, group_stocks]
                    # 2. 获取该组股票的T+1日收益
                    stock_returns = returns_next_day.loc[date, group_stocks]

                    # 3. 过滤掉市值或收益为NaN的股票
                    valid_mask = mv_weights.notna() & stock_returns.notna() & (mv_weights > 0)
                    if valid_mask.sum() == 0:
                        continue

                    mv_weights_valid = mv_weights[valid_mask]
                    stock_returns_valid = stock_returns[valid_mask]

                    # 4. 计算市值加权收益
                    # 公式: sum(return_i × weight_i) where weight_i = mv_i / sum(mv)
                    total_mv = mv_weights_valid.sum()
                    if total_mv > 0:
                        weighted_return = (stock_returns_valid * mv_weights_valid).sum() / total_mv
                        group_returns.loc[date, group] = weighted_return

        # 输出统计信息
        if logger:
            logger.info(f"  分组收益矩阵: {group_returns.shape}")
            logger.info(f"  有效数据点: {group_returns.notna().sum().sum():,}")

        # 扣除交易成本（如果启用）
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
