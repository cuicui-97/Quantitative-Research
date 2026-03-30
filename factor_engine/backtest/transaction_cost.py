"""
交易成本计算模块

负责计算换手率和交易成本
"""
import pandas as pd
import numpy as np
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class TransactionCostCalculator:
    """
    交易成本计算器

    支持三种成本：
    1. 佣金（commission）：双边，买卖都收
    2. 印花税（stamp_duty）：单边，仅卖出收
    3. 滑点（slippage）：双边，市场冲击成本
    """

    def __init__(
        self,
        commission_rate: float = 0.0003,
        stamp_duty_rate: float = 0.001,
        slippage_rate: float = 0.001
    ):
        """
        初始化交易成本参数

        Args:
            commission_rate: 佣金率（默认万3）
            stamp_duty_rate: 印花税率（默认千1，仅卖出）
            slippage_rate: 滑点率（默认千1）
        """
        self.commission_rate = commission_rate
        self.stamp_duty_rate = stamp_duty_rate
        self.slippage_rate = slippage_rate

        self.buy_cost_rate = commission_rate + slippage_rate
        self.sell_cost_rate = commission_rate + stamp_duty_rate + slippage_rate

        logger.info(f"交易成本计算器初始化完成")
        logger.info(f"  佣金率: {commission_rate:.4%}")
        logger.info(f"  印花税率: {stamp_duty_rate:.4%}")
        logger.info(f"  滑点率: {slippage_rate:.4%}")
        logger.info(f"  买入成本率: {self.buy_cost_rate:.4%}")
        logger.info(f"  卖出成本率: {self.sell_cost_rate:.4%}")

    def calculate_turnover(
        self,
        group_matrix: pd.DataFrame,
        group: int
    ) -> pd.Series:
        """
        计算每日换手率

        换手率 = (卖出股票数 + 买入股票数) / (2 × 当前持仓数)

        Args:
            group_matrix: 分组矩阵 (dates × stocks)，值为分组编号
            group: 目标分组编号

        Returns:
            pd.Series: 每日换手率（0-1之间），索引为日期
        """
        dates = group_matrix.index
        turnover_rates = []

        for i in range(len(dates)):
            if i == 0:
                turnover_rates.append(1.0)
            else:
                prev_date = dates[i - 1]
                curr_date = dates[i]

                prev_stocks = set(group_matrix.columns[group_matrix.loc[prev_date] == group])
                curr_stocks = set(group_matrix.columns[group_matrix.loc[curr_date] == group])

                if len(curr_stocks) == 0:
                    turnover_rates.append(0.0)
                    continue

                sold = prev_stocks - curr_stocks
                bought = curr_stocks - prev_stocks

                turnover_rate = (len(sold) + len(bought)) / (2 * len(curr_stocks))
                turnover_rates.append(turnover_rate)

        return pd.Series(turnover_rates, index=dates)

    def calculate_cost(
        self,
        group_returns: pd.DataFrame,
        group_matrix: pd.DataFrame,
        mv_matrix: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """
        计算交易成本并扣除

        Args:
            group_returns: 原始分组收益 (dates × groups)
            group_matrix: 分组矩阵 (dates × stocks)
            mv_matrix: 市值矩阵 (dates × stocks)，用于市值加权换手率计算

        Returns:
            pd.DataFrame: 扣除成本后的分组收益
        """
        adjusted_returns = group_returns.copy()

        for group in group_returns.columns:
            turnover = self.calculate_turnover(group_matrix, group)

            total_cost_rate = (self.buy_cost_rate + self.sell_cost_rate) / 2
            cost_series = turnover * total_cost_rate

            adjusted_returns[group] = group_returns[group] - cost_series

        return adjusted_returns
