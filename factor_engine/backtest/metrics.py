"""
绩效指标模块

负责计算因子回测的各项统计指标
"""
import pandas as pd
import numpy as np
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class PerformanceMetrics:
    """
    绩效指标计算器

    计算因子回测的各项统计指标：
    - 日均收益
    - 收益波动率
    - 夏普比率
    - 胜率
    - 累计收益
    """

    @staticmethod
    def calculate_statistics(
        group_returns: pd.DataFrame,
        logger: Optional[logging.Logger] = None
    ) -> pd.DataFrame:
        """
        计算分组统计指标

        Args:
            group_returns: 分组收益 DataFrame (dates × groups)
            logger: 日志记录器

        Returns:
            stats: 统计指标 DataFrame
                - mean_return: 日均收益（%）
                - std_return: 收益波动率（%）
                - sharpe_ratio: 夏普比率（年化）
                - win_rate: 胜率（%）
                - cumulative_return: 累计收益（%）
        """
        if logger:
            logger.info("计算统计指标...")

        # 添加多空组合到收益率DataFrame（最后一组 - 第一组）
        first_group = group_returns.columns[0]
        last_group = group_returns.columns[-1]
        long_short_col = f'Long-Short ({last_group}-{first_group})'

        # 将多空组合添加到group_returns
        group_returns_with_ls = group_returns.copy()
        group_returns_with_ls[long_short_col] = group_returns[last_group] - group_returns[first_group]

        # 一次性计算所有组（包括多空组合）的统计指标
        stats = pd.DataFrame({
            # 日均收益（转为百分比）
            'mean_return': group_returns_with_ls.mean() * 100,

            # 收益波动率（转为百分比）
            'std_return': group_returns_with_ls.std() * 100,

            # 夏普比率（年化，假设252个交易日）
            # Sharpe = (mean / std) × sqrt(252)
            'sharpe_ratio': group_returns_with_ls.mean() / group_returns_with_ls.std() * np.sqrt(252),

            # 胜率（正收益天数 / 总交易天数）
            'win_rate': (group_returns_with_ls > 0).sum() / group_returns_with_ls.notna().sum() * 100,

            # 累计收益（转为百分比）
            # Cumulative = prod(1 + r_i) - 1
            'cumulative_return': ((1 + group_returns_with_ls).prod() - 1) * 100
        })

        return stats

    @staticmethod
    def calculate_ic(
        factor_matrix: pd.DataFrame,
        return_matrix: pd.DataFrame,
        method: str = 'spearman'
    ) -> pd.Series:
        """
        计算信息系数（IC）

        IC衡量因子值与未来收益的相关性

        Args:
            factor_matrix: 因子矩阵（T日）
            return_matrix: 收益率矩阵
            method: 相关系数方法，'spearman'（秩相关）或'pearson'（线性相关）

        Returns:
            ic_series: 每日的IC值
        """
        # shift(-1) 将T+1日的收益前移到T日
        returns_next_day = return_matrix.shift(-1)

        ic_values = []
        for date in factor_matrix.index:
            # 获取该日的因子值和T+1日收益
            factor_vals = factor_matrix.loc[date]
            return_vals = returns_next_day.loc[date]

            # 过滤掉NaN值
            valid_mask = factor_vals.notna() & return_vals.notna()
            if valid_mask.sum() < 10:  # 至少需要10个有效样本
                ic_values.append(np.nan)
                continue

            factor_valid = factor_vals[valid_mask]
            return_valid = return_vals[valid_mask]

            # 计算相关系数
            if method == 'spearman':
                ic = factor_valid.corr(return_valid, method='spearman')
            else:
                ic = factor_valid.corr(return_valid, method='pearson')

            ic_values.append(ic)

        return pd.Series(ic_values, index=factor_matrix.index, name='IC')

    @staticmethod
    def calculate_ic_ir(ic_series: pd.Series) -> dict:
        """
        计算IC和IR（信息比率）

        Args:
            ic_series: IC时间序列

        Returns:
            dict: 包含mean_ic, std_ic, ir, ic_win_rate等指标
        """
        ic_valid = ic_series.dropna()

        return {
            'mean_ic': ic_valid.mean(),
            'std_ic': ic_valid.std(),
            'ir': ic_valid.mean() / ic_valid.std() if ic_valid.std() > 0 else 0,
            'ic_win_rate': (ic_valid > 0).sum() / len(ic_valid) * 100 if len(ic_valid) > 0 else 0,
            'mean_abs_ic': ic_valid.abs().mean()
        }


# ==================== 风险指标 ====================

def calculate_max_drawdown(returns: pd.Series) -> float:
    """
    计算最大回撤

    Args:
        returns: 收益率序列

    Returns:
        最大回撤（负数，如 -0.15 表示 15% 回撤）
    """
    if returns.empty or returns.isna().all():
        return 0.0

    # 计算累计收益
    cum_returns = (1 + returns).cumprod()

    # 计算历史最高点
    running_max = cum_returns.cummax()

    # 计算回撤
    drawdown = (cum_returns - running_max) / running_max

    return drawdown.min()


def calculate_calmar_ratio(returns: pd.Series, annualize: bool = True) -> float:
    """
    计算卡尔玛比率（年化收益 / 最大回撤）

    Args:
        returns: 收益率序列
        annualize: 是否年化（默认True）

    Returns:
        卡尔玛比率
    """
    if returns.empty or returns.isna().all():
        return 0.0

    # 计算年化收益
    if annualize:
        annual_return = (1 + returns.mean()) ** 252 - 1
    else:
        annual_return = returns.mean()

    # 计算最大回撤
    max_dd = calculate_max_drawdown(returns)

    # 卡尔玛比率 = 年化收益 / 最大回撤绝对值
    if max_dd == 0:
        return np.inf if annual_return > 0 else 0.0

    return annual_return / abs(max_dd)


def calculate_var(returns: pd.Series, confidence: float = 0.95) -> float:
    """
    计算风险价值 (Value at Risk, VaR)

    VaR表示在给定置信水平下，投资组合可能遭受的最大损失

    Args:
        returns: 收益率序列
        confidence: 置信水平（默认 95%）

    Returns:
        VaR 值（负数表示损失）
    """
    if returns.empty or returns.isna().all():
        return 0.0

    returns_valid = returns.dropna()

    if len(returns_valid) == 0:
        return 0.0

    # 计算分位数
    var = returns_valid.quantile(1 - confidence)

    return var


def calculate_cvar(returns: pd.Series, confidence: float = 0.95) -> float:
    """
    计算条件风险价值 (Conditional Value at Risk, CVaR)

    CVaR是VaR的改进版本，表示超过VaR的平均损失

    Args:
        returns: 收益率序列
        confidence: 置信水平（默认 95%）

    Returns:
        CVaR 值（负数表示损失）
    """
    if returns.empty or returns.isna().all():
        return 0.0

    returns_valid = returns.dropna()

    if len(returns_valid) == 0:
        return 0.0

    # 计算VaR
    var = calculate_var(returns, confidence)

    # 计算超过VaR的平均损失
    cvar = returns_valid[returns_valid <= var].mean()

    return cvar


def calculate_volatility(returns: pd.Series, annualize: bool = True) -> float:
    """
    计算波动率

    Args:
        returns: 收益率序列
        annualize: 是否年化（默认True，假设252个交易日）

    Returns:
        波动率
    """
    if returns.empty or returns.isna().all():
        return 0.0

    vol = returns.std()

    if annualize:
        vol = vol * np.sqrt(252)

    return vol


def calculate_downside_deviation(returns: pd.Series, mar: float = 0.0, annualize: bool = True) -> float:
    """
    计算下行偏差（Downside Deviation）

    只考虑低于最小可接受收益率（MAR）的波动

    Args:
        returns: 收益率序列
        mar: 最小可接受收益率（Minimum Acceptable Return），默认0
        annualize: 是否年化

    Returns:
        下行偏差
    """
    if returns.empty or returns.isna().all():
        return 0.0

    returns_valid = returns.dropna()

    # 只考虑低于MAR的收益
    downside_returns = returns_valid[returns_valid < mar] - mar

    if len(downside_returns) == 0:
        return 0.0

    # 计算下行偏差
    downside_dev = np.sqrt((downside_returns ** 2).mean())

    if annualize:
        downside_dev = downside_dev * np.sqrt(252)

    return downside_dev


def calculate_sortino_ratio(returns: pd.Series, mar: float = 0.0, risk_free_rate: float = 0.03) -> float:
    """
    计算索提诺比率（Sortino Ratio）

    与夏普比率类似，但只考虑下行风险

    Args:
        returns: 收益率序列
        mar: 最小可接受收益率，默认0
        risk_free_rate: 无风险利率（年化），默认3%

    Returns:
        索提诺比率
    """
    if returns.empty or returns.isna().all():
        return 0.0

    # 计算超额收益
    daily_rf = risk_free_rate / 252
    excess_returns = returns - daily_rf

    # 计算年化收益
    annual_return = (1 + returns.mean()) ** 252 - 1

    # 计算下行偏差
    downside_dev = calculate_downside_deviation(returns, mar, annualize=True)

    if downside_dev == 0:
        return np.inf if annual_return > risk_free_rate else 0.0

    # 索提诺比率
    sortino = (annual_return - risk_free_rate) / downside_dev

    return sortino


def calculate_omega_ratio(returns: pd.Series, threshold: float = 0.0) -> float:
    """
    计算欧米伽比率（Omega Ratio）

    衡量收益分布在阈值以上和以下的比例

    Args:
        returns: 收益率序列
        threshold: 阈值（默认0）

    Returns:
        欧米伽比率
    """
    if returns.empty or returns.isna().all():
        return 1.0

    returns_valid = returns.dropna()

    if len(returns_valid) == 0:
        return 1.0

    # 计算高于阈值的收益总和
    gains = returns_valid[returns_valid > threshold] - threshold
    gain_sum = gains.sum() if len(gains) > 0 else 0.0

    # 计算低于阈值的损失总和
    losses = threshold - returns_valid[returns_valid < threshold]
    loss_sum = losses.sum() if len(losses) > 0 else 0.0

    if loss_sum == 0:
        return np.inf if gain_sum > 0 else 1.0

    omega = gain_sum / loss_sum

    return omega
