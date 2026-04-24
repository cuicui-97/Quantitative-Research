import logging
import numpy as np
import pandas as pd
from pathlib import Path
from typing import List, Optional

from config.config import Config
from data_engine.processors.matrix_io import save_matrix

logger = logging.getLogger(__name__)


class IndustryMatrixBuilder:
    """
    申万行业标签矩阵构建器

    输出矩阵格式：
        index   = 交易日（YYYYMMDD 字符串）
        columns = 股票代码（ts_code）
        值      = 行业名称字符串（NaN 表示无行业记录）

    PIT 正确性：
        对每只股票，在 [in_date, out_date) 区间内填入对应行业名。
        out_date 为空表示至今仍属于该行业。
    """

    @staticmethod
    def build(
        members_file: Optional[Path] = None,
        trade_dates: Optional[pd.Index] = None,
        all_stocks: Optional[List[str]] = None,
        start_date: str = '20150101',
    ) -> dict:
        """
        同时构建一级和二级行业标签矩阵，保存并返回。

        Args:
            members_file: industry_members.csv 路径，默认 Config.INDUSTRY_DATA_FILE
            trade_dates: 交易日列表，默认从 trade_calendar.csv 读取
            all_stocks: 股票列表，默认从 all_companies_info.csv 读取
            start_date: 起始交易日（trade_dates 为 None 时生效）

        Returns:
            dict: {'l1': l1_matrix, 'l2': l2_matrix}
        """
        members_file = members_file or Config.INDUSTRY_DATA_FILE

        # 加载交易日
        if trade_dates is None:
            cal = pd.read_csv(Config.TRADE_CALENDAR_FILE, dtype=str)
            cal = cal[(cal['is_open'] == '1') & (cal['cal_date'] >= start_date)]
            trade_dates = pd.Index(cal['cal_date'].sort_values().values)
            logger.info(f"交易日历: {len(trade_dates)} 个交易日（{start_date} 至今）")

        # 加载股票列表
        if all_stocks is None:
            basic = pd.read_csv(Config.BASIC_DATA_FILE, dtype=str)
            all_stocks = basic['ts_code'].tolist()
            logger.info(f"股票列表: {len(all_stocks)} 只")

        # 加载行业成分数据
        logger.info(f"加载行业成分数据: {members_file}")
        members = pd.read_csv(members_file, dtype=str)
        members['in_date'] = members['in_date'].fillna('')
        members['out_date'] = members['out_date'].fillna('')
        logger.info(f"行业成分记录: {len(members)} 条")

        results = {}
        for level, name_col, output_file in [
            ('l1', 'l1_name', Config.INDUSTRY_L1_MATRIX_FILE),
            ('l2', 'l2_name', Config.INDUSTRY_L2_MATRIX_FILE),
        ]:
            logger.info(f"构建 {level} 行业矩阵（{name_col}）...")
            matrix = IndustryMatrixBuilder._build_one_level(
                members=members,
                trade_dates=trade_dates,
                all_stocks=all_stocks,
                name_col=name_col,
            )
            save_matrix(matrix, output_file)
            logger.info(f"已保存: {output_file}  非空率: {matrix.notna().mean().mean():.1%}")
            results[level] = matrix

        return results

    @staticmethod
    def _build_one_level(
        members: pd.DataFrame,
        trade_dates: pd.Index,
        all_stocks: List[str],
        name_col: str,
    ) -> pd.DataFrame:
        """
        构建单个层级的行业标签矩阵。

        算法：
        1. 对每条成分记录展开到 [in_date, out_date) 区间内的交易日
        2. 用 pivot_table 聚合成 (date, ts_code) → industry_name
        3. reindex 到目标交易日和股票列表
        """
        # 筛选有效记录
        df = members[members[name_col].notna() & (members[name_col] != '')].copy()
        if df.empty:
            logger.warning(f"没有有效的 {name_col} 数据")
            return pd.DataFrame(index=trade_dates, columns=all_stocks, dtype=object)

        trade_dates_arr = trade_dates.values  # numpy array，便于 searchsorted

        rows = []
        for _, row in df.iterrows():
            ts_code = row['ts_code']
            industry = row[name_col]
            in_date = row['in_date']
            out_date = row['out_date']

            if not in_date:
                continue

            # 找到 [in_date, out_date) 区间内的交易日
            lo = np.searchsorted(trade_dates_arr, in_date, side='left')
            if out_date:
                hi = np.searchsorted(trade_dates_arr, out_date, side='left')
            else:
                hi = len(trade_dates_arr)

            if lo >= hi:
                continue

            # 批量生成 (date, ts_code, industry) 三元组
            dates_in_range = trade_dates_arr[lo:hi]
            rows.append(pd.DataFrame({
                'trade_date': dates_in_range,
                'ts_code': ts_code,
                'industry': industry,
            }))

        if not rows:
            logger.warning(f"展开后无有效记录")
            return pd.DataFrame(index=trade_dates, columns=all_stocks, dtype=object)

        long_df = pd.concat(rows, ignore_index=True)
        logger.info(f"  展开后共 {len(long_df):,} 条 (date, stock) 记录")

        # pivot：同一 (date, ts_code) 理论上只有一个行业，用 last 兜底
        matrix = long_df.pivot_table(
            index='trade_date',
            columns='ts_code',
            values='industry',
            aggfunc='last',
        )

        # reindex 到目标交易日和股票列表
        matrix = matrix.reindex(index=trade_dates, columns=all_stocks)

        return matrix
