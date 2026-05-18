#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
财务报表 PIT 矩阵构建器 v3（预计算优化版）

优化点：
1. 预计算公告日-最新报告期映射表（supplementary/ann_date_latest_end.csv）
2. Worker 直接加载预计算数据，避免重复 groupby
3. 空间换时间，大幅加速全市场因子构建

使用方法：
    # 1. 先构建预计算映射表（只需运行一次，或数据更新时运行）
    python scripts/data_fetching/build_ann_date_latest_end.py

    # 2. 构建因子矩阵
    python financial_matrix_builder_v3.py --field n_income_attr_p
"""
import logging
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

from config.config import Config
from data_engine.processors.matrix_io import save_matrix

logger = logging.getLogger(__name__)

PREV_QUARTER = {
    '0331': '1231',
    '0630': '0331',
    '0930': '0630',
    '1231': '0930',
}

SENTINEL = '__INVALID__'


def _prev_quarter_end(end_date: str) -> str:
    year, mmdd = end_date[:4], end_date[4:]
    prev_mmdd = PREV_QUARTER[mmdd]
    prev_year = str(int(year) - 1) if mmdd == '0331' else year
    return prev_year + prev_mmdd


def _expand_to_trading_days(yoy_by_ann: pd.Series, trade_dates: pd.Index) -> pd.Series:
    """将公告日的同比值展开到所有交易日"""
    yoy_with_sentinel = yoy_by_ann.astype(object).fillna(SENTINEL)
    all_dates = yoy_with_sentinel.index.union(trade_dates).sort_values()
    result = yoy_with_sentinel.reindex(all_dates).ffill().reindex(trade_dates)
    return pd.to_numeric(result.replace(SENTINEL, np.nan), errors='coerce')


def _derive_single_quarter(pit_cum: pd.DataFrame, pit_sq: pd.DataFrame) -> pd.DataFrame:
    """从累计值宽表推算单季值宽表"""
    result = pd.DataFrame(index=pit_cum.index, dtype=float)

    for end_date in pit_cum.columns:
        if end_date[4:] == '0331':
            result[end_date] = pit_cum[end_date]
        else:
            prev_end = _prev_quarter_end(end_date)
            sq = (pit_cum[end_date] - pit_cum[prev_end]
                  if prev_end in pit_cum.columns
                  else pd.Series(np.nan, index=pit_cum.index))
            if end_date in pit_sq.columns:
                sq = sq.fillna(pit_sq[end_date])
            result[end_date] = sq

    for end_date in pit_sq.columns:
        if end_date not in result.columns:
            result[end_date] = pit_sq[end_date]

    return result.reindex(sorted(result.columns), axis=1)


def _winsorize_row(row: pd.Series, pct: float) -> pd.Series:
    valid = row.dropna()
    if len(valid) < 10:
        return row
    return row.clip(lower=valid.quantile(pct), upper=valid.quantile(1 - pct))


# ==================== 使用预计算映射表的 Worker ====================

def _worker_yoy_v3(args):
    """
    计算单只股票的同比增速序列（预计算优化版）

    优化点：直接加载预计算的 latest_end，避免 groupby
    """
    stock, stock_df, latest_end_df, field, trade_dates_arr = args
    trade_dates = pd.Index(trade_dates_arr)

    # 数据准备和去重
    TYPE_PRIORITY = {'4': 0, '1': 1, '3': 2, '2': 3}
    df = stock_df[['ann_date', 'end_date', 'report_type', field]].copy()
    df = df.dropna(subset=[field])
    if len(df) == 0:
        return stock, pd.Series(np.nan, index=trade_dates)

    df['_p'] = df['report_type'].map(TYPE_PRIORITY)
    df = (df.sort_values(['ann_date', 'end_date', '_p'])
            .drop_duplicates(subset=['ann_date', 'end_date'], keep='first')
            .drop(columns='_p'))

    # 核心优化：直接加载预计算的 latest_end
    latest_end = latest_end_df[latest_end_df['ts_code'] == stock].set_index('ann_date')['latest_end_date']

    if len(latest_end) == 0:
        return stock, pd.Series(np.nan, index=trade_dates)

    # 构建 PIT 宽表
    def to_pit_wide(sub_df):
        if len(sub_df) == 0:
            return pd.DataFrame()
        pv = sub_df.pivot(index='ann_date', columns='end_date', values=field)
        return pv.sort_index().ffill()

    cum_wide = to_pit_wide(df[df['report_type'].isin({'1', '4'})])
    sq_wide = to_pit_wide(df[df['report_type'].isin({'2', '3'})])

    if len(cum_wide.columns) == 0:
        return stock, pd.Series(np.nan, index=trade_dates)

    # 计算单季值
    sq = _derive_single_quarter(cum_wide, sq_wide)
    if len(sq.columns) == 0:
        return stock, pd.Series(np.nan, index=trade_dates)

    # 向量化同比计算
    sq_prev_shifted = sq.rename(columns=lambda c: str(int(c[:4]) + 1) + c[4:])
    common_cols = sq.columns.intersection(sq_prev_shifted.columns)

    if len(common_cols) == 0:
        return stock, pd.Series(np.nan, index=trade_dates)

    curr = sq[common_cols]
    prev = sq_prev_shifted[common_cols]
    yoy_wide = ((curr - prev) / prev).where(
        (curr.notna()) & (prev.notna()) & (prev > 0)
    )

    # MultiIndex reindex 取数
    yoy_stacked = yoy_wide.stack()
    multi_idx = pd.MultiIndex.from_arrays([
        latest_end.index,
        latest_end.values
    ], names=['ann_date', 'end_date'])

    yoy_by_ann = yoy_stacked.reindex(multi_idx)
    yoy_by_ann.index = latest_end.index

    # 展开到交易日
    result = _expand_to_trading_days(yoy_by_ann, trade_dates)

    return stock, result


def _worker_yoy_cumulative_v3(args):
    """计算累计值同比增速（预计算优化版）"""
    stock, stock_df, latest_end_df, field, trade_dates_arr = args
    trade_dates = pd.Index(trade_dates_arr)

    # 数据准备
    TYPE_PRIORITY = {'4': 0, '1': 1, '3': 2, '2': 3}
    df = stock_df[['ann_date', 'end_date', 'report_type', field]].copy()
    df = df.dropna(subset=[field])
    if len(df) == 0:
        return stock, pd.Series(np.nan, index=trade_dates)

    df['_p'] = df['report_type'].map(TYPE_PRIORITY)
    df = (df.sort_values(['ann_date', 'end_date', '_p'])
            .drop_duplicates(subset=['ann_date', 'end_date'], keep='first')
            .drop(columns='_p'))

    # 加载预计算的 latest_end
    latest_end = latest_end_df[latest_end_df['ts_code'] == stock].set_index('ann_date')['latest_end_date']

    if len(latest_end) == 0:
        return stock, pd.Series(np.nan, index=trade_dates)

    # 构建累计值 PIT 宽表
    cum_wide = df[df['report_type'].isin({'1', '4'})].pivot(
        index='ann_date', columns='end_date', values=field
    ).sort_index().ffill()

    if len(cum_wide.columns) == 0:
        return stock, pd.Series(np.nan, index=trade_dates)

    cum_wide = cum_wide.reindex(sorted(cum_wide.columns), axis=1)

    # 向量化同比计算
    cum_prev_shifted = cum_wide.rename(columns=lambda c: str(int(c[:4]) + 1) + c[4:])
    common_cols = cum_wide.columns.intersection(cum_prev_shifted.columns)

    if len(common_cols) == 0:
        return stock, pd.Series(np.nan, index=trade_dates)

    curr = cum_wide[common_cols]
    prev = cum_prev_shifted[common_cols]
    yoy_wide = ((curr - prev) / prev).where(
        (curr.notna()) & (prev.notna()) & (prev > 0)
    )

    # MultiIndex reindex 取数
    yoy_stacked = yoy_wide.stack()
    multi_idx = pd.MultiIndex.from_arrays([
        latest_end.index,
        latest_end.values
    ], names=['ann_date', 'end_date'])

    yoy_by_ann = yoy_stacked.reindex(multi_idx)
    yoy_by_ann.index = latest_end.index

    # 展开到交易日
    result = _expand_to_trading_days(yoy_by_ann, trade_dates)

    return stock, result


# ==================== FinancialMatrixBuilderV3 类 ====================

class FinancialMatrixBuilderV3:
    """财务报表 PIT 矩阵构建器 v3（预计算优化版）"""

    def __init__(
        self,
        data_file: Optional[Path] = None,
        latest_end_file: Optional[Path] = None,
        trade_dates: Optional[pd.Index] = None,
        start_date: str = '20150101',
        n_workers: int = None,
    ):
        self.data_file = data_file or Config.INCOME_DATA_FILE
        self.latest_end_file = latest_end_file or Config.SUPPLEMENTARY_DATA_DIR / 'ann_date_latest_end.csv'
        self._trade_dates = trade_dates
        self.start_date = start_date
        self.n_workers = n_workers or max(1, multiprocessing.cpu_count() // 2)
        self._df = None
        self._latest_end_df = None

    def _load_data(self) -> pd.DataFrame:
        if self._df is not None:
            return self._df
        logger.info(f"加载财务数据: {self.data_file}")
        df = pd.read_csv(
            self.data_file,
            dtype={'ann_date': str, 'f_ann_date': str, 'end_date': str, 'report_type': str}
        )
        df = df[df['report_type'].isin(['1', '2', '3', '4'])].copy()
        df = df[df['ann_date'].notna() & (df['ann_date'] != '')].copy()
        logger.info(f"有效记录: {len(df)} 条")
        self._df = df
        return df

    def _load_latest_end(self) -> pd.DataFrame:
        """加载预计算的公告日-最新报告期映射表"""
        if self._latest_end_df is not None:
            return self._latest_end_df

        if not self.latest_end_file.exists():
            raise FileNotFoundError(
                f"预计算映射表不存在: {self.latest_end_file}\n"
                f"请先运行: python scripts/data_fetching/build_ann_date_latest_end.py"
            )

        logger.info(f"加载预计算映射表: {self.latest_end_file}")
        df = pd.read_csv(
            self.latest_end_file,
            dtype={'ts_code': str, 'ann_date': str, 'latest_end_date': str, 'report_type': str}
        )
        logger.info(f"映射表记录: {len(df)} 条")
        self._latest_end_df = df
        return df

    def _load_trade_dates(self) -> pd.Index:
        if self._trade_dates is not None:
            return self._trade_dates
        cal = pd.read_csv(Config.TRADE_CALENDAR_FILE, dtype=str)
        cal = cal[(cal['is_open'] == '1') & (cal['cal_date'] >= self.start_date)]
        self._trade_dates = pd.Index(cal['cal_date'].sort_values().values)
        logger.info(f"交易日历: {len(self._trade_dates)} 个交易日")
        return self._trade_dates

    def _run_parallel(self, worker_fn, field: str, output_file: Path) -> pd.DataFrame:
        """通用并行执行框架（预计算优化）"""
        df = self._load_data()
        latest_end_df = self._load_latest_end()
        trade_dates = self._load_trade_dates()
        all_stocks = df['ts_code'].unique()
        logger.info(f"共 {len(all_stocks)} 只股票，{len(trade_dates)} 个交易日，{self.n_workers} 进程")

        grouped = df.groupby('ts_code')
        tasks = [
            (s, grouped.get_group(s).reset_index(drop=True), latest_end_df, field, trade_dates.values)
            for s in all_stocks
        ]

        result = pd.DataFrame(index=trade_dates, columns=all_stocks, dtype=float)
        with ProcessPoolExecutor(max_workers=self.n_workers,
                                 mp_context=multiprocessing.get_context('fork')) as executor:
            futures = {executor.submit(worker_fn, t): t[0] for t in tasks}
            done = 0
            for future in as_completed(futures):
                stock, series = future.result()
                result[stock] = series
                done += 1
                if done % 500 == 0:
                    logger.info(f"  进度: {done}/{len(all_stocks)}")

        logger.info(f"完成，非空率: {result.notna().mean().mean():.1%}")
        save_matrix(result, output_file)
        logger.info(f"已保存: {output_file}")
        return result

    def yoy(self, field: str, winsorize_pct: float = 0.01,
            output_file: Optional[Path] = None) -> pd.DataFrame:
        """构建同比增速矩阵（预计算优化版）"""
        logger.info(f"构建同比增速矩阵: {field}")
        if output_file is None:
            output_file = Config.MATRIX_DATA_DIR / f'{field}_yoy_matrix.csv'

        result = self._run_parallel(_worker_yoy_v3, field, output_file)

        if winsorize_pct > 0:
            logger.info(f"截面 winsorize ({winsorize_pct*100:.0f}%)...")
            result = result.apply(lambda row: _winsorize_row(row, winsorize_pct), axis=1)
            save_matrix(result, output_file)

        return result

    def yoy_cumulative(self, field: str, winsorize_pct: float = 0.01,
                       output_file: Optional[Path] = None) -> pd.DataFrame:
        """构建累计值同比增速矩阵（预计算优化版）"""
        logger.info(f"构建累计值同比增速矩阵: {field}")
        if output_file is None:
            output_file = Config.MATRIX_DATA_DIR / f'{field}_yoy_matrix.csv'

        result = self._run_parallel(_worker_yoy_cumulative_v3, field, output_file)

        if winsorize_pct > 0:
            logger.info(f"截面 winsorize ({winsorize_pct*100:.0f}%)...")
            result = result.apply(lambda row: _winsorize_row(row, winsorize_pct), axis=1)
            save_matrix(result, output_file)

        return result


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='财务矩阵构建器 v3（预计算优化版）')
    parser.add_argument('--field', default='n_income_attr_p', help='财务字段')
    parser.add_argument('--type', choices=['yoy', 'yoy_cum'],
                       default='yoy', help='计算类型')

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    builder = FinancialMatrixBuilderV3(start_date='20230101', n_workers=4)

    if args.type == 'yoy':
        builder.yoy(args.field)
    elif args.type == 'yoy_cum':
        builder.yoy_cumulative(args.field)
