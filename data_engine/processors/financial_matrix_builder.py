"""
财务报表 PIT 矩阵构建器（优化版）

优化点：
1. 提取公共工具函数，减少代码冗余
2. 统一 PIT 展开逻辑（哨兵值 + ffill）
3. 统一最新报告期取值逻辑（numpy 高级索引）
4. 所有 worker 函数使用相同的最新报告期识别逻辑

从财务报表数据（income.csv 等）构建每日 PIT 因子矩阵。
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


def _same_quarter_last_year(end_date: str) -> str:
    return str(int(end_date[:4]) - 1) + end_date[4:]


def _to_pit(sub: pd.DataFrame, field: str, trade_dates: pd.Index) -> pd.DataFrame:
    """将公告日数据转换为 PIT 宽表（展开到所有交易日）"""
    if len(sub) == 0:
        return pd.DataFrame(index=trade_dates)
    pv = sub.pivot_table(index='ann_date', columns='end_date',
                         values=field, aggfunc='last')
    all_idx = pv.index.union(trade_dates).sort_values()
    return pv.reindex(all_idx).ffill().reindex(trade_dates)


def _latest_col_per_row(table: pd.DataFrame) -> pd.Series:
    """对每行返回最右边非 NaN 列的列名（向量化）"""
    mask = table.notna().values
    mask_rev = mask[:, ::-1]
    rev_pos = mask_rev.argmax(axis=1)
    has_any = mask.any(axis=1)
    col_indices = np.where(has_any, mask.shape[1] - 1 - rev_pos, -1)

    result = pd.Series(index=table.index, dtype=object)
    result[has_any] = table.columns.values[col_indices[has_any]]
    return result


def _get_latest_end_date(stock_df: pd.DataFrame, end_date_filter: Optional[tuple] = None) -> pd.Series:
    """
    从原始数据获取每个公告日的最大报告期

    Args:
        stock_df: 原始财务数据
        end_date_filter: 可选，筛选特定报告期后缀，如 ('0630', '1231')

    Returns:
        Series: index=ann_date, value=max_end_date
    """
    df = stock_df.copy()
    if end_date_filter:
        df = df[df['end_date'].str.endswith(end_date_filter)]
    return df.groupby('ann_date')['end_date'].max()


def _lookup_latest_value(yoy_wide: pd.DataFrame, latest_end_date: pd.Series) -> pd.Series:
    """
    使用 numpy 高级索引，从 yoy_wide 中取每行最新报告期的同比值

    Args:
        yoy_wide: 同比宽表，index=ann_date, columns=end_date
        latest_end_date: 每个公告日的最大报告期

    Returns:
        Series: 每行的最新同比值（无效时为 NaN）
    """
    latest_end_date = latest_end_date.reindex(yoy_wide.index)

    row_idx = np.arange(len(yoy_wide))
    col_idx = [yoy_wide.columns.get_loc(c) if c in yoy_wide.columns else -1
               for c in latest_end_date]
    mask_valid = np.array(col_idx) >= 0

    result = pd.Series(np.nan, index=yoy_wide.index)
    if mask_valid.any():
        result.iloc[mask_valid] = yoy_wide.values[
            row_idx[mask_valid],
            np.array(col_idx)[mask_valid]
        ]
    return result


def _expand_to_trading_days(yoy_by_ann: pd.Series, trade_dates: pd.Index) -> pd.Series:
    """
    将公告日的同比值展开到所有交易日
    使用哨兵值 + ffill 确保无效值不回退到旧周期
    """
    yoy_with_sentinel = yoy_by_ann.astype(object).fillna(SENTINEL)
    all_dates = yoy_with_sentinel.index.union(trade_dates).sort_values()
    result = yoy_with_sentinel.reindex(all_dates).ffill().reindex(trade_dates)
    return pd.to_numeric(result.replace(SENTINEL, np.nan), errors='coerce')


def _derive_single_quarter(pit_cum: pd.DataFrame, pit_sq: pd.DataFrame) -> pd.DataFrame:
    """从累计值宽表推算单季值宽表（向量化）"""
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


# ==================== Worker 函数（精简版） ====================

def _worker_yoy(args):
    """计算单只股票的同比增速序列（标准季度，如净利润、营收）"""
    stock, stock_df, field, trade_dates_arr = args
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

    # 构建宽表（仅在公告日）
    def to_pit_wide(sub_df):
        if len(sub_df) == 0:
            return pd.DataFrame()
        pv = sub_df.pivot(index='ann_date', columns='end_date', values=field)
        return pv.sort_index().ffill()

    cum_wide = to_pit_wide(df[df['report_type'].isin({'1', '4'})])
    sq_wide = to_pit_wide(df[df['report_type'].isin({'2', '3'})])

    if len(cum_wide.columns) == 0:
        return stock, pd.Series(np.nan, index=trade_dates)

    # 计算单季值和同比
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

    # 取每行最新报告期的同比，展开到交易日
    latest_end_date = _get_latest_end_date(stock_df)
    yoy_by_ann = _lookup_latest_value(yoy_wide, latest_end_date)
    result = _expand_to_trading_days(yoy_by_ann, trade_dates)

    return stock, result


def _worker_yoy_cumulative(args):
    """计算累计值同比增速（适用于只有累计值的字段）"""
    stock, stock_df, field, trade_dates_arr = args
    trade_dates = pd.Index(trade_dates_arr)

    pit_cum, _ = _build_pit_tables(stock_df, field, trade_dates)
    if len(pit_cum.columns) == 0:
        return stock, pd.Series(np.nan, index=trade_dates)

    pit_cum = pit_cum.reindex(sorted(pit_cum.columns), axis=1)

    # 向量化同比计算
    cum_prev_shifted = pit_cum.rename(columns=lambda c: str(int(c[:4]) + 1) + c[4:])
    common_cols = pit_cum.columns.intersection(cum_prev_shifted.columns)

    curr = pit_cum[common_cols]
    prev = cum_prev_shifted[common_cols]
    yoy_wide = ((curr - prev) / prev).where(
        (curr.notna()) & (prev.notna()) & (prev > 0)
    )

    # 取最新报告期同比，展开到交易日
    latest_end_date = _get_latest_end_date(stock_df)
    yoy_by_ann = _lookup_latest_value(yoy_wide, latest_end_date)
    result = _expand_to_trading_days(yoy_by_ann, trade_dates)

    return stock, result


def _worker_yoy_semiannual(args):
    """计算半年度同比增速（专用于 ebitda 等只有半年报/年报的字段）"""
    stock, stock_df, field, trade_dates_arr = args
    trade_dates = pd.Index(trade_dates_arr)

    pit_cum, _ = _build_pit_tables(stock_df, field, trade_dates)
    if len(pit_cum.columns) == 0:
        return stock, pd.Series(np.nan, index=trade_dates)

    # 筛选 0630 和 1231
    cols_0630 = sorted([c for c in pit_cum.columns if c.endswith('0630')])
    cols_1231 = sorted([c for c in pit_cum.columns if c.endswith('1231')])

    if len(cols_0630) == 0:
        return stock, pd.Series(np.nan, index=trade_dates)

    # 构建 H1 和 H2
    h1 = pit_cum[cols_0630].copy()
    h1.columns = [c[:4] + 'H1' for c in h1.columns]

    h2_data = pit_cum[cols_1231].copy()
    h2_data.columns = [c[:4] + 'H2' for c in cols_1231]

    h1_cols_needed = [c[:4] + 'H1' for c in cols_1231]
    h1_for_h2 = (h1.reindex(columns=h1_cols_needed)
                 .rename(columns=lambda x: x[:4] + 'H2'))
    h2 = h2_data - h1_for_h2

    # 合并并计算同比
    semi_annual = pd.concat([h1, h2], axis=1)
    semi_annual = semi_annual.reindex(sorted(semi_annual.columns), axis=1)

    prev = semi_annual.shift(2, axis=1)
    mask = (semi_annual > 0) & (prev > 0)
    yoy_wide = ((semi_annual - prev) / prev).where(mask)

    # 取最新报告期同比（筛选 0630/1231），展开到交易日
    latest_end_date = _get_latest_end_date(stock_df, end_date_filter=('0630', '1231'))
    yoy_by_ann = _lookup_latest_value(yoy_wide, latest_end_date)
    result = _expand_to_trading_days(yoy_by_ann, trade_dates)

    return stock, result


# ==================== 其他 Worker（保持不变） ====================

def _worker_single_quarter(args):
    """计算 PIT 单季值序列"""
    stock, stock_df, field, trade_dates_arr = args
    trade_dates = pd.Index(trade_dates_arr)
    pit_cum, pit_sq = _build_pit_tables(stock_df, field, trade_dates)
    sq = _derive_single_quarter(pit_cum, pit_sq)

    if len(sq.columns) == 0:
        return stock, pd.Series(np.nan, index=trade_dates)

    latest_end = _latest_col_per_row(sq)
    col_pos = {c: i for i, c in enumerate(sq.columns)}
    result = pd.Series(np.nan, index=trade_dates)

    for i, end_date in enumerate(latest_end):
        if pd.notna(end_date) and end_date in col_pos:
            result.iloc[i] = sq.iloc[i, col_pos[end_date]]

    return stock, result


def _worker_cumulative(args):
    """计算 PIT 累计值序列"""
    stock, stock_df, field, trade_dates_arr = args
    trade_dates = pd.Index(trade_dates_arr)
    pit_cum, _ = _build_pit_tables(stock_df, field, trade_dates)

    if len(pit_cum.columns) == 0:
        return stock, pd.Series(np.nan, index=trade_dates)

    pit_cum = pit_cum.reindex(sorted(pit_cum.columns), axis=1)
    latest_end = _latest_col_per_row(pit_cum)
    col_pos = {c: i for i, c in enumerate(pit_cum.columns)}
    result = pd.Series(np.nan, index=trade_dates)

    for i, end_date in enumerate(latest_end):
        if pd.notna(end_date) and end_date in col_pos:
            result.iloc[i] = pit_cum.iloc[i, col_pos[end_date]]

    return stock, result


def _worker_ttm(args):
    """计算 TTM 序列"""
    stock, stock_df, field, trade_dates_arr = args
    trade_dates = pd.Index(trade_dates_arr)
    pit_cum, pit_sq = _build_pit_tables(stock_df, field, trade_dates)
    sq = _derive_single_quarter(pit_cum, pit_sq)

    if len(sq.columns) == 0:
        return stock, pd.Series(np.nan, index=trade_dates)

    latest_end = _latest_col_per_row(sq)
    col_map = {c: i for i, c in enumerate(sq.columns)}
    sq_arr = sq.values
    result = pd.Series(np.nan, index=trade_dates)

    for i in np.where(latest_end.notna().values)[0]:
        end_date = latest_end.iloc[i]
        quarters = [end_date]
        for _ in range(3):
            quarters.append(_prev_quarter_end(quarters[-1]))
        if not all(q in col_map for q in quarters):
            continue
        vals = [sq_arr[i, col_map[q]] for q in quarters]
        if any(np.isnan(v) for v in vals):
            continue
        result.iloc[i] = sum(vals)

    return stock, result


def _winsorize_row(row: pd.Series, pct: float) -> pd.Series:
    valid = row.dropna()
    if len(valid) < 10:
        return row
    return row.clip(lower=valid.quantile(pct), upper=valid.quantile(1 - pct))


def _build_pit_tables(stock_df: pd.DataFrame, field: str, trade_dates: pd.Index):
    """构建 PIT 宽表（累计 + 单季）"""
    TYPE_PRIORITY = {'4': 0, '1': 1, '3': 2, '2': 3}
    df = stock_df[['ann_date', 'end_date', 'report_type', field]].copy()
    df = df.dropna(subset=[field])
    df['_p'] = df['report_type'].map(TYPE_PRIORITY)
    df = (df.sort_values(['ann_date', 'end_date', '_p'])
            .drop_duplicates(subset=['ann_date', 'end_date'], keep='first')
            .drop(columns='_p'))

    t_cum = df[df['report_type'].isin({'1', '4'})][['ann_date', 'end_date', field]]
    t_sq = df[df['report_type'].isin({'2', '3'})][['ann_date', 'end_date', field]]
    return _to_pit(t_cum, field, trade_dates), _to_pit(t_sq, field, trade_dates)


# ==================== FinancialMatrixBuilder 类 ====================

class FinancialMatrixBuilder:
    """财务报表 PIT 矩阵构建器（优化版）"""

    def __init__(
        self,
        data_file: Optional[Path] = None,
        trade_dates: Optional[pd.Index] = None,
        start_date: str = '20150101',
        n_workers: int = None,
    ):
        self.data_file = data_file or Config.INCOME_DATA_FILE
        self._trade_dates = trade_dates
        self.start_date = start_date
        self.n_workers = n_workers or max(1, multiprocessing.cpu_count() // 2)
        self._df = None

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

    def _load_trade_dates(self) -> pd.Index:
        if self._trade_dates is not None:
            return self._trade_dates
        cal = pd.read_csv(Config.TRADE_CALENDAR_FILE, dtype=str)
        cal = cal[(cal['is_open'] == '1') & (cal['cal_date'] >= self.start_date)]
        self._trade_dates = pd.Index(cal['cal_date'].sort_values().values)
        logger.info(f"交易日历: {len(self._trade_dates)} 个交易日")
        return self._trade_dates

    def _run_parallel(self, worker_fn, field: str, output_file: Path) -> pd.DataFrame:
        """通用并行执行框架"""
        df = self._load_data()
        trade_dates = self._load_trade_dates()
        all_stocks = df['ts_code'].unique()
        logger.info(f"共 {len(all_stocks)} 只股票，{len(trade_dates)} 个交易日，{self.n_workers} 进程")

        grouped = df.groupby('ts_code')
        tasks = [
            (s, grouped.get_group(s).reset_index(drop=True), field, trade_dates.values)
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

    def pit_single_quarter(self, field: str, output_file: Optional[Path] = None) -> pd.DataFrame:
        logger.info(f"构建 PIT 单季矩阵: {field}")
        if output_file is None:
            output_file = Config.MATRIX_DATA_DIR / f'{field}_sq_matrix.csv'
        return self._run_parallel(_worker_single_quarter, field, output_file)

    def pit_cumulative(self, field: str, output_file: Optional[Path] = None) -> pd.DataFrame:
        logger.info(f"构建 PIT 累计矩阵: {field}")
        if output_file is None:
            output_file = Config.MATRIX_DATA_DIR / f'{field}_cum_matrix.csv'
        return self._run_parallel(_worker_cumulative, field, output_file)

    def yoy(self, field: str, winsorize_pct: float = 0.01,
            output_file: Optional[Path] = None) -> pd.DataFrame:
        logger.info(f"构建同比增速矩阵: {field}")
        if output_file is None:
            output_file = Config.MATRIX_DATA_DIR / f'{field}_yoy_matrix.csv'
        result = self._run_parallel(_worker_yoy, field, output_file)
        if winsorize_pct > 0:
            logger.info(f"截面 winsorize...")
            result = result.apply(lambda row: _winsorize_row(row, winsorize_pct), axis=1)
            save_matrix(result, output_file)
        return result

    def ttm(self, field: str, output_file: Optional[Path] = None) -> pd.DataFrame:
        logger.info(f"构建 TTM 矩阵: {field}")
        if output_file is None:
            output_file = Config.MATRIX_DATA_DIR / f'{field}_ttm_matrix.csv'
        return self._run_parallel(_worker_ttm, field, output_file)

    def yoy_semiannual(self, field: str, winsorize_pct: float = 0.01,
                       output_file: Optional[Path] = None) -> pd.DataFrame:
        logger.info(f"构建半年度同比增速矩阵: {field}")
        if output_file is None:
            output_file = Config.MATRIX_DATA_DIR / f'{field}_yoy_matrix.csv'
        result = self._run_parallel(_worker_yoy_semiannual, field, output_file)
        if winsorize_pct > 0:
            logger.info(f"截面 winsorize...")
            result = result.apply(lambda row: _winsorize_row(row, winsorize_pct), axis=1)
            save_matrix(result, output_file)
        return result

    def yoy_cumulative(self, field: str, winsorize_pct: float = 0.01,
                       output_file: Optional[Path] = None) -> pd.DataFrame:
        logger.info(f"构建累计值同比增速矩阵: {field}")
        if output_file is None:
            output_file = Config.MATRIX_DATA_DIR / f'{field}_yoy_matrix.csv'
        result = self._run_parallel(_worker_yoy_cumulative, field, output_file)
        if winsorize_pct > 0:
            logger.info(f"截面 winsorize...")
            result = result.apply(lambda row: _winsorize_row(row, winsorize_pct), axis=1)
            save_matrix(result, output_file)
        return result
