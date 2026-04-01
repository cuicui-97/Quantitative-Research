"""
财务报表 PIT 矩阵构建器

从财务报表数据（income.csv 等）构建每日 PIT 因子矩阵。

核心设计：
  两层结构：
  - 底层通用方法：pit_single_quarter(field) / pit_cumulative(field)
    任意财务字段均可复用，不需要重写底层逻辑
  - 上层因子方法：yoy(field) / ttm(field)
    基于底层矩阵做衍生计算

  单股票处理（向量化，不循环日期）：
  - pivot(index=ann_date, columns=end_date) 得到小宽表
  - ffill 到交易日，得到每日最新已知值
  - 列减列推算单季值
  - 列对列计算同比/TTM

  PIT 正确性：
  - 纳入 type=1/2/3/4，type=3/4 是追溯调整版本
  - 同一 (ann_date, end_date) 下：type 4>1（累计），3>2（单季）
  - 对每个交易日 T，只使用 ann_date <= T 的数据
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


def _prev_quarter_end(end_date: str) -> str:
    year, mmdd = end_date[:4], end_date[4:]
    prev_mmdd = PREV_QUARTER[mmdd]
    prev_year = str(int(year) - 1) if mmdd == '0331' else year
    return prev_year + prev_mmdd


def _same_quarter_last_year(end_date: str) -> str:
    return str(int(end_date[:4]) - 1) + end_date[4:]


def _build_pit_tables(stock_df: pd.DataFrame, field: str, trade_dates: pd.Index):
    """
    为单只股票构建 PIT 宽表（累计 + 单季）。

    对每个交易日 T，每个 end_date 列的值是 ann_date <= T 的最新已知值。
    type 优先级：同一 (ann_date, end_date) 下，4>1（累计），3>2（单季）。

    Args:
        stock_df: 单只股票的财务记录（含 report_type / ann_date / end_date / field）
        field: 财务字段名
        trade_dates: 交易日索引

    Returns:
        (pit_cum, pit_sq)
        pit_cum: index=trade_dates, columns=end_date，累计值
        pit_sq:  index=trade_dates, columns=end_date，单季值
    """
    TYPE_PRIORITY = {'4': 0, '1': 1, '3': 2, '2': 3}
    df = stock_df[['ann_date', 'end_date', 'report_type', field]].copy()
    df = df.dropna(subset=[field])
    df['_p'] = df['report_type'].map(TYPE_PRIORITY)
    df = (df.sort_values(['ann_date', 'end_date', '_p'])
            .drop_duplicates(subset=['ann_date', 'end_date'], keep='first')
            .drop(columns='_p'))

    def _to_pit(sub: pd.DataFrame) -> pd.DataFrame:
        if len(sub) == 0:
            return pd.DataFrame(index=trade_dates)
        pv = sub.pivot_table(index='ann_date', columns='end_date',
                             values=field, aggfunc='last')
        all_idx = pv.index.union(trade_dates).sort_values()
        return pv.reindex(all_idx).ffill().reindex(trade_dates)

    t_cum = df[df['report_type'].isin({'1', '4'})][['ann_date', 'end_date', field]]
    t_sq  = df[df['report_type'].isin({'2', '3'})][['ann_date', 'end_date', field]]
    return _to_pit(t_cum), _to_pit(t_sq)


def _derive_single_quarter(pit_cum: pd.DataFrame, pit_sq: pd.DataFrame) -> pd.DataFrame:
    """
    从累计值宽表推算单季值宽表（向量化，不循环日期）。

    Q1（0331）：单季 = 累计
    Q2/Q3/Q4：单季 = 当期累计 - 上季度累计
    推算失败时用 pit_sq 补充。
    """
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


def _latest_col_per_row(table: pd.DataFrame) -> pd.Series:
    """
    对每行返回最右边非 NaN 列的列名（向量化）。
    table 列必须已按升序排列。
    """
    cols = table.columns.values
    mask = table.notna().values
    result = pd.Series(index=table.index, dtype=object)
    for i, row_mask in enumerate(mask):
        idx = np.where(row_mask)[0]
        if len(idx) > 0:
            result.iloc[i] = cols[idx[-1]]
    return result


def _worker_single_quarter(args):
    """并行 worker：计算单只股票的 PIT 单季值序列"""
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
    """并行 worker：计算单只股票的 PIT 累计值序列（最新报告期）"""
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


def _worker_yoy(args):
    """并行 worker：计算单只股票的同比增速序列"""
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
        yoy_end = _same_quarter_last_year(end_date)
        if end_date not in col_map or yoy_end not in col_map:
            continue
        curr = sq_arr[i, col_map[end_date]]
        prev = sq_arr[i, col_map[yoy_end]]
        if np.isnan(curr) or np.isnan(prev) or prev <= 0:
            continue
        result.iloc[i] = (curr - prev) / prev

    return stock, result


def _worker_ttm(args):
    """并行 worker：计算单只股票的 TTM（滚动12个月）序列"""
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
        # TTM = 最近4个季度单季值之和
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


class FinancialMatrixBuilder:
    """
    财务报表 PIT 矩阵构建器

    底层通用方法（任意财务字段）：
        pit_single_quarter(field)   →  PIT 单季矩阵
        pit_cumulative(field)       →  PIT 累计矩阵（最新报告期累计值）
        yoy(field)                  →  同比增速矩阵
        ttm(field)                  →  TTM 滚动12个月矩阵

    用法：
        builder = FinancialMatrixBuilder()
        # 归母净利润单季
        builder.pit_single_quarter('n_income_attr_p')
        # 营收同比增速
        builder.yoy('revenue')
        # 净利润 TTM
        builder.ttm('n_income_attr_p')
    """

    def __init__(
        self,
        data_file: Optional[Path] = None,
        trade_dates: Optional[pd.Index] = None,
        start_date: str = '20150101',
        n_workers: int = None,
    ):
        """
        Args:
            data_file: 财务数据文件路径，默认 Config.INCOME_DATA_FILE
            trade_dates: 交易日列表，优先级高于 start_date
            start_date: 交易日起始日期
            n_workers: 并行进程数，默认 CPU 核数的一半
        """
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
        logger.info(f"交易日历: {len(self._trade_dates)} 个交易日（{self.start_date} 至今）")
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

    # ==================== 底层通用方法 ====================

    def pit_single_quarter(
        self,
        field: str,
        output_file: Optional[Path] = None,
    ) -> pd.DataFrame:
        """
        构建任意财务字段的 PIT 单季矩阵（dates × stocks）

        每个格子：T日能看到的最新报告期的单季值。
        用累计值（type=1/4）推算，单季值（type=2/3）补充。

        Args:
            field: 财务字段名，如 'n_income_attr_p'、'revenue'
            output_file: 输出路径，默认 matrices/{field}_sq_matrix.csv
        """
        logger.info(f"构建 PIT 单季矩阵: {field}")
        if output_file is None:
            output_file = Config.MATRIX_DATA_DIR / f'{field}_sq_matrix.csv'
        return self._run_parallel(_worker_single_quarter, field, output_file)

    def pit_cumulative(
        self,
        field: str,
        output_file: Optional[Path] = None,
    ) -> pd.DataFrame:
        """
        构建任意财务字段的 PIT 累计矩阵（dates × stocks）

        每个格子：T日能看到的最新报告期的累计值（如年初至今）。

        Args:
            field: 财务字段名
            output_file: 输出路径，默认 matrices/{field}_cum_matrix.csv
        """
        logger.info(f"构建 PIT 累计矩阵: {field}")
        if output_file is None:
            output_file = Config.MATRIX_DATA_DIR / f'{field}_cum_matrix.csv'
        return self._run_parallel(_worker_cumulative, field, output_file)

    def yoy(
        self,
        field: str,
        winsorize_pct: float = 0.01,
        output_file: Optional[Path] = None,
    ) -> pd.DataFrame:
        """
        构建任意财务字段的同比增速矩阵（dates × stocks）

        同比增速 = (当季单季值 - 去年同季单季值) / 去年同季单季值
        分母为负或为零时结果为 NaN，最后做截面 winsorize。

        Args:
            field: 财务字段名
            winsorize_pct: 截面 winsorize 比例（上下各去掉该比例极端值）
            output_file: 输出路径，默认 matrices/{field}_yoy_matrix.csv
        """
        logger.info(f"构建同比增速矩阵: {field}")
        if output_file is None:
            output_file = Config.MATRIX_DATA_DIR / f'{field}_yoy_matrix.csv'
        result = self._run_parallel(_worker_yoy, field, output_file)

        if winsorize_pct > 0:
            logger.info(f"截面 winsorize（上下各 {winsorize_pct:.0%}）...")
            result = result.apply(lambda row: _winsorize_row(row, winsorize_pct), axis=1)
            save_matrix(result, output_file)

        return result

    def ttm(
        self,
        field: str,
        output_file: Optional[Path] = None,
    ) -> pd.DataFrame:
        """
        构建任意财务字段的 TTM（滚动12个月）矩阵（dates × stocks）

        TTM = 最近4个季度单季值之和。
        任意一个季度缺失则为 NaN。

        Args:
            field: 财务字段名
            output_file: 输出路径，默认 matrices/{field}_ttm_matrix.csv
        """
        logger.info(f"构建 TTM 矩阵: {field}")
        if output_file is None:
            output_file = Config.MATRIX_DATA_DIR / f'{field}_ttm_matrix.csv'
        return self._run_parallel(_worker_ttm, field, output_file)
