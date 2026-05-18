#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
因子矩阵核心数据结构

内部使用 pandas DataFrame 存储，提供统一接口
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union, List, Dict, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class FactorMatrix:
    """
    因子矩阵数据结构

    Attributes:
        name: 因子名称（如 'pb', 'momentum_20d'）
        data: 核心数据，DataFrame (dates × stocks)
        description: 因子描述（可选）

    Example:
        >>> fm = FactorMatrix.from_csv('pb_matrix.csv', name='pb')
        >>> fm2 = fm.rank(axis=1)  # 截面排名
        >>> fm3 = fm2.zscore(axis=1)  # 截面标准化
    """
    name: str
    data: pd.DataFrame
    description: str = ""

    def __post_init__(self):
        """验证数据类型"""
        if not isinstance(self.data, pd.DataFrame):
            raise TypeError(f"data 必须是 DataFrame，得到 {type(self.data)}")
        # 确保数值类型
        self.data = self.data.astype(np.float32)

    # ==================== 创建方法 ====================

    @classmethod
    def from_csv(cls, path: Union[str, Path], name: Optional[str] = None, description: str = "") -> FactorMatrix:
        """从 CSV 文件加载"""
        path = Path(path)
        df = pd.read_csv(path, index_col=0)

        if name is None:
            name = path.stem.replace('_matrix', '')

        return cls(name=name, data=df, description=description)

    @classmethod
    def from_parquet(cls, path: Union[str, Path], name: Optional[str] = None, description: str = "") -> FactorMatrix:
        """从 Parquet 文件加载（更快、更省内存）"""
        path = Path(path)
        df = pd.read_parquet(path)

        if name is None:
            name = path.stem.replace('_matrix', '')

        return cls(name=name, data=df, description=description)

    @classmethod
    def from_pandas(cls, df: pd.DataFrame, name: str, description: str = "") -> FactorMatrix:
        """从 DataFrame 创建"""
        return cls(name=name, data=df.copy(), description=description)

    @classmethod
    def from_numpy(
        cls,
        values: np.ndarray,
        dates: Union[np.ndarray, List],
        stocks: Union[np.ndarray, List],
        name: str = "",
        description: str = ""
    ) -> FactorMatrix:
        """从 NumPy 数组创建"""
        df = pd.DataFrame(
            values.astype(np.float32),
            index=dates if isinstance(dates, pd.Index) else pd.Index(dates),
            columns=stocks
        )
        return cls(name=name, data=df, description=description)

    # ==================== 属性 ====================

    @property
    def shape(self) -> Tuple[int, int]:
        """矩阵形状"""
        return self.data.shape

    @property
    def values(self) -> np.ndarray:
        """底层 NumPy 数组"""
        return self.data.values

    @property
    def index(self) -> pd.Index:
        """日期索引"""
        return self.data.index

    @property
    def columns(self) -> pd.Index:
        """股票代码列"""
        return self.data.columns

    @property
    def dates(self) -> pd.Index:
        """日期索引（别名）"""
        return self.data.index

    @property
    def stocks(self) -> pd.Index:
        """股票代码（别名）"""
        return self.data.columns

    # ==================== 核心运算 ====================

    def rank(self, axis: int = 1, pct: bool = True) -> FactorMatrix:
        """截面排名"""
        result = self.data.rank(axis=axis, pct=pct)
        return FactorMatrix(
            name=f"{self.name}_rank",
            data=result,
            description=f"Rank of {self.name}"
        )

    def zscore(self, axis: int = 1) -> FactorMatrix:
        """截面标准化 (x - mean) / std"""
        mean = self.data.mean(axis=axis, skipna=True)
        std = self.data.std(axis=axis, skipna=True)

        if axis == 1:
            result = (self.data.sub(mean, axis=0)).div(std + 1e-10, axis=0)
        else:
            result = (self.data.sub(mean, axis=1)).div(std + 1e-10, axis=1)

        return FactorMatrix(
            name=f"{self.name}_zscore",
            data=result,
            description=f"Z-Score of {self.name}"
        )

    def pct_change(self, periods: int = 1) -> FactorMatrix:
        """百分比变化"""
        result = self.data.pct_change(periods)
        return FactorMatrix(
            name=f"{self.name}_chg{periods}",
            data=result,
            description=f"{periods}-period change of {self.name}"
        )

    def shift(self, periods: int = 1) -> FactorMatrix:
        """位移"""
        result = self.data.shift(periods)
        return FactorMatrix(
            name=f"{self.name}_shift{periods}",
            data=result,
            description=f"Shifted {self.name}"
        )

    def rolling_mean(self, window: int) -> FactorMatrix:
        """滚动均值"""
        result = self.data.rolling(window=window, min_periods=1).mean()
        return FactorMatrix(
            name=f"{self.name}_ma{window}",
            data=result,
            description=f"{window}-period MA of {self.name}"
        )

    def fillna(self, value: float = 0.0) -> FactorMatrix:
        """填充缺失值"""
        result = self.data.fillna(value)
        return FactorMatrix(
            name=f"{self.name}_filled",
            data=result,
            description=self.description
        )

    def clip(self, lower: Optional[float] = None, upper: Optional[float] = None) -> FactorMatrix:
        """截断极值"""
        result = self.data.clip(lower=lower, upper=upper)
        return FactorMatrix(
            name=f"{self.name}_clipped",
            data=result,
            description=self.description
        )

    # ==================== 矩阵运算 ====================

    def _binary_op(self, other, op, name_suffix: str) -> FactorMatrix:
        """通用二元运算（自动对齐）"""
        if isinstance(other, FactorMatrix):
            result = op(self.data, other.data)
        else:
            result = op(self.data, other)

        return FactorMatrix(
            name=f"{self.name}{name_suffix}",
            data=result
        )

    def __add__(self, other):
        return self._binary_op(other, lambda a, b: a + b, "_add")

    def __sub__(self, other):
        return self._binary_op(other, lambda a, b: a - b, "_sub")

    def __mul__(self, other):
        return self._binary_op(other, lambda a, b: a * b, "_mul")

    def __truediv__(self, other):
        return self._binary_op(other, lambda a, b: a / b, "_div")

    def __neg__(self):
        return FactorMatrix(
            name=f"neg_{self.name}",
            data=-self.data
        )

    # ==================== 对齐 ====================

    def align(self, other: FactorMatrix) -> Tuple[FactorMatrix, FactorMatrix]:
        """与另一个矩阵对齐（取交集）"""
        left, right = self.data.align(other.data, join='inner', axis=0)  # 对齐行
        left, right = left.align(right, join='inner', axis=1)  # 对齐列

        return (
            FactorMatrix(name=self.name, data=left, description=self.description),
            FactorMatrix(name=other.name, data=right, description=other.description)
        )

    def align_many(self, *others: FactorMatrix) -> List[FactorMatrix]:
        """与多个矩阵对齐"""
        # 找到所有矩阵的交集
        common_index = self.data.index
        common_columns = self.data.columns

        for other in others:
            common_index = common_index.intersection(other.data.index)
            common_columns = common_columns.intersection(other.data.columns)

        # 筛选所有矩阵
        result = [self.slice(common_index, common_columns)]
        for other in others:
            result.append(other.slice(common_index, common_columns))

        return result

    def slice(self, index: pd.Index, columns: pd.Index) -> FactorMatrix:
        """切片到指定索引和列"""
        result = self.data.loc[index, columns]
        return FactorMatrix(
            name=self.name,
            data=result,
            description=self.description
        )

    def slice_dates(self, start: Optional[str] = None, end: Optional[str] = None) -> FactorMatrix:
        """按日期范围切片"""
        if start is None and end is None:
            return self

        mask = pd.Series(True, index=self.data.index)
        if start is not None:
            mask &= self.data.index >= start
        if end is not None:
            mask &= self.data.index <= end

        return FactorMatrix(
            name=self.name,
            data=self.data.loc[mask],
            description=self.description
        )

    def slice_stocks(self, stocks: List[str]) -> FactorMatrix:
        """按股票列表切片"""
        valid_stocks = [s for s in stocks if s in self.data.columns]
        if len(valid_stocks) != len(stocks):
            missing = set(stocks) - set(valid_stocks)
            logger.warning(f"以下股票不存在: {missing}")

        return FactorMatrix(
            name=self.name,
            data=self.data[valid_stocks],
            description=self.description
        )

    # ==================== 统计 ====================

    def mean(self, axis: Optional[int] = None):
        """均值"""
        return self.data.mean(axis=axis, skipna=True)

    def std(self, axis: Optional[int] = None):
        """标准差"""
        return self.data.std(axis=axis, skipna=True)

    def min(self, axis: Optional[int] = None):
        """最小值"""
        return self.data.min(axis=axis, skipna=True)

    def max(self, axis: Optional[int] = None):
        """最大值"""
        return self.data.max(axis=axis, skipna=True)

    def sum(self, axis: Optional[int] = None):
        """求和"""
        return self.data.sum(axis=axis, skipna=True)

    def count(self, axis: Optional[int] = None):
        """非空计数"""
        return self.data.count(axis=axis)

    def info(self) -> Dict:
        """矩阵信息"""
        return {
            'name': self.name,
            'shape': self.shape,
            'memory_mb': self.data.memory_usage(deep=True).sum() / 1024 / 1024,
            'null_ratio': self.data.isna().mean().mean(),
            'description': self.description
        }

    # ==================== 存储 ====================

    def to_csv(self, path: Union[str, Path]):
        """保存为 CSV"""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.data.to_csv(path)
        logger.info(f"矩阵已保存: {path}")

    def to_parquet(self, path: Union[str, Path], compression: str = 'snappy'):
        """保存为 Parquet（更快、更省空间）"""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.data.to_parquet(path, engine='pyarrow', compression=compression, index=True)
        logger.info(f"矩阵已保存: {path} (压缩: {compression})")

    def to_pandas(self) -> pd.DataFrame:
        """返回底层 DataFrame（直接引用，不拷贝）"""
        return self.data

    def to_numpy(self) -> np.ndarray:
        """返回 NumPy 数组"""
        return self.data.values

    # ==================== 显示 ====================

    def __repr__(self) -> str:
        return f"FactorMatrix('{self.name}', shape={self.shape})"

    def head(self, n: int = 5) -> pd.DataFrame:
        """查看前 n 行"""
        return self.data.head(n)

    def tail(self, n: int = 5) -> pd.DataFrame:
        """查看后 n 行"""
        return self.data.tail(n)
