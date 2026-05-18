#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
统一因子构建框架 v3.0

设计目标：
1. 全面矩阵化计算（无逐股票循环）
2. 自动中间结果缓存与复用
3. 声明式因子配置（YAML/JSON）
4. 延迟执行与计算图优化
5. 严格防未来函数（所有数据标记可用时点）

数据时点约定：
- 'open': T日开盘时可用（如昨日收盘价）
- 'close': T日收盘后可用（如当日收盘价、成交量）
- 'next_open': T+1日开盘前可用（如日终指标：市值、PE等）
"""
from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any, Union, Set, Tuple
from collections import defaultdict

import numpy as np
import pandas as pd
import yaml

from config.config import Config
from data_engine.core.factor_matrix import FactorMatrix
from data_engine.processors.matrix_io import save_matrix, load_matrix

logger = logging.getLogger(__name__)


class DataAvailability(Enum):
    """数据可用时点"""
    OPEN = "open"           # T日开盘时可用
    CLOSE = "close"         # T日收盘后可用
    NEXT_OPEN = "next_open" # T+1日开盘前可用


@dataclass(frozen=True)
class DataSource:
    """
    带时点信息的数据源

    核心机制：根据目标使用时点自动调整数据滞后
    """
    name: str
    matrix: pd.DataFrame
    availability: DataAvailability

    def get_for(self, target_when: DataAvailability) -> pd.DataFrame:
        """
        获取适用于目标时点的数据

        例：
        - 市值(availability=NEXT_OPEN).get_for(OPEN) → shift(1)
        - 收盘价(availability=CLOSE).get_for(OPEN) → shift(1)
        """
        if self.availability == target_when:
            return self.matrix

        # 需要滞后以确保无未来函数
        if self.availability == DataAvailability.CLOSE and target_when == DataAvailability.OPEN:
            return self.matrix.shift(1)
        if self.availability == DataAvailability.NEXT_OPEN and target_when == DataAvailability.OPEN:
            return self.matrix.shift(1)
        if self.availability == DataAvailability.NEXT_OPEN and target_when == DataAvailability.CLOSE:
            return self.matrix  # 收盘时使用当日日终数据是安全的

        raise ValueError(f"不支持的数据时点转换: {self.availability} -> {target_when}")


@dataclass
class FactorDefinition:
    """因子定义（声明式）"""
    name: str                           # 因子名称
    description: str                    # 因子描述
    formula: str                        # 公式表达式（如 'close.pct_change(20)'）
    dependencies: List[str]             # 依赖的数据源
    availability: DataAvailability = DataAvailability.CLOSE  # 因子可用时点
    output_name: Optional[str] = None   # 输出文件名
    params: Dict[str, Any] = field(default_factory=dict)  # 额外参数


class ComputationGraph:
    """
    计算图 - 延迟执行核心

    管理因子间的依赖关系，优化执行顺序，复用中间结果
    """

    def __init__(self):
        self.nodes: Dict[str, FactorDefinition] = {}
        self.edges: Dict[str, Set[str]] = defaultdict(set)  # factor -> its dependencies
        self._cache: Dict[str, pd.DataFrame] = {}  # 中间结果缓存
        self._cache_hits = 0
        self._cache_misses = 0

    def add_factor(self, definition: FactorDefinition):
        """添加因子到计算图"""
        self.nodes[definition.name] = definition
        for dep in definition.dependencies:
            self.edges[definition.name].add(dep)

    def get_execution_order(self) -> List[str]:
        """
        拓扑排序获取执行顺序

        确保依赖的因子先计算
        """
        visited = set()
        order = []

        def visit(node: str):
            if node in visited:
                return
            visited.add(node)
            for dep in self.edges.get(node, []):
                if dep in self.nodes:  # 只处理也是因子的依赖
                    visit(dep)
            order.append(node)

        for node in self.nodes:
            visit(node)

        return order

    def get_or_compute(self, name: str, compute_fn: Callable[[], pd.DataFrame]) -> pd.DataFrame:
        """获取缓存结果或计算"""
        if name in self._cache:
            self._cache_hits += 1
            logger.debug(f"缓存命中: {name}")
            return self._cache[name]

        self._cache_misses += 1
        result = compute_fn()
        self._cache[name] = result
        return result

    def clear_cache(self):
        """清空缓存"""
        self._cache.clear()
        self._cache_hits = 0
        self._cache_misses = 0

    def get_cache_stats(self) -> Dict[str, int]:
        """获取缓存统计"""
        return {
            'hits': self._cache_hits,
            'misses': self._cache_misses,
            'size': len(self._cache)
        }


class DataLoader:
    """
    统一数据加载器

    管理所有原始数据的加载和时点标记
    """

    # 数据时点映射
    DATA_AVAILABILITY = {
        'open': DataAvailability.OPEN,
        'high': DataAvailability.CLOSE,
        'low': DataAvailability.CLOSE,
        'close': DataAvailability.CLOSE,
        'vol': DataAvailability.CLOSE,
        'amount': DataAvailability.CLOSE,
        'circ_mv': DataAvailability.NEXT_OPEN,  # 日终指标
        'turnover': DataAvailability.NEXT_OPEN,  # 日终计算
    }

    def __init__(self, dates: Optional[pd.DatetimeIndex] = None,
                 stocks: Optional[List[str]] = None):
        self.dates = dates or self._load_trading_dates()
        self.stocks = stocks or self._load_stock_list()
        self._loaded_data: Dict[str, DataSource] = {}
        self._timings: Dict[str, float] = {}

    def _load_trading_dates(self) -> pd.DatetimeIndex:
        """加载交易日历"""
        cal = pd.read_csv(Config.TRADE_CALENDAR_FILE, dtype=str)
        cal = cal[cal['is_open'] == '1']
        return pd.DatetimeIndex(pd.to_datetime(cal['cal_date'], format='%Y%m%d'))

    def _load_stock_list(self) -> List[str]:
        """加载股票列表"""
        basic = pd.read_csv(Config.BASIC_DATA_DIR / 'all_companies_info.csv')
        return basic['ts_code'].tolist()

    def load(self, name: str) -> DataSource:
        """
        加载指定数据源

        自动根据数据类型标记可用时点
        """
        if name in self._loaded_data:
            return self._loaded_data[name]

        t0 = time.time()
        logger.info(f"加载数据源: {name}")

        if name in ['open', 'high', 'low', 'close', 'vol', 'amount']:
            matrix = self._load_ohlcv_column(name)
        elif name == 'circ_mv':
            matrix = self._load_market_value()
        elif name == 'returns':
            matrix = self._load_returns()
        elif name == 'tradable':
            matrix = self._load_tradable()
        else:
            # 尝试从矩阵文件加载
            matrix = self._load_from_matrix_file(name)

        availability = self.DATA_AVAILABILITY.get(name, DataAvailability.CLOSE)

        data_source = DataSource(
            name=name,
            matrix=matrix,
            availability=availability
        )

        self._loaded_data[name] = data_source
        elapsed = time.time() - t0
        self._timings[name] = elapsed
        logger.info(f"  完成: {matrix.shape}, 耗时: {elapsed:.2f}s")

        return data_source

    def _load_ohlcv_column(self, column: str) -> pd.DataFrame:
        """加载OHLCV单列（矩阵化，无循环）"""
        from data_engine.processors.data_loader import build_price_matrix
        return build_price_matrix(self.dates, self.stocks, column, logger=logger)

    def _load_market_value(self) -> pd.DataFrame:
        """加载流通市值矩阵"""
        mv_file = Config.SUPPLEMENTARY_DATA_DIR / 'daily_basic.csv'
        if not mv_file.exists():
            raise FileNotFoundError(f"市值数据不存在: {mv_file}")

        df = pd.read_csv(mv_file, dtype={'trade_date': str, 'ts_code': str})
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')

        # 过滤并透视
        df = df[df['ts_code'].isin(self.stocks)]
        df = df[df['trade_date'].isin(self.dates)]

        matrix = df.pivot(index='trade_date', columns='ts_code', values='circ_mv')
        matrix = matrix.reindex(index=self.dates, columns=self.stocks)

        return matrix

    def _load_returns(self) -> pd.DataFrame:
        """加载收益率矩阵"""
        returns_file = Config.MATRIX_DATA_DIR / 'open_return_matrix.csv'
        if returns_file.exists():
            return load_matrix(returns_file)
        # 如果没有预计算，从close计算
        close = self.load('close').matrix
        returns = close.pct_change().shift(-1)  # T日买入T+1日卖出的收益
        return returns

    def _load_tradable(self) -> pd.DataFrame:
        """加载可交易性矩阵"""
        tradable_file = Config.MATRIX_DATA_DIR / 'tradability_matrix.csv'
        if tradable_file.exists():
            return load_matrix(tradable_file)
        # 默认全部可交易
        return pd.DataFrame(0, index=self.dates, columns=self.stocks, dtype=np.float32)

    def _load_from_matrix_file(self, name: str) -> pd.DataFrame:
        """从矩阵文件加载"""
        matrix_file = Config.MATRIX_DATA_DIR / f'{name}_matrix.csv'
        if matrix_file.exists():
            return load_matrix(matrix_file)
        raise ValueError(f"未知数据源: {name}")

    def get_timings(self) -> Dict[str, float]:
        """获取加载耗时统计"""
        return self._timings.copy()


class FormulaEvaluator:
    """
    公式求值器

    支持自然的 pandas 方法链调用语法
    例: close.pct_change(20).shift(1) / close.pct_change(60)
    """

    # 支持的全局函数
    SAFE_GLOBALS = {
        'abs': abs,
        'max': max,
        'min': min,
        'sum': sum,
        'len': len,
        'np': np,
        'pd': pd,
    }

    @classmethod
    def evaluate(cls, formula: str, data: Dict[str, pd.DataFrame],
                 params: Dict[str, Any] = None) -> pd.DataFrame:
        """
        求值公式

        Args:
            formula: 公式字符串（如 'close.pct_change(20)'）
            data: 依赖数据字典 {变量名: DataFrame}
            params: 额外参数
        """
        params = params or {}

        try:
            # 创建安全的求值环境
            # 允许 pandas DataFrame 的方法链调用
            env = {
                "__builtins__": {},
                **cls.SAFE_GLOBALS,
                **data,
                **params
            }

            # 安全求值
            result = eval(formula, env)

            if isinstance(result, pd.Series):
                result = result.to_frame().T if result.index.name else result
            if not isinstance(result, pd.DataFrame):
                raise ValueError(f"公式结果必须是DataFrame，得到 {type(result)}")

            return result

        except Exception as e:
            logger.error(f"公式求值失败: {formula}, 错误: {e}")
            raise


class UnifiedFactorBuilder:
    """
    统一因子构建器

    整合所有功能的统一入口
    """

    def __init__(self,
                 start_date: Optional[str] = None,
                 end_date: Optional[str] = None,
                 config_file: Optional[Path] = None):
        self.start_date = start_date
        self.end_date = end_date
        self.config_file = config_file

        self.data_loader = DataLoader()
        self.computation_graph = ComputationGraph()
        self._timings: Dict[str, float] = {}

        # 日期过滤
        if start_date or end_date:
            dates = self.data_loader.dates
            if start_date:
                dates = dates[dates >= start_date]
            if end_date:
                dates = dates[dates <= end_date]
            self.data_loader.dates = dates

        # 加载配置
        if config_file:
            self._load_config(config_file)

    def _load_config(self, config_file: Path):
        """从YAML加载因子配置"""
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        for factor_config in config.get('factors', []):
            definition = FactorDefinition(**factor_config)
            self.computation_graph.add_factor(definition)

    def add_factor(self, definition: FactorDefinition):
        """添加因子定义"""
        self.computation_graph.add_factor(definition)

    def build(self, factor_names: Optional[List[str]] = None,
              save: bool = True) -> Dict[str, pd.DataFrame]:
        """
        构建指定因子（或全部因子）

        Args:
            factor_names: 要构建的因子列表，None表示全部
            save: 是否保存到文件

        Returns:
            因子名称 -> 矩阵 的字典
        """
        execution_order = self.computation_graph.get_execution_order()

        if factor_names:
            execution_order = [n for n in execution_order if n in factor_names]

        logger.info("=" * 70)
        logger.info(f"开始构建 {len(execution_order)} 个因子")
        logger.info("=" * 70)

        results = {}

        for name in execution_order:
            definition = self.computation_graph.nodes[name]

            result = self._build_single_factor(definition)
            results[name] = result

            if save:
                self._save_factor(definition, result)

        # 输出统计
        logger.info("=" * 70)
        logger.info("构建完成")
        logger.info(f"缓存统计: {self.computation_graph.get_cache_stats()}")
        logger.info(f"数据加载耗时: {sum(self.data_loader.get_timings().values()):.2f}s")
        logger.info("=" * 70)

        return results

    def _build_single_factor(self, definition: FactorDefinition) -> pd.DataFrame:
        """构建单个因子"""
        logger.info(f"构建因子: {definition.name} - {definition.description}")
        t0 = time.time()

        def compute():
            # 加载依赖数据
            data = {}
            for dep_name in definition.dependencies:
                data_source = self.data_loader.load(dep_name)
                # 根据因子目标时点调整数据
                data[dep_name] = data_source.get_for(definition.availability)

            # 求值公式
            result = FormulaEvaluator.evaluate(
                definition.formula,
                data,
                definition.params
            )

            return result

        # 使用计算图缓存
        result = self.computation_graph.get_or_compute(definition.name, compute)

        elapsed = time.time() - t0
        self._timings[definition.name] = elapsed
        logger.info(f"  完成: {result.shape}, 耗时: {elapsed:.2f}s")

        return result

    def _save_factor(self, definition: FactorDefinition, matrix: pd.DataFrame):
        """保存因子矩阵"""
        output_name = definition.output_name or f"{definition.name}_matrix"
        output_path = Config.MATRIX_DATA_DIR / f"{output_name}.csv"

        fm = FactorMatrix.from_pandas(
            matrix,
            name=definition.name,
            description=definition.description
        )
        fm.to_csv(output_path)

        valid_ratio = matrix.notna().sum().sum() / matrix.size
        logger.info(f"  保存: {output_path.name}, 有效数据: {valid_ratio:.2%}")

    def get_timings(self) -> Dict[str, float]:
        """获取构建耗时统计"""
        return self._timings.copy()


# ==================== 预定义因子构建器 ====================

class MomentumFactorBuilder(UnifiedFactorBuilder):
    """动量/反转因子构建器"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # 预定义动量因子
        factors = [
            FactorDefinition(
                name='reversal_5d',
                description='5日反转',
                formula='-close.pct_change(5)',
                dependencies=['close'],
                availability=DataAvailability.CLOSE
            ),
            FactorDefinition(
                name='momentum_20d',
                description='20日动量',
                formula='close.pct_change(20)',
                dependencies=['close'],
                availability=DataAvailability.CLOSE
            ),
            FactorDefinition(
                name='momentum_60d',
                description='60日动量',
                formula='close.pct_change(60)',
                dependencies=['close'],
                availability=DataAvailability.CLOSE
            ),
            FactorDefinition(
                name='volatility_20d',
                description='20日收益率波动率',
                formula='close.pct_change().rolling_std(20)',
                dependencies=['close'],
                availability=DataAvailability.CLOSE
            ),
        ]

        for f in factors:
            self.add_factor(f)


class LiquidityFactorBuilder(UnifiedFactorBuilder):
    """流动性因子构建器"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # 先定义中间结果：日换手率
        # 注意：turnover_daily 使用 T日amount和T-1日市值（开盘时点）
        self.add_factor(FactorDefinition(
            name='turnover_daily',
            description='日换手率',
            formula='amount / (circ_mv.shift(1) * 100 + 1e-10)',
            dependencies=['amount', 'circ_mv'],
            availability=DataAvailability.CLOSE  # T日收盘后可计算
        ))

        factors = [
            FactorDefinition(
                name='amihud_20d',
                description='Amihud非流动性指标',
                formula='(abs(close.pct_change()) / (amount * 1000 + 1e-10)).rolling_mean(20) * 1e9',
                dependencies=['close', 'amount'],
                availability=DataAvailability.CLOSE
            ),
            FactorDefinition(
                name='turnover_20d',
                description='20日平均换手率',
                formula='turnover_daily.rolling_mean(20)',
                dependencies=['turnover_daily'],
                availability=DataAvailability.CLOSE
            ),
            FactorDefinition(
                name='turnover_vol_20d',
                description='20日换手率波动率',
                formula='turnover_daily.rolling_std(20)',
                dependencies=['turnover_daily'],
                availability=DataAvailability.CLOSE
            ),
            FactorDefinition(
                name='vp_corr_20d',
                description='20日价量相关系数',
                formula='close.rolling_corr(vol, 20)',
                dependencies=['close', 'vol'],
                availability=DataAvailability.CLOSE
            ),
        ]

        for f in factors:
            self.add_factor(f)


class ValueFactorBuilder(UnifiedFactorBuilder):
    """价值因子构建器（需要财务数据支持）"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        factors = [
            FactorDefinition(
                name='pb',
                description='市净率',
                formula='circ_mv * 10000 / net_asset',
                dependencies=['circ_mv', 'net_asset'],
                availability=DataAvailability.NEXT_OPEN
            ),
        ]

        for f in factors:
            self.add_factor(f)


def build_all_factors(factor_types: Optional[List[str]] = None,
                      start_date: Optional[str] = None,
                      end_date: Optional[str] = None) -> Dict[str, pd.DataFrame]:
    """
    一键构建所有标准因子

    Args:
        factor_types: 因子类型列表 ['momentum', 'liquidity']，None表示全部
        start_date: 开始日期
        end_date: 结束日期
    """
    builders = {
        'momentum': MomentumFactorBuilder,
        'liquidity': LiquidityFactorBuilder,
    }

    if factor_types is None:
        factor_types = list(builders.keys())

    all_results = {}

    for factor_type in factor_types:
        if factor_type not in builders:
            logger.warning(f"未知因子类型: {factor_type}")
            continue

        logger.info(f"\n{'='*70}")
        logger.info(f"构建 {factor_type} 因子")
        logger.info(f"{'='*70}")

        builder = builders[factor_type](start_date=start_date, end_date=end_date)
        results = builder.build(save=True)
        all_results.update(results)

    return all_results


if __name__ == '__main__':
    # 示例用法
    import argparse

    parser = argparse.ArgumentParser(description='统一因子构建框架')
    parser.add_argument('--type', choices=['momentum', 'liquidity', 'all'],
                       default='all', help='因子类型')
    parser.add_argument('--start-date', default='20150101', help='开始日期')
    parser.add_argument('--end-date', help='结束日期')
    parser.add_argument('--config', help='配置文件路径')

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    if args.config:
        # 从配置文件构建
        builder = UnifiedFactorBuilder(
            config_file=Path(args.config),
            start_date=args.start_date,
            end_date=args.end_date
        )
        builder.build()
    else:
        # 使用预定义构建器
        types = ['momentum', 'liquidity'] if args.type == 'all' else [args.type]
        build_all_factors(types, args.start_date, args.end_date)
