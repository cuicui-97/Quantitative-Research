#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
统一因子构建框架演示

展示 v3.0 框架的核心功能：
1. 声明式因子配置
2. 中间结果自动复用
3. 严格的数据时点控制
"""
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import logging

from data_engine.core.unified_factor_builder import (
    UnifiedFactorBuilder,
    MomentumFactorBuilder,
    LiquidityFactorBuilder,
    FactorDefinition,
    DataAvailability,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def demo_predefined_builders():
    """演示：使用预定义构建器"""
    logger.info("=" * 70)
    logger.info("演示 1: 使用预定义构建器")
    logger.info("=" * 70)

    # 动量因子
    logger.info("\n>>> 构建动量因子")
    momentum_builder = MomentumFactorBuilder(start_date='20230101')
    momentum_results = momentum_builder.build(save=False)

    logger.info(f"构建完成: {len(momentum_results)} 个因子")
    for name, matrix in momentum_results.items():
        logger.info(f"  {name}: shape={matrix.shape}, "
                   f"valid={matrix.notna().sum().sum() / matrix.size:.1%}")

    # 流动性因子（展示中间结果复用）
    logger.info("\n>>> 构建流动性因子（中间结果复用）")
    liquidity_builder = LiquidityFactorBuilder(start_date='20230101')
    liquidity_results = liquidity_builder.build(save=False)

    logger.info(f"构建完成: {len(liquidity_results)} 个因子")
    # 注意：turnover_daily 被 turnover_20d, turnover_vol_20d 等复用
    cache_stats = liquidity_builder.computation_graph.get_cache_stats()
    logger.info(f"缓存统计: 命中 {cache_stats['hits']}, 未命中 {cache_stats['misses']}")


def demo_custom_factors():
    """演示：自定义因子"""
    logger.info("\n" + "=" * 70)
    logger.info("演示 2: 自定义因子")
    logger.info("=" * 70)

    builder = UnifiedFactorBuilder(start_date='20230101')

    # 自定义动量因子
    builder.add_factor(FactorDefinition(
        name='custom_momentum',
        description='自定义10日动量',
        formula='close.pct_change(10)',
        dependencies=['close'],
        availability=DataAvailability.CLOSE
    ))

    # 自定义波动率因子
    builder.add_factor(FactorDefinition(
        name='custom_volatility',
        description='自定义10日波动率',
        formula='close.pct_change().rolling_std(10)',
        dependencies=['close'],
        availability=DataAvailability.CLOSE
    ))

    results = builder.build(save=False)

    logger.info(f"构建完成: {len(results)} 个因子")
    for name, matrix in results.items():
        logger.info(f"  {name}: mean={matrix.mean().mean():.4f}, "
                   f"std={matrix.std().mean():.4f}")


def demo_future_safety():
    """演示：数据时点安全机制"""
    logger.info("\n" + "=" * 70)
    logger.info("演示 3: 数据时点安全")
    logger.info("=" * 70)

    # 说明：市值是日终数据（NEXT_OPEN），在开盘时使用时自动滞后
    builder = UnifiedFactorBuilder(start_date='20230101')

    builder.add_factor(FactorDefinition(
        name='turnover_safe',
        description='安全的换手率（使用T-1市值计算T日）',
        formula='amount / (circ_mv.get_for(availability) * 100 + 1e-10)',
        dependencies=['amount', 'circ_mv'],
        availability=DataAvailability.OPEN  # 开盘时可用
    ))

    # 实际公式中使用 circ_mv.shift(1) 来确保无未来函数
    # 框架内部会自动处理

    logger.info("说明：当因子目标时点为 OPEN 时，circ_mv 自动使用 shift(1)")
    logger.info("确保 T 日的因子值不会用到 T 日收盘后才知的市值数据")


def demo_yaml_config():
    """演示：从YAML配置文件构建"""
    logger.info("\n" + "=" * 70)
    logger.info("演示 4: 从YAML配置构建")
    logger.info("=" * 70)

    config_path = project_root / 'config' / 'factors' / 'momentum.yaml'

    if config_path.exists():
        logger.info(f"从配置文件加载: {config_path}")
        builder = UnifiedFactorBuilder(
            config_file=config_path,
            start_date='20230101'
        )
        results = builder.build(save=False)
        logger.info(f"从配置构建: {len(results)} 个因子")
    else:
        logger.warning(f"配置文件不存在: {config_path}")
        logger.info("请先创建配置文件")


def demo_performance_comparison():
    """演示：性能对比"""
    logger.info("\n" + "=" * 70)
    logger.info("演示 5: 性能统计")
    logger.info("=" * 70)

    import time

    builder = LiquidityFactorBuilder(start_date='20230101')

    t0 = time.time()
    results = builder.build(save=False)
    elapsed = time.time() - t0

    logger.info(f"构建 {len(results)} 个因子，总耗时: {elapsed:.2f}s")

    # 数据加载耗时
    data_timings = builder.data_loader.get_timings()
    logger.info(f"数据加载耗时:")
    for name, t in sorted(data_timings.items(), key=lambda x: x[1], reverse=True):
        logger.info(f"  {name}: {t:.2f}s")

    # 因子计算耗时
    factor_timings = builder.get_timings()
    logger.info(f"因子计算耗时:")
    for name, t in sorted(factor_timings.items(), key=lambda x: x[1], reverse=True):
        logger.info(f"  {name}: {t:.2f}s")

    # 缓存统计
    cache_stats = builder.computation_graph.get_cache_stats()
    logger.info(f"缓存统计:")
    logger.info(f"  命中: {cache_stats['hits']}")
    logger.info(f"  未命中: {cache_stats['misses']}")
    logger.info(f"  缓存大小: {cache_stats['size']}")


if __name__ == '__main__':
    logger.info("\n" + "=" * 70)
    logger.info("统一因子构建框架 v3.0 演示")
    logger.info("=" * 70)

    # 运行演示
    demo_predefined_builders()
    demo_custom_factors()
    demo_future_safety()
    demo_yaml_config()
    demo_performance_comparison()

    logger.info("\n" + "=" * 70)
    logger.info("演示完成！")
    logger.info("=" * 70)
