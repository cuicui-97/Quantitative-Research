"""
因子分析结果导出模块

支持多种格式导出：
- CSV: 基础格式，兼容性好
- Excel: 多sheet，支持格式化
- JSON: API友好，易于解析
- Parquet: 高性能，压缩存储
"""
import pandas as pd
from pathlib import Path
from typing import Dict, Optional, Union
import logging

logger = logging.getLogger(__name__)


class ResultExporter:
    """分析结果导出器"""

    def __init__(self, output_dir: Union[str, Path]):
        """
        初始化导出器

        Args:
            output_dir: 输出目录路径
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)

    def export_csv(
        self,
        data: pd.DataFrame,
        filename: str,
        index: bool = True,
        encoding: str = 'utf-8-sig'
    ) -> Path:
        """
        导出为CSV格式

        Args:
            data: 要导出的DataFrame
            filename: 输出文件名
            index: 是否保存索引
            encoding: 编码格式（默认utf-8-sig，Excel兼容）

        Returns:
            输出文件路径
        """
        filepath = self.output_dir / filename
        data.to_csv(filepath, index=index, encoding=encoding)
        self.logger.info(f"CSV已导出: {filepath}")
        return filepath

    def export_excel(
        self,
        data_dict: Dict[str, pd.DataFrame],
        filename: str,
        index: bool = True
    ) -> Path:
        """
        导出为Excel格式（多sheet）

        Args:
            data_dict: {sheet_name: dataframe} 字典
            filename: 输出文件名
            index: 是否保存索引

        Returns:
            输出文件路径

        Example:
            >>> exporter.export_excel({
            ...     'metrics': df_metrics,
            ...     'ic_analysis': df_ic,
            ...     'group_returns': df_group_returns
            ... }, 'factor_analysis.xlsx')
        """
        filepath = self.output_dir / filename

        try:
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                for sheet_name, df in data_dict.items():
                    # Excel sheet名称限制：最多31个字符
                    sheet_name = str(sheet_name)[:31]
                    df.to_excel(writer, sheet_name=sheet_name, index=index)

            self.logger.info(f"Excel已导出: {filepath} ({len(data_dict)} sheets)")
            return filepath
        except ImportError:
            self.logger.error("需要安装 openpyxl: pip install openpyxl")
            raise

    def export_json(
        self,
        data: pd.DataFrame,
        filename: str,
        orient: str = 'records',
        indent: int = 2
    ) -> Path:
        """
        导出为JSON格式

        Args:
            data: 要导出的DataFrame
            filename: 输出文件名
            orient: JSON格式
                - 'records': [{column -> value}, ...]
                - 'index': {index -> {column -> value}}
                - 'columns': {column -> {index -> value}}
                - 'values': 只有值的数组
                - 'table': {schema: {...}, data: [...]}
            indent: 缩进空格数

        Returns:
            输出文件路径
        """
        filepath = self.output_dir / filename
        data.to_json(
            filepath,
            orient=orient,
            force_ascii=False,  # 支持中文
            indent=indent
        )
        self.logger.info(f"JSON已导出: {filepath}")
        return filepath

    def export_parquet(
        self,
        data: pd.DataFrame,
        filename: str,
        compression: str = 'snappy',
        index: bool = True
    ) -> Path:
        """
        导出为Parquet格式（高性能列式存储）

        Args:
            data: 要导出的DataFrame
            filename: 输出文件名
            compression: 压缩算法
                - 'snappy': 快速压缩（推荐）
                - 'gzip': 高压缩比
                - 'brotli': 更高压缩比
                - 'none': 不压缩
            index: 是否保存索引

        Returns:
            输出文件路径
        """
        filepath = self.output_dir / filename

        try:
            data.to_parquet(
                filepath,
                index=index,
                compression=compression,
                engine='pyarrow'
            )
            self.logger.info(f"Parquet已导出: {filepath}")
            return filepath
        except ImportError:
            self.logger.error("需要安装 pyarrow: pip install pyarrow")
            raise

    def export_all(
        self,
        data: pd.DataFrame,
        base_name: str,
        formats: Optional[list] = None
    ) -> Dict[str, Path]:
        """
        导出为多种格式

        Args:
            data: 要导出的DataFrame
            base_name: 基础文件名（不含扩展名）
            formats: 要导出的格式列表，默认 ['csv', 'json', 'parquet']

        Returns:
            {格式: 文件路径} 字典
        """
        if formats is None:
            formats = ['csv', 'json', 'parquet']

        results = {}

        if 'csv' in formats:
            results['csv'] = self.export_csv(data, f"{base_name}.csv")

        if 'json' in formats:
            results['json'] = self.export_json(data, f"{base_name}.json")

        if 'parquet' in formats:
            results['parquet'] = self.export_parquet(data, f"{base_name}.parquet")

        self.logger.info(f"已导出 {len(results)} 种格式")
        return results


def export_factor_analysis(
    metrics: pd.DataFrame,
    ic_series: pd.Series,
    group_returns: pd.DataFrame,
    output_dir: Union[str, Path],
    factor_name: str,
    export_format: str = 'csv'
) -> Dict[str, Path]:
    """
    导出因子分析结果的便捷函数

    Args:
        metrics: 绩效指标DataFrame
        ic_series: IC时间序列
        group_returns: 分组收益DataFrame
        output_dir: 输出目录
        factor_name: 因子名称
        export_format: 导出格式 ('csv', 'excel', 'json', 'parquet', 'all')

    Returns:
        {文件类型: 文件路径} 字典
    """
    exporter = ResultExporter(output_dir)
    results = {}

    if export_format == 'csv':
        results['metrics'] = exporter.export_csv(metrics, f'{factor_name}_metrics.csv')
        results['ic'] = exporter.export_csv(ic_series.to_frame(), f'{factor_name}_ic.csv')
        results['returns'] = exporter.export_csv(group_returns, f'{factor_name}_returns.csv')

    elif export_format == 'excel':
        results['excel'] = exporter.export_excel({
            'metrics': metrics,
            'ic_analysis': ic_series.to_frame(),
            'group_returns': group_returns
        }, f'{factor_name}_analysis.xlsx')

    elif export_format == 'json':
        results['metrics'] = exporter.export_json(metrics, f'{factor_name}_metrics.json')
        results['ic'] = exporter.export_json(ic_series.to_frame(), f'{factor_name}_ic.json')
        results['returns'] = exporter.export_json(group_returns, f'{factor_name}_returns.json')

    elif export_format == 'parquet':
        results['metrics'] = exporter.export_parquet(metrics, f'{factor_name}_metrics.parquet')
        results['ic'] = exporter.export_parquet(ic_series.to_frame(), f'{factor_name}_ic.parquet')
        results['returns'] = exporter.export_parquet(group_returns, f'{factor_name}_returns.parquet')

    elif export_format == 'all':
        # 导出所有格式
        results['csv_metrics'] = exporter.export_csv(metrics, f'{factor_name}_metrics.csv')
        results['csv_ic'] = exporter.export_csv(ic_series.to_frame(), f'{factor_name}_ic.csv')
        results['csv_returns'] = exporter.export_csv(group_returns, f'{factor_name}_returns.csv')

        results['excel'] = exporter.export_excel({
            'metrics': metrics,
            'ic_analysis': ic_series.to_frame(),
            'group_returns': group_returns
        }, f'{factor_name}_analysis.xlsx')

        results['json_metrics'] = exporter.export_json(metrics, f'{factor_name}_metrics.json')

    return results
