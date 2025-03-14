"""
工具模块

该模块包含用于文本处理、输出格式化等辅助功能的工具。
"""

# 版本信息
__version__ = '1.0.0'

# 导出主要类
from .pubmed_processor import PubmedProcessor
from .text_processor import TextProcessor
from .output_formatter import OutputFormatter

__all__ = ['PubmedProcessor', 'TextProcessor', 'OutputFormatter']