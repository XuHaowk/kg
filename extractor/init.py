"""
实体关系提取模块

该模块包含用于从生物医学文本中提取实体和关系的工具。
"""

# 版本信息
__version__ = '1.0.0'

# 导出主要类
from .entity_extractor import EntityExtractor
from .relation_extractor import RelationExtractor
from .kimi_client import KimiClient

__all__ = ['EntityExtractor', 'RelationExtractor', 'KimiClient']