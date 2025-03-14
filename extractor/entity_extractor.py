#!/usr/bin/env python3
"""
实体提取模块

该模块基于大语言模型从生物医学文本中提取实体。
"""

import os
import sys
import json
from typing import Dict, List, Any

# 确保导入路径正确 - Windows路径处理
script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(script_dir)

from config import ENTITY_TYPES, ENTITY_EXTRACTION_TEMPERATURE
from extractor.kimi_client import KimiClient


class EntityExtractor:
    """从文本中提取生物医学实体的工具类"""
    
    def __init__(self, allowed_types=None, temperature=ENTITY_EXTRACTION_TEMPERATURE):
        """
        初始化实体提取器
        
        Args:
            allowed_types: 允许提取的实体类型列表
            temperature: 提取时的温度参数
        """
        self.allowed_types = allowed_types or ENTITY_TYPES
        self.temperature = temperature
        self.client = KimiClient()
    
    def extract_entities(self, text: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        从文本中提取实体
        
        Args:
            text: 输入文本
            
        Returns:
            按类型组织的实体字典
        """
        # 创建提示词
        prompt = self._create_extraction_prompt(text)
        
        # 调用大语言模型
        response = self.client.generate_completion(
            prompt=prompt,
            temperature=self.temperature,
            max_tokens=4000
        )
        
        # 解析响应
        content = response.get("choices", [{}])[0].get("message", {}).get("content", "")
        if not content:
            print("警告: API返回了空响应")
            return {entity_type: [] for entity_type in self.allowed_types}
        
        return self._parse_response(content)
    
    def _create_extraction_prompt(self, text: str) -> str:
        """创建用于实体提取的提示词"""
        entity_types_str = ", ".join(self.allowed_types)
        
        prompt = f"""你是一个专业的生物医学实体识别专家，请从以下相关文本中提取"{entity_types_str}"类型的实体。

请按照以下JSON格式返回结果：
{{
  "疾病": [
    {{"text": "矽肺", "occurrences": 5}},
    {{"text": "肺纤维化", "occurrences": 2}}
  ],
  "基因": [
    {{"text": "IL-6", "occurrences": 3}},
    {{"text": "TNF-α", "occurrences": 1}}
  ],
  ...其他类型
}}

注意事项：
1. 只返回JSON格式的结果，不要添加任何解释或额外文本
2. 确保准确识别实体，避免误识别
3. 如果某类实体没有发现，返回空列表
4. 统计每个实体在文本中出现的次数
5. 同一实体的不同表达形式（如全称和缩写）算作不同实体
6. 包含所有指定的实体类型，即使没有找到该类型的实体

以下是需要提取实体的文本：

"""
        
        # 添加文本内容(如果太长则截断)
        max_text_length = 15000  # 保留足够长的上下文
        if len(text) > max_text_length:
            prompt += text[:max_text_length] + "...(文本已截断)"
        else:
            prompt += text
        
        return prompt
    
    def _parse_response(self, response: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        解析API响应
        
        Args:
            response: API返回的文本
            
        Returns:
            解析后的实体字典
        """
        # 创建默认返回结构
        result = {entity_type: [] for entity_type in self.allowed_types}
        
        try:
            # 尝试解析JSON
            json_str = self._extract_json_from_text(response)
            if not json_str:
                print("警告: 无法从响应中提取JSON")
                return result
                
            parsed_data = json.loads(json_str)
            
            # 确保所有实体类型都存在
            for entity_type in self.allowed_types:
                if entity_type in parsed_data and isinstance(parsed_data[entity_type], list):
                    result[entity_type] = parsed_data[entity_type]
            
            # 验证每个实体的格式
            for entity_type, entities in result.items():
                valid_entities = []
                for entity in entities:
                    if isinstance(entity, dict) and 'text' in entity:
                        # 确保有occurrences字段
                        if 'occurrences' not in entity:
                            entity['occurrences'] = 1
                        valid_entities.append(entity)
                result[entity_type] = valid_entities
                
        except Exception as e:
            print(f"解析实体提取响应时出错: {e}")
        
        return result
    
    def _extract_json_from_text(self, text: str) -> str:
        """从文本中提取JSON字符串"""
        # 先尝试寻找JSON代码块
        if "```json" in text and "```" in text.split("```json", 1)[1]:
            json_block = text.split("```json", 1)[1].split("```", 1)[0].strip()
            return json_block

        # 如果没有代码块，尝试寻找JSON对象
        start_idx = text.find('{')
        if start_idx == -1:
            return ""
            
        # 找到对应的闭合括号
        bracket_count = 0
        for i in range(start_idx, len(text)):
            if text[i] == '{':
                bracket_count += 1
            elif text[i] == '}':
                bracket_count -= 1
                if bracket_count == 0:
                    return text[start_idx:i+1]
                    
        # 如果没有找到完整的JSON对象
        return ""