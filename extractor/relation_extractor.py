#!/usr/bin/env python3
"""
关系提取模块

该模块基于大语言模型从生物医学文本中提取实体间关系。
"""

import os
import sys
import json
from typing import Dict, List, Any

# 确保导入路径正确 - Windows路径处理
script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(script_dir)

from config import RELATION_TYPES, RELATION_EXTRACTION_TEMPERATURE
from extractor.kimi_client import KimiClient


class RelationExtractor:
    """从文本中提取实体间关系的工具类"""
    
    def __init__(self, allowed_relation_types=None, temperature=RELATION_EXTRACTION_TEMPERATURE):
        """
        初始化关系提取器
        
        Args:
            allowed_relation_types: 允许提取的关系类型列表
            temperature: 提取时的温度参数
        """
        self.allowed_relation_types = allowed_relation_types or RELATION_TYPES
        self.temperature = temperature
        self.client = KimiClient()
    
    def extract_relations(self, text: str, entities: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """
        从文本中提取实体间关系
        
        Args:
            text: 输入文本
            entities: 按类型组织的实体字典
            
        Returns:
            关系列表
        """
        # 如果没有足够的实体，直接返回空列表
        if not entities or sum(len(ents) for ents in entities.values()) < 2:
            return []
            
        # 将实体扁平化为单一列表
        flat_entities = []
        for entity_type, entity_list in entities.items():
            for entity in entity_list:
                flat_entities.append({
                    "text": entity["text"],
                    "type": entity_type
                })
        
        # 创建提示词
        prompt = self._create_extraction_prompt(text, flat_entities)
        
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
            return []
        
        return self._parse_response(content)
    
    def _create_extraction_prompt(self, text: str, entities: List[Dict[str, str]]) -> str:
        """创建用于关系提取的提示词"""
        relation_types_str = ", ".join(self.allowed_relation_types)
        entities_str = json.dumps(entities, ensure_ascii=False)
        
        prompt = f"""你是一个专业的生物医学关系提取专家，请识别以下文本中实体之间的关系。

已知实体列表:
{entities_str}

请从文本中提取这些实体之间的关系，关系类型包括: {relation_types_str}
请按照以下JSON格式返回结果:
[
  {{
    "source": {{"text": "IL-6", "type": "基因"}},
    "target": {{"text": "矽肺", "type": "疾病"}},
    "relation": "相关",
    "confidence": 0.9
  }},
  {{
    "source": {{"text": "TNF-α", "type": "基因"}},
    "target": {{"text": "炎症", "type": "生物过程"}},
    "relation": "上调",
    "confidence": 0.85
  }}
]

注意事项:
1. 只返回JSON格式的结果，不要添加任何解释或额外文本
2. 确保准确识别关系，避免误识别
3. confidence字段表示关系的置信度，范围为0-1
4. 只提取有明确证据支持的关系
5. source和target必须来自给定的实体列表
6. 如果没有发现任何关系，返回空数组 []

以下是需要提取关系的文本:

"""
        
        # 添加文本内容(如果太长则截断)
        max_text_length = 15000  # 保留足够长的上下文
        if len(text) > max_text_length:
            prompt += text[:max_text_length] + "...(文本已截断)"
        else:
            prompt += text
        
        return prompt
    
    def _parse_response(self, response: str) -> List[Dict[str, Any]]:
        """
        解析API响应
        
        Args:
            response: API返回的文本
            
        Returns:
            解析后的关系列表
        """
        try:
            # 尝试解析JSON
            json_str = self._extract_json_from_text(response)
            if not json_str:
                print("警告: 无法从响应中提取JSON")
                return []
                
            parsed_data = json.loads(json_str)
            
            # 确保是列表
            if not isinstance(parsed_data, list):
                if isinstance(parsed_data, dict) and "relations" in parsed_data:
                    parsed_data = parsed_data["relations"]
                else:
                    print("警告: 解析的数据不是关系列表")
                    return []
            
            # 验证每个关系的格式
            valid_relations = []
            for relation in parsed_data:
                if not isinstance(relation, dict):
                    continue
                    
                if not all(k in relation for k in ["source", "target", "relation"]):
                    continue
                    
                # 验证source和target
                for entity_field in ["source", "target"]:
                    entity = relation[entity_field]
                    if not isinstance(entity, dict) or "text" not in entity or "type" not in entity:
                        break
                else:
                    # 确保有confidence字段
                    if "confidence" not in relation:
                        relation["confidence"] = 0.5
                        
                    # 验证关系类型
                    if relation["relation"] in self.allowed_relation_types:
                        valid_relations.append(relation)
            
            return valid_relations
                
        except Exception as e:
            print(f"解析关系提取响应时出错: {e}")
            return []
    
    def _extract_json_from_text(self, text: str) -> str:
        """从文本中提取JSON字符串"""
        # 先尝试寻找JSON代码块
        if "```json" in text and "```" in text.split("```json", 1)[1]:
            json_block = text.split("```json", 1)[1].split("```", 1)[0].strip()
            return json_block
        
        # 尝试找到JSON数组
        start_idx = text.find('[')
        if start_idx != -1:
            # 找到对应的闭合括号
            bracket_count = 0
            for i in range(start_idx, len(text)):
                if text[i] == '[':
                    bracket_count += 1
                elif text[i] == ']':
                    bracket_count -= 1
                    if bracket_count == 0:
                        return text[start_idx:i+1]
        
        # 如果找不到数组，尝试寻找JSON对象
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
                    
        # 如果没有找到完整的JSON
        return ""