#!/usr/bin/env python3
"""
知识图谱文件合并工具

该脚本用于合并多个知识图谱JSON文件，
解决从多个文献源提取实体关系后无法生成统一图谱的问题。
特别添加了对中文编码的处理，确保输出文件中的中文正确显示。
"""

import os
import sys
import json
import argparse
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any

def setup_logger():
    """设置日志记录器"""
    logger = logging.getLogger("KG_Merger")
    logger.setLevel(logging.INFO)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # 创建格式化器
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    
    # 添加处理器
    logger.addHandler(console_handler)
    
    return logger

def normalize_entity_text(text):
    """
    对实体文本进行规范化处理，统一不同写法的相同实体
    并处理可能的编码问题
    
    Args:
        text: 原始实体文本
        
    Returns:
        规范化后的文本
    """
    if not isinstance(text, str):
        return ""
    
    # 处理编码问题
    try:
        # 如果是bytes，尝试解码
        if isinstance(text, bytes):
            text = text.decode('utf-8')
    except Exception:
        try:
            # 尝试其他可能的编码
            text = text.decode('gb18030')
        except Exception:
            # 如果都失败了，返回原始值的字符串表示
            return str(text)
    
    # 将文本统一为小写（仅处理英文）
    # 中文实体保持原样
    has_chinese = any('\u4e00' <= char <= '\u9fff' for char in text)
    if not has_chinese:
        text = text.lower()
    
    # 移除常见前缀/后缀
    text = text.replace("the ", "").replace(" protein", "").replace(" gene", "")
    
    # 标准化空格
    text = " ".join(text.split())
    
    return text.strip()

def find_kg_files(input_path: str) -> List[str]:
    """
    查找所有知识图谱JSON文件
    
    Args:
        input_path: 输入路径（文件或目录）
        
    Returns:
        文件路径列表
    """
    logger = logging.getLogger("KG_Merger")
    json_files = []
    
    if os.path.isfile(input_path):
        if input_path.endswith('.json'):
            json_files.append(input_path)
    else:
        # 递归遍历目录寻找knowledge_graph.json文件
        for root, _, files in os.walk(input_path):
            for file in files:
                if file == "knowledge_graph.json" or file.endswith('_graph.json'):
                    json_files.append(os.path.join(root, file))
                # 也考虑entities.json和relations.json文件对
                elif file == "entities.json" or file == "relations.json":
                    file_path = os.path.join(root, file)
                    if file_path not in json_files:
                        json_files.append(file_path)
    
    logger.info(f"找到 {len(json_files)} 个JSON文件")
    return json_files

def load_json_file(file_path: str) -> Dict:
    """
    加载JSON文件
    
    Args:
        file_path: JSON文件路径
        
    Returns:
        JSON数据字典
    """
    logger = logging.getLogger("KG_Merger")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"成功加载: {file_path}")
        return data
    except UnicodeDecodeError:
        # 如果UTF-8解码失败，尝试其他编码
        try:
            with open(file_path, 'r', encoding='gb18030') as f:
                data = json.load(f)
            logger.info(f"使用GB18030编码成功加载: {file_path}")
            return data
        except Exception as e:
            logger.error(f"使用备用编码加载文件时出错 {file_path}: {str(e)}")
            return {}
    except Exception as e:
        logger.error(f"加载文件时出错 {file_path}: {str(e)}")
        return {}

def merge_kg_data(files: List[str], min_confidence=0.0, max_entities=0, entity_types=None) -> Dict:
    """
    合并多个知识图谱文件
    
    Args:
        files: 文件路径列表
        min_confidence: 最低置信度阈值 (0.0-1.0)
        max_entities: 每种类型的最大实体数量 (0表示无限制)
        entity_types: 要包含的实体类型列表
        
    Returns:
        合并后的知识图谱数据
    """
    logger = logging.getLogger("KG_Merger")
    
    # 初始化合并结果
    merged_data = {
        "entities": {},
        "relations": [],
        "metadata": {
            "source_count": 0,
            "entity_count": 0,
            "relation_count": 0,
            "sources": []
        }
    }
    
    # 实体ID映射，用于防止添加重复实体
    entity_map = {}
    
    # 跟踪已处理的关系，用于去重
    processed_relations = set()
    
    # 关系类型映射，统一命名
    relation_type_map = {
        "inhibits": "抑制",
        "activates": "激活",
        "treats": "治疗",
        "causes": "引起",
        "binds": "结合",
        "expresses": "表达",
        "regulates": "调节",
        "phosphorylates": "磷酸化",
        "degrades": "降解", 
        "extracted_from": "提取自",
        "part_of": "组分",
        "isolated_from": "分离自",
        "converts_to": "转化为",
        "metabolizes_to": "代谢为",
        "upregulates": "上调",
        "downregulates": "下调",
        "blocks": "阻断",
        "mediates": "介导",
        "correlates_with": "相关",
        "marks": "标志",
        "indicates": "指示"
    }
    
    # 分类处理文件
    kg_files = []
    entity_files = []
    relation_files = []
    
    for file in files:
        file_name = os.path.basename(file)
        if file_name == "knowledge_graph.json" or file_name.endswith('_graph.json'):
            kg_files.append(file)
        elif file_name == "entities.json":
            entity_files.append(file)
        elif file_name == "relations.json":
            relation_files.append(file)
    
    # 首先处理完整的知识图谱文件
    for file_path in kg_files:
        data = load_json_file(file_path)
        if not data:
            continue
        
        # 合并实体
        if "entities" in data:
            for entity_type, entities in data["entities"].items():
                # 如果指定了实体类型且当前类型不在列表中，则跳过
                if entity_types and entity_type not in entity_types:
                    continue
                    
                if entity_type not in merged_data["entities"]:
                    merged_data["entities"][entity_type] = []
                
                for entity in entities:
                    # 规范化实体文本
                    entity_text = entity.get("text", "")
                    normalized_text = normalize_entity_text(entity_text)
                    
                    # 如果规范化后文本为空，则跳过
                    if not normalized_text:
                        continue
                    
                    entity_key = (normalized_text, entity_type)
                    if entity_key not in entity_map:
                        # 确保使用原始文本
                        entity_copy = entity.copy()
                        # 添加新实体
                        merged_data["entities"][entity_type].append(entity_copy)
                        entity_map[entity_key] = entity_copy
                    else:
                        # 更新已有实体的出现次数
                        existing_entity = entity_map[entity_key]
                        existing_entity["occurrences"] = existing_entity.get("occurrences", 1) + entity.get("occurrences", 1)
        
        # 合并关系
        if "relations" in data:
            for relation in data["relations"]:
                # 检查置信度
                confidence = relation.get("confidence", 0.5)
                if confidence < min_confidence:
                    continue
                    
                source = relation.get("source", {})
                target = relation.get("target", {})
                
                # 规范化源实体和目标实体文本
                source_text = normalize_entity_text(source.get("text", ""))
                target_text = normalize_entity_text(target.get("text", ""))
                
                # 获取实体类型
                source_type = source.get("type", "")
                target_type = target.get("type", "")
                
                # 如果指定了实体类型且源或目标类型不在列表中，则跳过
                if entity_types and (source_type not in entity_types or target_type not in entity_types):
                    continue
                
                # 统一关系类型
                rel_type = relation.get("relation", "")
                if rel_type in relation_type_map:
                    rel_type = relation_type_map[rel_type]
                
                # 创建关系的唯一标识
                rel_key = (source_text, source_type, target_text, target_type, rel_type)
                
                if rel_key not in processed_relations:
                    # 使用规范化后的文本创建新关系
                    new_relation = relation.copy()
                    # 更新关系类型
                    new_relation["relation"] = rel_type
                    
                    # 确保source和target使用原始文本
                    if "source" in new_relation and "text" in new_relation["source"]:
                        new_relation["source"]["text"] = source.get("text", "")
                    if "target" in new_relation and "text" in new_relation["target"]:
                        new_relation["target"]["text"] = target.get("text", "")
                    
                    merged_data["relations"].append(new_relation)
                    processed_relations.add(rel_key)
        
        # 合并元数据
        if "metadata" in data:
            if "sources" in data["metadata"]:
                merged_data["metadata"]["sources"].extend(data["metadata"]["sources"])
            merged_data["metadata"]["source_count"] += data["metadata"].get("source_count", 1)
    
    # 处理独立的实体文件和关系文件
    for i, entity_file in enumerate(entity_files):
        entities_data = load_json_file(entity_file)
        if not entities_data:
            continue
        
        # 查找关联的关系文件
        relation_file = None
        entity_dir = os.path.dirname(entity_file)
        relation_path = os.path.join(entity_dir, "relations.json")
        if relation_path in relation_files:
            relation_file = relation_path
        
        # 合并实体
        for entity_type, entities in entities_data.items():
            # 如果指定了实体类型且当前类型不在列表中，则跳过
            if entity_types and entity_type not in entity_types:
                continue
                
            if entity_type not in merged_data["entities"]:
                merged_data["entities"][entity_type] = []
            
            for entity in entities:
                # 规范化实体文本
                entity_text = entity.get("text", "")
                normalized_text = normalize_entity_text(entity_text)
                
                # 如果规范化后文本为空，则跳过
                if not normalized_text:
                    continue
                
                entity_key = (normalized_text, entity_type)
                if entity_key not in entity_map:
                    # 确保使用原始文本
                    entity_copy = entity.copy()
                    # 添加新实体
                    merged_data["entities"][entity_type].append(entity_copy)
                    entity_map[entity_key] = entity_copy
                else:
                    # 更新已有实体的出现次数
                    existing_entity = entity_map[entity_key]
                    existing_entity["occurrences"] = existing_entity.get("occurrences", 1) + entity.get("occurrences", 1)
        
        # 合并关系（如果有匹配的关系文件）
        if relation_file:
            relations_data = load_json_file(relation_file)
            if relations_data:
                for relation in relations_data:
                    # 检查置信度
                    confidence = relation.get("confidence", 0.5)
                    if confidence < min_confidence:
                        continue
                        
                    source = relation.get("source", {})
                    target = relation.get("target", {})
                    
                    # 规范化源实体和目标实体文本
                    source_text = normalize_entity_text(source.get("text", ""))
                    target_text = normalize_entity_text(target.get("text", ""))
                    
                    # 获取实体类型
                    source_type = source.get("type", "")
                    target_type = target.get("type", "")
                    
                    # 如果指定了实体类型且源或目标类型不在列表中，则跳过
                    if entity_types and (source_type not in entity_types or target_type not in entity_types):
                        continue
                    
                    # 统一关系类型
                    rel_type = relation.get("relation", "")
                    if rel_type in relation_type_map:
                        rel_type = relation_type_map[rel_type]
                    
                    # 创建关系的唯一标识
                    rel_key = (source_text, source_type, target_text, target_type, rel_type)
                    
                    if rel_key not in processed_relations:
                        # 使用规范化后的文本创建新关系
                        new_relation = relation.copy()
                        # 更新关系类型
                        new_relation["relation"] = rel_type
                        
                        # 确保source和target使用原始文本
                        if "source" in new_relation and "text" in new_relation["source"]:
                            new_relation["source"]["text"] = source.get("text", "")
                        if "target" in new_relation and "text" in new_relation["target"]:
                            new_relation["target"]["text"] = target.get("text", "")
                        
                        merged_data["relations"].append(new_relation)
                        processed_relations.add(rel_key)
    
    # 如果指定了最大实体数量限制
    if max_entities > 0:
        for entity_type, entities in merged_data["entities"].items():
            # 按出现次数排序实体
            sorted_entities = sorted(entities, key=lambda x: x.get("occurrences", 1), reverse=True)
            # 限制数量
            merged_data["entities"][entity_type] = sorted_entities[:max_entities]
    
    # 更新统计信息
    total_entities = sum(len(entities) for entities in merged_data["entities"].values())
    merged_data["metadata"]["entity_count"] = total_entities
    merged_data["metadata"]["relation_count"] = len(merged_data["relations"])
    
    logger.info(f"合并完成: {total_entities}个实体, {len(merged_data['relations'])}个关系")
    return merged_data

def export_merged_relations_to_csv(merged_data, output_path):
    """
    将合并后的关系导出为CSV，确保中文字符正确显示
    
    Args:
        merged_data: 合并后的数据
        output_path: 输出文件路径
    """
    logger = logging.getLogger("KG_Merger")
    
    # 提取所有关系
    relations_data = []
    
    for relation in merged_data["relations"]:
        source = relation.get("source", {}).get("text", "")
        target = relation.get("target", {}).get("text", "")
        rel_type = relation.get("relation", "")
        confidence = relation.get("confidence", 0.5)
        
        relations_data.append({
            "source": source,
            "target": target,
            "relation": rel_type,
            "weight": confidence
        })
    
    # 创建DataFrame
    relations_df = pd.DataFrame(relations_data)
    
    # 确保输出目录存在
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    # 使用UTF-8编码并添加BOM标记
    relations_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    logger.info(f"关系已导出到CSV: {output_path}")
    return output_path

def export_merged_entities_to_csv(merged_data, output_path):
    """
    将合并后的实体导出为CSV，确保中文字符正确显示
    
    Args:
        merged_data: 合并后的数据
        output_path: 输出文件路径
    """
    logger = logging.getLogger("KG_Merger")
    
    # 提取所有实体
    entities_data = []
    
    for entity_type, entities in merged_data["entities"].items():
        for entity in entities:
            text = entity.get("text", "")
            occurrences = entity.get("occurrences", 1)
            
            entities_data.append({
                "text": text,
                "type": entity_type,
                "occurrences": occurrences
            })
    
    # 创建DataFrame
    entities_df = pd.DataFrame(entities_data)
    
    # 确保输出目录存在
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    # 使用UTF-8编码并添加BOM标记
    entities_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    logger.info(f"实体已导出到CSV: {output_path}")
    return output_path

def main():
    """主入口函数"""
    # 配置参数解析
    parser = argparse.ArgumentParser(description='知识图谱文件合并工具')
    parser.add_argument('input', help='输入文件或目录路径')
    parser.add_argument('--output', '-o', default='merged_knowledge_graph.json', help='输出文件路径')
    parser.add_argument('--verbose', '-v', action='store_true', help='显示详细日志')
    parser.add_argument('--min-confidence', type=float, default=0.0, help='最低置信度阈值 (0.0-1.0)')
    parser.add_argument('--max-entities', type=int, default=0, help='每种类型的最大实体数量 (0表示无限制)')
    parser.add_argument('--entity-types', nargs='+', help='要包含的实体类型列表')
    parser.add_argument('--export-csv', action='store_true', help='同时导出CSV格式')
    
    args = parser.parse_args()
    
    # 设置日志
    logger = setup_logger()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # 查找所有知识图谱文件
    json_files = find_kg_files(args.input)
    
    if not json_files:
        logger.error(f"在 {args.input} 中没有找到知识图谱文件")
        return
    
    # 合并知识图谱数据
    merged_data = merge_kg_data(
        json_files, 
        min_confidence=args.min_confidence,
        max_entities=args.max_entities,
        entity_types=args.entity_types
    )
    
    # 保存合并后的文件
    try:
        # 确保输出目录存在
        output_dir = os.path.dirname(args.output)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
            
        # 保存JSON文件，确保使用UTF-8编码
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(merged_data, f, ensure_ascii=False, indent=2)
        logger.info(f"合并后的知识图谱已保存到: {args.output}")
        
        # 导出CSV文件
        if args.export_csv or True:  # 默认总是导出CSV
            # 导出关系CSV
            relations_csv_path = args.output.replace('.json', '_relations.csv')
            export_merged_relations_to_csv(merged_data, relations_csv_path)
            
            # 导出实体CSV
            entities_csv_path = args.output.replace('.json', '_entities.csv')
            export_merged_entities_to_csv(merged_data, entities_csv_path)
            
            logger.info(f"CSV文件已导出: {relations_csv_path}, {entities_csv_path}")
    except Exception as e:
        logger.error(f"保存合并文件时出错: {str(e)}")

if __name__ == "__main__":
    main()