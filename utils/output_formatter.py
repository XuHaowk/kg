#!/usr/bin/env python3
"""
输出格式化模块

该模块将实体和关系数据格式化为不同的输出格式，如JSON、CSV或RDF。
"""

import os
import json
import csv
import pandas as pd
from typing import Dict, List, Any


class OutputFormatter:
    """格式化实体和关系数据为各种输出格式"""
    
    def format_output(self, entities: Dict[str, List[Dict[str, Any]]], 
                    relations: List[Dict[str, Any]], 
                    metadata: List[Dict[str, Any]], 
                    output_format: str, 
                    output_path: str) -> Dict[str, Any]:
        """
        格式化并保存输出
        
        Args:
            entities: 按类型组织的实体字典
            relations: 关系列表
            metadata: 文献元数据列表
            output_format: 输出格式 (json, csv, rdf)
            output_path: 输出目录路径
            
        Returns:
            包含输出结果的字典
        """
        # 确保输出目录存在
        os.makedirs(output_path, exist_ok=True)
        
        # 处理JSON格式
        if output_format == 'json':
            return self._save_json(entities, relations, metadata, output_path)
        
        # 处理CSV格式
        elif output_format == 'csv':
            return self._save_csv(entities, relations, metadata, output_path)
        
        # 处理RDF格式
        elif output_format == 'rdf':
            return self._save_rdf(entities, relations, metadata, output_path)
        
        # 默认使用JSON格式
        else:
            print(f"警告: 不支持的输出格式 '{output_format}'，使用默认的JSON格式")
            return self._save_json(entities, relations, metadata, output_path)
    
    def _save_json(self, entities: Dict[str, List[Dict[str, Any]]], 
                 relations: List[Dict[str, Any]], 
                 metadata: List[Dict[str, Any]], 
                 output_path: str) -> Dict[str, Any]:
        """
        保存为JSON格式
        
        Args:
            entities: 按类型组织的实体字典
            relations: 关系列表
            metadata: 文献元数据列表
            output_path: 输出目录路径
            
        Returns:
            包含输出结果的字典
        """
        # 构建知识图谱数据
        kg_data = {
            "entities": entities,
            "relations": relations,
            "metadata": {
                "source_count": len(metadata),
                "entity_count": sum(len(ents) for ents in entities.values()),
                "relation_count": len(relations),
                "sources": metadata
            }
        }
        
        # 保存知识图谱JSON文件
        kg_file_path = os.path.join(output_path, "knowledge_graph.json")
        with open(kg_file_path, 'w', encoding='utf-8') as f:
            json.dump(kg_data, f, ensure_ascii=False, indent=2)
        
        print(f"知识图谱JSON已保存: {kg_file_path}")
        
        # 保存单独的实体和关系文件
        entities_file_path = os.path.join(output_path, "entities.json")
        with open(entities_file_path, 'w', encoding='utf-8') as f:
            json.dump(entities, f, ensure_ascii=False, indent=2)
        
        relations_file_path = os.path.join(output_path, "relations.json")
        with open(relations_file_path, 'w', encoding='utf-8') as f:
            json.dump(relations, f, ensure_ascii=False, indent=2)
        
        return {
            "format": "json",
            "kg_file": kg_file_path,
            "entities_file": entities_file_path,
            "relations_file": relations_file_path
        }
    
    def _save_csv(self, entities: Dict[str, List[Dict[str, Any]]], 
                relations: List[Dict[str, Any]], 
                metadata: List[Dict[str, Any]], 
                output_path: str) -> Dict[str, Any]:
        """
        保存为CSV格式
        
        Args:
            entities: 按类型组织的实体字典
            relations: 关系列表
            metadata: 文献元数据列表
            output_path: 输出目录路径
            
        Returns:
            包含输出结果的字典
        """
        # 保存实体CSV文件
        entities_rows = []
        for entity_type, entity_list in entities.items():
            for entity in entity_list:
                entities_rows.append({
                    "entity_id": f"{entity_type}_{len(entities_rows) + 1}",
                    "text": entity.get("text", ""),
                    "type": entity_type,
                    "occurrences": entity.get("occurrences", 1)
                })
        
        entities_df = pd.DataFrame(entities_rows)
        entities_file_path = os.path.join(output_path, "entities.csv")
        entities_df.to_csv(entities_file_path, index=False, encoding='utf-8')
        
        # 创建实体ID映射
        entity_id_map = {}
        for row in entities_rows:
            entity_text = row["text"]
            entity_type = row["type"]
            entity_id = row["entity_id"]
            entity_id_map[(entity_text, entity_type)] = entity_id
        
        # 保存关系CSV文件
        relations_rows = []
        for i, relation in enumerate(relations):
            source = relation.get("source", {})
            target = relation.get("target", {})
            
            source_text = source.get("text", "")
            source_type = source.get("type", "")
            target_text = target.get("text", "")
            target_type = target.get("type", "")
            
            source_id = entity_id_map.get((source_text, source_type), "")
            target_id = entity_id_map.get((target_text, target_type), "")
            
            if source_id and target_id:
                relations_rows.append({
                    "relation_id": f"REL_{i + 1}",
                    "source_id": source_id,
                    "source_text": source_text,
                    "target_id": target_id,
                    "target_text": target_text,
                    "relation_type": relation.get("relation", ""),
                    "confidence": relation.get("confidence", 0.5)
                })
        
        relations_df = pd.DataFrame(relations_rows)
        relations_file_path = os.path.join(output_path, "relations.csv")
        relations_df.to_csv(relations_file_path, index=False, encoding='utf-8')
        
        # 保存元数据CSV文件
        metadata_df = pd.DataFrame(metadata)
        metadata_file_path = os.path.join(output_path, "metadata.csv")
        metadata_df.to_csv(metadata_file_path, index=False, encoding='utf-8')
        
        # 同时生成JSON文件以便后续处理
        self._save_json(entities, relations, metadata, output_path)
        
        return {
            "format": "csv",
            "entities_file": entities_file_path,
            "relations_file": relations_file_path,
            "metadata_file": metadata_file_path
        }
    
    def _save_rdf(self, entities: Dict[str, List[Dict[str, Any]]], 
                relations: List[Dict[str, Any]], 
                metadata: List[Dict[str, Any]], 
                output_path: str) -> Dict[str, Any]:
        """
        保存为RDF格式（简化版）
        
        Args:
            entities: 按类型组织的实体字典
            relations: 关系列表
            metadata: 文献元数据列表
            output_path: 输出目录路径
            
        Returns:
            包含输出结果的字典
        """
        # 创建简单的RDF表示（N-Triples格式）
        triples = []
        
        # 添加前缀定义
        prefixes = """
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix bio: <http://example.org/biomedical/> .
"""
        
        # 为实体创建三元组
        entity_id_map = {}
        entity_counter = 1
        
        for entity_type, entity_list in entities.items():
            for entity in entity_list:
                entity_text = entity.get("text", "").replace('"', '\\"')
                entity_id = f"bio:entity_{entity_counter}"
                entity_id_map[(entity_text, entity_type)] = entity_id
                entity_counter += 1
                
                triples.append(f'{entity_id} rdf:type bio:{entity_type} .')
                triples.append(f'{entity_id} rdfs:label "{entity_text}"^^xsd:string .')
                triples.append(f'{entity_id} bio:occurrences "{entity.get("occurrences", 1)}"^^xsd:integer .')
        
        # 为关系创建三元组
        for relation in relations:
            source = relation.get("source", {})
            target = relation.get("target", {})
            
            source_text = source.get("text", "")
            source_type = source.get("type", "")
            target_text = target.get("text", "")
            target_type = target.get("type", "")
            relation_type = relation.get("relation", "")
            confidence = relation.get("confidence", 0.5)
            
            source_id = entity_id_map.get((source_text, source_type))
            target_id = entity_id_map.get((target_text, target_type))
            
            if source_id and target_id:
                triples.append(f'{source_id} bio:{relation_type} {target_id} .')
                triples.append(f'_:b{len(triples)} rdf:type bio:RelationStatement .')
                triples.append(f'_:b{len(triples)-1} bio:hasSource {source_id} .')
                triples.append(f'_:b{len(triples)-2} bio:hasTarget {target_id} .')
                triples.append(f'_:b{len(triples)-3} bio:relationType "{relation_type}"^^xsd:string .')
                triples.append(f'_:b{len(triples)-4} bio:confidence "{confidence}"^^xsd:float .')
        
        # 保存RDF文件
        rdf_file_path = os.path.join(output_path, "knowledge_graph.ttl")
        with open(rdf_file_path, 'w', encoding='utf-8') as f:
            f.write(prefixes)
            f.write("\n".join(triples))
        
        print(f"RDF文件已保存: {rdf_file_path}")
        
        # 同时生成JSON文件以便后续处理
        self._save_json(entities, relations, metadata, output_path)
        
        return {
            "format": "rdf",
            "rdf_file": rdf_file_path
        }