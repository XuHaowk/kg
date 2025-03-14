"""
PubMed JSON文件处理模块，用于从PubMed JSON格式提取文本
"""

import json
import os
from typing import List, Dict, Any, Tuple, Optional


class PubmedProcessor:
    """处理PubMed JSON格式文件，提取标题和摘要"""
    
    @staticmethod
    def load_pubmed_json(file_path: str) -> List[Dict[str, Any]]:
        """
        从文件加载PubMed JSON数据
        
        Args:
            file_path: JSON文件路径
            
        Returns:
            文章列表
        """
        try:
            # Windows编码处理
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data
        except Exception as e:
            print(f"加载PubMed JSON文件出错: {e}")
            return []
    
    @staticmethod
    def extract_text_from_article(article: Dict[str, Any]) -> str:
        """
        从单篇文章提取文本内容（标题+摘要）
        
        Args:
            article: 文章数据字典
            
        Returns:
            提取的文本
        """
        pmid = article.get("pmid", "Unknown")
        title = article.get("title", "")
        abstract = article.get("abstract", "")
        
        # 构建文本，包含PMID以便于追踪
        text = f"PMID: {pmid}\n"
        text += f"标题: {title}\n"
        text += f"摘要: {abstract}\n"
        
        return text
    
    @staticmethod
    def extract_metadata_from_article(article: Dict[str, Any]) -> Dict[str, Any]:
        """
        从单篇文章提取元数据
        
        Args:
            article: 文章数据字典
            
        Returns:
            元数据字典
        """
        metadata = {
            "pmid": article.get("pmid", ""),
            "title": article.get("title", ""),
            "authors": article.get("authors", ""),
            "journal": article.get("journal", ""),
            "publication_date": article.get("publication_date", ""),
            "chemicals": article.get("chemicals", ""),
            "mesh_terms": article.get("mesh_terms", "")
        }
        return metadata
    
    @staticmethod
    def extract_chemical_terms(article: Dict[str, Any]) -> List[str]:
        """
        从文章中提取化学物质术语，作为实体提取的辅助信息
        
        Args:
            article: 文章数据字典
            
        Returns:
            化学物质名称列表
        """
        chemicals = article.get("chemicals", "")
        if not chemicals:
            return []
        
        # 分割化学物质字段
        terms = chemicals.split("; ")
        
        # 清理术语
        clean_terms = []
        for term in terms:
            # 移除常见前缀如"0 (" 或数字代码
            if " (" in term:
                term = term.split(" (")[1].rstrip(")")
            clean_terms.append(term)
        
        return clean_terms
    
    @staticmethod
    def process_pubmed_file(file_path: str) -> Tuple[str, List[Dict[str, Any]]]:
        """
        处理单个PubMed JSON文件，提取所有文章的文本和元数据
        
        Args:
            file_path: JSON文件路径
            
        Returns:
            合并的文本内容，元数据列表
        """
        articles = PubmedProcessor.load_pubmed_json(file_path)
        
        if not articles:
            return "", []
        
        # 提取所有文章的文本和元数据
        all_text = ""
        metadata_list = []
        
        for article in articles:
            text = PubmedProcessor.extract_text_from_article(article)
            metadata = PubmedProcessor.extract_metadata_from_article(article)
            
            all_text += text + "\n\n" + "-" * 80 + "\n\n"
            metadata_list.append(metadata)
        
        return all_text, metadata_list
    
    @staticmethod
    def process_multiple_files(file_paths: List[str]) -> Tuple[str, List[Dict[str, Any]]]:
        """
        处理多个PubMed JSON文件
        
        Args:
            file_paths: JSON文件路径列表
            
        Returns:
            合并的文本内容，元数据列表
        """
        all_text = ""
        all_metadata = []
        
        for file_path in file_paths:
            print(f"处理文件: {file_path}")
            text, metadata = PubmedProcessor.process_pubmed_file(file_path)
            all_text += text
            all_metadata.extend(metadata)
        
        return all_text, all_metadata