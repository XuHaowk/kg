"""
文本预处理工具，用于清洗和分段生物医学文本
"""

import re
from typing import List, Dict, Any
import os
import sys

# Windows路径处理
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from config import MAX_CHUNK_SIZE, OVERLAP_SIZE


class TextProcessor:
    """文本预处理工具类"""
    
    def __init__(self, max_chunk_size=MAX_CHUNK_SIZE, overlap_size=OVERLAP_SIZE):
        """
        初始化文本处理器
        
        Args:
            max_chunk_size: 分块的最大字符数
            overlap_size: 块之间的重叠字符数
        """
        self.max_chunk_size = max_chunk_size
        self.overlap_size = overlap_size
    
    def clean_text(self, text: str) -> str:
        """
        清洗文本
        
        Args:
            text: 输入文本
            
        Returns:
            清洗后的文本
        """
        # 删除多余空格
        text = re.sub(r'\s+', ' ', text)
        
        # 删除特殊字符
        text = re.sub(r'[^\w\s.,;:?!()[\]{}\-–—\'\"''""°©®™%$€£¥+=#@&*\\/<>^|_`~]', '', text)
        
        # 规范化引号
        text = text.replace('"', '"').replace('"', '"').replace(''', "'").replace(''', "'")
        
        # 规范化破折号
        text = text.replace('–', '-').replace('—', '-')
        
        return text.strip()
    
    def split_text_into_chunks(self, text: str) -> List[str]:
        """
        将文本分割成块
        
        Args:
            text: 输入文本
            
        Returns:
            文本块列表
        """
        # 如果文本长度小于最大块大小，直接返回
        if len(text) <= self.max_chunk_size:
            return [text]
        
        # 按换行符分割
        paragraphs = text.split('\n')
        
        chunks = []
        current_chunk = ""
        
        for paragraph in paragraphs:
            # 如果当前段落很长，进一步分割
            if len(paragraph) > self.max_chunk_size:
                if current_chunk:
                    chunks.append(current_chunk)
                    current_chunk = ""
                
                # 按句子分割长段落
                sentences = re.split(r'(?<=[.!?])\s+', paragraph)
                temp_chunk = ""
                
                for sentence in sentences:
                    if len(temp_chunk) + len(sentence) + 1 <= self.max_chunk_size:
                        if temp_chunk:
                            temp_chunk += " " + sentence
                        else:
                            temp_chunk = sentence
                    else:
                        if temp_chunk:
                            chunks.append(temp_chunk)
                        
                        # 如果单个句子超过最大块大小，强制分割
                        if len(sentence) > self.max_chunk_size:
                            # 按固定长度分割
                            for i in range(0, len(sentence), self.max_chunk_size - self.overlap_size):
                                end = min(i + self.max_chunk_size, len(sentence))
                                chunks.append(sentence[i:end])
                        else:
                            temp_chunk = sentence
                
                if temp_chunk:
                    current_chunk = temp_chunk
            
            # 正常处理段落
            elif len(current_chunk) + len(paragraph) + 2 <= self.max_chunk_size:
                if current_chunk:
                    current_chunk += "\n\n" + paragraph
                else:
                    current_chunk = paragraph
            else:
                chunks.append(current_chunk)
                current_chunk = paragraph
        
        # 添加最后一个块
        if current_chunk:
            chunks.append(current_chunk)
        
        # 添加块之间的重叠
        if self.overlap_size > 0 and len(chunks) > 1:
            overlapped_chunks = []
            
            for i in range(len(chunks)):
                if i == 0:
                    overlapped_chunks.append(chunks[i])
                else:
                    # 获取前一个块的末尾
                    prev_end = chunks[i-1][-self.overlap_size:] if len(chunks[i-1]) > self.overlap_size else chunks[i-1]
                    overlapped_chunks.append(prev_end + chunks[i])
            
            chunks = overlapped_chunks
        
        return chunks
    
    def extract_entities_from_text(self, text: str, entity_types: List[str]) -> Dict[str, List[str]]:
        """
        从文本中提取实体（规则基础方法，辅助大模型）
        
        Args:
            text: 输入文本
            entity_types: 要提取的实体类型列表
            
        Returns:
            按类型组织的实体字典
        """
        results = {entity_type: [] for entity_type in entity_types}
        
        # 匹配疾病实体的简单规则
        if "疾病" in entity_types or "Disease" in entity_types:
            disease_patterns = [
                r"矽肺",
                r"肺纤维化",
                r"尘肺",
                r"尘肺病",
                r"矽肺病",
                r"肺炎",
                r"肺气肿",
                r"肺结节",
                r"肺动脉高压",
                r"慢性支气管炎"
            ]
            
            entity_type = "疾病" if "疾病" in entity_types else "Disease"
            for pattern in disease_patterns:
                if re.search(pattern, text):
                    if pattern not in results[entity_type]:
                        results[entity_type].append(pattern)
        
        # 匹配生物标志物的简单规则
        if "生物标志物" in entity_types or "Biomarker" in entity_types:
            biomarker_patterns = [
                r"IL-\d+",
                r"TNF-\w+",
                r"TGF-\w+",
                r"CCL\d+",
                r"CXCL\d+"
            ]
            
            entity_type = "生物标志物" if "生物标志物" in entity_types else "Biomarker"
            for pattern in biomarker_patterns:
                matches = re.finditer(pattern, text)
                for match in matches:
                    if match.group() not in results[entity_type]:
                        results[entity_type].append(match.group())
        
        return results