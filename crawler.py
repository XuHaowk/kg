#!/usr/bin/env python3
"""
PubMed爬虫模块

该模块用于从PubMed搜索并下载文献数据，支持批量下载和自动重试。
"""

import os
import sys
import time
import json
import logging
from datetime import datetime, timedelta
import pandas as pd
import random

# 添加当前目录到系统路径 - 适配Windows
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(script_dir)

# 确保Biopython已安装，Windows环境下的路径处理
try:
    from Bio import Entrez, Medline
except ImportError:
    print("错误: Biopython库未安装。请使用以下命令安装:")
    print("conda install -c conda-forge biopython")
    print("或")
    print("pip install biopython")
    sys.exit(1)

class PubMedCrawler:
    """PubMed文献爬虫类"""
    
    def __init__(self, email, api_key="", batch_size=100, 
                 retry_count=3, sleep_between_retries=5, output_dir="output"):
        """
        初始化PubMed爬虫
        
        Args:
            email (str): 用于NCBI Entrez API的电子邮箱
            api_key (str): NCBI API密钥，可选，有助于提高访问速率限制
            batch_size (int): 每批次获取的文献数量
            retry_count (int): 请求失败时的最大重试次数
            sleep_between_retries (int): 重试之间的等待时间(秒)
            output_dir (str): 输出目录路径
        """
        self.email = email
        self.api_key = api_key
        self.batch_size = batch_size
        self.retry_count = retry_count
        self.sleep_between_retries = sleep_between_retries
        
        # 确保输出目录存在 - Windows路径处理
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 配置Entrez
        Entrez.email = email
        if api_key:
            Entrez.api_key = api_key
        
        # 配置日志
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """设置日志记录器"""
        logger = logging.getLogger("PubMedCrawler")
        logger.setLevel(logging.INFO)
        
        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # 创建文件处理器 - Windows路径处理
        log_path = os.path.join(self.output_dir, f"pubmed_crawler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        file_handler = logging.FileHandler(log_path, encoding='utf-8')  # 添加编码处理
        file_handler.setLevel(logging.DEBUG)
        
        # 创建格式化器
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        
        # 添加处理器
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        
        return logger
    
    def search_pubmed(self, term, date_range=None, max_results=100, sort="relevance"):
        """
        搜索PubMed文献并下载结果
        
        Args:
            term (str): 搜索词
            date_range (tuple): 日期范围，格式(开始日期, 结束日期)，如("2020/01/01", "2023/12/31")
            max_results (int): 最大搜索结果数
            sort (str): 排序方式，可选值: "relevance", "pub_date"
            
        Returns:
            dict: 包含搜索结果的字典
        """
        self.logger.info(f"开始搜索 '{term}'")
        search_term = term
        
        # 添加日期范围
        if date_range:
            search_term += f" AND {date_range[0]}:{date_range[1]}[PDAT]"
        
        self.logger.info(f"完整搜索词: {search_term}")
        
        try:
            # 执行搜索
            handle = self._safe_entrez_call(
                Entrez.esearch,
                db="pubmed",
                term=search_term,
                retmax=max_results,
                sort=sort
            )
            
            record = Entrez.read(handle)
            handle.close()
            
            id_list = record["IdList"]
            count = int(record["Count"])
            
            self.logger.info(f"找到 {count} 条结果，将获取 {len(id_list)} 条")
            
            if not id_list:
                self.logger.warning("没有找到符合条件的文献")
                return {"count": 0, "ids": []}
            
            # 分批获取文献详情
            all_records = []
            for i in range(0, len(id_list), self.batch_size):
                batch_ids = id_list[i:i+self.batch_size]
                self.logger.info(f"获取批次 {i//self.batch_size+1}/{len(id_list)//self.batch_size+1} (IDs: {len(batch_ids)})")
                
                batch_records = self._fetch_details(batch_ids)
                all_records.extend(batch_records)
                
                # 保存批次结果 - Windows路径处理
                batch_df = pd.DataFrame(batch_records)
                batch_number = i//self.batch_size + 1
                self.save_batch_results(batch_df, batch_number, self.output_dir)
                
                # 添加随机延迟以避免过快请求
                if i + self.batch_size < len(id_list):
                    sleep_time = random.uniform(2.0, 5.0)
                    self.logger.info(f"等待 {sleep_time:.2f} 秒后获取下一批次...")
                    time.sleep(sleep_time)
            
            # 保存所有结果
            all_df = pd.DataFrame(all_records)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_path = os.path.join(self.output_dir, f"pubmed_results_all_{timestamp}.csv")
            json_path = os.path.join(self.output_dir, f"pubmed_results_all_{timestamp}.json")
            
            # Windows编码处理
            all_df.to_csv(csv_path, index=False, encoding='utf-8')
            all_df.to_json(json_path, orient='records', force_ascii=False, indent=2)
            
            self.logger.info(f"所有结果已保存到 {csv_path} 和 {json_path}")
            
            return {"count": count, "ids": id_list, "results": all_records}
            
        except Exception as e:
            self.logger.error(f"搜索过程中出错: {str(e)}")
            return {"error": str(e)}
    
    def _safe_entrez_call(self, func, **kwargs):
        """
        安全执行Entrez API调用，支持重试
        
        Args:
            func: Entrez函数
            **kwargs: 传递给func的参数
            
        Returns:
            函数的返回值
        
        Raises:
            Exception: 多次重试后仍然失败
        """
        for attempt in range(self.retry_count + 1):
            try:
                return func(**kwargs)
            except Exception as e:
                if attempt < self.retry_count:
                    wait_time = self.sleep_between_retries * (attempt + 1)
                    self.logger.warning(f"API调用失败 (尝试 {attempt+1}/{self.retry_count+1}): {str(e)}")
                    self.logger.warning(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"API调用失败，已达到最大重试次数: {str(e)}")
                    raise
    
    def _fetch_details(self, id_list):
        """
        获取文献的详细信息
        
        Args:
            id_list (list): PMID列表
            
        Returns:
            list: 包含文献详细信息的字典列表
        """
        self.logger.info(f"获取 {len(id_list)} 篇文献的详细信息")
        
        # 获取详细信息
        handle = self._safe_entrez_call(
            Entrez.efetch,
            db="pubmed",
            id=",".join(id_list),
            rettype="medline",
            retmode="text"
        )
        
        # 解析Medline格式
        records = list(Medline.parse(handle))
        handle.close()
        
        # 处理每条记录
        processed_records = []
        for record in records:
            try:
                processed_record = self._process_record(record)
                processed_records.append(processed_record)
            except Exception as e:
                self.logger.error(f"处理记录时出错 (PMID: {record.get('PMID', 'Unknown')}): {str(e)}")
        
        self.logger.info(f"成功处理 {len(processed_records)}/{len(records)} 条记录")
        return processed_records
    
    def _process_record(self, record):
        """
        处理单条Medline记录
        
        Args:
            record (dict): Medline记录
            
        Returns:
            dict: 处理后的记录
        """
        pmid = record.get("PMID", "")
        
        # 提取作者列表
        authors = record.get("AU", [])
        if authors:
            authors_str = "; ".join(authors)
        else:
            authors_str = ""
        
        # 提取出版日期
        try:
            if "DP" in record:
                pub_date = record["DP"]
            elif "PDAT" in record:
                pub_date = record["PDAT"]
            else:
                pub_date = ""
        except:
            pub_date = ""
        
        # 提取MeSH术语
        mesh_terms = []
        for field in ["MH", "OT"]:
            if field in record:
                terms = record[field]
                if isinstance(terms, list):
                    mesh_terms.extend(terms)
                elif isinstance(terms, str):
                    mesh_terms.append(terms)
        
        mesh_terms_str = "; ".join(mesh_terms) if mesh_terms else ""
        
        # 提取化学物质
        chemicals = []
        if "RN" in record:
            rn_data = record["RN"]
            if isinstance(rn_data, list):
                chemicals.extend(rn_data)
            elif isinstance(rn_data, str):
                chemicals.append(rn_data)
        
        if "NM" in record:
            nm_data = record["NM"]
            if isinstance(nm_data, list):
                chemicals.extend(nm_data)
            elif isinstance(nm_data, str):
                chemicals.append(nm_data)
        
        chemicals_str = "; ".join(chemicals) if chemicals else ""
        
        # 构建处理后的记录
        processed_record = {
            "pmid": pmid,
            "title": record.get("TI", ""),
            "abstract": record.get("AB", ""),
            "authors": authors_str,
            "journal": record.get("JT", ""),
            "publication_date": pub_date,
            "publication_type": "; ".join(record.get("PT", [])),
            "mesh_terms": mesh_terms_str,
            "chemicals": chemicals_str,
            "language": "; ".join(record.get("LA", [])),
            "doi": record.get("LID", record.get("AID", "")),
        }
        
        return processed_record
    
    def save_batch_results(self, batch_df, batch_number, output_dir='output'):
        """
        保存单个批次的文献结果
        
        为每个批次生成三种类型的文件：
        1. CSV格式的详细数据
        2. JSON格式的结构化数据
        3. 文本格式的统计报告
        
        Args:
            batch_df (DataFrame): 要保存的批次数据
            batch_number (int): 批次编号
            output_dir (str): 输出目录路径
        """
        if batch_df.empty:
            self.logger.warning(f"批次 {batch_number} 没有要保存的数据")
            return
        
        try:
            # 创建输出目录 - Windows路径处理
            os.makedirs(output_dir, exist_ok=True)
        
            # 生成时间戳
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
            # 保存为CSV
            csv_path = os.path.join(output_dir, f'pubmed_results_batch_{batch_number}_{timestamp}.csv')
            batch_df.to_csv(csv_path, index=False, encoding='utf-8')
        
            # 保存为JSON
            json_path = os.path.join(output_dir, f'pubmed_results_batch_{batch_number}_{timestamp}.json')
            batch_df.to_json(json_path, orient='records', force_ascii=False, indent=2)
        
            # 生成批次统计报告
            stats_path = os.path.join(output_dir, f'pubmed_stats_batch_{batch_number}_{timestamp}.txt')
            with open(stats_path, 'w', encoding='utf-8') as f:
                f.write(f"PubMed文献检索统计报告 - 批次 {batch_number}\n")
                f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"记录数量: {len(batch_df)}\n\n")
                
                # 有摘要的文献数量
                has_abstract = batch_df['abstract'].astype(bool).sum()
                f.write(f"有摘要的文献数量: {has_abstract} ({has_abstract/len(batch_df)*100:.1f}%)\n")
                
                # 按语言统计
                if 'language' in batch_df.columns:
                    f.write("\n语言分布:\n")
                    languages = []
                    for lang_str in batch_df['language'].dropna():
                        languages.extend([l.strip() for l in lang_str.split(';')])
                    
                    lang_counts = pd.Series(languages).value_counts()
                    for lang, count in lang_counts.items():
                        f.write(f"  {lang}: {count} ({count/len(batch_df)*100:.1f}%)\n")
                
                # 按年份统计
                if 'publication_date' in batch_df.columns:
                    f.write("\n年份分布:\n")
                    years = []
                    for date_str in batch_df['publication_date'].dropna():
                        try:
                            year = date_str.split(' ')[0]
                            if year.isdigit() and len(year) == 4:
                                years.append(year)
                        except:
                            pass
                    
                    year_counts = pd.Series(years).value_counts().sort_index()
                    for year, count in year_counts.items():
                        f.write(f"  {year}: {count} ({count/len(batch_df)*100:.1f}%)\n")
            
            self.logger.info(f"批次 {batch_number} 的结果已保存到:")
            self.logger.info(f"  CSV: {csv_path}")
            self.logger.info(f"  JSON: {json_path}")
            self.logger.info(f"  统计报告: {stats_path}")
            
        except Exception as e:
            self.logger.error(f"保存批次 {batch_number} 结果时出错: {str(e)}")


def main():
    """直接运行模块时的入口函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='PubMed文献爬虫')
    parser.add_argument('--email', '-e', required=True, help='用于NCBI Entrez API的电子邮箱')
    parser.add_argument('--api-key', '-k', default='', help='NCBI API密钥')
    parser.add_argument('--term', '-t', default='Silicosis', help='搜索词')
    parser.add_argument('--start-date', '-s', default='2010/01/01', help='开始日期 (YYYY/MM/DD)')
    parser.add_argument('--end-date', '-d', default='', help='结束日期 (YYYY/MM/DD)，默认为当前日期')
    parser.add_argument('--max-results', '-m', type=int, default=100, help='最大结果数')
    parser.add_argument('--output-dir', '-o', default='output', help='输出目录')
    parser.add_argument('--batch-size', '-b', type=int, default=100, help='每批次获取的文献数量')
    
    args = parser.parse_args()
    
    # 设置结束日期
    if not args.end_date:
        args.end_date = datetime.now().strftime('%Y/%m/%d')
    
    # 创建爬虫实例
    crawler = PubMedCrawler(
        email=args.email,
        api_key=args.api_key,
        batch_size=args.batch_size,
        output_dir=args.output_dir
    )
    
    # 执行搜索
    crawler.search_pubmed(
        term=args.term,
        date_range=(args.start_date, args.end_date),
        max_results=args.max_results
    )


if __name__ == "__main__":
    main()