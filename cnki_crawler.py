#!/usr/bin/env python3
"""
CNKI爬虫模块

该模块用于从中国知网(CNKI)搜索并下载文献数据，支持批量下载和自动重试。
"""

import os
import sys
import time
import json
import logging
import re
import random
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote

# 添加当前目录到系统路径
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(script_dir)

class CNKICrawler:
    """CNKI文献爬虫类"""
    
    def __init__(self, username="", password="", batch_size=100, 
                 retry_count=3, sleep_between_retries=5, output_dir="output"):
        """
        初始化CNKI爬虫
        
        Args:
            username (str): CNKI账号用户名（可选）
            password (str): CNKI账号密码（可选）
            batch_size (int): 每批次获取的文献数量
            retry_count (int): 请求失败时的最大重试次数
            sleep_between_retries (int): 重试之间的等待时间(秒)
            output_dir (str): 输出目录路径
        """
        self.username = username
        self.password = password
        self.batch_size = batch_size
        self.retry_count = retry_count
        self.sleep_between_retries = sleep_between_retries
        
        # 确保输出目录存在
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 会话对象，用于保持登录状态
        self.session = requests.Session()
        # 设置请求头，模拟浏览器
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        })
        
        # 配置日志
        self.logger = self._setup_logger()
        
        # 是否已登录
        self.is_logged_in = False
    
    def _setup_logger(self):
        """设置日志记录器"""
        logger = logging.getLogger("CNKICrawler")
        logger.setLevel(logging.INFO)
        
        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # 创建文件处理器
        log_path = os.path.join(self.output_dir, f"cnki_crawler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        # 创建格式化器
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        
        # 添加处理器
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        
        return logger
    
    def login(self):
        """登录CNKI（如果提供了凭据）"""
        if not self.username or not self.password:
            self.logger.warning("未提供CNKI账号信息，将以游客模式访问（可能受到限制）")
            return False
        
        try:
            self.logger.info(f"尝试登录CNKI，用户名: {self.username}")
            # 这里实现登录逻辑，包括获取登录页面、提交表单等
            # 注意：由于CNKI的登录机制复杂，可能需要处理验证码等，这里是简化示例
            
            login_url = "https://login.cnki.net/login/"
            # 获取登录页以获取必要的cookies和token
            response = self.session.get(login_url)
            
            # 提取登录表单中的token或其他必要字段
            soup = BeautifulSoup(response.text, "html.parser")
            # 假设token在一个隐藏的input字段中
            token = soup.find("input", {"name": "token"})
            token_value = token["value"] if token else ""
            
            # 构建登录数据
            login_data = {
                "username": self.username,
                "password": self.password,
                "token": token_value,
                # 其他必要的字段
            }
            
            # 提交登录表单
            login_response = self.session.post(login_url, data=login_data)
            
            # 检查登录是否成功
            if "登录成功" in login_response.text or "我的CNKI" in login_response.text:
                self.logger.info("CNKI登录成功")
                self.is_logged_in = True
                return True
            else:
                self.logger.error("CNKI登录失败，请检查账号密码")
                return False
                
        except Exception as e:
            self.logger.error(f"登录过程中出错: {str(e)}")
            return False
    
    def search_cnki(self, term, date_range=None, max_results=100, db_code="CJFD"):
        """
        搜索CNKI文献并下载结果
        
        Args:
            term (str): 搜索词
            date_range (tuple): 日期范围，格式(开始日期, 结束日期)，如("2020/01/01", "2023/12/31")
            max_results (int): 最大搜索结果数
            db_code (str): 数据库代码，CJFD为中国学术期刊，CDFD为博士论文，CMFD为硕士论文
            
        Returns:
            dict: 包含搜索结果的字典
        """
        self.logger.info(f"开始搜索 '{term}'")
        
        try:
            # 尝试登录（如果提供了凭据）
            if not self.is_logged_in and self.username and self.password:
                self.login()
            
            # 构建搜索URL
            search_url = "https://kns.cnki.net/kns8/AdvSearch"
            
            # 准备搜索参数
            search_term = term
            # 将日期范围转换为CNKI格式
            date_filter = ""
            if date_range:
                start_date = date_range[0].replace("/", "-")
                end_date = date_range[1].replace("/", "-")
                date_filter = f"&publishdate_from={start_date}&publishdate_to={end_date}"
            
            # 编码搜索参数
            encoded_term = quote(search_term)
            
            # 构建完整搜索URL
            full_search_url = f"{search_url}?kw={encoded_term}&korder=SU&pageindex=1&pagesize={min(50, max_results)}&dbcode={db_code}{date_filter}"
            
            # 执行搜索请求
            response = self._safe_request(full_search_url)
            
            if not response:
                raise Exception("搜索请求失败")
            
            # 解析搜索结果页面获取总结果数
            soup = BeautifulSoup(response.text, "html.parser")
            result_info = soup.find("div", class_="search-result")
            count_text = result_info.find("div", class_="pager").text if result_info else ""
            count_match = re.search(r'共(\d+)条结果', count_text)
            count = int(count_match.group(1)) if count_match else 0
            
            self.logger.info(f"找到 {count} 条结果，将获取 {min(count, max_results)} 条")
            
            if count == 0:
                self.logger.warning("没有找到符合条件的文献")
                return {"count": 0, "results": []}
            
            # 计算需要爬取的页数
            page_size = 50  # CNKI每页显示的结果数
            page_count = min((max_results + page_size - 1) // page_size, (count + page_size - 1) // page_size)
            
            # 分页获取结果
            all_records = []
            for page in range(1, page_count + 1):
                self.logger.info(f"获取第 {page}/{page_count} 页")
                page_url = f"{search_url}?kw={encoded_term}&korder=SU&pageindex={page}&pagesize={page_size}&dbcode={db_code}{date_filter}"
                page_response = self._safe_request(page_url)
                
                if not page_response:
                    self.logger.error(f"获取第 {page} 页失败，跳过")
                    continue
                
                # 解析当前页的文献列表
                page_records = self._parse_search_results(page_response.text)
                all_records.extend(page_records)
                
                # 检查是否已达到最大结果数
                if len(all_records) >= max_results:
                    all_records = all_records[:max_results]
                    break
                
                # 添加随机延迟以避免过快请求
                if page < page_count:
                    sleep_time = random.uniform(3.0, 7.0)
                    self.logger.info(f"等待 {sleep_time:.2f} 秒后获取下一页...")
                    time.sleep(sleep_time)
            
            # 保存结果
            all_df = pd.DataFrame(all_records)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_path = os.path.join(self.output_dir, f"cnki_results_{timestamp}.csv")
            json_path = os.path.join(self.output_dir, f"cnki_results_{timestamp}.json")
            
            # 保存为CSV和JSON
            all_df.to_csv(csv_path, index=False, encoding='utf-8')
            all_df.to_json(json_path, orient='records', force_ascii=False, indent=2)
            
            self.logger.info(f"所有结果已保存到 {csv_path} 和 {json_path}")
            
            return {"count": count, "results": all_records}
        
        except Exception as e:
            self.logger.error(f"搜索过程中出错: {str(e)}")
            return {"error": str(e)}
    
    def _safe_request(self, url, method="get", **kwargs):
        """
        安全执行请求，支持重试
        
        Args:
            url: 请求URL
            method: 请求方法（get或post）
            **kwargs: 传递给请求方法的参数
            
        Returns:
            Response对象或None（失败时）
        """
        for attempt in range(self.retry_count + 1):
            try:
                if method.lower() == "post":
                    response = self.session.post(url, **kwargs, timeout=30)
                else:
                    response = self.session.get(url, **kwargs, timeout=30)
                
                # 检查响应状态
                response.raise_for_status()
                return response
                
            except Exception as e:
                if attempt < self.retry_count:
                    wait_time = self.sleep_between_retries * (attempt + 1)
                    self.logger.warning(f"请求失败 (尝试 {attempt+1}/{self.retry_count+1}): {str(e)}")
                    self.logger.warning(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"请求失败，已达到最大重试次数: {str(e)}")
                    return None
    
    def _parse_search_results(self, html_content):
        """
        解析搜索结果HTML页面
        
        Args:
            html_content: 搜索结果页面的HTML内容
            
        Returns:
            list: 包含文献详细信息的字典列表
        """
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            records = []
            
            # 查找文献列表
            results = soup.find("div", class_="result-table-list")
            if not results:
                return records
            
            # 查找所有文献条目
            articles = results.find_all("tr", class_="result-table-tr")
            
            for article in articles:
                try:
                    # 提取标题和链接
                    title_element = article.find("a", class_="title")
                    title = title_element.text.strip() if title_element else ""
                    link = title_element["href"] if title_element and "href" in title_element.attrs else ""
                    
                    # 提取作者
                    author_element = article.find("td", class_="author")
                    authors = author_element.text.strip() if author_element else ""
                    
                    # 提取来源（期刊）
                    source_element = article.find("td", class_="source")
                    source = source_element.text.strip() if source_element else ""
                    
                    # 提取日期
                    date_element = article.find("td", class_="date")
                    date = date_element.text.strip() if date_element else ""
                    
                    # 提取摘要（需要额外请求详情页）
                    abstract = ""
                    if link:
                        abstract = self._fetch_abstract(link)
                    
                    # 构建记录
                    record = {
                        "title": title,
                        "authors": authors,
                        "source": source,
                        "publication_date": date,
                        "abstract": abstract,
                        "link": link,
                        "database": "CNKI"
                    }
                    
                    records.append(record)
                except Exception as e:
                    self.logger.warning(f"解析文献条目时出错: {str(e)}")
                    continue
            
            return records
            
        except Exception as e:
            self.logger.error(f"解析搜索结果时出错: {str(e)}")
            return []
    
    def _fetch_abstract(self, article_link):
        """
        从文章详情页获取摘要
        
        Args:
            article_link: 文章详情页链接
            
        Returns:
            str: 摘要文本
        """
        try:
            # 构建完整URL
            if article_link.startswith("/"):
                article_url = f"https://kns.cnki.net{article_link}"
            else:
                article_url = article_link
            
            # 请求详情页
            response = self._safe_request(article_url)
            if not response:
                return ""
            
            # 解析详情页
            soup = BeautifulSoup(response.text, "html.parser")
            
            # 查找摘要部分
            abstract_div = soup.find("div", class_="abstract-text")
            if abstract_div:
                return abstract_div.text.strip()
            
            return ""
            
        except Exception as e:
            self.logger.warning(f"获取摘要时出错: {str(e)}")
            return ""


def main():
    """直接运行模块时的入口函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='CNKI文献爬虫')
    parser.add_argument('--username', '-u', default='', help='CNKI账号用户名')
    parser.add_argument('--password', '-p', default='', help='CNKI账号密码')
    parser.add_argument('--term', '-t', default='矽肺', help='搜索词')
    parser.add_argument('--start-date', '-s', default='2010/01/01', help='开始日期 (YYYY/MM/DD)')
    parser.add_argument('--end-date', '-d', default='', help='结束日期 (YYYY/MM/DD)，默认为当前日期')
    parser.add_argument('--max-results', '-m', type=int, default=100, help='最大结果数')
    parser.add_argument('--output-dir', '-o', default='output', help='输出目录')
    parser.add_argument('--db-code', '-db', default='CJFD', choices=['CJFD', 'CDFD', 'CMFD'], 
                        help='数据库代码：CJFD(期刊), CDFD(博士论文), CMFD(硕士论文)')
    
    args = parser.parse_args()
    
    # 设置结束日期
    if not args.end_date:
        args.end_date = datetime.now().strftime('%Y/%m/%d')
    
    # 创建爬虫实例
    crawler = CNKICrawler(
        username=args.username,
        password=args.password,
        output_dir=args.output_dir
    )
    
    # 执行搜索
    crawler.search_cnki(
        term=args.term,
        date_range=(args.start_date, args.end_date),
        max_results=args.max_results,
        db_code=args.db_code
    )


if __name__ == "__main__":
    main()