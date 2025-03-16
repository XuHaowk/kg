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
from urllib.parse import quote, urljoin
import urllib3

# 禁用不安全请求警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
        # 设置请求头，模拟最新版Edge浏览器
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
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
            
            # 访问主页，获取初始Cookie
            self.session.get("https://www.cnki.net/", verify=False)
            
            # 访问登录页面
            login_page_url = "https://login.cnki.net/"
            login_page_response = self.session.get(login_page_url, verify=False)
            
            # 提取登录所需的参数
            soup = BeautifulSoup(login_page_response.text, "html.parser")
            
            # 查找表单
            login_form = soup.find("form", {"id": "loginform"})
            if not login_form:
                self.logger.error("无法找到登录表单，可能网站结构已更改")
                return False
                
            # 获取必要的参数
            csrf_token = soup.find("input", {"name": "__RequestVerificationToken"})
            if csrf_token:
                csrf_token = csrf_token.get("value", "")
            else:
                self.logger.warning("未找到CSRF令牌，尝试继续登录")
                csrf_token = ""
            
            # 构建登录数据
            login_data = {
                "username": self.username,
                "password": self.password,
                "__RequestVerificationToken": csrf_token,
                "rememberme": "true",
                "returnUrl": "https://www.cnki.net/"
            }
            
            # 获取登录提交URL
            login_action_url = login_form.get("action", "")
            if not login_action_url:
                login_action_url = "https://login.cnki.net/login"
            elif not login_action_url.startswith("http"):
                login_action_url = urljoin(login_page_url, login_action_url)
            
            # 提交登录表单
            login_headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "https://login.cnki.net",
                "Referer": login_page_url
            }
            
            login_response = self.session.post(
                login_action_url, 
                data=login_data, 
                headers=login_headers,
                allow_redirects=True,
                verify=False
            )
            
            # 检查登录是否成功
            # 方法1：检查重定向到欢迎页
            if "login.cnki.net/login?returnUrl=" in login_response.url:
                self.logger.error("登录失败，仍在登录页面")
                return False
                
            # 方法2：访问个人中心页面验证登录状态
            profile_url = "https://my.cnki.net/"
            profile_response = self.session.get(profile_url, verify=False)
            
            if self.username in profile_response.text or "我的CNKI" in profile_response.text:
                self.logger.info("CNKI登录成功")
                self.is_logged_in = True
                return True
            else:
                self.logger.error("CNKI登录失败，请检查账号密码")
                return False
                
        except Exception as e:
            self.logger.error(f"登录过程中出错: {str(e)}")
            return False
    
    def search_cnki(self, term, date_range=None, max_results=100, db_code="CDFD"):
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
            login_success = False
            if self.username and self.password:
                login_success = self.login()
            
            if not login_success:
                self.logger.info("将以游客模式进行搜索（结果可能受限）")
            
            # 访问高级搜索页面
            adv_search_url = "https://kns.cnki.net/kns8/AdvSearch"
            adv_search_response = self.session.get(adv_search_url, verify=False)
            
            # 获取必要的Cookie和参数
            search_response = self.session.get("https://kns.cnki.net/kns8/AdvSearch", verify=False)
            
            # 准备搜索参数
            encoded_term = quote(term)
            # 将日期格式从YYYY/MM/DD转换为CNKI格式YYYY-MM-DD
            date_filter = ""
            if date_range:
                start_date = date_range[0].replace("/", "-")
                end_date = date_range[1].replace("/", "-")
                date_filter = f"&publishdate_from={start_date}&publishdate_to={end_date}"
            
            # 构建KNS8API检索参数
            current_time = int(time.time() * 1000)
            search_params = {
                "pageindex": "1",
                "pagesize": str(min(max_results, 50)),  # CNKI每页最多50条
                "dbcode": db_code,
                "kw": term,
                "searchtype": "0",  # 0表示"精确"，1表示"模糊"
                "_": str(current_time)
            }
            
            # 构建检索URL
            search_api_url = "https://kns.cnki.net/kns8api/Navi/AdvSearchHandler"
            
            # 构建检索请求头
            search_headers = {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": "https://kns.cnki.net/kns8/AdvSearch",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
            }
            
            # 发送检索请求
            search_api_response = self.session.post(
                search_api_url,
                data=search_params,
                headers=search_headers,
                verify=False
            )
            
            # 解析检索结果
            try:
                search_json = search_api_response.json()
                search_token = search_json.get("token", "")
            except:
                self.logger.error("无法解析搜索API响应")
                search_token = ""
            
            # 如果获取到token，进行结果检索
            if not search_token:
                self.logger.error("未获取到搜索令牌，无法继续检索")
                return {"count": 0, "results": []}
            
            # 构建结果页面URL
            result_page_url = f"https://kns.cnki.net/kns8/Brief/GetGridTableHtml"
            
            # 构建结果请求参数
            result_params = {
                "IsSearch": "true",
                "QueryJson": json.dumps({
                    "Platform": "",
                    "DBCode": db_code,
                    "KuaKuCode": "",
                    "QNode": {
                        "QGroup": [{
                            "Key": "Subject",
                            "Title": "",
                            "Logic": 1,
                            "Items": [{
                                "Key": "KYKW",
                                "Title": "关键词",
                                "Logic": 1,
                                "Items": [],
                                "ChildItems": [{
                                    "Key": "ML",
                                    "Title": "检索词",
                                    "Logic": 1,
                                    "Items": [{
                                        "Field": "SU",
                                        "Name": "主题",
                                        "Value": term,
                                        "Logic": 1,
                                        "Condition": 2
                                    }]
                                }]
                            }],
                            "ChildItems": []
                        }]
                    },
                    "CodeLang": "ch"
                }),
                "PageName": "defaultresult",
                "DBCode": db_code,
                "KuaKuCodes": "",
                "CurPage": "1",
                "RecordsCntPerPage": str(min(max_results, 50)),
                "CurDisplayMode": "listmode",
                "CurrSortField": "",
                "CurrSortFieldType": "desc",
                "IsSortSearch": "false",
                "IsSentenceSearch": "false",
                "Subject": ""
            }
            
            # 加入日期范围限制
            if date_range:
                result_params["QueryJson"] = result_params["QueryJson"].replace(
                    "CodeLang\": \"ch\"", 
                    f"CodeLang\": \"ch\", \"PublishTimeFrom\": \"{start_date}\", \"PublishTimeTo\": \"{end_date}\""
                )
            
            # 发起结果请求
            result_headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": "https://kns.cnki.net/kns8/defaultresult/index"
            }
            
            result_response = self.session.post(
                result_page_url,
                data=result_params,
                headers=result_headers,
                verify=False
            )
            
            # 解析结果HTML
            soup = BeautifulSoup(result_response.text, "html.parser")
            
            # 获取总结果数
            count_info = soup.find("div", class_="pager")
            count = 0
            if count_info:
                count_match = re.search(r'共(\d+)条结果', count_info.text)
                if count_match:
                    count = int(count_match.group(1))
            
            self.logger.info(f"找到 {count} 条结果，将获取 {min(count, max_results)} 条")
            
            if count == 0:
                self.logger.warning("没有找到符合条件的文献")
                return {"count": 0, "results": []}
            
            # 解析搜索结果列表
            articles = []
            article_items = soup.find_all("tr", attrs={"class": ["odd", "even"]})
            
            for item in article_items:
                try:
                    # 提取标题和链接
                    title_element = item.find("a", class_="fz14")
                    if not title_element:
                        continue
                        
                    title = title_element.text.strip()
                    link = title_element.get("href", "")
                    
                    # 提取作者
                    author_element = item.find("td", class_="author")
                    authors = author_element.text.strip() if author_element else ""
                    
                    # 提取期刊/来源
                    source_element = item.find("td", class_="source")
                    source = source_element.text.strip() if source_element else ""
                    
                    # 提取发表日期
                    date_element = item.find("td", class_="date")
                    pub_date = date_element.text.strip() if date_element else ""
                    
                    # 提取摘要（需要额外请求详情页）
                    abstract = self._fetch_abstract(link) if link else ""
                    
                    # 构建文章记录
                    article = {
                        "title": title,
                        "authors": authors,
                        "source": source,
                        "publication_date": pub_date,
                        "abstract": abstract,
                        "link": link,
                        "database": "CNKI"
                    }
                    
                    articles.append(article)
                    
                except Exception as e:
                    self.logger.warning(f"解析文章条目时出错: {str(e)}")
                    continue
            
            # 如果需要获取更多结果，处理分页
            if count > 50 and max_results > 50:
                total_pages = min((max_results + 49) // 50, (count + 49) // 50)
                
                for page in range(2, total_pages + 1):
                    self.logger.info(f"获取第 {page}/{total_pages} 页")
                    
                    # 更新页码参数
                    result_params["CurPage"] = str(page)
                    
                    # 发起下一页请求
                    page_response = self.session.post(
                        result_page_url,
                        data=result_params,
                        headers=result_headers,
                        verify=False
                    )
                    
                    # 解析页面结果
                    page_soup = BeautifulSoup(page_response.text, "html.parser")
                    page_items = page_soup.find_all("tr", attrs={"class": ["odd", "even"]})
                    
                    for item in page_items:
                        try:
                            # 提取标题和链接
                            title_element = item.find("a", class_="fz14")
                            if not title_element:
                                continue
                                
                            title = title_element.text.strip()
                            link = title_element.get("href", "")
                            
                            # 提取作者
                            author_element = item.find("td", class_="author")
                            authors = author_element.text.strip() if author_element else ""
                            
                            # 提取期刊/来源
                            source_element = item.find("td", class_="source")
                            source = source_element.text.strip() if source_element else ""
                            
                            # 提取发表日期
                            date_element = item.find("td", class_="date")
                            pub_date = date_element.text.strip() if date_element else ""
                            
                            # 提取摘要（需要额外请求详情页）
                            abstract = self._fetch_abstract(link) if link else ""
                            
                            # 构建文章记录
                            article = {
                                "title": title,
                                "authors": authors,
                                "source": source,
                                "publication_date": pub_date,
                                "abstract": abstract,
                                "link": link,
                                "database": "CNKI"
                            }
                            
                            articles.append(article)
                            
                            # 检查是否已达到最大结果数
                            if len(articles) >= max_results:
                                break
                                
                        except Exception as e:
                            self.logger.warning(f"解析文章条目时出错: {str(e)}")
                            continue
                    
                    # 检查是否已达到最大结果数
                    if len(articles) >= max_results:
                        break
                    
                    # 添加随机延迟以避免过快请求
                    sleep_time = random.uniform(2.0, 5.0)
                    self.logger.info(f"等待 {sleep_time:.2f} 秒后获取下一页...")
                    time.sleep(sleep_time)
            
            # 保存结果
            all_df = pd.DataFrame(articles)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_path = os.path.join(self.output_dir, f"cnki_results_{timestamp}.csv")
            json_path = os.path.join(self.output_dir, f"cnki_results_{timestamp}.json")
            
            # 保存为CSV和JSON
            all_df.to_csv(csv_path, index=False, encoding='utf-8')
            all_df.to_json(json_path, orient='records', force_ascii=False, indent=2)
            
            self.logger.info(f"所有结果已保存到 {csv_path} 和 {json_path}")
            
            return {"count": count, "results": articles}
        
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
                    response = self.session.post(url, **kwargs, timeout=30, verify=False)
                else:
                    response = self.session.get(url, **kwargs, timeout=30, verify=False)
                
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
    
    def _fetch_abstract(self, article_href):
        """
        从文章详情页获取摘要
        
        Args:
            article_href: 文章详情页链接（相对路径）
            
        Returns:
            str: 摘要文本
        """
        try:
            # 构建完整URL
            article_url = urljoin("https://kns.cnki.net", article_href)
            
            # 请求详情页
            self.logger.info(f"获取文章摘要: {article_url}")
            response = self._safe_request(article_url)
            if not response:
                return ""
            
            # 解析详情页
            soup = BeautifulSoup(response.text, "html.parser")
            
            # 查找摘要部分（尝试多种可能的类名或结构）
            abstract_div = None
            
            # 方法1: 使用class查找
            abstract_div = soup.find("div", class_="abstract-text")
            
            # 方法2: 查找带有"摘要"标签的div
            if not abstract_div:
                abstract_label = soup.find("label", text=re.compile("摘要"))
                if abstract_label:
                    abstract_div = abstract_label.find_next("div")
            
            # 方法3: 使用包含特定ID的div
            if not abstract_div:
                abstract_div = soup.find("div", id=re.compile("abstract"))
            
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
    parser.add_argument('--max-results', '-m', type=int, default=10000, help='最大结果数')
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