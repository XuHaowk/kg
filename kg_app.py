#!/usr/bin/env python3
"""
基于大语言模型的文本实体提取系统

该程序集成了PubMed与CNKI文献爬取、文本处理、实体关系提取和知识图谱构建功能，
提供用户友好的界面来执行整个工作流程。
"""

import os
import sys
import json
import time
import re
import threading
import tkinter as tk
from tkinter import ttk, messagebox, filedialog, scrolledtext
from datetime import datetime, timedelta
import configparser
import webbrowser
import importlib
import logging

# 添加项目根目录到系统路径 - Windows路径处理
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(script_dir)

# 导入项目模块
from crawler import PubMedCrawler
from batch_process import process_batch
from extractor.kimi_client import KimiClient
from kg_builder import KnowledgeGraphBuilder
from cnki_selenium_fixed import CNKIWebScraper


# Try to import the CNKI Selenium integration
try:
    from cnki_selenium_integration import CNKISeleniumIntegration, is_selenium_available, show_installation_instructions
    SELENIUM_AVAILABLE = is_selenium_available()
except ImportError:
    SELENIUM_AVAILABLE = False
class KGApp:
    """文献知识图谱应用主类"""

    def __init__(self, root):
        """
        初始化应用程序
    
        Args:
            root: tkinter根窗口
        """
        self.root = root
        self.root.title("基于大语言模型的文本实体关系提取系统")
        self.root.geometry("900x700")
        self.root.minsize(900, 700)
    
        # 先创建结果目录，再加载配置 (修改顺序)
        self.results_dir = os.path.join(script_dir, "results")
        os.makedirs(self.results_dir, exist_ok=True)
    
        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            filename=os.path.join(self.results_dir, 'app.log'),
            filemode='a'
        )
        self.logger = logging.getLogger("KGApp")

        # 创建配置对象
        self.config = configparser.ConfigParser()
        self.config_file = os.path.join(script_dir, "app_config.ini")
        self.load_config()
    
        # 创建选项卡控件
        self.tabControl = ttk.Notebook(root)
        
        # 创建各个选项卡
        self.login_tab = ttk.Frame(self.tabControl)
        self.search_tab = ttk.Frame(self.tabControl)
        self.process_tab = ttk.Frame(self.tabControl)
        self.about_tab = ttk.Frame(self.tabControl)
        
        # 添加选项卡到控件
        self.tabControl.add(self.login_tab, text="登录配置")
        self.tabControl.add(self.search_tab, text="文献搜索")
        self.tabControl.add(self.process_tab, text="数据处理")
        self.tabControl.add(self.about_tab, text="关于系统")
        
        # 放置选项卡控件
        self.tabControl.pack(expand=1, fill="both")
        
        # 初始化各个选项卡内容
        self.setup_login_tab()
        self.setup_search_tab()
        self.setup_process_tab()
        self.setup_about_tab()
        
        # 初始化运行状态变量
        self.is_running = False
        self.process_thread = None
        
        # 检查API配置同步
        self.check_api_config_sync()

    def check_api_config_sync(self):
        """检查app_config.ini和config.py中的API密钥是否同步"""
        try:
            # 检查config.py文件
            config_py_path = os.path.join(script_dir, "config.py")
            if not os.path.exists(config_py_path):
                self.logger.warning("找不到config.py文件")
                return
            
            # 读取config.py中的API密钥
            config_py_api_key = ""
            with open(config_py_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.startswith("KIMI_API_KEY = "):
                        # 提取引号中的内容
                        match = re.search(r'KIMI_API_KEY = ["\'](.+?)["\']', line)
                        if match:
                            config_py_api_key = match.group(1)
                        break
            
            # 获取app_config.ini中的API密钥
            app_config_api_key = self.config.get('API', 'moonshot_api_key', fallback='')
            
            # 比较两个文件中的API密钥
            if app_config_api_key and config_py_api_key != app_config_api_key:
                self.logger.warning("API密钥不同步，正在更新config.py")
                # 调用save_api_config方法进行同步
                self.save_api_config()
                
                # 强制重新导入config模块
                try:
                    import config
                    importlib.reload(config)
                    self.logger.info("已重新加载config模块")
                except Exception as e:
                    self.logger.error(f"重新加载config模块时出错: {str(e)}")
        except Exception as e:
            self.logger.error(f"检查API配置同步时出错: {str(e)}")

    def load_config(self):
        """加载配置文件，若不存在则创建默认配置"""
        try:
            if os.path.exists(self.config_file):
                self.config.read(self.config_file, encoding='utf-8')  # 添加encoding参数
            else:
                # 创建默认配置
                self.config['API'] = {
                    'ncbi_email': '',
                    'ncbi_api_key': '',
                    'moonshot_api_key': '',
                    'cnki_username': '',
                    'cnki_password': ''
                }
                self.config['Search'] = {
                    'search_terms': '输入关键词，多个使用逗号分隔',
                    'start_date': (datetime.now() - timedelta(days=365*5)).strftime('%Y/%m/%d'),
                    'end_date': datetime.now().strftime('%Y/%m/%d'),
                    'max_results': '1000',
                    'database': 'pubmed',
                    'search_mode': 'separate',
                    'cnki_db_code': 'CJFD'
                }
                self.config['Process'] = {
                    'output_dir': os.path.join(self.results_dir, 'output').replace('\\', '\\\\'),  # 转义反斜杠
                    'output_format': 'json',
                    'parallel': 'True',
                    'max_workers': '4'
                }
                self.save_config()
        except Exception as e:
            self.logger.error(f"加载配置文件时出错: {str(e)}")
            messagebox.showerror("配置加载错误", f"加载配置文件时出错: {str(e)}")

    def save_config(self):
        """保存配置到文件"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:  # 添加encoding参数
                self.config.write(f)
        except Exception as e:
            self.logger.error(f"保存配置文件时出错: {str(e)}")
            messagebox.showerror("配置保存错误", f"保存配置文件时出错: {str(e)}")
            
    def setup_login_tab(self):
        """设置登录配置选项卡"""
        # 创建主框架
        main_frame = ttk.Frame(self.login_tab, padding="10")
        main_frame.pack(fill="both", expand=True)
        
        # 创建标题
        ttk.Label(main_frame, text="API密钥配置", font=("Arial", 16, "bold")).grid(
            row=0, column=0, columnspan=2, sticky="w", pady=(0, 20))
        
        # NCBI配置
        ttk.Label(main_frame, text="NCBI配置", font=("Arial", 12, "bold")).grid(
            row=1, column=0, columnspan=2, sticky="w", pady=(10, 5))
        
        ttk.Label(main_frame, text="电子邮箱:").grid(row=2, column=0, sticky="w", pady=2)
        self.ncbi_email_var = tk.StringVar(value=self.config.get('API', 'ncbi_email', fallback=''))
        ttk.Entry(main_frame, textvariable=self.ncbi_email_var, width=50).grid(
            row=2, column=1, sticky="w", pady=2)
        
        ttk.Label(main_frame, text="API密钥:").grid(row=3, column=0, sticky="w", pady=2)
        self.ncbi_api_key_var = tk.StringVar(value=self.config.get('API', 'ncbi_api_key', fallback=''))
        ttk.Entry(main_frame, textvariable=self.ncbi_api_key_var, width=50).grid(
            row=3, column=1, sticky="w", pady=2)
        
        ttk.Label(main_frame, text="注: 获取NCBI API密钥可提高抓取速度，每秒可查询10次").grid(
            row=4, column=0, columnspan=2, sticky="w", pady=(0, 10))
        
        # CNKI配置（新增）
        ttk.Label(main_frame, text="CNKI配置", font=("Arial", 12, "bold")).grid(
            row=5, column=0, columnspan=2, sticky="w", pady=(10, 5))
        
        ttk.Label(main_frame, text="用户名:").grid(row=6, column=0, sticky="w", pady=2)
        self.cnki_username_var = tk.StringVar(value=self.config.get('API', 'cnki_username', fallback=''))
        ttk.Entry(main_frame, textvariable=self.cnki_username_var, width=50).grid(
            row=6, column=1, sticky="w", pady=2)
        
        ttk.Label(main_frame, text="密码:").grid(row=7, column=0, sticky="w", pady=2)
        self.cnki_password_var = tk.StringVar(value=self.config.get('API', 'cnki_password', fallback=''))
        ttk.Entry(main_frame, textvariable=self.cnki_password_var, width=50, show="*").grid(
            row=7, column=1, sticky="w", pady=2)
        
        ttk.Label(main_frame, text="注: CNKI账号用于访问中国知网文献，不提供则使用游客模式（受限）").grid(
            row=8, column=0, columnspan=2, sticky="w", pady=(0, 10))
        
        # Moonshot配置
        ttk.Label(main_frame, text="Moonshot AI配置", font=("Arial", 12, "bold")).grid(
            row=9, column=0, columnspan=2, sticky="w", pady=(10, 5))
        
        ttk.Label(main_frame, text="API密钥:").grid(row=10, column=0, sticky="w", pady=2)
        self.moonshot_api_key_var = tk.StringVar(value=self.config.get('API', 'moonshot_api_key', fallback=''))
        ttk.Entry(main_frame, textvariable=self.moonshot_api_key_var, width=50).grid(
            row=10, column=1, sticky="w", pady=2)
        
        ttk.Label(main_frame, text="注: 需要Moonshot API密钥进行实体关系提取").grid(
            row=11, column=0, columnspan=2, sticky="w", pady=(0, 10))
        
        # 保存按钮
        ttk.Button(main_frame, text="保存配置", command=self.save_api_config).grid(
            row=12, column=0, columnspan=2, pady=20)
        
        # 说明文本
        help_text = (
            "系统使用说明:\n\n"
            "1. 配置API密钥和账号\n"
            "   - NCBI API密钥用于从PubMed获取文献\n"
            "   - CNKI账号用于从中国知网获取文献\n"
            "   - Moonshot API密钥用于实体关系提取\n\n"
            "2. 在'文献搜索'选项卡中设置搜索参数并获取文献\n\n"
            "3. 在'数据处理'选项卡中处理文献并构建知识图谱\n\n"
            "4. 知识图谱将保存为HTML格式，可以在浏览器中查看"
        )
        text_area = scrolledtext.ScrolledText(main_frame, width=70, height=15, wrap=tk.WORD)
        text_area.grid(row=13, column=0, columnspan=2, pady=10)
        text_area.insert(tk.INSERT, help_text)
        text_area.config(state=tk.DISABLED)

    def save_api_config(self):
        """保存API配置到app_config.ini和config.py文件"""
        # 获取用户输入的API配置
        ncbi_email = self.ncbi_email_var.get()
        ncbi_api_key = self.ncbi_api_key_var.get()
        moonshot_api_key = self.moonshot_api_key_var.get()
        cnki_username = self.cnki_username_var.get()
        cnki_password = self.cnki_password_var.get()
        
        self.logger.info("正在保存API配置...")
        
        # 保存到app_config.ini
        self.config['API'] = {
            'ncbi_email': ncbi_email,
            'ncbi_api_key': ncbi_api_key,
            'moonshot_api_key': moonshot_api_key,
            'cnki_username': cnki_username,
            'cnki_password': cnki_password
        }
        self.save_config()
        self.logger.info("已保存到app_config.ini")
        
        # 同步更新config.py文件
        try:
            # 获取config.py文件路径
            config_py_path = os.path.join(script_dir, "config.py")
            
            # 检查文件是否存在
            if os.path.exists(config_py_path):
                # 读取现有的config.py内容
                with open(config_py_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # 替换API密钥
                new_content = []
                lines = content.splitlines()
                kimi_api_key_updated = False
                
                for line in lines:
                    if line.startswith("KIMI_API_KEY = "):
                        new_line = f'KIMI_API_KEY = "{moonshot_api_key}"'
                        new_content.append(new_line)
                        kimi_api_key_updated = True
                    else:
                        new_content.append(line)
                
                # 如果没有找到KIMI_API_KEY定义，则添加到文件末尾
                if not kimi_api_key_updated and moonshot_api_key:
                    new_content.append(f'KIMI_API_KEY = "{moonshot_api_key}"')
                
                # 写回更新后的内容
                with open(config_py_path, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(new_content))
                
                self.logger.info(f"已更新config.py中的API密钥")
                
                # 尝试重新加载config模块
                try:
                    import config
                    importlib.reload(config)
                    self.logger.info("已重新加载config模块")
                except Exception as e:
                    self.logger.error(f"重新加载config模块时出错: {str(e)}")
                
                # 显示成功消息
                messagebox.showinfo("配置已保存", "API配置已成功保存到app_config.ini和config.py")
            else:
                self.logger.warning("找不到config.py文件")
                messagebox.showwarning("文件不存在", "无法找到config.py文件，API密钥仅保存到app_config.ini")
        except Exception as e:
            self.logger.error(f"更新config.py时出错: {str(e)}")
            # 仅显示app_config.ini保存成功的消息，并附带错误信息
            messagebox.showwarning("部分保存成功", 
                             f"API配置已保存到app_config.ini，但更新config.py时出错: {str(e)}")

    def setup_search_tab(self):
        """设置文献搜索选项卡"""
        # 创建主框架
        main_frame = ttk.Frame(self.search_tab, padding="10")
        main_frame.pack(fill="both", expand=True)

        # 创建标题
        ttk.Label(main_frame, text="文献搜索", font=("Arial", 16, "bold")).grid(
            row=0, column=0, columnspan=2, sticky="w", pady=(0, 20))

        # 数据库选择
        ttk.Label(main_frame, text="数据库:").grid(row=1, column=0, sticky="w", pady=2)
        self.database_var = tk.StringVar(value=self.config.get('Search', 'database', fallback='pubmed'))
        database_frame = ttk.Frame(main_frame)
        database_frame.grid(row=1, column=1, sticky="w", pady=2)
        ttk.Radiobutton(database_frame, text="PubMed", variable=self.database_var, 
                       value="pubmed", command=self.update_search_options).grid(row=0, column=0, sticky="w", padx=5)
        ttk.Radiobutton(database_frame, text="CNKI中国知网", variable=self.database_var, 
                       value="cnki", command=self.update_search_options).grid(row=0, column=1, sticky="w", padx=5)


        # 搜索参数
        ttk.Label(main_frame, text="搜索关键词(用逗号分隔):").grid(row=2, column=0, sticky="w", pady=2)
        self.search_terms_var = tk.StringVar(value=self.config.get('Search', 'search_terms', fallback='输入关键词，多个使用逗号分隔'))
        ttk.Entry(main_frame, textvariable=self.search_terms_var, width=50).grid(
            row=2, column=1, sticky="w", pady=2)

        ttk.Label(main_frame, text="起始日期(YYYY/MM/DD):").grid(row=3, column=0, sticky="w", pady=2)
        self.start_date_var = tk.StringVar(value=self.config.get('Search', 'start_date'))
        ttk.Entry(main_frame, textvariable=self.start_date_var, width=20).grid(
            row=3, column=1, sticky="w", pady=2)

        ttk.Label(main_frame, text="结束日期(YYYY/MM/DD):").grid(row=4, column=0, sticky="w", pady=2)
        self.end_date_var = tk.StringVar(value=self.config.get('Search', 'end_date'))
        ttk.Entry(main_frame, textvariable=self.end_date_var, width=20).grid(
            row=4, column=1, sticky="w", pady=2)

        ttk.Label(main_frame, text="最大结果数:").grid(row=5, column=0, sticky="w", pady=2)
        self.max_results_var = tk.StringVar(value=self.config.get('Search', 'max_results', fallback='1000'))
        ttk.Entry(main_frame, textvariable=self.max_results_var, width=10).grid(
            row=5, column=1, sticky="w", pady=2)

        # 添加搜索模式选项
        ttk.Label(main_frame, text="搜索模式:").grid(row=6, column=0, sticky="w", pady=2)
        self.search_mode_var = tk.StringVar(value=self.config.get('Search', 'search_mode', fallback='separate'))
        search_mode_frame = ttk.Frame(main_frame)
        search_mode_frame.grid(row=6, column=1, sticky="w", pady=2)
        ttk.Radiobutton(search_mode_frame, text="分别搜索每个关键词", variable=self.search_mode_var, 
                       value="separate").grid(row=0, column=0, sticky="w", padx=5)
        ttk.Radiobutton(search_mode_frame, text="搜索所有关键词同时出现 (AND)", variable=self.search_mode_var, 
                       value="combined").grid(row=0, column=1, sticky="w", padx=5)

        # CNKI特定选项
        self.cnki_frame = ttk.LabelFrame(main_frame, text="CNKI特定选项", padding=5)
        self.cnki_frame.grid(row=7, column=0, columnspan=2, sticky="we", pady=5)

        ttk.Label(self.cnki_frame, text="数据库类型:").grid(row=0, column=0, sticky="w", pady=2)
        self.cnki_db_code_var = tk.StringVar(value=self.config.get('Search', 'cnki_db_code', fallback='CJFD'))
        db_codes = {"CJFD": "中国学术期刊", "CDFD": "博士论文", "CMFD": "硕士论文"}
        cnki_db_combo = ttk.Combobox(self.cnki_frame, textvariable=self.cnki_db_code_var, width=15)
        cnki_db_combo['values'] = [f"{code} ({desc})" for code, desc in db_codes.items()]
        cnki_db_combo.grid(row=0, column=1, sticky="w", pady=2)
        cnki_db_combo.current(0)

        # 默认根据当前数据库设置显示或隐藏CNKI特定选项
        self.update_search_options()

        # 保存搜索设置按钮
        ttk.Button(main_frame, text="保存搜索设置", command=self.save_search_config).grid(
            row=8, column=0, sticky="w", pady=10)

        # 开始爬取按钮
        ttk.Button(main_frame, text="开始文献爬取", command=self.start_crawling).grid(
            row=8, column=1, sticky="w", pady=10)

        # 日志框
        ttk.Label(main_frame, text="爬取日志:", font=("Arial", 10, "bold")).grid(
            row=9, column=0, columnspan=2, sticky="w", pady=(10, 5))

        self.search_log = scrolledtext.ScrolledText(main_frame, width=80, height=15, wrap=tk.WORD)
        self.search_log.grid(row=10, column=0, columnspan=2, pady=5)
        self.search_log.config(state=tk.DISABLED)

    def update_search_options(self):
        """根据选择的数据库更新搜索选项"""
        if self.database_var.get() == "cnki":
            self.cnki_frame.grid(row=7, column=0, columnspan=2, sticky="we", pady=5)
        else:
            self.cnki_frame.grid_remove()

    def save_search_config(self):
        """保存搜索配置"""
        # 从CNKI数据库代码中提取实际代码值（如"CJFD (中国学术期刊)"中提取"CJFD"）
        cnki_db_code = self.cnki_db_code_var.get().split(" ")[0] if " " in self.cnki_db_code_var.get() else self.cnki_db_code_var.get()
        
        self.config['Search'] = {
            'search_terms': self.search_terms_var.get(),
            'start_date': self.start_date_var.get(),
            'end_date': self.end_date_var.get(),
            'max_results': self.max_results_var.get(),
            'database': self.database_var.get(),
            'search_mode': self.search_mode_var.get(),
            'cnki_db_code': cnki_db_code
        }
        self.save_config()
        messagebox.showinfo("配置已保存", "搜索配置已成功保存")

    def crawl_pubmed(self, email, api_key, search_terms, date_range, max_results, output_dir):
        """
        PubMed文献爬取线程
    
        Args:
            email: NCBI请求用户邮箱
            api_key: NCBI API密钥
            search_terms: 搜索关键词列表
            date_range: 日期范围元组(开始日期, 结束日期)
            max_results: 最大结果数
            output_dir: 输出目录
        """
        try:
            self.append_to_log(self.search_log, f"开始PubMed文献爬取")
        
            # 创建PubMed爬虫实例
            crawler = PubMedCrawler(
                email=email,
                api_key=api_key,
                batch_size=100,
                output_dir=output_dir
            )
        
            # 获取搜索模式
            search_mode = self.config.get('Search', 'search_mode', fallback='separate')
        
            # 根据搜索模式执行搜索
            if search_mode == 'combined' and len(search_terms) > 1:
                # 使用AND操作符组合所有关键词
                combined_term = " AND ".join(f"({term})" for term in search_terms)
                self.append_to_log(self.search_log, f"搜索词: {combined_term} (关键词同时出现)")
                self.append_to_log(self.search_log, f"日期范围: {date_range[0]} - {date_range[1]}")
                self.append_to_log(self.search_log, f"最大结果数: {max_results}")
                self.append_to_log(self.search_log, f"输出目录: {output_dir}")
            
                results = crawler.search_pubmed(
                    term=combined_term,
                    date_range=date_range,
                    max_results=max_results
                )
            
                if results and 'count' in results:
                    count = results['count']
                    self.append_to_log(self.search_log, f"找到 {count} 条相关文献")
                    total_count = count
                else:
                    self.append_to_log(self.search_log, "搜索未返回有效结果")
                    total_count = 0
                
            else:
                # 原始方式：分别搜索每个关键词
                self.append_to_log(self.search_log, f"搜索词: {', '.join(search_terms)} (分别搜索)")
                self.append_to_log(self.search_log, f"日期范围: {date_range[0]} - {date_range[1]}")
                self.append_to_log(self.search_log, f"最大结果数: {max_results}")
                self.append_to_log(self.search_log, f"输出目录: {output_dir}")
            
                total_count = 0
                for term in search_terms:
                    self.append_to_log(self.search_log, f"\n搜索关键词: {term}")
                    results = crawler.search_pubmed(
                        term=term,
                        date_range=date_range,
                        max_results=max_results
                    )
                
                    if results and 'count' in results:
                        count = results['count']
                        self.append_to_log(self.search_log, f"找到 {count} 条相关文献")
                        total_count += count
                    else:
                        self.append_to_log(self.search_log, "搜索未返回有效结果")
        
            # 查看爬取的文件列表
            file_list = []
            for root, _, files in os.walk(output_dir):
                for file in files:
                    if file.endswith('.json'):
                        file_list.append(os.path.join(root, file))
        
            self.append_to_log(self.search_log, f"\n爬取完成，共获取 {total_count} 条文献")
            self.append_to_log(self.search_log, f"生成 {len(file_list)} 个文件:")
        
            for file_path in file_list:
                rel_path = os.path.relpath(file_path, output_dir)
                self.append_to_log(self.search_log, f"  - {rel_path}")
        
            self.append_to_log(self.search_log, "\n可以进入'数据处理'选项卡开始处理爬取的文献")
        
        except Exception as e:
            self.logger.error(f"爬取过程中发生错误: {str(e)}")
            self.append_to_log(self.search_log, f"爬取过程中发生错误: {str(e)}")
            import traceback
            self.append_to_log(self.search_log, traceback.format_exc())
        finally:
            self.is_running = False

    def crawl_cnki(self, username, password, search_terms, date_range, max_results, output_dir, db_code="CJFD"):
        """
        CNKI文献爬取线程
        
        Args:
            username: CNKI账号用户名
            password: CNKI账号密码
            search_terms: 搜索关键词列表
            date_range: 日期范围元组(开始日期, 结束日期)
            max_results: 最大结果数
            output_dir: 输出目录
            db_code: CNKI数据库代码
        """
        try:
            self.append_to_log(self.search_log, f"开始CNKI文献爬取")
            
            # 导入CNKI爬虫模块
            try:
                from cnki_crawler import CNKICrawler
            except ImportError:
                self.append_to_log(self.search_log, "错误: 找不到CNKI爬虫模块，请确保cnki_crawler.py文件在正确位置")
                self.is_running = False
                return
            
            # 创建CNKI爬虫实例
            crawler = CNKICrawler(
                username=username,
                password=password,
                batch_size=100,
                output_dir=output_dir
            )
            
            # 获取搜索模式
            search_mode = self.config.get('Search', 'search_mode', fallback='separate')
            
            # 根据搜索模式执行搜索
            if search_mode == 'combined' and len(search_terms) > 1:
                # 使用AND操作符组合所有关键词
                combined_term = " AND ".join(f"({term})" for term in search_terms)
                self.append_to_log(self.search_log, f"搜索词: {combined_term} (关键词同时出现)")
                self.append_to_log(self.search_log, f"日期范围: {date_range[0]} - {date_range[1]}")
                self.append_to_log(self.search_log, f"最大结果数: {max_results}")
                self.append_to_log(self.search_log, f"输出目录: {output_dir}")
                self.append_to_log(self.search_log, f"数据库: CNKI {db_code}")
                
                results = crawler.search_cnki(
                    term=combined_term,
                    date_range=date_range,
                    max_results=max_results,
                    db_code=db_code
                )
                
                if results and 'count' in results:
                    count = results['count']
                    self.append_to_log(self.search_log, f"找到 {count} 条相关文献")
                    total_count = count
                else:
                    self.append_to_log(self.search_log, "搜索未返回有效结果")
                    total_count = 0
                    
            else:
                # 分别搜索每个关键词
                self.append_to_log(self.search_log, f"搜索词: {', '.join(search_terms)} (分别搜索)")
                self.append_to_log(self.search_log, f"日期范围: {date_range[0]} - {date_range[1]}")
                self.append_to_log(self.search_log, f"最大结果数: {max_results}")
                self.append_to_log(self.search_log, f"输出目录: {output_dir}")
                self.append_to_log(self.search_log, f"数据库: CNKI {db_code}")
                
                total_count = 0
                for term in search_terms:
                    self.append_to_log(self.search_log, f"\n搜索关键词: {term}")
                    results = crawler.search_cnki(
                        term=term,
                        date_range=date_range,
                        max_results=max_results,
                        db_code=db_code
                    )
                    
                    if results and 'count' in results:
                        count = results['count']
                        self.append_to_log(self.search_log, f"找到 {count} 条相关文献")
                        total_count += count
                    else:
                        self.append_to_log(self.search_log, "搜索未返回有效结果")
            
            # 查看爬取的文件列表
            file_list = []
            for root, _, files in os.walk(output_dir):
                for file in files:
                    if file.endswith('.json'):
                        file_list.append(os.path.join(root, file))
            
            self.append_to_log(self.search_log, f"\n爬取完成，共获取 {total_count} 条文献")
            self.append_to_log(self.search_log, f"生成 {len(file_list)} 个文件:")
            
            for file_path in file_list:
                rel_path = os.path.relpath(file_path, output_dir)
                self.append_to_log(self.search_log, f"  - {rel_path}")
            
            self.append_to_log(self.search_log, "\n可以进入'数据处理'选项卡开始处理爬取的文献")
            
        except Exception as e:
            self.logger.error(f"爬取过程中发生错误: {str(e)}")
            self.append_to_log(self.search_log, f"爬取过程中发生错误: {str(e)}")
            import traceback
            self.append_to_log(self.search_log, traceback.format_exc())
        finally:
            self.is_running = False

    def crawl_cnki_with_edge(self, search_term, date_range, max_results, output_dir, db_code):
        """Crawl CNKI using Edge browser crawler"""
        try:
            # Create the Edge crawler
            from cnki_edge_crawler import CNKIEdgeCrawler
            crawler = CNKIEdgeCrawler(output_dir=output_dir)
        
            # Perform search
            results = crawler.search_cnki(
                term=search_term,
                date_range=date_range,
                max_results=max_results,
                db_code=db_code
            )
        
            if "error" in results:
                self.append_to_log(self.search_log, f"爬取过程中发生错误: {results['error']}")
            else:
                count = results.get("count", 0)
                crawled = len(results.get("results", []))
            
                self.append_to_log(self.search_log, f"\n爬取完成!")
                self.append_to_log(self.search_log, f"找到 {count} 条结果, 爬取 {crawled} 篇文献")
            
                if "json_path" in results:
                    self.append_to_log(self.search_log, f"结果已保存为JSON: {results['json_path']}")
                if "csv_path" in results:
                    self.append_to_log(self.search_log, f"结果已保存为CSV: {results['csv_path']}")
            
                self.append_to_log(self.search_log, "\n可以进入'数据处理'选项卡开始处理爬取的文献")
            
        except Exception as e:
            self.logger.error(f"Edge浏览器爬取CNKI过程中发生错误: {str(e)}")
            self.append_to_log(self.search_log, f"爬取过程中发生错误: {str(e)}")
            import traceback
            self.append_to_log(self.search_log, traceback.format_exc())
        finally:
            self.is_running = False
    
    def crawl_cnki_with_undetected(self, search_term, date_range, max_results, output_dir, db_code):
        """Crawl CNKI using undetected-chromedriver crawler"""
        try:
            # Create the undetected crawler
            from cnki_undetected_crawler import CNKIUndetectedCrawler
            crawler = CNKIUndetectedCrawler(output_dir=output_dir)
        
            # Perform search
            results = crawler.search_cnki(
                term=search_term,
                date_range=date_range,
                max_results=max_results,
                db_code=db_code
            )
        
            if "error" in results:
                self.append_to_log(self.search_log, f"爬取过程中发生错误: {results['error']}")
            else:
                count = results.get("count", 0)
                crawled = len(results.get("results", []))
            
                self.append_to_log(self.search_log, f"\n爬取完成!")
                self.append_to_log(self.search_log, f"找到 {count} 条结果, 爬取 {crawled} 篇文献")
            
                if "json_path" in results:
                    self.append_to_log(self.search_log, f"结果已保存为JSON: {results['json_path']}")
                if "csv_path" in results:
                    self.append_to_log(self.search_log, f"结果已保存为CSV: {results['csv_path']}")
            
                self.append_to_log(self.search_log, "\n可以进入'数据处理'选项卡开始处理爬取的文献")
            
        except Exception as e:
            self.logger.error(f"Undetected爬虫爬取CNKI过程中发生错误: {str(e)}")
            self.append_to_log(self.search_log, f"爬取过程中发生错误: {str(e)}")
            import traceback
            self.append_to_log(self.search_log, traceback.format_exc())
        finally:
            self.is_running = False
    
    def crawl_cnki_with_direct(self, search_term, date_range, max_results, output_dir, db_code):
        """Crawl CNKI using direct HTTP requests (without browser automation)"""
        try:
            # Create the direct crawler
            from cnki_direct_crawler import CNKIDirectCrawler
            crawler = CNKIDirectCrawler(output_dir=output_dir)
        
            # Perform search
            results = crawler.search_cnki(
                term=search_term,
                date_range=date_range,
                max_results=max_results,
                db_code=db_code
            )
        
            if "error" in results:
                self.append_to_log(self.search_log, f"爬取过程中发生错误: {results['error']}")
            else:
                count = results.get("count", 0)
                crawled = len(results.get("results", []))
            
                self.append_to_log(self.search_log, f"\n爬取完成!")
                self.append_to_log(self.search_log, f"找到 {count} 条结果, 爬取 {crawled} 篇文献")
            
                if "json_path" in results:
                    self.append_to_log(self.search_log, f"结果已保存为JSON: {results['json_path']}")
                if "csv_path" in results:
                    self.append_to_log(self.search_log, f"结果已保存为CSV: {results['csv_path']}")
            
                self.append_to_log(self.search_log, "\n可以进入'数据处理'选项卡开始处理爬取的文献")
            
        except Exception as e:
            self.logger.error(f"Direct HTTP爬取CNKI过程中发生错误: {str(e)}")
            self.append_to_log(self.search_log, f"爬取过程中发生错误: {str(e)}")
            import traceback
            self.append_to_log(self.search_log, traceback.format_exc())
        finally:
            self.is_running = False
    def start_crawling(self):
        """开始爬取文献"""
        if self.is_running:
            messagebox.showwarning("操作进行中", "已有操作正在进行，请等待完成")
            return

        # 获取选择的数据库
        database = self.database_var.get()

        # 根据不同数据库获取必要参数
        if database == "pubmed":
            # 获取API配置
            email = self.ncbi_email_var.get()
            api_key = self.ncbi_api_key_var.get()
    
            # 检查必填参数
            if not email or '@' not in email:
                messagebox.showerror("参数错误", "请输入有效的邮箱地址")
                return
        elif database == "cnki":
            # CNKI可以不需要账号密码，使用游客模式
            pass
        else:
            messagebox.showerror("参数错误", "未支持的数据库类型")
            return

        # 获取搜索参数
        search_terms = [term.strip() for term in self.search_terms_var.get().split(',')]
        date_range = (self.start_date_var.get(), self.end_date_var.get())
        try:
            max_results = int(self.max_results_var.get())
        except ValueError:
            messagebox.showerror("参数错误", "最大结果数必须是整数")
            return

        # 获取搜索模式
        search_mode = self.search_mode_var.get()

        # 清空日志
        self.search_log.config(state=tk.NORMAL)
        self.search_log.delete(1.0, tk.END)
        self.search_log.config(state=tk.DISABLED)

        # 创建输出目录
        output_dir = os.path.join(self.results_dir, f'{database}_data', 
                             datetime.now().strftime("%Y%m%d_%H%M%S"))
        os.makedirs(output_dir, exist_ok=True)

        # 启动爬取线程
        self.is_running = True

        if database == "pubmed":
            self.process_thread = threading.Thread(
                target=self.crawl_pubmed,
                args=(email, api_key, search_terms, date_range, max_results, output_dir)
            )
            self.process_thread.daemon = True
            self.process_thread.start()
        else:  # cnki
            username = self.cnki_username_var.get()
            password = self.cnki_password_var.get()
            cnki_db_code = self.cnki_db_code_var.get().split(" ")[0] if " " in self.cnki_db_code_var.get() else self.cnki_db_code_var.get()
    
            # 获取选择的爬虫方法
            crawler_method = "api"  # 默认值
            if hasattr(self, 'cnki_method_var'):
                crawler_method = self.cnki_method_var.get()
        
            if crawler_method == "edge":
                # 使用Edge浏览器爬虫
                try:
                    from cnki_edge_crawler import CNKIEdgeCrawler
                
                    self.append_to_log(self.search_log, f"开始使用Edge浏览器爬取CNKI文献")
                    self.append_to_log(self.search_log, f"搜索词: {', '.join(search_terms)}")
                    self.append_to_log(self.search_log, f"日期范围: {date_range[0]} - {date_range[1]}")
                    self.append_to_log(self.search_log, f"最大结果数: {max_results}")
                    self.append_to_log(self.search_log, f"输出目录: {output_dir}")
                    self.append_to_log(self.search_log, f"数据库: CNKI {cnki_db_code}")
                
                    # 准备搜索词
                    search_term = search_terms[0]
                    if search_mode == "combined" and len(search_terms) > 1:
                        search_term = " AND ".join([f"({term})" for term in search_terms])
                
                    # 启动Edge爬虫线程
                    self.process_thread = threading.Thread(
                        target=self.crawl_cnki_with_edge,
                        args=(search_term, date_range, max_results, output_dir, cnki_db_code)
                    )
                    self.process_thread.daemon = True
                    self.process_thread.start()
                
                except ImportError:
                    self.append_to_log(self.search_log, "Edge浏览器方法不可用。请确保cnki_edge_crawler.py在正确位置。")
                    self.is_running = False
                    return
            elif crawler_method == "undetected":
                # 使用增强型Chrome爬虫
                try:
                    from cnki_undetected_crawler import CNKIUndetectedCrawler
                
                    self.append_to_log(self.search_log, f"开始使用增强型Chrome爬取CNKI文献")
                    self.append_to_log(self.search_log, f"搜索词: {', '.join(search_terms)}")
                    self.append_to_log(self.search_log, f"日期范围: {date_range[0]} - {date_range[1]}")
                    self.append_to_log(self.search_log, f"最大结果数: {max_results}")
                    self.append_to_log(self.search_log, f"输出目录: {output_dir}")
                    self.append_to_log(self.search_log, f"数据库: CNKI {cnki_db_code}")
                
                    # 准备搜索词
                    search_term = search_terms[0]
                    if search_mode == "combined" and len(search_terms) > 1:
                        search_term = " AND ".join([f"({term})" for term in search_terms])
                
                    # 启动增强型Chrome爬虫线程
                    self.process_thread = threading.Thread(
                        target=self.crawl_cnki_with_undetected,
                        args=(search_term, date_range, max_results, output_dir, cnki_db_code)
                    )
                    self.process_thread.daemon = True
                    self.process_thread.start()
                
                except ImportError:
                    self.append_to_log(self.search_log, "增强型Chrome浏览器方法不可用。请确保cnki_undetected_crawler.py在正确位置。")
                    self.append_to_log(self.search_log, "请安装所需库: pip install undetected-chromedriver pandas requests")
                    self.is_running = False
                    return
            elif crawler_method == "selenium":
                # 使用Selenium爬虫
                self.append_to_log(self.search_log, f"开始使用Selenium爬取CNKI文献")
                self.append_to_log(self.search_log, f"搜索词: {', '.join(search_terms)}")
                self.append_to_log(self.search_log, f"日期范围: {date_range[0]} - {date_range[1]}")
                self.append_to_log(self.search_log, f"最大结果数: {max_results}")
                self.append_to_log(self.search_log, f"输出目录: {output_dir}")
                self.append_to_log(self.search_log, f"数据库: CNKI {cnki_db_code}")
        
                # 准备搜索词
                search_term = search_terms[0]
                if search_mode == "combined" and len(search_terms) > 1:
                    search_term = " AND ".join([f"({term})" for term in search_terms])
        
                # 创建集成实例
                self.cnki_integration = CNKISeleniumIntegration(logger=self.logger)
        
                # 定义回调函数
                def selenium_callback(results):
                    if results["status"] == "error":
                        self.append_to_log(self.search_log, f"爬取过程中发生错误: {results['message']}")
                        self.is_running = False
                        return
            
                    self.append_to_log(self.search_log, f"\n爬取完成，共获取 {len(results['results'])} 条文献")
            
                    if results.get('csv_path'):
                        self.append_to_log(self.search_log, f"CSV文件已保存到: {results['csv_path']}")
            
                    if results.get('json_path'):
                        self.append_to_log(self.search_log, f"JSON文件已保存到: {results['json_path']}")
            
                    self.append_to_log(self.search_log, "\n可以进入'数据处理'选项卡开始处理爬取的文献")
                    self.is_running = False
        
                # 启动Selenium爬虫
                self.process_thread = self.cnki_integration.start_crawler(
                    username=username,
                    password=password,
                    term=search_term,
                    date_range=date_range,
                    max_results=max_results,
                    db_code=cnki_db_code,
                    output_dir=output_dir,
                    headless=False,  # 设置为True会隐藏浏览器窗口
                    callback=selenium_callback,
                    use_manual_mode=False
                )
        
                if not self.process_thread:
                    # Selenium启动失败
                    self.append_to_log(self.search_log, "Selenium启动失败，请检查是否已安装所需库")
                    self.append_to_log(self.search_log, "需要安装: pip install selenium webdriver-manager pandas")
                    self.is_running = False
            elif crawler_method == "manual":
                # 使用手动模式
                self.append_to_log(self.search_log, f"开始使用手动模式爬取CNKI文献")
                self.append_to_log(self.search_log, f"搜索词: {', '.join(search_terms)}")
                self.append_to_log(self.search_log, f"日期范围: {date_range[0]} - {date_range[1]}")
                self.append_to_log(self.search_log, f"最大结果数: {max_results}")
                self.append_to_log(self.search_log, f"输出目录: {output_dir}")
                self.append_to_log(self.search_log, f"数据库: CNKI {cnki_db_code}")
        
                # 准备搜索词
                search_term = search_terms[0]
                if search_mode == "combined" and len(search_terms) > 1:
                    search_term = " AND ".join([f"({term})" for term in search_terms])
        
                # 创建集成实例
                self.cnki_integration = CNKISeleniumIntegration(logger=self.logger)
        
                # 定义回调函数
                def selenium_callback(results):
                    if results["status"] == "error":
                        self.append_to_log(self.search_log, f"爬取过程中发生错误: {results['message']}")
                        self.is_running = False
                        return
            
                    self.append_to_log(self.search_log, f"\n爬取完成，共获取 {len(results['results'])} 条文献")
            
                    if results.get('csv_path'):
                        self.append_to_log(self.search_log, f"CSV文件已保存到: {results['csv_path']}")
            
                    if results.get('json_path'):
                        self.append_to_log(self.search_log, f"JSON文件已保存到: {results['json_path']}")
            
                    self.append_to_log(self.search_log, "\n可以进入'数据处理'选项卡开始处理爬取的文献")
                    self.is_running = False
        
                # 启动Selenium爬虫（手动模式）
                self.process_thread = self.cnki_integration.start_crawler(
                    username=username,
                    password=password,
                    term=search_term,
                    date_range=date_range,
                    max_results=max_results,
                    db_code=cnki_db_code,
                    output_dir=output_dir,
                    headless=False,  # 必须显示浏览器窗口
                    callback=selenium_callback,
                    use_manual_mode=True  # 使用手动模式
                )
        
                if not self.process_thread:
                    # Selenium启动失败
                    self.append_to_log(self.search_log, "Selenium启动失败，请检查是否已安装所需库")
                    self.append_to_log(self.search_log, "需要安装: pip install selenium webdriver-manager pandas")
                    self.is_running = False
            else:
                # 使用传统爬虫
                self.process_thread = threading.Thread(
                    target=self.crawl_cnki,
                    args=(username, password, search_terms, date_range, max_results, output_dir, cnki_db_code)
                )
                self.process_thread.daemon = True
                self.process_thread.start()

    def append_to_log(self, log_widget, message):
        """向日志控件添加消息"""
        log_widget.config(state=tk.NORMAL)
        log_widget.insert(tk.END, message + "\n")
        log_widget.see(tk.END)
        log_widget.config(state=tk.DISABLED)
        self.root.update()

    def setup_process_tab(self):
        """设置数据处理选项卡"""
        # 创建主框架
        main_frame = ttk.Frame(self.process_tab, padding="10")
        main_frame.pack(fill="both", expand=True)
        
        # 创建标题
        ttk.Label(main_frame, text="数据处理与知识图谱构建", font=("Arial", 16, "bold")).grid(
            row=0, column=0, columnspan=3, sticky="w", pady=(0, 20))
        
        # 处理参数
        ttk.Label(main_frame, text="输出目录:").grid(row=1, column=0, sticky="w", pady=2)
        self.output_dir_var = tk.StringVar(value=self.config.get('Process', 'output_dir', fallback=''))
        ttk.Entry(main_frame, textvariable=self.output_dir_var, width=50).grid(
            row=1, column=1, sticky="we", pady=2)
        ttk.Button(main_frame, text="浏览...", command=self.select_output_dir).grid(
            row=1, column=2, sticky="w", padx=5, pady=2)
        
        ttk.Label(main_frame, text="输出格式:").grid(row=2, column=0, sticky="w", pady=2)
        self.output_format_var = tk.StringVar(value=self.config.get('Process', 'output_format', fallback='json'))
        format_combo = ttk.Combobox(main_frame, textvariable=self.output_format_var, width=10, 
                                   values=["json", "csv", "rdf"])
        format_combo.grid(row=2, column=1, sticky="w", pady=2)
        
        # 并行处理选项
        self.parallel_var = tk.BooleanVar(value=self.config.getboolean('Process', 'parallel', fallback=True))
        ttk.Checkbutton(main_frame, text="启用并行处理", variable=self.parallel_var).grid(
            row=3, column=0, sticky="w", pady=2)
        
        ttk.Label(main_frame, text="工作进程数:").grid(row=3, column=1, sticky="w", pady=2)
        self.max_workers_var = tk.StringVar(value=self.config.get('Process', 'max_workers', fallback='4'))
        ttk.Entry(main_frame, textvariable=self.max_workers_var, width=5).grid(
            row=3, column=2, sticky="w", pady=2)
        
        # 文件选择框架
        file_frame = ttk.LabelFrame(main_frame, text="输入文件选择", padding=10)
        file_frame.grid(row=4, column=0, columnspan=3, sticky="we", pady=10)
        
        # 选择选项
        self.input_option_var = tk.StringVar(value="latest")
        ttk.Radiobutton(file_frame, text="使用最新爬取的文献", variable=self.input_option_var, 
                      value="latest").grid(row=0, column=0, sticky="w", pady=2)
        ttk.Radiobutton(file_frame, text="选择单个文件", variable=self.input_option_var, 
                      value="file").grid(row=1, column=0, sticky="w", pady=2)
        ttk.Radiobutton(file_frame, text="选择目录", variable=self.input_option_var, 
                      value="directory").grid(row=2, column=0, sticky="w", pady=2)
        
        # 文件选择按钮
        self.file_path_var = tk.StringVar()
        ttk.Entry(file_frame, textvariable=self.file_path_var, width=50).grid(
            row=1, column=1, sticky="we", pady=2)
        ttk.Button(file_frame, text="选择文件", command=self.select_input_file).grid(
            row=1, column=2, sticky="w", padx=5, pady=2)
        
        # 目录选择按钮
        self.dir_path_var = tk.StringVar()
        ttk.Entry(file_frame, textvariable=self.dir_path_var, width=50).grid(
            row=2, column=1, sticky="we", pady=2)
        ttk.Button(file_frame, text="选择目录", command=self.select_input_dir).grid(
            row=2, column=2, sticky="w", padx=5, pady=2)
        
        # 保存处理设置按钮
        ttk.Button(main_frame, text="保存处理设置", command=self.save_process_config).grid(
            row=5, column=0, sticky="w", pady=10)
        
        # 开始处理按钮
        ttk.Button(main_frame, text="开始处理", command=self.start_processing).grid(
            row=5, column=1, sticky="w", pady=10)
        
        # 查看知识图谱按钮
        ttk.Button(main_frame, text="查看最新知识图谱", command=self.view_knowledge_graph).grid(
            row=5, column=2, sticky="w", pady=10)
        
        # 添加合并知识图谱文件按钮
        ttk.Button(main_frame, text="合并知识图谱文件", command=self.merge_kg_files).grid(
            row=5, column=3, sticky="w", pady=10)
        
        # 日志框
        ttk.Label(main_frame, text="处理日志:", font=("Arial", 10, "bold")).grid(
            row=6, column=0, columnspan=3, sticky="w", pady=(10, 5))
        
        self.process_log = scrolledtext.ScrolledText(main_frame, width=80, height=15, wrap=tk.WORD)
        self.process_log.grid(row=7, column=0, columnspan=3, pady=5)
        self.process_log.config(state=tk.DISABLED)

    def select_output_dir(self):
        """选择输出目录"""
        dir_path = filedialog.askdirectory(title="选择输出目录")
        if dir_path:
            self.output_dir_var.set(dir_path)

    def select_input_file(self):
        """选择输入文件"""
        file_path = filedialog.askopenfilename(
            title="选择JSON文件",
            filetypes=[("JSON文件", "*.json"), ("所有文件", "*.*")]
        )
        if file_path:
            self.file_path_var.set(file_path)
            self.input_option_var.set("file")

    def select_input_dir(self):
        """选择输入目录"""
        dir_path = filedialog.askdirectory(title="选择包含JSON文件的目录")
        if dir_path:
            self.dir_path_var.set(dir_path)
            self.input_option_var.set("directory")

    def save_process_config(self):
        """保存处理配置"""
        self.config['Process'] = {
            'output_dir': self.output_dir_var.get(),
            'output_format': self.output_format_var.get(),
            'parallel': str(self.parallel_var.get()),
            'max_workers': self.max_workers_var.get()
        }
        self.save_config()
        messagebox.showinfo("配置已保存", "处理配置已成功保存")

    def get_input_files(self):
        """获取输入文件列表"""
        input_option = self.input_option_var.get()
        
        if input_option == "file":
            file_path = self.file_path_var.get()
            if not file_path or not os.path.exists(file_path):
                messagebox.showerror("文件错误", "请选择有效的输入文件")
                return []
            return [file_path]
            
        elif input_option == "directory":
            dir_path = self.dir_path_var.get()
            if not dir_path or not os.path.exists(dir_path):
                messagebox.showerror("目录错误", "请选择有效的输入目录")
                return []
                
            # 查找目录中的所有JSON文件
            json_files = []
            for root, _, files in os.walk(dir_path):
                for file in files:
                    if file.endswith('.json'):
                        json_files.append(os.path.join(root, file))
            
            if not json_files:
                messagebox.showerror("文件错误", "所选目录中没有找到JSON文件")
                return []
                
            return json_files
            
        else:  # "latest"
            # 获取当前选择的数据库类型
            database = self.config.get('Search', 'database', fallback='pubmed')
            
            # 查找最新的数据目录
            data_dir = os.path.join(self.results_dir, f'{database}_data')
            if not os.path.exists(data_dir):
                messagebox.showerror("目录错误", f"找不到{database.upper()}数据目录，请先爬取文献")
                return []
                
            # 获取最新的子目录
            subdirs = [os.path.join(data_dir, d) for d in os.listdir(data_dir) 
                      if os.path.isdir(os.path.join(data_dir, d))]
            
            if not subdirs:
                messagebox.showerror("目录错误", f"找不到{database.upper()}数据子目录，请先爬取文献")
                return []
                
            latest_dir = max(subdirs, key=os.path.getmtime)
            
            # 查找目录中的所有JSON文件
            json_files = []
            for root, _, files in os.walk(latest_dir):
                for file in files:
                    if file.endswith('.json'):
                        json_files.append(os.path.join(root, file))
            
            if not json_files:
                messagebox.showerror("文件错误", f"最新的{database.upper()}数据目录中没有找到JSON文件")
                return []
                
            return json_files
    def cleanup(self):
        """关闭资源并清理"""
        # 停止可能运行的Selenium爬虫
        if hasattr(self, 'cnki_integration') and self.cnki_integration:
            self.cnki_integration.stop_crawler()
    def start_processing(self):
        """开始处理文献和构建知识图谱"""
        if self.is_running:
            messagebox.showwarning("操作进行中", "已有操作正在进行，请等待完成")
            return
        
        # 获取输入文件
        input_files = self.get_input_files()
        if not input_files:
            return
        
        # 获取处理参数
        output_dir = self.output_dir_var.get()
        if not output_dir:
            # 使用默认路径
            output_dir = os.path.join(self.results_dir, 'output', 
                                     datetime.now().strftime("%Y%m%d_%H%M%S"))
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        output_format = self.output_format_var.get()
        parallel = self.parallel_var.get()
        
        try:
            max_workers = int(self.max_workers_var.get())
            if max_workers < 1:
                max_workers = 1
        except ValueError:
            max_workers = 4
        
        # 清空日志
        self.process_log.config(state=tk.NORMAL)
        self.process_log.delete(1.0, tk.END)
        self.process_log.config(state=tk.DISABLED)
        
        # 再次检查API密钥配置
        moonshot_api_key = self.config.get('API', 'moonshot_api_key', fallback='')
        if not moonshot_api_key:
            self.append_to_log(self.process_log, "警告: 未设置Moonshot API密钥，实体提取可能会失败")
            
        # 启动处理线程
        self.is_running = True
        self.process_thread = threading.Thread(
            target=self.process_files,
            args=(input_files, output_dir, output_format, parallel, max_workers)
        )
        self.process_thread.daemon = True
        self.process_thread.start()

    def process_files(self, input_files, output_dir, output_format, parallel, max_workers):
        """
        文献处理和知识图谱构建线程
        
        Args:
            input_files: 输入文件列表
            output_dir: 输出目录
            output_format: 输出格式
            parallel: 是否并行处理
            max_workers: 并行工作进程数
        """
        try:
            self.append_to_log(self.process_log, f"开始处理 {len(input_files)} 个文件")
            self.append_to_log(self.process_log, f"输出目录: {output_dir}")
            self.append_to_log(self.process_log, f"输出格式: {output_format}")
            self.append_to_log(self.process_log, f"并行处理: {'是' if parallel else '否'}")
            if parallel:
                self.append_to_log(self.process_log, f"并行工作进程数: {max_workers}")
            
            # 调用batch_process函数处理文件
            result = process_batch(
                input_files=input_files,
                output_dir=output_dir,
                output_format=output_format,
                parallel=parallel,
                max_workers=max_workers,
                verbose=True
            )
            
            self.append_to_log(self.process_log, "处理完成，开始构建知识图谱...")
            
            # 处理生成的知识图谱JSON文件
            kg_json_files = []
            for root, _, files in os.walk(output_dir):
                for file in files:
                    if file == "knowledge_graph.json":
                        kg_json_files.append(os.path.join(root, file))
            
            if not kg_json_files:
                self.append_to_log(self.process_log, "警告：未找到生成的知识图谱JSON文件")
                return
            
            # 使用最新的知识图谱文件
            latest_kg_file = max(kg_json_files, key=os.path.getmtime)
            self.append_to_log(self.process_log, f"使用最新知识图谱文件: {latest_kg_file}")
            
            # 构建和可视化知识图谱
            kg_builder = KnowledgeGraphBuilder(latest_kg_file)
            kg_builder.build_graph()
            
            # 导出为CSV
            nodes_csv, edges_csv = kg_builder.export_to_csv()
            self.append_to_log(self.process_log, f"生成CSV文件: {nodes_csv}, {edges_csv}")
            
            # 导出为GraphML
            graphml_path = kg_builder.export_to_graphml()
            self.append_to_log(self.process_log, f"生成GraphML文件: {graphml_path}")
            
            # 生成HTML可视化
            html_path = kg_builder.visualize_html()
            self.append_to_log(self.process_log, f"生成HTML可视化: {html_path}")
            self.latest_html_path = html_path
            
            # 生成统计信息
            stats = kg_builder.generate_statistics()
            self.append_to_log(self.process_log, f"知识图谱统计: 节点总数 {stats.get('节点总数', 0)}, 边总数 {stats.get('边总数', 0)}")
            
            node_type_stats = stats.get("节点类型统计", {})
            for node_type, count in node_type_stats.items():
                self.append_to_log(self.process_log, f"  {node_type}: {count} 个节点")
            
            # 完成
            self.append_to_log(self.process_log, "知识图谱构建完成！")
            
        except Exception as e:
            self.logger.error(f"处理过程中发生错误: {str(e)}")
            self.append_to_log(self.process_log, f"处理过程中发生错误: {str(e)}")
            import traceback
            self.append_to_log(self.process_log, traceback.format_exc())
        finally:
            self.is_running = False

    def view_knowledge_graph(self):
        """查看最新生成的知识图谱"""
        # 查找最新的HTML可视化文件
        if hasattr(self, 'latest_html_path') and os.path.exists(self.latest_html_path):
            # 使用Windows系统默认浏览器打开
            os.startfile(self.latest_html_path)
        else:
            # 如果没有找到latest_html_path，尝试查找
            html_files = []
            output_dir = self.output_dir_var.get()
            if not output_dir or not os.path.exists(output_dir):
                # 尝试在results目录下查找
                output_dir = os.path.join(self.results_dir, 'output')
                
            if os.path.exists(output_dir):
                for root, _, files in os.walk(output_dir):
                    for file in files:
                        if file == "knowledge_graph.html":
                            html_files.append(os.path.join(root, file))
            
            if html_files:
                # 使用最新的HTML文件
                latest_html = max(html_files, key=os.path.getmtime)
                # 使用Windows系统默认浏览器打开
                os.startfile(latest_html)
                self.latest_html_path = latest_html
            else:
                messagebox.showerror("错误", "找不到知识图谱可视化文件")
    
    def merge_kg_files(self):
        """合并多个知识图谱文件为一个统一的知识图谱"""
        if self.is_running:
            messagebox.showwarning("操作进行中", "已有操作正在进行，请等待完成")
            return
        
        # 让用户选择包含知识图谱文件的目录
        dir_path = filedialog.askdirectory(title="选择包含知识图谱文件的目录")
        if not dir_path:
            return
        
        # 创建高级选项对话框
        advanced_dialog = tk.Toplevel(self.root)
        advanced_dialog.title("知识图谱合并高级选项")
        advanced_dialog.geometry("500x400")
        advanced_dialog.resizable(False, False)
        advanced_dialog.transient(self.root)
        advanced_dialog.grab_set()
        
        # 创建选项框架
        options_frame = ttk.Frame(advanced_dialog, padding=15)
        options_frame.pack(fill="both", expand=True)
        
        # 最低置信度选项
        ttk.Label(options_frame, text="最低置信度阈值 (0.0-1.0):").grid(row=0, column=0, sticky="w", pady=5)
        min_confidence_var = tk.DoubleVar(value=0.0)
        ttk.Spinbox(options_frame, from_=0.0, to=1.0, increment=0.1, textvariable=min_confidence_var, width=5).grid(
            row=0, column=1, sticky="w", pady=5)
        
        # 最大实体数量选项
        ttk.Label(options_frame, text="每种类型的最大实体数量 (0表示无限制):").grid(row=1, column=0, sticky="w", pady=5)
        max_entities_var = tk.IntVar(value=0)
        ttk.Spinbox(options_frame, from_=0, to=1000, increment=10, textvariable=max_entities_var, width=5).grid(
            row=1, column=1, sticky="w", pady=5)
        
        # 实体类型选择
        ttk.Label(options_frame, text="要包含的实体类型:").grid(row=2, column=0, sticky="w", pady=5)
        entity_types_frame = ttk.Frame(options_frame)
        entity_types_frame.grid(row=3, column=0, columnspan=2, sticky="w", pady=5)
        
        # 常见实体类型
        common_types = ["疾病", "药物", "靶点", "生物过程", "基因", "蛋白质", "生物标志物"]
        entity_type_vars = {}
        
        for i, entity_type in enumerate(common_types):
            var = tk.BooleanVar(value=True)
            entity_type_vars[entity_type] = var
            ttk.Checkbutton(entity_types_frame, text=entity_type, variable=var).grid(
                row=i//3, column=i%3, sticky="w", padx=10, pady=2)
        
        # 输出目录选项
        ttk.Label(options_frame, text="输出目录:").grid(row=4, column=0, sticky="w", pady=10)
        output_dir_var = tk.StringVar(value=os.path.join(self.results_dir, 'merged_output', 
                                               datetime.now().strftime("%Y%m%d_%H%M%S")))
        ttk.Entry(options_frame, textvariable=output_dir_var, width=40).grid(row=4, column=1, sticky="w", pady=10)
        ttk.Button(options_frame, text="浏览...", 
                   command=lambda: output_dir_var.set(filedialog.askdirectory(title="选择输出目录"))).grid(
            row=4, column=2, sticky="w", padx=5, pady=10)
        
        # 确认和取消按钮
        buttons_frame = ttk.Frame(options_frame)
        buttons_frame.grid(row=5, column=0, columnspan=3, pady=20)
        
        # 取消按钮
        ttk.Button(buttons_frame, text="取消", command=advanced_dialog.destroy).grid(row=0, column=0, padx=10)
        
        # 确认按钮
        def confirm_and_merge():
            # 收集选项
            min_confidence = min_confidence_var.get()
            max_entities = max_entities_var.get()
            selected_entity_types = [entity_type for entity_type, var in entity_type_vars.items() if var.get()]
            output_dir = output_dir_var.get()
            
            # 确保输出目录存在
            os.makedirs(output_dir, exist_ok=True)
            
            # 关闭对话框
            advanced_dialog.destroy()
            
            # 清空日志
            self.process_log.config(state=tk.NORMAL)
            self.process_log.delete(1.0, tk.END)
            self.process_log.config(state=tk.DISABLED)
            
            # 添加日志
            self.append_to_log(self.process_log, f"开始合并知识图谱文件...")
            self.append_to_log(self.process_log, f"源目录: {dir_path}")
            self.append_to_log(self.process_log, f"输出目录: {output_dir}")
            self.append_to_log(self.process_log, f"最低置信度: {min_confidence}")
            self.append_to_log(self.process_log, f"每种类型最大实体数: {max_entities if max_entities > 0 else '无限制'}")
            self.append_to_log(self.process_log, f"选定的实体类型: {', '.join(selected_entity_types) if selected_entity_types else '全部'}")
            
            # 设置输出文件路径
            output_path = os.path.join(output_dir, "merged_knowledge_graph.json")
            
            # 启动合并线程
            self.is_running = True
            self.process_thread = threading.Thread(
                target=self.run_merge_process,
                args=(dir_path, output_path, min_confidence, max_entities, selected_entity_types)
            )
            self.process_thread.daemon = True
            self.process_thread.start()
        
        ttk.Button(buttons_frame, text="开始合并", command=confirm_and_merge).grid(row=0, column=1, padx=10)

    def run_merge_process(self, input_path, output_path, min_confidence=0.0, max_entities=0, entity_types=None):
        """
        运行知识图谱合并进程
        
        Args:
            input_path: 包含知识图谱文件的输入目录
            output_path: 合并后的输出文件路径
            min_confidence: 最低置信度阈值 (0.0-1.0)
            max_entities: 每种类型的最大实体数量 (0表示无限制)
            entity_types: 要包含的实体类型列表
        """
        try:
            # 首先检查是否需要创建merge_kg_files.py文件
            merge_script_path = os.path.join(script_dir, "merge_kg_files.py")
            
            if not os.path.exists(merge_script_path):
                # 创建merge_kg_files.py文件
                self.append_to_log(self.process_log, "未找到merge_kg_files.py，正在创建...")
                
                # 创建合并脚本内容
                merge_script_content = """#!/usr/bin/env python3
\"\"\"
知识图谱文件合并工具

该脚本用于合并多个知识图谱JSON文件，
解决从多个文献源提取实体关系后无法生成统一图谱的问题。
\"\"\"

import os
import sys
import json
import argparse
import logging
from datetime import datetime
from typing import Dict, List, Any

def setup_logger():
    \"\"\"设置日志记录器\"\"\"
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
    \"\"\"
    对实体文本进行规范化处理，统一不同写法的相同实体
    
    Args:
        text: 原始实体文本
        
    Returns:
        规范化后的文本
    \"\"\"
    if not text:
        return ""
    
    # 将文本统一为小写（仅处理英文）
    # 中文实体保持原样
    has_chinese = any('\\u4e00' <= char <= '\\u9fff' for char in text)
    if not has_chinese:
        text = text.lower()
    
    # 移除常见前缀/后缀
    text = text.replace("the ", "").replace(" protein", "").replace(" gene", "")
    
    # 标准化空格
    text = " ".join(text.split())
    
    return text.strip()

def find_kg_files(input_path: str) -> List[str]:
    \"\"\"
    查找所有知识图谱JSON文件
    
    Args:
        input_path: 输入路径（文件或目录）
        
    Returns:
        文件路径列表
    \"\"\"
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
    \"\"\"
    加载JSON文件
    
    Args:
        file_path: JSON文件路径
        
    Returns:
        JSON数据字典
    \"\"\"
    logger = logging.getLogger("KG_Merger")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"成功加载: {file_path}")
        return data
    except Exception as e:
        logger.error(f"加载文件时出错 {file_path}: {str(e)}")
        return {}

def merge_kg_data(files: List[str], min_confidence=0.0, max_entities=0, entity_types=None) -> Dict:
    \"\"\"
    合并多个知识图谱文件
    
    Args:
        files: 文件路径列表
        min_confidence: 最低置信度阈值 (0.0-1.0)
        max_entities: 每种类型的最大实体数量 (0表示无限制)
        entity_types: 要包含的实体类型列表
        
    Returns:
        合并后的知识图谱数据
    \"\"\"
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

def main():
    \"\"\"主入口函数\"\"\"
    # 配置参数解析
    parser = argparse.ArgumentParser(description='知识图谱文件合并工具')
    parser.add_argument('input', help='输入文件或目录路径')
    parser.add_argument('--output', '-o', default='merged_knowledge_graph.json', help='输出文件路径')
    parser.add_argument('--verbose', '-v', action='store_true', help='显示详细日志')
    parser.add_argument('--min-confidence', type=float, default=0.0, help='最低置信度阈值 (0.0-1.0)')
    parser.add_argument('--max-entities', type=int, default=0, help='每种类型的最大实体数量 (0表示无限制)')
    parser.add_argument('--entity-types', nargs='+', help='要包含的实体类型列表')
    
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
            
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(merged_data, f, ensure_ascii=False, indent=2)
        logger.info(f"合并后的知识图谱已保存到: {args.output}")
    except Exception as e:
        logger.error(f"保存合并文件时出错: {str(e)}")

if __name__ == "__main__":
    main()
"""
                
                try:
                    with open(merge_script_path, 'w', encoding='utf-8') as f:
                        f.write(merge_script_content)
                    self.append_to_log(self.process_log, "合并脚本文件已成功创建")
                except Exception as e:
                    self.append_to_log(self.process_log, f"创建合并脚本文件时出错: {str(e)}")
                    self.is_running = False
                    return
            
            # 导入模块
            sys.path.append(script_dir)
            try:
                self.append_to_log(self.process_log, "正在导入合并模块...")
                from merge_kg_files import find_kg_files, merge_kg_data
            except ImportError:
                # 如果无法导入，尝试使用exec运行脚本
                self.append_to_log(self.process_log, "无法导入模块，尝试使用备选方法...")
                
                # 执行命令行脚本
                import subprocess
                
                cmd = [
                    sys.executable, 
                    merge_script_path, 
                    input_path,
                    "--output", output_path,
                    "--min-confidence", str(min_confidence),
                    "--max-entities", str(max_entities)
                ]
                
                if entity_types:
                    cmd.extend(["--entity-types"] + entity_types)
                
                self.append_to_log(self.process_log, f"执行命令: {' '.join(cmd)}")
                
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding='utf-8'
                )
                
                # 实时获取输出
                while True:
                    output = process.stdout.readline()
                    if output == '' and process.poll() is not None:
                        break
                    if output:
                        self.append_to_log(self.process_log, output.strip())
                
                # 获取错误输出
                stderr = process.stderr.read()
                if stderr:
                    self.append_to_log(self.process_log, f"错误: {stderr}")
                
                if process.returncode != 0:
                    self.append_to_log(self.process_log, f"命令执行失败，返回代码: {process.returncode}")
                    self.is_running = False
                    return
                
                self.append_to_log(self.process_log, "文件已合并完成")
                
                # 检查输出文件是否存在
                if not os.path.exists(output_path):
                    self.append_to_log(self.process_log, f"错误: 输出文件未创建: {output_path}")
                    self.is_running = False
                    return
                    
                self.append_to_log(self.process_log, "开始构建知识图谱...")
                
                # 导入KG构建器
                try:
                    from kg_builder import KnowledgeGraphBuilder
                    
                    # 构建知识图谱
                    kg_builder = KnowledgeGraphBuilder(output_path)
                    kg_builder.build_graph()
                    
                    # 导出为CSV
                    nodes_csv, edges_csv = kg_builder.export_to_csv()
                    self.append_to_log(self.process_log, f"生成CSV文件: {os.path.basename(nodes_csv)}, {os.path.basename(edges_csv)}")
                    
                    # 导出为GraphML
                    graphml_path = kg_builder.export_to_graphml()
                    self.append_to_log(self.process_log, f"生成GraphML文件: {os.path.basename(graphml_path)}")
                    
                    # 生成HTML可视化
                    html_path = kg_builder.visualize_html()
                    self.append_to_log(self.process_log, f"生成HTML可视化: {os.path.basename(html_path)}")
                    self.latest_html_path = html_path
                    
                    # 生成统计信息
                    stats = kg_builder.generate_statistics()
                    self.append_to_log(self.process_log, f"知识图谱统计: 节点总数 {stats.get('节点总数', 0)}, 边总数 {stats.get('边总数', 0)}")
                    
                    node_type_stats = stats.get("节点类型统计", {})
                    for node_type, count in node_type_stats.items():
                        self.append_to_log(self.process_log, f"  {node_type}: {count} 个节点")
                    
                    # 完成
                    self.append_to_log(self.process_log, "合并知识图谱并生成可视化完成！")
                    
                except Exception as kg_error:
                    self.logger.error(f"构建知识图谱时出错: {str(kg_error)}")
                    self.append_to_log(self.process_log, f"构建知识图谱时出错: {str(kg_error)}")
                    import traceback
                    self.append_to_log(self.process_log, traceback.format_exc())
                
                self.is_running = False
                return
            
            # 查找所有知识图谱文件
            self.append_to_log(self.process_log, "正在查找知识图谱文件...")
            json_files = find_kg_files(input_path)
            
            if not json_files:
                self.append_to_log(self.process_log, f"错误: 在 {input_path} 中没有找到知识图谱文件")
                self.is_running = False
                return
            
            self.append_to_log(self.process_log, f"找到 {len(json_files)} 个知识图谱文件")
            
            # 合并知识图谱数据
            self.append_to_log(self.process_log, "正在合并知识图谱数据...")
            merged_data = merge_kg_data(
                json_files, 
                min_confidence=min_confidence,
                max_entities=max_entities,
                entity_types=entity_types
            )
            
            # 确保输出目录存在
            output_dir = os.path.dirname(output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
            
            # 保存合并后的文件
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(merged_data, f, ensure_ascii=False, indent=2)
            
            self.append_to_log(self.process_log, f"合并后的知识图谱已保存到: {output_path}")
            
            # 使用合并后的文件生成知识图谱可视化
            self.append_to_log(self.process_log, "正在生成知识图谱可视化...")
            
            from kg_builder import KnowledgeGraphBuilder
            kg_builder = KnowledgeGraphBuilder(output_path)
            kg_builder.build_graph()
            
            # 导出为CSV
            nodes_csv, edges_csv = kg_builder.export_to_csv()
            self.append_to_log(self.process_log, f"生成CSV文件: {os.path.basename(nodes_csv)}, {os.path.basename(edges_csv)}")
            
            # 导出为GraphML
            graphml_path = kg_builder.export_to_graphml()
            self.append_to_log(self.process_log, f"生成GraphML文件: {os.path.basename(graphml_path)}")
            
            # 生成HTML可视化
            html_path = kg_builder.visualize_html()
            self.append_to_log(self.process_log, f"生成HTML可视化: {os.path.basename(html_path)}")
            self.latest_html_path = html_path
            
            # 生成统计信息
            stats = kg_builder.generate_statistics()
            self.append_to_log(self.process_log, f"知识图谱统计: 节点总数 {stats.get('节点总数', 0)}, 边总数 {stats.get('边总数', 0)}")
            
            node_type_stats = stats.get("节点类型统计", {})
            for node_type, count in node_type_stats.items():
                self.append_to_log(self.process_log, f"  {node_type}: {count} 个节点")
            
            # 完成
            self.append_to_log(self.process_log, "合并知识图谱并生成可视化完成！")
            
        except Exception as e:
            self.logger.error(f"合并知识图谱时出错: {str(e)}")
            self.append_to_log(self.process_log, f"合并知识图谱时出错: {str(e)}")
            import traceback
            self.append_to_log(self.process_log, traceback.format_exc())
        finally:
            self.is_running = False

    def setup_about_tab(self):
        """设置关于系统选项卡"""
        # 创建主框架
        main_frame = ttk.Frame(self.about_tab, padding="10")
        main_frame.pack(fill="both", expand=True)
        
        # 创建标题
        ttk.Label(main_frame, text="关于文献知识图谱构建系统", font=("Arial", 16, "bold")).pack(
            anchor="w", pady=(0, 20))
        
        # 系统介绍
        info_text = (
            "文献知识图谱构建系统是一个专门为研究设计的文献挖掘和知识图谱构建工具。"
            "系统使用PubMed API与CNKI接口获取研究文献，并利用大语言模型提取生物医学实体和关系，"
            "构建可视化的知识图谱，帮助研究人员快速了解研究领域的知识结构。\n\n"
            
            "系统主要功能:\n"
            "1. PubMed和CNKI文献自动爬取与存储\n"
            "2. 基于大语言模型的生物医学实体识别\n"
            "3. 实体间关系提取与知识三元组生成\n"
            "4. 知识图谱构建与交互式可视化\n"
            "5. 多源知识图谱合并与分析\n\n"
            
            "技术栈:\n"
            "- Python 3.9+\n"
            "- Biopython (PubMed API访问)\n"
            "- BeautifulSoup4 (CNKI网页解析)\n"
            "- Tkinter (图形界面)\n"
            "- NetworkX (图数据处理)\n"
            "- PyVis (知识图谱可视化)\n"
            "- Moonshot AI API (大语言模型服务)\n\n"
            
            "系统适用于疾病研究人员、医学文献研究者以及生物信息学工作者。\n\n"
            
            "©2025 基于大语言模型的文本实体提取系统 v1.0.0"
        )
        
        text_area = scrolledtext.ScrolledText(main_frame, width=80, height=20, wrap=tk.WORD)
        text_area.pack(fill="both", expand=True, pady=10)
        text_area.insert(tk.INSERT, info_text)
        text_area.config(state=tk.DISABLED)
        
        # 底部按钮框架
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill="x", pady=20)
        
        # 打开项目链接按钮
        ttk.Button(button_frame, text="项目GitHub", 
                  command=lambda: webbrowser.open("https://github.com/example/kg")).pack(
            side="left", padx=10)
        
        # 查看文档按钮
        ttk.Button(button_frame, text="查看文档", 
                  command=self.view_documentation).pack(side="left", padx=10)
        
        # 检查更新按钮
        ttk.Button(button_frame, text="检查更新", 
                  command=self.check_updates).pack(side="left", padx=10)

    def view_documentation(self):
        """查看系统文档"""
        docs_path = os.path.join(script_dir, "docs", "index.html")
        if os.path.exists(docs_path):
            os.startfile(docs_path)
        else:
            messagebox.showinfo("文档不可用", "系统文档当前不可用")

    def check_updates(self):
        """检查系统更新"""
        # 模拟检查更新过程
        messagebox.showinfo("检查更新", "当前版本已是最新版本 (v1.0.0)")


def main():
    """程序主入口"""
    root = tk.Tk()
    # Windows系统下图标路径处理
    icon_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "icon.ico")
    if os.path.exists(icon_path):
        root.iconbitmap(icon_path)
    app = KGApp(root)
    
    # 添加关闭窗口时的清理操作
    def on_closing():
        app.cleanup()
        root.destroy()
    
    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()

if __name__ == "__main__":
    main()



