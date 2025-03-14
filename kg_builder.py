#!/usr/bin/env python3
"""
知识图谱构建与可视化模块

该模块用于从实体和关系数据构建知识图谱，支持多种格式导出和可视化。
"""

import json
import os
import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd
from pyvis.network import Network
import argparse

class KnowledgeGraphBuilder:
    """生物医学知识图谱构建与可视化工具"""
    
    def __init__(self, json_file_path):
        """
        初始化知识图谱构建器
        
        参数:
            json_file_path: 知识图谱JSON文件路径
        """
        self.json_file_path = json_file_path
        self.output_dir = os.path.dirname(json_file_path)
        self.data = None
        self.graph = nx.MultiDiGraph()
        self.load_data()
    
    def load_data(self):
        """加载JSON数据"""
        try:
            # Windows编码处理
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
            print(f"成功加载知识图谱数据：{len(self.data.get('entities', {}))} 种实体类型，"
                  f"{len(self.data.get('relations', []))} 个关系")
        except Exception as e:
            print(f"加载JSON文件出错: {e}")
            self.data = {"entities": {}, "relations": []}
    
    def build_graph(self):
        """从JSON数据构建图"""
        if not self.data:
            print("没有数据可以构建图")
            return
        
        # 添加实体节点
        for entity_type, entities in self.data.get('entities', {}).items():
            for entity in entities:
                # 使用实体文本作为节点ID
                entity_text = entity.get('text', '') or entity.get('name', '')
                if not entity_text:
                    continue
                
                # 添加节点，确保不重复
                if not self.graph.has_node(entity_text):
                    self.graph.add_node(entity_text,
                                       type=entity_type,
                                       label=entity_text,
                                       occurrences=entity.get('occurrences', 1))
        
        # 添加关系边
        for relation in self.data.get('relations', []):
            source = relation.get('source', {}).get('text', '')
            target = relation.get('target', {}).get('text', '')
            relation_type = relation.get('relation', '')
            
            if not source or not target or not relation_type:
                continue
            
            # 确保源和目标节点存在
            if not self.graph.has_node(source) or not self.graph.has_node(target):
                continue
            
            # 添加边
            self.graph.add_edge(source, target,
                               label=relation_type,
                               weight=relation.get('confidence', 0.5))
        
        print(f"成功构建知识图谱：{len(self.graph.nodes())} 个节点，{len(self.graph.edges())} 条边")
    
    def export_to_csv(self, nodes_filename='kg_nodes.csv', edges_filename='kg_edges.csv'):
        """导出为CSV格式"""
        if not self.graph:
            print("没有图可以导出")
            return None, None
        
        # 导出节点
        nodes_data = []
        for node, attrs in self.graph.nodes(data=True):
            nodes_data.append({
                'id': node,
                'label': attrs.get('label', node),
                'type': attrs.get('type', ''),
                'occurrences': attrs.get('occurrences', 1)
            })
        
        nodes_df = pd.DataFrame(nodes_data)
        nodes_path = os.path.join(self.output_dir, nodes_filename)
        # Windows编码处理
        nodes_df.to_csv(nodes_path, index=False, encoding='utf-8')
        
        # 导出边
        edges_data = []
        for source, target, attrs in self.graph.edges(data=True):
            edges_data.append({
                'source': source,
                'target': target,
                'relation': attrs.get('label', ''),
                'weight': attrs.get('weight', 0.5)
            })
        
        edges_df = pd.DataFrame(edges_data)
        edges_path = os.path.join(self.output_dir, edges_filename)
        # Windows编码处理
        edges_df.to_csv(edges_path, index=False, encoding='utf-8')
        
        print(f"CSV文件已保存: {nodes_path}, {edges_path}")
        return nodes_path, edges_path
    
    def export_to_graphml(self, filename='knowledge_graph.graphml'):
        """导出为GraphML格式（Gephi等工具可用）"""
        if not self.graph:
            print("没有图可以导出")
            return None
        
        # 导出为GraphML
        graphml_path = os.path.join(self.output_dir, filename)
        nx.write_graphml(self.graph, graphml_path, encoding='utf-8')  # 添加encoding参数
        
        print(f"GraphML文件已保存: {graphml_path}")
        return graphml_path
    
    def visualize_html(self, filename='knowledge_graph.html'):
        """生成交互式HTML可视化"""
        if not self.graph:
            print("没有图可以可视化")
            return None
        
        # 创建PyVis网络图
        # 修改初始设置，更适合Windows浏览器
        net = Network(height='800px', width='100%', notebook=False, directed=True, 
                    bgcolor='#ffffff', font_color='#000000')
        
        # 为不同类型的实体设置不同的颜色
        color_map = {
            '疾病': '#FF6666',
            'Disease': '#FF6666',
            '药物': '#66CC66',
            'Drug': '#66CC66',
            '靶点': '#6666FF',
            'Target': '#6666FF',
            '生物过程': '#FFCC66',
            'BiologicalProcess': '#FFCC66',
            '基因': '#66CCFF',
            'Gene': '#66CCFF',
            '蛋白质': '#CC66CC',
            'Protein': '#CC66CC',
            '生物标志物': '#CCCC66',
            'Biomarker': '#CCCC66'
        }
        
        # 添加节点
        for node, attrs in self.graph.nodes(data=True):
            node_type = attrs.get('type', '')
            color = color_map.get(node_type, '#AAAAAA')
            size = 20 + (attrs.get('occurrences', 1) * 5)  # 根据出现次数调整大小
            
            net.add_node(node, label=attrs.get('label', node), 
                        title=f"类型: {node_type}<br>出现次数: {attrs.get('occurrences', 1)}",
                        color=color, size=size)
        
        # 添加边
        for source, target, attrs in self.graph.edges(data=True):
            label = attrs.get('label', '')
            weight = attrs.get('weight', 0.5)
            width = 1 + (weight * 5)  # 根据权重调整边的宽度
            
            net.add_edge(source, target, label=label, width=width, 
                        title=f"关系: {label}<br>置信度: {weight:.2f}")
        
        # 设置物理布局选项 - 调整参数适应Windows浏览器
        net.repulsion(node_distance=150, spring_length=150, damping=0.09)
        net.set_options("""
        var options = {
          "nodes": {
            "font": {
              "size": 14,
              "face": "Arial"
            },
            "borderWidth": 2,
            "shadow": true
          },
          "edges": {
            "font": {
              "size": 12,
              "face": "Arial"
            },
            "smooth": {
              "type": "continuous",
              "forceDirection": "none"
            }
          },
          "physics": {
            "barnesHut": {
              "gravitationalConstant": -8000,
              "springConstant": 0.01,
              "springLength": 150
            },
            "minVelocity": 0.75
          }
        }
        """)
        
        # 保存HTML文件
        html_path = os.path.join(self.output_dir, filename)
        net.save_graph(html_path)
        print(f"HTML可视化文件已保存: {html_path}")
        return html_path
    
    def generate_statistics(self):
        """生成知识图谱统计信息"""
        if not self.graph:
            print("没有图可以分析")
            return {}
        
        stats = {
            "节点总数": len(self.graph.nodes()),
            "边总数": len(self.graph.edges()),
            "节点类型统计": {},
            "关系类型统计": {},
            "度数最高的节点": [],
            "中心性最高的节点": []
        }
        
        # 统计节点类型
        node_types = {}
        for _, attrs in self.graph.nodes(data=True):
            node_type = attrs.get('type', '未知')
            node_types[node_type] = node_types.get(node_type, 0) + 1
        
        stats["节点类型统计"] = node_types
        
        # 统计关系类型
        relation_types = {}
        for _, _, attrs in self.graph.edges(data=True):
            relation = attrs.get('label', '未知')
            relation_types[relation] = relation_types.get(relation, 0) + 1
        
        stats["关系类型统计"] = relation_types
        
        # 找出度数最高的节点
        degree_centrality = nx.degree_centrality(self.graph)
        top_degree_nodes = sorted(degree_centrality.items(), key=lambda x: x[1], reverse=True)[:10]
        stats["度数最高的节点"] = [{"节点": node, "度数中心性": round(score, 3)} 
                             for node, score in top_degree_nodes]
        
        # 找出中介中心性最高的节点
        try:
            betweenness_centrality = nx.betweenness_centrality(self.graph)
            top_betweenness_nodes = sorted(betweenness_centrality.items(), key=lambda x: x[1], reverse=True)[:10]
            stats["中心性最高的节点"] = [{"节点": node, "中介中心性": round(score, 3)} 
                                 for node, score in top_betweenness_nodes]
        except:
            stats["中心性最高的节点"] = "图结构不支持计算中介中心性"
        # 将统计信息保存到JSON文件
        stats_path = os.path.join(self.output_dir, 'kg_statistics.json')
        # Windows编码处理
        with open(stats_path, 'w', encoding='utf-8') as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
        
        print(f"统计信息已保存: {stats_path}")
        return stats
    
    def visualize_matplotlib(self, filename='knowledge_graph.png'):
        """使用Matplotlib生成静态图可视化"""
        if not self.graph:
            print("没有图可以可视化")
            return None
        
        # 创建一个新图形
        plt.figure(figsize=(12, 10))
        
        # 为不同类型的实体设置不同的颜色
        color_map = {
            '疾病': 'red',
            'Disease': 'red',
            '药物': 'green',
            'Drug': 'green',
            '靶点': 'blue',
            'Target': 'blue',
            '生物过程': 'orange',
            'BiologicalProcess': 'orange',
            '基因': 'cyan',
            'Gene': 'cyan',
            '蛋白质': 'purple',
            'Protein': 'purple',
            '生物标志物': 'yellow',
            'Biomarker': 'yellow'
        }
        
        # 为每个节点准备颜色
        node_colors = []
        for node, attrs in self.graph.nodes(data=True):
            node_type = attrs.get('type', '')
            node_colors.append(color_map.get(node_type, 'gray'))
        
        # 计算节点大小
        node_sizes = []
        for node, attrs in self.graph.nodes(data=True):
            occurrences = attrs.get('occurrences', 1)
            node_sizes.append(100 + (occurrences * 20))  # 根据出现次数调整大小
        
        # 计算边宽度
        edge_widths = []
        for u, v, attrs in self.graph.edges(data=True):
            weight = attrs.get('weight', 0.5)
            edge_widths.append(1 + weight * 2)
        
        # 绘制图
        pos = nx.spring_layout(self.graph, k=0.3, iterations=50, seed=42)  # 使用弹簧布局
        
        # 绘制节点
        nx.draw_networkx_nodes(self.graph, pos, node_color=node_colors, node_size=node_sizes, alpha=0.8)
        
        # 绘制边
        nx.draw_networkx_edges(self.graph, pos, width=edge_widths, alpha=0.5, arrows=True, arrowsize=10)
        
        # 绘制标签
        nx.draw_networkx_labels(self.graph, pos, font_size=8, font_family='sans-serif')
        
        # 添加标题
        plt.title("矽肺文献知识图谱", fontsize=16)
        
        # 保存图像
        png_path = os.path.join(self.output_dir, filename)
        plt.savefig(png_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"静态图像已保存: {png_path}")
        return png_path


def main():
    """直接运行时的入口函数"""
    parser = argparse.ArgumentParser(description='知识图谱构建与可视化工具')
    parser.add_argument('input', help='知识图谱JSON文件路径')
    parser.add_argument('--csv', action='store_true', help='导出为CSV文件')
    parser.add_argument('--graphml', action='store_true', help='导出为GraphML文件')
    parser.add_argument('--html', action='store_true', help='生成HTML可视化')
    parser.add_argument('--png', action='store_true', help='生成PNG静态图')
    parser.add_argument('--stats', action='store_true', help='生成统计信息')
    parser.add_argument('--all', action='store_true', help='生成所有输出')
    
    args = parser.parse_args()
    
    # 创建知识图谱构建器
    kg_builder = KnowledgeGraphBuilder(args.input)
    
    # 构建图
    kg_builder.build_graph()
    
    # 导出与可视化
    if args.all or args.csv:
        kg_builder.export_to_csv()
    
    if args.all or args.graphml:
        kg_builder.export_to_graphml()
    
    if args.all or args.html:
        kg_builder.visualize_html()
    
    if args.all or args.png:
        kg_builder.visualize_matplotlib()
    
    if args.all or args.stats:
        kg_builder.generate_statistics()
    
    if not (args.csv or args.graphml or args.html or args.png or args.stats or args.all):
        print("没有指定任何输出格式。使用 --help 查看可用选项。")


if __name__ == "__main__":
    main()