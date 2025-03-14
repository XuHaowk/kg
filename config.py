"""
配置文件，包含API密钥和模型参数设置
"""

# Kimi API配置 - 这里设置默认值，可通过UI修改
KIMI_API_KEY = "sk-FCOx1OcyyEnYczlxcoW7KlVnrZ7r6RqGMDW9erwUkWIGIEOS"
KIMI_API_ENDPOINT = "https://api.moonshot.cn/v1"
KIMI_MODEL = "moonshot-v1-8k"  # 使用Kimi的大上下文模型

# 实体提取配置
ENTITY_TYPES = ["疾病", "药物", "靶点", "生物过程", "基因", "蛋白质", "生物标志物"]
MAX_ENTITIES_PER_TYPE = 1000000  # 每种类型最多提取的实体数量
ENTITY_EXTRACTION_TEMPERATURE = 0.1  # 实体提取时的温度参数，较低以提高精确度

# 关系提取配置
RELATION_TYPES = [
    # 治疗关系
    "治疗", "预防", "诊断", "引起", "加重", "缓解", "副作用",
    # 分子作用关系
    "靶向", "抑制", "激活", "结合", "表达", "调节", "磷酸化", "降解", 
    # 药物来源与转化关系
    "提取自", "组分", "分离自", "转化为", "代谢为",
    # 机制关系
    "通过", "上调", "下调", "阻断", "介导",
    # 相关性关系
    "相关", "标志", "指示"
]
RELATION_EXTRACTION_TEMPERATURE = 0.2  # 关系提取时的温度参数
MAX_RELATIONS_PER_ENTITY_PAIR = 99999  # 每对实体之间最多提取的关系数量

# 文本处理配置
MAX_CHUNK_SIZE = 8000  # 文本分块的最大字符数
OVERLAP_SIZE = 500  # 文本块之间的重叠字符数

# 输出配置 - Windows路径处理
import os
OUTPUT_FORMAT = "json"  # 可选: "json", "csv", "rdf"
# 使用相对路径，避免硬编码绝对路径
script_dir = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(script_dir, "data", "output")