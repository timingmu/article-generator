# 导入必要的库
print("开始导入模块...")
from dotenv import load_dotenv
import os
import boto3
import sqlite3
import json
from datetime import datetime
import csv
import sys

# 添加 src 目录到 Python 路径
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, 'src')
if src_dir not in sys.path:
    sys.path.append(src_dir)

print("准备导入配置...")
from config import (
    AWS_CONFIG, 
    MODEL_CONFIG, 
    PATH_CONFIG,
    TOPIC_CONFIG,
    get_topic_prompt
)
print("配置导入完成")

# 确保在创建客户端之前加载环境变量
load_dotenv()

# 添加环境变量调试信息的打印
print("环境变量检查:")
print(f"AWS_REGION: {os.getenv('AWS_REGION')}")
print(f"AWS_ACCESS_KEY_ID: {os.getenv('AWS_ACCESS_KEY_ID', 'Not found')[:5]}...")
print(f"MODEL_ID: {os.getenv('MODEL_ID', 'Not found')}")

class TopicGenerator:
    def __init__(self):
        # 使用配置文件中的设置初始化 AWS Bedrock 客户端
        self.bedrock = boto3.client(**AWS_CONFIG)
        # 从配置文件获取模型 ID
        self.model_id = MODEL_CONFIG['model_id']
        self.db_path = 'topics.db'
        self.setup_database()
        # 添加 CSV 输出路径
        self.output_dir = os.path.expanduser(PATH_CONFIG['topic_output'])
        # 确保输出目录存在
        os.makedirs(self.output_dir, exist_ok=True)
        self.total_topics = 0  # 用于统计总话题数
        self.similarity_threshold = TOPIC_CONFIG['similarity_threshold']  # 添加相似度阈值配置
    
    def setup_database(self):
        """创建数据库表结构"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS topics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT UNIQUE,
                    category TEXT,
                    created_at TIMESTAMP
                )
            ''')
    
    def generate_topics(self, category, num_topics=TOPIC_CONFIG['topics_per_category']):
        # 使用配置文件中的提示词模板
        prompt = get_topic_prompt(category, num_topics)
        
        # 构建 API 请求体
        request_body = {
            "anthropic_version": "bedrock-2023-05-31",
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "max_tokens": MODEL_CONFIG['max_tokens'],
            "temperature": MODEL_CONFIG['temperature'],
            "top_p": MODEL_CONFIG['top_p']
        }
        
        try:
            # 调用 Bedrock API
            response = self.bedrock.invoke_model(
                modelId=self.model_id,
                body=json.dumps(request_body)
            )
            
            # 打印调试信息
            print("API 响应:", response)
            
            # 解析响应内容
            response_body = json.loads(response['body'].read())
            print("响应内容:", response_body)
            
            # 处理生成的话题
            topics = response_body['content'][0]['text'].strip().split('\n')
            topics = [topic.strip() for topic in topics if topic.strip()]
            
            # 打印生成的话题
            print(f"\n=== 已生成 {category} 类别的话题 ===")
            for topic in topics:
                print(topic)
            print("===========================\n")
            
            return topics
            
        except Exception as e:
            print(f"API 调用出错: {str(e)}")
            print(f"请求体: {request_body}")
            raise e
    
    def calculate_similarity(self, str1, str2):
        """计算两个字符串的相似度"""
        # 将字符串转换为集合，计算交集和并集
        set1 = set(str1)
        set2 = set(str2)
        
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        
        if union == 0:
            return 0
        
        return intersection / union
    
    def is_duplicate(self, topic):
        """检查话题是否重复"""
        with sqlite3.connect(self.db_path) as conn:
            # 1. 精确匹配
            cursor = conn.execute('SELECT 1 FROM topics WHERE topic = ?', (topic,))
            if cursor.fetchone():
                print(f"精确匹配到重复话题: {topic}")
                return True
            
            # 2. 模糊匹配
            cursor = conn.execute('SELECT topic FROM topics')
            existing_topics = cursor.fetchall()
            
            for (existing_topic,) in existing_topics:
                similarity = self.calculate_similarity(topic, existing_topic)
                if similarity > self.similarity_threshold:
                    print(f"发现相似话题:\n- 新话题: {topic}\n- 已存在: {existing_topic}\n- 相似度: {similarity:.2f}")
                    return True
            
            return False
    
    def save_topics(self, topics, category):
        """保存新话题到数据库"""
        new_topics = []
        with sqlite3.connect(self.db_path) as conn:
            for topic in topics:
                if not self.is_duplicate(topic):
                    try:
                        conn.execute(
                            'INSERT INTO topics (topic, category, created_at) VALUES (?, ?, ?)',
                            (topic, category, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                        )
                        new_topics.append(topic)
                        print(f"成功保存新话题: {topic}")
                    except sqlite3.IntegrityError:
                        print(f"数据库插入错误，话题已存在: {topic}")
        return new_topics

    def save_to_csv(self, all_topics):
        """将话题保存为 CSV 文件"""
        # 设置输出目录
        self.output_dir = os.path.expanduser(PATH_CONFIG['topic_output'])
        # 确保输出目录存在
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 生成文件名：topic_数量_日期
        today = datetime.now().strftime('%Y%m%d')
        filename = f"topic_{len(all_topics)}_{today}.csv"
        filepath = os.path.join(self.output_dir, filename)
        
        # 写入 CSV 文件
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # 写入表头
            writer.writerow(['Number', 'Category', 'Topic'])
            # 写入数据
            for index, (topic, category) in enumerate(all_topics, 1):
                writer.writerow([index, category, topic])
        
        print(f"\n话题已保存到: {filepath}")
        return filepath

    def daily_task(self):
        all_topics = []  # 存储所有生成的话题
        categories = TOPIC_CONFIG['categories']
        
        for category in categories:
            try:
                topics = self.generate_topics(category)
                new_topics = self.save_topics(topics, category)
                # 将新话题添加到总列表中
                all_topics.extend([(topic, category) for topic in new_topics])
                
                print(f"\n成功保存 {category} 类别的 {len(new_topics)} 个新话题")
                print("新保存的话题：")
                for topic in new_topics:
                    print(f"- {topic}")
            except Exception as e:
                print(f"生成 {category} 类别话题时出错: {str(e)}")
        
        # 保存所有话题到 CSV
        if all_topics:
            csv_path = self.save_to_csv(all_topics)
            print(f"\n总共生成了 {len(all_topics)} 个新话题")

    def get_topic_stats(self):
        """获取话题库统计信息"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('''
                SELECT 
                    category,
                    COUNT(*) as count,
                    MIN(created_at) as earliest,
                    MAX(created_at) as latest
                FROM topics 
                GROUP BY category
            ''')
            stats = cursor.fetchall()
            
            print("\n=== 话题库统计 ===")
            total = 0
            for category, count, earliest, latest in stats:
                print(f"\n类别: {category}")
                print(f"话题数量: {count}")
                print(f"最早记录: {earliest}")
                print(f"最新记录: {latest}")
                total += count
            print(f"\n总话题数: {total}")
            print("================\n")

    def search_similar_topics(self, topic, threshold=0.8):
        """搜索相似话题"""
        similar_topics = []
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT topic, category, created_at FROM topics')
            for existing_topic, category, created_at in cursor:
                similarity = self.calculate_similarity(topic, existing_topic)
                if similarity > threshold:
                    similar_topics.append({
                        'topic': existing_topic,
                        'category': category,
                        'created_at': created_at,
                        'similarity': similarity
                    })
        return similar_topics

# 主程序入口
if __name__ == "__main__":
    try:
        print("正在初始化 TopicGenerator...")
        generator = TopicGenerator()
        print("初始化成功，开始生成话题...")
        generator.daily_task()
    except Exception as e:
        print(f"发生错误: {str(e)}")