import pandas as pd
import json
import boto3
import os
from datetime import datetime
import sys
import csv
import asyncio
import concurrent.futures
from typing import List, Dict
import time

# 添加当前目录到 Python 路径
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

# 从当前目录导入配置
from config import (
    AWS_CONFIG, 
    MODEL_CONFIG, 
    PATH_CONFIG,
    get_analysis_prompt
)

class ArticleAnalyzer:
    def __init__(self, max_concurrent=5):
        self.bedrock = boto3.client(**AWS_CONFIG)
        self.model_id = MODEL_CONFIG['model_id']
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # 设置输出目录和文件
        self.output_dir = os.path.expanduser('~/Desktop/code/characteristics')
        os.makedirs(self.output_dir, exist_ok=True)
        self.processed_count = 0
        
        # 创建输出文件
        self.date_prefix = datetime.now().strftime('%Y%m%d')
        self.output_file = os.path.join(
            self.output_dir,
            f'{self.date_prefix}_characteristic.csv'
        )
        
        # 创建 CSV 文件并写入表头
        with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['number', 'category', 'sub_category', 'author', 'characteristic'])
        
        # 设置数据文件路径
        self.data_file = PATH_CONFIG['feature_pool']
    
    def write_result(self, result, number):
        """写入单个结果到 CSV 和独立的分析文件"""
        try:
            # 写入 CSV
            with open(self.output_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    number,
                    result['category'],
                    result['sub_category'],
                    result['author'],
                    result['characteristic']
                ])
            
            # 为每个类别-子类别组合创建单独的分析文件
            # 使用类别和子类别作为文件名
            safe_category = result['category'].replace(' ', '-')
            safe_sub_category = result['sub_category'].replace(' ', '-').replace('&', 'and')
            
            analysis_file = os.path.join(
                self.output_dir,
                f'analysis-{safe_category}-{safe_sub_category}-{self.date_prefix}.json'
            )
            
            # 保存分析结果到独立文件
            with open(analysis_file, 'w', encoding='utf-8') as f:
                json.dump(result['characteristic'], f, ensure_ascii=False, indent=2)
                
            print(f"已保存分析结果到: {analysis_file}")
            
            self.processed_count = number
            
        except Exception as e:
            print(f"写入结果时出错：{str(e)}")
            raise e

    async def analyze_single_author(self, author: str, articles: List[Dict], category: str, sub_category: str, number: int):
        """分析单个作者的文章"""
        async with self.semaphore:
            prompt = get_analysis_prompt(author, articles)
            
            try:
                # 调用 Claude API
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: self.bedrock.invoke_model(
                        modelId=self.model_id,
                        body=json.dumps({
                            "anthropic_version": "bedrock-2023-05-31",
                            "messages": [
                                {
                                    "role": "user",
                                    "content": prompt
                                }
                            ],
                            "max_tokens": MODEL_CONFIG['max_tokens'],
                            "temperature": MODEL_CONFIG['temperature']
                        })
                    )
                )
                
                # 解析响应
                response_body = json.loads(response['body'].read())
                analysis = response_body['content'][0]['text']
                
                print(f"\n作者 {author} 的分析结果：")
                print(analysis)
                
                result = {
                    'category': category,
                    'sub_category': sub_category,
                    'author': author,
                    'characteristic': analysis,
                    'status': 'success'
                }
                
                # 写入结果
                self.write_result(result, number)
                
                return result
                    
            except Exception as e:
                print(f"分析作者 {author} 的文章时出错：{str(e)}")
                error_result = {
                    'category': category,
                    'sub_category': sub_category,
                    'author': author,
                    'characteristic': str(e),
                    'status': 'error'
                }
                self.write_result(error_result, number)
                return error_result

    async def analyze_articles(self):
        """并发分析所有作者的文章"""
        start_time = time.time()
        
        try:
            # 加载数据
            df = self.load_articles()
            author_groups = self.get_author_articles(df)
            
            # 创建任务列表
            tasks = []
            for i, (author, group) in enumerate(author_groups, 1):
                articles = group.to_dict('records')
                category = articles[0].get('category', '未分类')
                sub_category = articles[0].get('sub-category', '未分类')
                
                task = self.analyze_single_author(author, articles, category, sub_category, i)
                tasks.append(task)
            
            # 并发执行所有任务
            results = await asyncio.gather(*tasks)
            
            # 统计成功的结果
            successful_results = [r for r in results if r['status'] == 'success']
            
            end_time = time.time()
            print(f"\n总共分析了 {len(successful_results)} 位作者的文章")
            print(f"总耗时：{end_time - start_time:.2f} 秒")
            print(f"平均每位作者耗时：{(end_time - start_time) / len(results):.2f} 秒")
            
        except Exception as e:
            print(f"分析过程出错：{str(e)}")
            raise e

    def load_articles(self):
        """加载文章数据"""
        print(f"正在从 {self.data_file} 加载文章数据...")
        try:
            # 检查文件是否存在
            if not os.path.exists(self.data_file):
                raise FileNotFoundError(f"找不到数据文件：{self.data_file}")
            
            # 读取 Excel 文件
            df = pd.read_excel(self.data_file)
            
            # 检查必需的列
            required_columns = ['author', 'headline', 'abstract', 'content', 'category', 'sub-category']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Excel 文件缺少必需的列：{', '.join(missing_columns)}")
            
            # 检查���据是否为空
            if df.empty:
                raise ValueError("Excel 文件中没有数据")
            
            print(f"成功加载 {len(df)} 条文章记录")
            
            # 打印数据预览
            print("\n数据预览：")
            print(df[['author', 'category', 'sub-category']].head())
            print("\n")
            
            return df
            
        except Exception as e:
            print(f"加载文章数据时出错：{str(e)}")
            raise e
    
    def get_author_articles(self, df):
        """按作者分组获取文章"""
        print("正在按作者分组文章...")
        try:
            # 按作者分组并
            groups = df.groupby('author')
            author_groups = [(name, group) for name, group in groups]
            print(f"共有 {len(author_groups)} 位作者")
            return author_groups
        except Exception as e:
            print(f"分组文章时出错：{str(e)}")
            raise e

    def load_analysis_results(self):
        """加载分析结果"""
        analysis_dir = os.path.expanduser(PATH_CONFIG['analysis_output'])
        results = {}  # 使用字典来存储每个作者的最新结果
        
        print(f"正在从 {analysis_dir} 加载分析结果...")
        
        if not os.path.exists(analysis_dir):
            print(f"警告：分析结果目录不存在：{analysis_dir}")
            return []
        
        # 获取所有文件并按时间戳排序
        files = []
        for file in os.listdir(analysis_dir):
            if file.startswith('analysis_') and (file.endswith('.json') or file.endswith('.txt')):
                file_path = os.path.join(analysis_dir, file)
                files.append((file, file_path, os.path.getmtime(file_path)))
        
        # 按时间戳降序排序
        files.sort(key=lambda x: x[2], reverse=True)
        
        # 处理每个文件
        for file, file_path, _ in files:
            try:
                # 从文件名中提取作者名
                author = file.replace('analysis_', '').split('_')[0]
                
                # 如果这个作者已经有了有效结果，跳过
                if author in results:
                    continue
                
                # 读取文件内容
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                
                # 跳过空文件
                if not content:
                    print(f"警告：文件为空：{file}")
                    continue
                
                # 尝试解析 JSON
                try:
                    if file.endswith('.json'):
                        analysis = json.loads(content)
                    else:
                        analysis = content
                    
                    results[author] = {
                        'author': author,
                        'analysis': analysis,
                        'file': file
                    }
                    print(f"成功加载文件：{file}")
                    
                except json.JSONDecodeError:
                    if file.endswith('.json'):
                        print(f"警告：JSON 解析失败：{file}")
                        continue
                    else:
                        results[author] = {
                            'author': author,
                            'analysis': content,
                            'file': file
                        }
                        print(f"成功加载文件：{file}")
                        
            except Exception as e:
                print(f"处理文件 {file} 时出错：{str(e)}")
                continue
        
        result_list = list(results.values())
        print(f"\n共加载了 {len(result_list)} 个有效的分析结果")
        return result_list

def main():
    """主函数"""
    analyzer = ArticleAnalyzer(max_concurrent=10)  # 增加并发数到 10
    
    try:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        asyncio.run(analyzer.analyze_articles())
        
    except KeyboardInterrupt:
        print("\n程序被用户中断")
        # 确保文件名更新
        analyzer.update_csv_name()
    except Exception as e:
        print(f"程序执行出错：{str(e)}")
        import traceback
        print(traceback.format_exc())
        # 确保文件名更新
        analyzer.update_csv_name()

if __name__ == "__main__":
    main() 