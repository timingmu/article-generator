import json
import os
from datetime import datetime
import boto3
import csv
from typing import List, Dict
import pandas as pd
import asyncio
import sys
import re

from config import (
    AWS_CONFIG,
    MODEL_CONFIG,
    PATH_CONFIG,
    get_article_prompt,
    DEFAULT_STYLE,
    get_proofreading_prompt
)

class ArticleGenerator:
    def __init__(self, max_concurrent=20, language='en'):
        self.bedrock = boto3.client(**AWS_CONFIG)
        self.model_id = MODEL_CONFIG['model_id']
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.language = language
        
        # 设置输出目录
        self.output_dir = os.path.expanduser(PATH_CONFIG['article_output'])
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 初始化计数器和日期
        self.processed_count = 0
        self.date_prefix = datetime.now().strftime('%Y%m%d')
        
        # 创建输出文件，根据语言设置不同的文件名
        lang_suffix = 'pt' if language == 'pt' else 'en'
        self.output_file = os.path.join(
            self.output_dir,
            f'{self.date_prefix}_articles_{lang_suffix}_{self.processed_count + 1}.csv'
        )
        
        # 创建 CSV 文件并写入新的表头
        with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'title',
                'description',
                'contents',
                'original_content',
                'category',
                'sub_category',
                'country',
                'language'
            ])
        
        # 添加校对统计
        self.proofread_success = 0
        self.proofread_failed = 0
    
    def load_topics(self) -> List[Dict]:
        """从最新的 topic CSV 文件加载话题"""
        topic_dir = os.path.expanduser(PATH_CONFIG['topic_output'])
        if not os.path.exists(topic_dir):
            raise FileNotFoundError(f"找不到话题目录：{topic_dir}")
        
        # 获取最新的 topic 文件
        topic_files = [f for f in os.listdir(topic_dir) if f.startswith('topic_') and f.endswith('.csv')]
        if not topic_files:
            raise FileNotFoundError("未找到话题文件")
        
        latest_file = max(topic_files, key=lambda x: os.path.getmtime(os.path.join(topic_dir, x)))
        topic_file = os.path.join(topic_dir, latest_file)
        
        # 读取话题
        df = pd.read_csv(topic_file)
        topics = df.to_dict('records')
        print(f"从 {latest_file} 加载了 {len(topics)} 个话题")
        return topics
    
    def load_writing_styles(self) -> Dict[str, Dict[str, str]]:
        """加载写作风格分析结果"""
        # 从特征池目录读取分析结果
        analysis_dir = os.path.expanduser('~/Desktop/code/characteristics')
        results = {}
        
        print("\n=== 开始加载写作风格 ===")
        print(f"分析结果目录: {analysis_dir}")
        
        if not os.path.exists(analysis_dir):
            print(f"创建分析结果目录：{analysis_dir}")
            os.makedirs(analysis_dir)
        
        # 获取所有分析文件
        files = [f for f in os.listdir(analysis_dir) 
                if f.endswith('.json') and 
                (f.startswith('analysis_') or f.startswith('analysis-'))]
        
        if not files:
            print("\n未找到分析结果文件，将使用默认写作风格")
            return results
        
        for file_name in files:
            try:
                file_path = os.path.join(analysis_dir, file_name)
                print(f"\n处理文件: {file_name}")
                
                # 统一处理文件名格式
                normalized_name = (file_name.replace('analysis_', '')
                                      .replace('analysis-', '')
                                      .replace('.json', '')
                                      .replace('_', '-')
                                      .replace(' & ', '-'))
                
                parts = normalized_name.split('-')
                
                if len(parts) >= 3:
                    category = parts[0]
                    # 找到日期部分（8位数字）
                    date_index = -1
                    for i, part in enumerate(parts):
                        if part.isdigit() and len(part) == 8:
                            date_index = i
                            break
                    
                    if date_index > 1:
                        # 还原子类别中的 & 符号
                        sub_category = ' '.join(parts[1:date_index]).replace('-and-', ' & ')
                        
                        print(f"解析结果:")
                        print(f"- 类别: {category}")
                        print(f"- 子类别: {sub_category}")
                        print(f"- 日期: {parts[date_index]}")
                        
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = json.load(f)
                        
                        if category not in results:
                            results[category] = {}
                        results[category][sub_category] = content
                        print(f"成功加载 {category}-{sub_category} 的写作风格")
                
            except Exception as e:
                print(f"处理文件 {file_name} 时出错：{str(e)}")
                continue
        
        print(f"\n成功加载了 {sum(len(cat) for cat in results.values())} 个类别的写作风格")
        return results

    async def write_article(self, article_data: Dict):
        """写入文章"""
        try:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    article_data['title'],
                    article_data['description'],
                    article_data['content'],
                    article_data.get('original_content', ''),
                    article_data['category'],
                    article_data['sub_category'],
                    'PH',
                    'en'
                ])
        except Exception as e:
            print(f"写入文章时出错：{str(e)}")
            raise e

    def clean_text(self, text):
        """清理文本内容，保留JSON格式"""
        # 移除多余的空格但保留换行符
        text = re.sub(r'[ \t]+', ' ', text)
        # 移除多余的引号
        text = text.strip('",')
        return text.strip()

    async def generate_single_article(self, topic_info: Dict, writing_styles: Dict[str, Dict[str, str]]) -> Dict:
        """异步生成单篇文章"""
        async with self.semaphore:
            try:
                category = topic_info['Category']
                sub_category = topic_info['Sub_Category']
                
                print(f"\n正在生成主题 '{topic_info['Topic']}' 的文章...")
                
                # 标准化类别和子类别名称以进行比较
                def normalize_category(cat: str) -> str:
                    return cat.lower().replace(' & ', ' and ').replace('-', ' ')
                
                normalized_category = normalize_category(category)
                normalized_sub_category = normalize_category(sub_category)
                
                # 在写作风格中查找匹配项
                matched_style = None
                for cat, sub_cats in writing_styles.items():
                    if normalize_category(cat) == normalized_category:
                        for sub_cat, style in sub_cats.items():
                            if normalize_category(sub_cat) == normalized_sub_category:
                                matched_style = style
                                break
                        if matched_style:
                            break
                
                if matched_style:
                    analysis_results = matched_style
                    print(f"使用 {category}-{sub_category} 的专属写作风格")
                else:
                    print(f"警告：未找到 {category}-{sub_category} 的写作风格，使用默认风格")
                    analysis_results = json.dumps(DEFAULT_STYLE, ensure_ascii=False, indent=2)
                
                # 生成提示词
                prompt = get_article_prompt(
                    topic=topic_info['Topic'],
                    category=category,
                    sub_category=sub_category,
                    analysis_results=analysis_results,
                    language=self.language
                )
                
                # 用 API
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
                article_content = response_body['content'][0]['text'].strip()
                
                # 1. 提取标题
                title_match = re.search(r'Title:\s*(.+?)(?=\[|$)', article_content)
                title = title_match.group(1).strip() if title_match else topic_info['Topic']
                
                # 2. 提取并清理内容数组
                content_match = re.search(r'\[([\s\S]+)\]', article_content)
                if content_match:
                    content_text = content_match.group(1)
                    # 使用正则表达式提取所有引号中的文本
                    paragraphs = re.findall(r'\"([^\"]+?)\"(?:\s*,\s*|$)', content_text)
                    # 清理段落
                    cleaned_paragraphs = []
                    for p in paragraphs:
                        # 移除多余的引号、转义字符和空白
                        cleaned_p = p.strip().replace('\\"', '').replace('\\', '').strip('"')
                        if cleaned_p and not cleaned_p.isspace():
                            cleaned_paragraphs.append(cleaned_p)
                    
                    # 创建干净的 JSON 数组
                    content_str = json.dumps(cleaned_paragraphs, ensure_ascii=False)
                else:
                    content_str = json.dumps([article_content], ensure_ascii=False)
                
                # 准备文章数据
                article_data = {
                    'title': title,
                    'description': '',
                    'content': content_str,
                    'original_content': content_str,
                    'category': category,
                    'sub_category': sub_category,
                    'country': 'BR' if self.language == 'pt' else 'PH',
                    'language': self.language
                }
                
                # 保存原始内容
                article_data['original_content'] = article_data['content']
                
                # 校对步骤
                try:
                    proofread_result = await self.proofread_article(
                        title=article_data['title'],
                        content=article_data['content'],
                        language=article_data['language']
                    )
                    
                    # 更新文章数据
                    article_data['title'] = proofread_result['title']
                    article_data['content'] = proofread_result['content']
                    self.proofread_success += 1
                    
                except Exception as e:
                    print(f"校对步骤失败，使用原始内容：{str(e)}")
                    self.proofread_failed += 1
                
                # 继续原有的写入流程
                await self.write_article(article_data)
                return article_data
                
            except Exception as e:
                print(f"处理文章内容时出错：{str(e)}")
                raise e

    async def generate_all_articles(self):
        """异步生成所有文���"""
        try:
            # 加载话题和写作风格
            topics = self.load_topics()
            writing_styles = self.load_writing_styles()
            
            print(f"\n开始生成 {len(topics)} 篇文章...")
            
            # 创建任务列表
            tasks = []
            for topic in topics:
                task = self.generate_single_article(topic, writing_styles)
                tasks.append(task)
            
            # 并发执行所有任务
            results = await asyncio.gather(*tasks)
            
            print(f"\n完成！共生成 {len(results)} 篇文章")
            
        except Exception as e:
            print(f"生成文章过程出错：{str(e)}")
            raise e

    async def proofread_article(self, title: str, content: str, language: str = 'en') -> Dict:
        """校对文章内容"""
        try:
            # 生成校对提示词
            prompt = get_proofreading_prompt(title, content, language)
            
            # 调用 API
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
                        "temperature": 0.3  # 使用较低的温度以确保稳定性
                    })
                )
            )
            
            # 解析响应
            response_body = json.loads(response['body'].read())
            proofread_result = json.loads(response_body['content'][0]['text'])
            
            return proofread_result
            
        except Exception as e:
            print(f"校对文章时出错：{str(e)}")
            # 如果校对失败，返回原始内容
            return {
                'title': title,
                'content': content
            }

def main():
    """主函数"""
    try:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        # 创建英文生成器
        en_generator = ArticleGenerator(max_concurrent=20, language='en')
        # 创建葡萄牙语生成器
        pt_generator = ArticleGenerator(max_concurrent=20, language='pt')
        
        # 并发运行两种语言的生成任务
        async def run_generators():
            await asyncio.gather(
                en_generator.generate_all_articles(),
                pt_generator.generate_all_articles()
            )
        
        asyncio.run(run_generators())
        
    except Exception as e:
        print(f"程序执行出错：{str(e)}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    main()