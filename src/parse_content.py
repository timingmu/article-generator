import pandas as pd
import ast
import csv
import re

def clean_text(text):
    """清理文本内容"""
    # 替换实际的换行符和多余空格为单个空格
    text = text.replace('\n', ' ')
    text = re.sub(r'\s+', ' ', text)
    # 移除引号和多余的逗号
    text = text.strip('",')
    return text.strip()

def extract_title_and_content(text):
    """提取标题和内容段落"""
    try:
        # 按 \n\n 分割所有内容
        parts = text.split('\n\n')
        if len(parts) >= 2:
            # 第一部分是标题
            title = clean_text(parts[0])
            # 清理每个段落并构建内容字符串
            content_paragraphs = [clean_text(p) for p in parts[1:]]
            # 构造正确的字符串格式
            content_str = f'["{", ".join(content_paragraphs)}"]'
            return title, content_str
        return text.strip(), '[]'
    except:
        return text.strip(), '[]'

def parse_content(file_path):
    """解析文章内容"""
    try:
        df = pd.read_csv(file_path)
        output_file = file_path.replace('.csv', '_parsed.csv')
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['title', 'content', 'category', 'country', 'language'])
            
            processed_count = 0
            for _, row in df.iterrows():
                try:
                    if pd.isna(row['contents']):
                        continue
                    
                    contents_list = ast.literal_eval(row['contents'])
                    if not contents_list or not contents_list[0]:
                        continue
                    
                    article = contents_list[0]
                    title, content = extract_title_and_content(article)
                    
                    writer.writerow([
                        title,
                        content,  # 现在是格式化的字符串
                        row.get('category', ''),
                        row.get('country', ''),
                        row.get('language', '')
                    ])
                    
                    processed_count += 1
                    print(f"\n处理文章 {processed_count}: {title}")
                    
                except Exception as e:
                    print(f"处理行时出错: {str(e)}")
                    continue
            
        print(f"\n成功解析 {processed_count} 篇文章")
        print(f"结果已保存到: {output_file}")
        
    except Exception as e:
        print(f"处理文件时出错: {str(e)}")

def main():
    """主函数"""
    file_path = input("请输入要处理的 CSV 文件路径: ")
    parse_content(file_path)

if __name__ == "__main__":
    main() 