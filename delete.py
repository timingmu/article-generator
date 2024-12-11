import sqlite3
import csv
import os
from datetime import datetime

def backup_and_clear_database():
    # 设置输出目录
    output_dir = os.path.expanduser('~/Desktop/code/topic/topic_generated')
    os.makedirs(output_dir, exist_ok=True)
    
    # 生成带时间戳的文件名
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(output_dir, f'database_backup_{timestamp}.csv')
    
    with sqlite3.connect('topics.db') as conn:
        # 1. 首先导出数据
        cursor = conn.execute('SELECT id, topic, category, created_at FROM topics')
        
        # 获取数据
        rows = cursor.fetchall()
        if not rows:
            print("数据库为空，无需备份")
            return
            
        # 写入CSV文件
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # 写入表头
            writer.writerow(['ID', '话题', '类别', '创建时间'])
            # 写入数据
            writer.writerows(rows)
        
        print(f"数据已备份到: {output_file}")
        print(f"共备份 {len(rows)} 条记录")
        
        # 2. 然后清空数据库
        response = input("\n数据已备份，是否确定清空数据库？(yes/no): ")
        if response.lower() == 'yes':
            conn.execute('DELETE FROM topics')
            conn.commit()
            print("数据库已清空")
        else:
            print("操作已取消，数据库保持不变")

if __name__ == "__main__":
    try:
        backup_and_clear_database()
    except Exception as e:
        print(f"发生错误: {str(e)}")