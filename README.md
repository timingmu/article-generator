# Topic Generator

自动生成主题文章的 Python 工具。

## 功能

- 自动生成主题文章
- 支持多种写作风格
- CSV 导出
- 并发生成
- AWS Bedrock 集成

## 使用方法

```python
from src.article_generator import ArticleGenerator

generator = ArticleGenerator()
generator.generate_all_articles()
```
