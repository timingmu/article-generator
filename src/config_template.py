# 配置文件模板
AWS_CONFIG = {
    'service_name': 'bedrock-runtime',
    'region_name': 'your-region',
    'aws_access_key_id': 'your-access-key-id',
    'aws_secret_access_key': 'your-secret-access-key'
}

MODEL_CONFIG = {
    'model_id': 'your-model-id',
    'max_tokens': 4000,
    'temperature': 0.7,
    'top_p': 0.9
}

PATH_CONFIG = {
    'topic_output': '~/path/to/topic/output',
    'article_output': '~/path/to/article/output',
    'analysis_output': '~/path/to/analysis/output'
}
