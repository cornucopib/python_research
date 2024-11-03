from elasticsearch import Elasticsearch

# 连接到 Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

# 检查连接是否成功
if not es.ping():
    raise ValueError("Connection failed")

# 定义索引名称
index_name = 'test_index'

# 使用 _count API 获取文档总数
response = es.count(index=index_name)

# 输出文档总数
total_documents = response['count']
print(f"Total documents in '{index_name}': {total_documents}")