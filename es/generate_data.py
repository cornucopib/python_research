import random
from elasticsearch import Elasticsearch, helpers

# 连接到 Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

# 检查连接是否成功
if not es.ping():
    raise ValueError("Connection failed")

# 创建索引
index_name = 'test_index'
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)

# 定义映射
mapping = {
    "mappings": {
        "properties": {
            "division_desc": {
                "type": "nested",
                "properties": {
                    "region": {"type": "keyword"},
                    "repOffice": {"type": "keyword"}
                }
            }
        }
    }
}

es.indices.create(index=index_name, body=mapping)

def get_random_array(array):
    min_samples = 1
    max_samples = len(array)
    num_samples = random.randint(min_samples, max_samples)
    return list(set(random.sample(array, num_samples)))

# 生成 20 万个文档
a_values = []
for i in range(0,10):
    a_values.append(f"{1000+i}")



b_values = []
for j in range(0,10):
    b_values.append(f"{2000+i}")

print("Data generation ongoing.")

# 使用 bulk API 插入数据
actions = []
for i in range(200000):
    doc = {
        "_index": index_name,
        "_id": i,
        "_source": {
            "division_desc": [
                {"region": get_random_array(a_values), "repOffice": get_random_array(b_values)}
            ]
        }
    }
    actions.append(doc)

# 批量插入数据
helpers.bulk(es, actions)

print("Data generation complete.")