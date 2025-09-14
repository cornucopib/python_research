import random
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, helpers

# 连接Elasticsearch
es = Elasticsearch(["http://localhost:9200"])

# 索引名称
index_name = "person_test"

# 如果索引存在，则删除它（仅用于测试，生产环境不要这样）
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)

# 创建索引
es.indices.create(index=index_name, body={
    "mappings": {
        "properties": {
            "name": {"type": "keyword"},
            "age": {"type": "integer"},
            "birthday": {"type": "date", "format": "yyyy-MM-dd"}
        }
    }
})

# 生成随机姓名
def generate_name():
    first_names = ['张', '王', '李', '赵', '陈', '黄', '周', '吴', '刘', '孙']
    last_names = ['明', '芳', '静', '伟', '秀英', '强', '磊', '洋', '艳', '勇', '杰', '娟', '涛', '超', '鹏', '华', '平', '刚', '辉', '兰']
    return random.choice(first_names) + random.choice(last_names)

# 生成随机年龄
def generate_age():
    return random.randint(18, 80)

# 生成随机生日（1950-01-01 到 2005-12-31）
def generate_birthday():
    start_date = datetime(1950, 1, 1)
    end_date = datetime(2005, 12, 31)
    days_between = (end_date - start_date).days
    random_days = random.randint(0, days_between)
    return (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")

# 生成文档
def generate_documents(n):
    for i in range(1, n+1):
        yield {
            "_index": index_name,
            "_id": i,
            "_source": {
                "name": generate_name(),
                "age": generate_age(),
                "birthday": generate_birthday()
            }
        }

# 批量插入
def bulk_insert(es, documents, chunk_size=1000):
    try:
        helpers.bulk(es, documents, chunk_size=chunk_size)
        print("数据插入完成")
    except Exception as e:
        print(f"插入数据时发生错误: {e}")

# 插入14万条数据
total_docs = 140000
documents = generate_documents(total_docs)
bulk_insert(es, documents, chunk_size=1000)