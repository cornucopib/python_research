from elasticsearch import Elasticsearch
import time
import random
import string
from itertools import islice  # 导入islice

# 初始化Elasticsearch客户端
es = Elasticsearch("http://localhost:9200")


# 生成4000个随机ID
def generate_random_id():
    return ''.join(random.choices(string.digits, k=19))


# 生成4000个随机ID
test_ids = [generate_random_id() for _ in range(4000)]


# 定义分批查询函数
def batched(iterable, n):
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            break
        yield batch


# 测试单次查询4000个ID的时间
def test_single_query(ids):
    start_time = time.time()

    response = es.search(
        index="my_index",
        body={
            "query": {
                "terms": {
                    "id": ids
                }
            },
            "_source": ["id", "name", "age"]
        }
    )

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Single query for {len(ids)} IDs took {elapsed_time:.4f} seconds")
    return elapsed_time


# 测试分批查询4000个ID的时间
def test_batched_query(ids, batch_size=1024):
    total_time = 0
    num_batches = 0

    for batch in batched(ids, batch_size):
        start_time = time.time()

        response = es.search(
            index="my_index",
            body={
                "query": {
                    "terms": {
                        "id": batch
                    }
                },
                "_source": ["id", "name", "age"]
            }
        )

        end_time = time.time()
        elapsed_time = end_time - start_time
        total_time += elapsed_time
        num_batches += 1

    average_time = total_time / num_batches
    print(
        f"Batched query for {len(ids)} IDs (batch size: {batch_size}) took an average of {average_time:.4f} seconds per batch")
    return total_time


# 执行测试
if __name__ == "__main__":
    # 测试单次查询4000个ID
    single_query_time = test_single_query(test_ids)

    # 测试分批查询4000个ID（每次1024个）
    batched_query_time = test_batched_query(test_ids, batch_size=1024)

    # 比较两种方法的总时间
    print(f"Total time for single query: {single_query_time:.4f} seconds")
    print(f"Total time for batched query: {batched_query_time:.4f} seconds")