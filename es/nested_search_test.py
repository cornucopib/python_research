from elasticsearch import Elasticsearch, helpers
from faker import Faker
import random
from datetime import datetime, timedelta, time
import json
import time

# 初始化Faker实例
fake = Faker()

# 初始化Elasticsearch客户端
es = Elasticsearch(["http://localhost:9200"])  # 根据实际情况修改连接信息

# 定义索引映射
person_mapping = {
    "mappings": {
        "properties": {
            "id": {"type": "keyword"},
            "name": {"type": "text"},
            "age": {"type": "integer"},
            "birthday": {"type": "date"},
            "region": {"type": "keyword"},
            "rep_office": {"type": "integer"},
            "rep": {"type": "integer"}
        }
    }
}

group_mapping = {
    "mappings": {
        "properties": {
            "group_id": {"type": "keyword"},
            "group_name": {"type": "text"},
            "person_list": {
                "type": "nested",
                "properties": {
                    "id": {"type": "keyword"},
                    "age": {"type": "integer"},
                    "name": {"type": "text"},
                    "birthday": {"type": "date"}
                }
            }
        }
    }
}

# 创建索引
def create_indices():
    if not es.indices.exists(index="person"):
        es.indices.create(index="person", body=person_mapping)
        print("Created 'person' index.")

    if not es.indices.exists(index="group"):
        es.indices.create(index="group", body=group_mapping)
        print("Created 'group' index.")


# 数据生成函数
def generate_person():
    return {
        "id": fake.uuid4(),
        "name": fake.name(),
        "age": random.randint(18, 80),
        "birthday": (datetime.now() - timedelta(days=random.randint(365 * 18, 365 * 80))).isoformat(),
        "region": fake.city(),
        "rep_office": random.choice([10, 11, 12]),
        "rep": random.choice([9, 10, 11])
    }


# 批量插入person数据
def load_person_data(num_records=1_500_000):
    actions = (
        {
            "_index": "person",
            "_id": generate_person()["id"],
            "_source": generate_person()
        }
        for _ in range(num_records)
    )

    print(f"开始插入 {num_records} 条person数据...")
    success, failed = helpers.bulk(es, actions, chunk_size=500, request_timeout=60)
    print(f"成功插入 {success} 条记录，失败 {failed} 条记录")


# 数据生成函数
def generate_group(person_ids):
    group_id = fake.uuid4()
    group_name = fake.company()
    person_list_ids = random.sample(person_ids, random.randint(10, 100))  # 每个group包含10-100个person
    return {
        "group_id": group_id,
        "group_name": group_name,
        "person_list": [{"id": pid} for pid in person_list_ids]
    }


# 批量插入group数据
def load_group_data(num_records=5000):
    # 获取所有person的ID
    person_ids = [doc["_id"] for doc in es.search(index="person", size=10000)["hits"]["hits"]]
    if len(person_ids) < num_records:
        print("Warning: Not enough person records to create all groups.")
        return

    actions = (
        {
            "_index": "group",
            "_id": generate_group(person_ids)["group_id"],
            "_source": generate_group(person_ids)
        }
        for _ in range(num_records)
    )

    print(f"开始插入 {num_records} 条group数据...")
    success, failed = helpers.bulk(es, actions, chunk_size=500, request_timeout=60)
    print(f"成功插入 {success} 条记录，失败 {failed} 条记录")


# 查询并统计
# 查询并统计
def aggregate_group_data():
    # 第一步：滚动查询获取符合条件的person ID
    def get_person_ids():
        start_time = time.time()
        print("开始获取符合条件的person ID...")

        query = {
            "size": 1000,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"rep_office": 11}},
                        {"term": {"rep": 10}}
                    ]
                }
            }
        }

        response = es.search(index="person", body=query, scroll="1m")
        scroll_id = response['_scroll_id']
        person_ids = [hit['_id'] for hit in response['hits']['hits']]

        while len(response['hits']['hits']) > 0:
            response = es.scroll(scroll_id=scroll_id, scroll="1m")
            scroll_id = response['_scroll_id']
            hits = response['hits']['hits']

            if hits:
                person_ids.extend([hit['_id'] for hit in hits])

        end_time = time.time()
        print(
            f"获取person ID完成，共获取到 {len(person_ids)} 个符合条件的person ID，耗时: {end_time - start_time:.2f} 秒")
        return person_ids

    # 第二步：根据person ID查询group索引并进行统计
    def aggregate_groups(person_ids_batch):
        start_time = time.time()
        print(f"开始处理批次，包含 {len(person_ids_batch)} 个person ID...")

        query = {
            "size": 0,
            "aggs": {
                "groups": {
                    "terms": {
                        "field": "group_id"
                    },
                    "aggs": {
                        "filtered_persons": {
                            "nested": {
                                "path": "person_list"
                            },
                            "aggs": {
                                "matched_persons": {
                                    "filter": {
                                        "terms": {
                                            "person_list.id": person_ids_batch
                                        }
                                    },
                                    "aggs": {
                                        "total_count": {
                                            "value_count": {
                                                "field": "person_list.id"
                                            }
                                        },
                                        "age_gt_35": {
                                            "filter": {
                                                "range": {
                                                    "person_list.age": {
                                                        "gt": 35
                                                    }
                                                }
                                            },
                                            "aggs": {
                                                "count_age_gt_35": {
                                                    "value_count": {
                                                        "field": "person_list.id"
                                                    }
                                                }
                                            }
                                        },
                                        "birthday_after_2000": {
                                            "filter": {
                                                "range": {
                                                    "person_list.birthday": {
                                                        "gt": "2000-01-01"
                                                    }
                                                }
                                            },
                                            "aggs": {
                                                "count_birthday_after_2000": {
                                                    "value_count": {
                                                        "field": "person_list.id"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        response = es.search(index="group", body=query)
        end_time = time.time()
        print(f"处理批次完成，耗时: {end_time - start_time:.2f} 秒")
        return response

    # 获取符合条件的person ID
    person_ids = get_person_ids()
    if not person_ids:
        print("没有符合条件的person记录")
        return

    # 分批处理person IDs
    batch_size = 65536  # 每批次最多65536个ID
    all_results = {}
    total_batches = (len(person_ids) + batch_size - 1) // batch_size
    batch_start_time = time.time()

    for i in range(0, len(person_ids), batch_size):
        batch = person_ids[i:i + batch_size]
        result = aggregate_groups(batch)

        # 合并结果
        for bucket in result['aggregations']['groups']['buckets']:
            group_id = bucket['key']
            if group_id not in all_results:
                all_results[group_id] = {
                    "group_name": None,
                    "total_count": 0,
                    "count_age_gt_35": 0,
                    "count_birthday_after_2000": 0
                }

            all_results[group_id]['group_name'] = bucket.get('key_as_string', 'Unknown Group')
            all_results[group_id]['total_count'] += bucket['filtered_persons']['matched_persons']['total_count'][
                'value']
            all_results[group_id]['count_age_gt_35'] += \
            bucket['filtered_persons']['matched_persons']['age_gt_35']['count_age_gt_35']['value']
            all_results[group_id]['count_birthday_after_2000'] += \
            bucket['filtered_persons']['matched_persons']['birthday_after_2000']['count_birthday_after_2000']['value']

    batch_end_time = time.time()
    print(f"所有批次处理完成，总耗时: {batch_end_time - batch_start_time:.2f} 秒")

    # 保存最终结果
    save_start_time = time.time()
    print("开始保存最终结果...")
    with open('group_statistics.json', 'w') as f:
        json.dump(all_results, f, indent=2)
    save_end_time = time.time()
    print(f"保存最终结果完成，耗时: {save_end_time - save_start_time:.2f} 秒")

def main():
    # 创建索引
    # create_indices()

    # 加载person数据
    # load_person_data(1_500_000)

    # 加载group数据
    # load_group_data(5000)

    # 查询并统计
    aggregate_group_data()

if __name__ == "__main__":
    main()