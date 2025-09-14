import time
import json

from datetime import datetime, timedelta
from elasticsearch import Elasticsearch


def test_similarity_query(es_host, index_name, name, age, birthday):
    """
    执行相似度查询并测量性能
    """
    es = Elasticsearch([es_host])

    # 将生日转换为时间戳（毫秒）
    birthday_dt = datetime.strptime(birthday, "%Y-%m-%d")
    birthday_ts = int(birthday_dt.timestamp() * 1000)

    # 构建查询 - 修复所有类型转换问题
    query = {
        "query": {
            "function_score": {
                "query": {"match_all": {}},
                "functions": [
                    {
                        "script_score": {
                            "script": {
                                "source": """
                                    // 名称相似度评分 - 修复类型转换
                                    if (doc['name'].size() == 0) {
                                        return 1;
                                    }

                                    String inputName = params.input_name;
                                    String docName = doc['name'].value;

                                    // 修复：显式类型转换
                                    int common = 0;
                                    int minLen = (int) Math.min(inputName.length(), docName.length());
                                    for (int i = 0; i < minLen; i++) {
                                        if (inputName.charAt(i) == docName.charAt(i)) {
                                            common++;
                                        }
                                    }

                                    if (common > 5) return 6;
                                    else if (common > 2) return 3;
                                    else return 1;
                                """,
                                "params": {
                                    "input_name": name
                                }
                            }
                        }
                    },
                    {
                        "script_score": {
                            "script": {
                                "source": """
                                    // 年龄相似度评分
                                    if (doc['age'].size() == 0) {
                                        return 1;
                                    }

                                    int inputAge = params.input_age;
                                    int docAge = (int) doc['age'].value;
                                    int diff = (int) Math.abs(inputAge - docAge);

                                    if (diff <= 5) return 5;
                                    else if (diff <= 3) return 2;
                                    else return 1;
                                """,
                                "params": {
                                    "input_age": age
                                }
                            }
                        }
                    },
                    # 生日相似度评分脚本优化版本
                    {
                        "script_score": {
                            "script": {
                                "source": """
                                    // 生日相似度评分 - 使用推荐方法
                                    if (doc['birthday'].size() == 0) {
                                        return 1;
                                    }

                                    long inputBirthday = params.input_birthday;
                                    long docBirthday = doc['birthday'].value.toInstant().toEpochMilli();
                                    long diff = (long) Math.abs(docBirthday - inputBirthday);
                                    long daysDiff = diff / (24 * 60 * 60 * 1000);

                                    if (daysDiff < 1) return 5;
                                    else if (daysDiff < 3) return 4;
                                    else return 1;
                                """,
                                "params": {
                                    "input_birthday": birthday_ts
                                }
                            }
                        }
                    }
                ],
                "score_mode": "sum",
                "boost_mode": "replace"
            }
        },
        "sort": [
            {
                "_score": {
                    "order": "desc"
                }
            }
        ],
        "size": 10
    }

    # 执行查询并测量时间
    start_time = time.time()
    try:
        response = es.search(index=index_name, body=query)
    except Exception as e:
        print(f"错误详情: {getattr(e, 'info', '无详细信息')}")
        raise
    end_time = time.time()

    # 提取结果
    took = response.get('took', 0)  # Elasticsearch处理时间(ms)
    total_hits = response['hits']['total']['value']
    hits = response['hits']['hits']

    # 计算总耗时
    total_time_ms = (end_time - start_time) * 1000

    # 打印结果
    print(f"查询耗时: {took}ms (Elasticsearch), {total_time_ms:.2f}ms (总)")
    print(f"匹配文档数: {total_hits}")
    print("前10个结果:")

    for i, hit in enumerate(hits):
        score = hit['_score']
        source = hit['_source']
        print(
            f"  {i + 1}. 分数: {score:.2f}, 姓名: {source['name']}, 年龄: {source['age']}, 生日: {source['birthday']}")

    return {
        "es_time_ms": took,
        "total_time_ms": total_time_ms,
        "total_hits": total_hits,
        "hits": hits
    }


if __name__ == "__main__":
    ES_HOST = "http://localhost:9200"
    INDEX_NAME = "person_test"

    # 测试参数
    test_name = "张三"
    test_age = 30
    test_birthday = "1990-01-01"

    result = test_similarity_query(ES_HOST, INDEX_NAME, test_name, test_age, test_birthday)
