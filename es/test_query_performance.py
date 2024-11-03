import time

from elasticsearch import Elasticsearch

# 连接到 Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

# 检查连接是否成功
if not es.ping():
    raise ValueError("Connection failed")

# 定义查询
query = {
    "query": {
        "nested": {
            "path": "division_desc",
            "query": {
                "bool": {
                    "must": [
                        {
                            "script": {
                                "script": {
                                    "source": """
                                    // 获取 division_desc.region 字段的值
                                    def division_region = doc['division_desc.region'];
                                    def regions = params.region;
                                    
                                    if(!division_region.isEmpty()&&!division_region.contains('All')){
                                       if(regions==null||regions.isEmpty()){
                                         return false;
                                       }
                                       for(def item:regions){
                                         if(!division_region.contains(item)){
                                           return false;
                                         }
                                       }
                                    }
                                    
                                    // 获取 division_desc.repOffice 字段的值
                                    def division_rep_office = doc['division_desc.repOffice'];
                                    def repOffice = params.repOffice;
                                    
                                    if(!division_rep_office.isEmpty()&&!division_rep_office.contains('All')){
                                       if(repOffice==null||repOffice.isEmpty()){
                                         return false;
                                       }
                                       for(def item:repOffice){
                                         if(!division_rep_office.contains(item)){
                                           return false;
                                         }
                                       }
                                    }
                                    return true;         
                                    """,
                                    "params": {
                                        "region": [
                                            "1002",
                                            "1004",
                                            "a1"
                                        ],
                                        "repOffice": [
                                            "2009",
                                            "2009",
                                            "2009",
                                            "2009",
                                            "2009",
                                            "All",
                                            "2009",
                                            "2009",
                                            "2010",
                                            "1000000"
                                        ]
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }
    }
}

# 记录开始时间
start_time = time.time()

# 执行查询
response = es.search(index='test_index', body=query)

# 记录结束时间
end_time = time.time()

# 输出查询结果和性能指标
print(f"Query took {end_time - start_time:.2f} seconds")
print(f"Total hits: {response['hits']['total']['value']}")
