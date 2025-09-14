import requests
import json
import time
from datetime import datetime


def monitor_cluster(es_host, interval=5, duration=60):
    """
    监控Elasticsearch集群性能指标
    """
    print(f"开始监控集群，间隔: {interval}秒，持续时间: {duration}秒")
    print("时间戳,状态,节点数,分片数,未分配分片,CPU使用率%,堆内存使用率%,查询延迟(ms)")

    end_time = time.time() + duration
    headers = {'Content-Type': 'application/json'}

    while time.time() < end_time:
        try:
            # 获取集群健康状态
            health_url = f"{es_host}/_cluster/health"
            health_response = requests.get(health_url, headers=headers)
            health_data = health_response.json()

            # 获取节点状态
            nodes_url = f"{es_host}/_nodes/stats"
            nodes_response = requests.get(nodes_url, headers=headers)
            nodes_data = nodes_response.json()

            # 提取关键指标
            status = health_data.get('status', 'unknown')
            node_count = health_data.get('number_of_nodes', 0)
            shard_count = health_data.get('active_shards', 0)
            unassigned_shards = health_data.get('unassigned_shards', 0)

            # 提取CPU和内存使用率
            cpu_percent = 0
            heap_percent = 0
            for node_id, node_info in nodes_data.get('nodes', {}).items():
                cpu_percent = node_info.get('os', {}).get('cpu', {}).get('percent', 0)
                heap_percent = node_info.get('jvm', {}).get('mem', {}).get('heap_used_percent', 0)
                break  # 只取第一个节点的数据

            # 获取查询延迟（如果有）
            search_latency = 0
            if 'indices' in nodes_data.get('nodes', {}).get(next(iter(nodes_data.get('nodes', {}))), {}):
                search_stats = nodes_data['nodes'][next(iter(nodes_data['nodes']))]['indices']['search']
                if search_stats['query_total'] > 0:
                    search_latency = search_stats['query_time_in_millis'] / search_stats['query_total']

            # 打印监控数据
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(
                f"{timestamp},{status},{node_count},{shard_count},{unassigned_shards},{cpu_percent},{heap_percent},{search_latency:.2f}")

            time.sleep(interval)

        except Exception as e:
            print(f"监控出错: {e}")
            time.sleep(interval)


def get_detailed_cluster_stats(es_host):
    """
    获取详细的集群统计信息
    """
    headers = {'Content-Type': 'application/json'}

    print("=== 集群详细统计 ===")

    # 集群健康
    health_url = f"{es_host}/_cluster/health"
    health_response = requests.get(health_url, headers=headers)
    health_data = health_response.json()
    print(f"集群状态: {health_data.get('status')}")
    print(f"节点数: {health_data.get('number_of_nodes')}")
    print(f"数据节点数: {health_data.get('number_of_data_nodes')}")
    print(
        f"活动分片: {health_data.get('active_primary_shards')} (主) + {health_data.get('active_shards') - health_data.get('active_primary_shards')} (副)")
    print(f"未分配分片: {health_data.get('unassigned_shards')}")

    # 节点统计
    nodes_url = f"{es_host}/_nodes/stats"
    nodes_response = requests.get(nodes_url, headers=headers)
    nodes_data = nodes_response.json()

    print("\n=== 节点统计 ===")
    for node_id, node_info in nodes_data.get('nodes', {}).items():
        print(f"节点 {node_info.get('name')}:")
        print(f"  CPU使用率: {node_info.get('os', {}).get('cpu', {}).get('percent')}%")
        print(f"  堆内存使用: {node_info.get('jvm', {}).get('mem', {}).get('heap_used_percent')}%")
        print(
            f"  磁盘使用: {node_info.get('fs', {}).get('total', {}).get('available_in_bytes') / (1024 ** 3):.2f} GB 可用")

    # 索引统计
    indices_url = f"{es_host}/_stats"
    indices_response = requests.get(indices_url, headers=headers)
    indices_data = indices_response.json()

    print("\n=== 索引统计 ===")
    for index_name, index_stats in indices_data.get('indices', {}).items():
        print(f"索引 {index_name}:")
        print(f"  文档数: {index_stats.get('primaries', {}).get('docs', {}).get('count')}")
        print(f"  大小: {index_stats.get('primaries', {}).get('store', {}).get('size_in_bytes') / (1024 ** 2):.2f} MB")
        print(f"  查询次数: {index_stats.get('primaries', {}).get('search', {}).get('query_total')}")


if __name__ == "__main__":
    ES_HOST = "http://localhost:9200"

    # 获取一次详细统计
    get_detailed_cluster_stats(ES_HOST)

    # 开始实时监控（运行60秒，每5秒更新一次）
    monitor_cluster(ES_HOST, interval=5, duration=60)