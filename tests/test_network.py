from re import error
import docker
import requests
from concurrent.futures import ThreadPoolExecutor
import time

LB_URL = "http://localhost:3490"
LB_SERVICE_NAME = "lb"


def fetch_node_id(_):
    try:
        res = requests.get(LB_URL, timeout=2.0)
        if "X-Backend-Id" in res.headers:
            return res.headers["X-Backend-Id"]
        return res.text.splitlines()[0]
    except Exception as e:
        return f"error: {e}"


def test_leas_connection():
    TOTAL_REQUESTS = 1000
    CONCURENT_WORKERS = 50

    with ThreadPoolExecutor(max_workers=CONCURENT_WORKERS) as executor:
        results = list(executor.map(fetch_node_id, range(TOTAL_REQUESTS)))

    errors = [r for r in results if r.startswith("ERROR")]
    assert len(errors) == 0, f"Были сетевые ошибки: {errors}"

    node_counts = {}
    for node in results:
        node_counts[node] = node_counts.get(node, 0) + 1

    print(f"\n[Распределение нагрузки]: {node_counts}")

    assert len(node_counts) >= 2, f"Все запросы ушли в одну ноду: {node_counts}"

    for node, count in node_counts.items():
        assert count < TOTAL_REQUESTS, f"Нода {node} монополизировала трафик"
