from re import error
import docker
import requests
from concurrent.futures import ThreadPoolExecutor
import time
import random
import subprocess
import pytest

SERVERS = ["backend1", "backend2", "backend3"]
LB_URL = "http://127.0.0.1:3490"
LB_SERVICE_NAME = "lb"


def fetch_node_id(_):
    try:
        res = requests.get(LB_URL, timeout=2.0)
        if "X-Backend-Id" in res.headers:
            return res.headers["X-Backend-Id"]
        return res.text.splitlines()[0]
    except Exception as e:
        return f"error: {e}"


def send_request(_):
    try:
        res = requests.get(LB_URL, timeout=2.5)
        return {
            "status": "ok",
            "node": res.headers.get("X-Backend-Id", res.text.splitlines()[0]),
        }
    except Exception as e:
        return {"status": "error", "error": type(e).__name__}


def test_leas_connection():
    TOTAL_REQUESTS = 10000
    CONCURENT_WORKERS = 100

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


@pytest.fixture(autouse=True)
def ensure_all_backends_alive():
    """Перед каждым тестом и после него гарантируем, что все контейнеры запущены"""
    subprocess.run(["docker", "compose", "start"] + SERVERS, check=True)
    time.sleep(1.0)  # Даем 1 сек на прогрев портов
    yield
    subprocess.run(["docker", "compose", "start"] + SERVERS, check=True)


def test_chaos_node_failure():
    """Тушим один бэкенд прямо во время активного трафика"""
    TOTAL = 300
    WORKERS = 15

    # Функция-диверсант: ждет 0.1 сек и глушит бэкенд
    def killer():
        time.sleep(0.1)
        subprocess.run(["docker", "compose", "kill", "backend1"], check=True)

    with ThreadPoolExecutor(max_workers=WORKERS + 1) as pool:
        kill_future = pool.submit(killer)
        results = list(pool.map(send_request, range(TOTAL)))
        kill_future.result()

    successful_nodes = {r["node"] for r in results if r["status"] == "ok"}
    success_count = sum(1 for r in results if r["status"] == "ok")

    print(f"\n[Успешно]: {success_count}/{TOTAL}. Живые ноды: {successful_nodes}")
    assert (success_count / TOTAL) > 0.85, (
        f"Слишком много потерь при падении ноды: {success_count}/{TOTAL}"
    )
