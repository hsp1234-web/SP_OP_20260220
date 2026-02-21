"""
test_parallel_download.py — 並行 API 下載效能測試

測試 FinMind API 在不同併發數下的表現：
  - 30 workers
  - 60 workers

使用 20 個交易日 × 2 datasets (TXO + TX) = 40 API calls per round
測量: 成功數, 429 數, 失敗數, 平均回應時間, 總耗時
"""

import os
import sys
import time
import json
import requests
from pathlib import Path
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# 設定台北時區
TZ_TAIPEI = timezone(timedelta(hours=8))

# 載入 .env
PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_DIR.parent))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_DIR / ".env")
except ImportError:
    # 手動讀取 .env
    env_file = PROJECT_DIR / ".env"
    if env_file.exists():
        for line in env_file.read_text().strip().splitlines():
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                os.environ.setdefault(k.strip(), v.strip())

API_TOKEN = os.getenv("FINMIND_API_TOKEN", "")
API_URL = "https://api.finmindtrade.com/api/v4/data"

# 測試用交易日 (近期已確定有資料的日期)
TEST_DATES = [
    "2025-06-19", "2025-06-18", "2025-06-17", "2025-06-16", "2025-06-13",
    "2025-06-12", "2025-06-11", "2025-06-10", "2025-06-09", "2025-06-06",
    "2025-06-05", "2025-06-04", "2025-06-03", "2025-06-02", "2025-05-29",
    "2025-05-28", "2025-05-27", "2025-05-26", "2025-05-23", "2025-05-22",
]

DATASETS = [
    ("TaiwanOptionTick", "TXO"),
    ("TaiwanFuturesTick", "TX"),
]


def single_api_call(dataset: str, data_id: str, date: str) -> dict:
    """發送單一 API 請求，回傳結果統計"""
    params = {
        "dataset": dataset,
        "data_id": data_id,
        "start_date": date,
        "end_date": date,
    }
    if API_TOKEN:
        params["token"] = API_TOKEN

    start = time.perf_counter()
    try:
        resp = requests.get(API_URL, params=params, timeout=30)
        latency_ms = (time.perf_counter() - start) * 1000

        status_code = resp.status_code
        try:
            body = resp.json()
            data_count = len(body.get("data", []))
            msg = body.get("msg", "")
        except ValueError:
            data_count = 0
            msg = "JSON parse error"

        return {
            "date": date,
            "dataset": dataset,
            "data_id": data_id,
            "status_code": status_code,
            "data_count": data_count,
            "latency_ms": round(latency_ms, 1),
            "msg": msg,
            "error": None,
        }
    except Exception as e:
        latency_ms = (time.perf_counter() - start) * 1000
        return {
            "date": date,
            "dataset": dataset,
            "data_id": data_id,
            "status_code": -1,
            "data_count": 0,
            "latency_ms": round(latency_ms, 1),
            "msg": "",
            "error": str(e),
        }


def build_task_list(dates: list) -> list:
    """建立所有 API 請求任務"""
    tasks = []
    for date in dates:
        for dataset, data_id in DATASETS:
            tasks.append((dataset, data_id, date))
    return tasks


def run_parallel_test(workers: int, tasks: list) -> dict:
    """執行並行下載測試"""
    now = datetime.now(TZ_TAIPEI).strftime("%H:%M:%S")
    print(f"\n{'='*60}")
    print(f"  🧪 測試: {workers} 併發 × {len(tasks)} API calls")
    print(f"  ⏰ 開始: {now}")
    print(f"{'='*60}")

    results = []
    start_total = time.perf_counter()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(single_api_call, ds, did, dt): (ds, did, dt)
            for ds, did, dt in tasks
        }
        
        for future in as_completed(future_map):
            result = future.result()
            results.append(result)
            
            # 簡短進度
            icon = "✅" if result["status_code"] == 200 and result["data_count"] > 0 else \
                   "🔸" if result["status_code"] == 200 else \
                   "🧊" if result["status_code"] == 429 else "❌"
            print(f"  {icon} {result['date']} {result['data_id']:3s} | "
                  f"HTTP {result['status_code']} | "
                  f"{result['data_count']:>6,} 筆 | "
                  f"{result['latency_ms']:>7.0f}ms"
                  f"{' | ' + result['msg'][:40] if result['status_code'] != 200 else ''}")

    total_sec = time.perf_counter() - start_total

    # 統計
    success = sum(1 for r in results if r["status_code"] == 200 and r["data_count"] > 0)
    empty = sum(1 for r in results if r["status_code"] == 200 and r["data_count"] == 0)
    code_429 = sum(1 for r in results if r["status_code"] == 429)
    errors = sum(1 for r in results if r["status_code"] not in (200, 429))
    avg_latency = sum(r["latency_ms"] for r in results) / len(results) if results else 0
    min_latency = min(r["latency_ms"] for r in results) if results else 0
    max_latency = max(r["latency_ms"] for r in results) if results else 0

    # 檢查 429 的 msg
    msgs_429 = set(r["msg"][:60] for r in results if r["status_code"] == 429)

    stats = {
        "workers": workers,
        "total_requests": len(tasks),
        "success": success,
        "empty": empty,
        "code_429": code_429,
        "errors": errors,
        "total_sec": round(total_sec, 2),
        "avg_latency_ms": round(avg_latency, 1),
        "min_latency_ms": round(min_latency, 1),
        "max_latency_ms": round(max_latency, 1),
        "throughput_per_sec": round(len(tasks) / total_sec, 2),
    }

    print(f"\n  📊 結果統計 ({workers} 併發):")
    print(f"  ├── 成功:       {success}/{len(tasks)}")
    print(f"  ├── 空資料:     {empty}")
    print(f"  ├── 429 限速:   {code_429}")
    if msgs_429:
        for m in msgs_429:
            print(f"  │   └── {m}")
    print(f"  ├── 其他錯誤:   {errors}")
    print(f"  ├── 總耗時:     {total_sec:.2f}s")
    print(f"  ├── 平均延遲:   {avg_latency:.0f}ms")
    print(f"  ├── 最快/最慢:  {min_latency:.0f}ms / {max_latency:.0f}ms")
    print(f"  └── 吞吐量:     {stats['throughput_per_sec']:.1f} req/s")

    return stats


def main():
    now = datetime.now(TZ_TAIPEI).strftime("%Y-%m-%d %H:%M:%S")
    print(f"╔{'═'*58}╗")
    print(f"║  FinMind 並行下載效能測試                                ║")
    print(f"║  {now}                                  ║")
    print(f"╚{'═'*58}╝")
    print(f"\n  API Token: {'已設定 (' + API_TOKEN[:8] + '...)' if API_TOKEN else '未設定 (匿名)'}")
    print(f"  測試日期數: {len(TEST_DATES)} 天")
    print(f"  Datasets: {', '.join(d[0] for d in DATASETS)}")
    print(f"  每輪 API calls: {len(TEST_DATES) * len(DATASETS)}")

    tasks = build_task_list(TEST_DATES)

    # ── Round 1: 30 併發 ──
    stats_30 = run_parallel_test(30, tasks)

    # 冷卻 5 秒，避免兩輪之間互相影響
    print(f"\n  ⏳ 冷卻 5 秒...")
    time.sleep(5)

    # ── Round 2: 60 併發 ──
    stats_60 = run_parallel_test(60, tasks)

    # ── 總結比較 ──
    print(f"\n{'━'*60}")
    print(f"  📋 總結比較")
    print(f"{'━'*60}")
    print(f"  {'指標':<20} {'30 併發':>15} {'60 併發':>15}")
    print(f"  {'─'*50}")
    print(f"  {'成功':<20} {stats_30['success']:>15} {stats_60['success']:>15}")
    print(f"  {'429 限速':<20} {stats_30['code_429']:>15} {stats_60['code_429']:>15}")
    print(f"  {'總耗時':<20} {str(stats_30['total_sec'])+'s':>15} {str(stats_60['total_sec'])+'s':>15}")
    print(f"  {'平均延遲':<18} {str(stats_30['avg_latency_ms'])+'ms':>15} {str(stats_60['avg_latency_ms'])+'ms':>15}")
    print(f"  {'吞吐量':<18} {str(stats_30['throughput_per_sec'])+' r/s':>15} {str(stats_60['throughput_per_sec'])+' r/s':>15}")
    print(f"\n  {'🏆 結論':}")
    
    if stats_30['code_429'] == 0 and stats_60['code_429'] == 0:
        better = 60 if stats_60['total_sec'] < stats_30['total_sec'] else 30
        print(f"  兩種都沒有 429，{better} 併發更快。可安全使用 60 併發。")
    elif stats_60['code_429'] > stats_30['code_429']:
        print(f"  60 併發觸發更多 429 ({stats_60['code_429']} vs {stats_30['code_429']})。")
        print(f"  建議使用 30 併發或適當降低。")
    else:
        print(f"  30 併發已觸發 429。需要更保守的策略。")


if __name__ == "__main__":
    if not API_TOKEN:
        print("⚠️  未設定 FINMIND_API_TOKEN，將使用匿名模式 (額度有限)")
        print("   請在 .env 檔案中設定 Token 後再執行")
        resp = input("   繼續執行嗎? (y/N): ")
        if resp.lower() != 'y':
            sys.exit(0)
    main()
