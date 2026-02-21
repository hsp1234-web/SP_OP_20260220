import os
import time
from pathlib import Path
from datetime import datetime

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

class ConsoleColors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def format_size(size_bytes):
    if size_bytes == 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int((len(str(size_bytes)) - 1) / 3)
    p = pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

def print_banner():
    print(f"{ConsoleColors.OKCYAN}{ConsoleColors.BOLD}")
    print("=" * 85)
    print(" 🚀 QuantDataPipeline - Data Health Check & Real-time Parsing Dashboard")
    print("=" * 85)
    print(f"{ConsoleColors.ENDC}")

def scan_and_report(drive_path="/content/drive/MyDrive/QuantData/data"):
    base = Path(drive_path)
    if not base.exists():
        print(f"{ConsoleColors.FAIL}❌ 無法存取資料目錄: {base}{ConsoleColors.ENDC}")
        print(f"{ConsoleColors.WARNING}請確認您是否已經掛載 Google Drive，或者指定的路徑是否正確。{ConsoleColors.ENDC}")
        return

    print_banner()
    print(f"📂 目標資料夾: {ConsoleColors.UNDERLINE}{base}{ConsoleColors.ENDC}")
    print(f"🕒 掃描時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if not HAS_POLARS:
        print(f"{ConsoleColors.WARNING}⚠️ 未偵測到 Polars 套件，將僅進行基本檔案掃描 (無效能解析)。{ConsoleColors.ENDC}")
    print("-" * 85)
    
    total_valid_files = 0
    total_corrupted = 0
    total_size_bytes = 0
    total_rows = 0
    
    datasets = ["TaiwanOptionTick", "TaiwanFuturesTick", "GreeksFeatures"]
    
    # 掃描結構
    years = sorted([d for d in base.iterdir() if d.is_dir() and d.name.isdigit()])
    
    if not years:
        print(f"{ConsoleColors.WARNING}⚠️ 目錄中未發現年份資料夾！{ConsoleColors.ENDC}")
        return

    for year_dir in years:
        year = year_dir.name
        print(f"\n{ConsoleColors.BOLD}📅 {year} 年資料分布{ConsoleColors.ENDC}")
        
        for dataset in datasets:
            dataset_dir = year_dir / dataset
            if dataset_dir.exists():
                files = list(dataset_dir.glob("*.parquet"))
                
                valid_files = [f for f in files if f.stat().st_size > 1024]
                empty_files = [f for f in files if f.stat().st_size <= 1024]
                
                dataset_size = sum(f.stat().st_size for f in valid_files)
                dataset_rows = 0
                
                # 利用 Polars 進行「真實解析」獲取總筆數
                if HAS_POLARS and valid_files:
                    try:
                        # 使用 scan_parquet 僅抓取 metadata 層級的筆數，速度極快
                        df_scan = pl.scan_parquet(str(dataset_dir / "*.parquet"))
                        dataset_rows = df_scan.select(pl.len()).collect().item()
                    except Exception:
                        # 若 wildcard 失敗，改用個別加總 (保險)
                        for f in valid_files:
                            try:
                                dataset_rows += pl.scan_parquet(str(f)).select(pl.len()).collect().item()
                            except Exception:
                                pass
                
                total_size_bytes += dataset_size
                total_valid_files += len(valid_files)
                total_corrupted += len(empty_files)
                total_rows += dataset_rows
                
                if files:
                    min_date = min([f.stem.split('_')[-1] for f in valid_files]) if valid_files else "N/A"
                    max_date = max([f.stem.split('_')[-1] for f in valid_files]) if valid_files else "N/A"
                    
                    status_color = ConsoleColors.OKGREEN if len(valid_files) > 200 else ConsoleColors.WARNING
                    corrupt_warn = f"{ConsoleColors.FAIL}(破損: {len(empty_files)}){ConsoleColors.ENDC}" if empty_files else ""
                    
                    row_str = f" | {dataset_rows:>10,} 筆" if HAS_POLARS else ""
                    print(f"  ├─ {ConsoleColors.OKBLUE}{dataset:<18}{ConsoleColors.ENDC} | {status_color}{len(valid_files):>3} 檔{ConsoleColors.ENDC}{row_str} | 大小: {format_size(dataset_size):>8} | {min_date} ~ {max_date} {corrupt_warn}")
                else:
                    print(f"  ├─ {ConsoleColors.FAIL}{dataset:<18}{ConsoleColors.ENDC} |   0 檔 (無內容)")
            else:
                print(f"  ├─ {ConsoleColors.WARNING}{dataset:<18}{ConsoleColors.ENDC} | 目錄不存在")
        print("  └─" + "-"*80)

    print("\n" + "=" * 85)
    print(f"{ConsoleColors.BOLD}💡 整體健康度總結 (Health Summary){ConsoleColors.ENDC}")
    print(f"  📌 總有效檔案數: {ConsoleColors.OKGREEN}{total_valid_files:,} 個{ConsoleColors.ENDC}")
    if HAS_POLARS:
        print(f"  📈 總數據載入筆數: {ConsoleColors.OKCYAN}{total_rows:,} 筆 (含特徵對齊後資料){ConsoleColors.ENDC}")
    print(f"  ⚠️ 總損毀破口數: {ConsoleColors.FAIL}{total_corrupted:,} 個{ConsoleColors.ENDC}")
    print(f"  💾 總占用空間量: {ConsoleColors.OKBLUE}{format_size(total_size_bytes)}{ConsoleColors.ENDC}")
    print("=" * 85)
    print("📋 建議: 若「筆數」遠低於預期，可能是下載不完全；若出現「破損」，請刪除該檔案並重啟管線。")
    print("=" * 85)

if __name__ == "__main__":
    scan_and_report()
