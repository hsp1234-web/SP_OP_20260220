import os
from pathlib import Path

def scan_drive(drive_path="/content/drive/MyDrive/QuantData/data"):
    base = Path(drive_path)
    if not base.exists():
        print(f"❌ 找不到目錄: {base}")
        return
    
    print(f"🔍 開始掃描: {base}\n")
    
    for year_dir in sorted(base.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
            
        print(f"📁 {year_dir.name} 年:")
        
        for dataset in ["TaiwanOptionTick", "TaiwanFuturesTick", "GreeksFeatures"]:
            dataset_dir = year_dir / dataset
            if dataset_dir.exists():
                files = list(dataset_dir.glob("*.parquet"))
                
                # 計算檔案大小分佈
                valid_files = [f for f in files if f.stat().st_size > 1024]
                empty_files = [f for f in files if f.stat().st_size <= 1024]
                
                if files:
                    min_date = min([f.stem.split('_')[-1] for f in files])
                    max_date = max([f.stem.split('_')[-1] for f in files])
                    print(f"    ├─ {dataset}: {len(valid_files)} 個有效檔案, {len(empty_files)} 個損毀/空殼檔案")
                    print(f"    │  (最新日期: {max_date}, 最舊日期: {min_date})")
                else:
                    print(f"    ├─ {dataset}: 0 個檔案")
            else:
                print(f"    ├─ {dataset}: 目錄不存在")
        print("    └─ ")

if __name__ == "__main__":
    scan_drive()
