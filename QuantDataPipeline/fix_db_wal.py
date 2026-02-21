import os
from pathlib import Path

def cleanup_wal(db_path_str):
    p = Path(db_path_str)
    for ext in ["", "-wal", "-shm"]:
        f = p.parent / (p.name + ext)
        if f.exists():
            print(f"Removing {f}")
            f.unlink()

cleanup_wal("/content/drive/MyDrive/QuantData/status.db")
cleanup_wal("/content/local_data/status.db")
