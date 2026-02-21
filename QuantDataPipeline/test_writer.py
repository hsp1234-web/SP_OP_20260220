from core.config import DATA_DIR as c_dir
import storage.parquet_writer as pw

print(f"Config dir at load: {c_dir}")
print(f"Writer dir at load: {pw.DATA_DIR}")

import core.config
core.config.DATA_DIR = "NEW_TEST_DIR"

print(f"Config dir after change: {core.config.DATA_DIR}")
print(f"Writer dir after change: {pw.DATA_DIR}")
