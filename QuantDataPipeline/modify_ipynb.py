import json
import os
import re

with open("colab_launcher.ipynb", "r", encoding="utf-8") as f:
    nb = json.load(f)

# Find the code cell
cell = None
for c in nb["cells"]:
    if c["cell_type"] == "code":
        cell = c
        break

if not cell:
    print("Code cell not found")
    exit(1)

source = "".join(cell["source"])

# Replace LOG_FONT_SIZE
old_sync = """#@markdown ### 💾 儲存與 Drive 同步
SYNC_TO_DRIVE = True #@param {type:"boolean"}
DRIVE_PATH = '/content/drive/MyDrive/QuantData' #@param {type:"string"}
RESTORE_FROM_DRIVE = False #@param {type:"boolean"}
CLEANUP_AFTER_SYNC = True #@param {type:"boolean"}

# ═══════════════════════════════════════════════════════════════"""

new_sync = """#@markdown ### 💾 儲存與 Drive 同步
SYNC_TO_DRIVE = True #@param {type:"boolean"}
DRIVE_PATH = '/content/drive/MyDrive/QuantData' #@param {type:"string"}
RESTORE_FROM_DRIVE = False #@param {type:"boolean"}
CLEANUP_AFTER_SYNC = True #@param {type:"boolean"}
#@markdown ---
#@markdown ### 👁️ 介面設定
LOG_FONT_SIZE = '10px' #@param {type:"string"}

# ═══════════════════════════════════════════════════════════════"""
source = source.replace(old_sync, new_sync)

# Replace 6px font
old_font = """    <div style="width: 100%; height: auto; background-color: transparent; color: #00FF00;
                font-family: 'Fira Code', monospace; font-size: 6px; overflow: hidden;
                padding: 6px; box-sizing: border-box;">
        {''.join(LOG_BUFFER)}
    </div>"""

new_font = """    <div style="width: 100%; height: auto; background-color: transparent; color: #00FF00;
                font-family: 'Fira Code', monospace; font-size: {LOG_FONT_SIZE}; overflow: hidden;
                padding: 6px; box-sizing: border-box;">
        {''.join(LOG_BUFFER)}
    </div>"""
source = source.replace(old_font, new_font)

# Hardware specs
old_header = """header('🚀 QuantDataPipeline (V2 批次架構) 啟動中')
log('📅', f'範圍: {lookback_label}')
log('🔧', f'Greeks: {"開" if not SKIP_GREEKS else "關"} | 分支: {BRANCH}')"""

new_header = """header('🚀 QuantDataPipeline (V2 批次架構) 啟動中')
log('📅', f'範圍: {lookback_label}')
log('🔧', f'Greeks: {"開" if not SKIP_GREEKS else "關"} | 分支: {BRANCH}')

# 硬體偵測
try:
    import psutil
    cores = multiprocessing.cpu_count()
    ram_gb = round(psutil.virtual_memory().total / (1024**3), 1)
    log('💻', f'硬體規格: {cores} 核心, {ram_gb} GB RAM')
except Exception:
    log('💻', f'硬體規格: {multiprocessing.cpu_count()} 核心')"""
source = source.replace(old_header, new_header)

# Move execution_logs creation
before = """# ── SQLite 佇列日誌執行緒 ──
log_queue = queue.Queue()
_init_conn = sqlite3.connect(DB_PATH)
_init_conn.execute("CREATE TABLE IF NOT EXISTS execution_logs (timestamp TEXT, icon TEXT, message TEXT)")
_init_conn.commit()
_init_conn.close()

def sqlite_log_worker():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    while True:
        item = log_queue.get()
        if item is None: break
        conn.execute("INSERT INTO execution_logs VALUES (?, ?, ?)", item)
        conn.commit()
        log_queue.task_done()

threading.Thread(target=sqlite_log_worker, daemon=True).start()
log('📝', 'SQLite 日誌系統啟動完畢 (佇列模式)')

if SYNC_TO_DRIVE and drive_data:
    drive_db = drive_data / 'status.db'
    if drive_db.exists():
        shutil.copy2(drive_db, DB_PATH)
        log('📋', '已從 Drive 還原狀態庫 (status.db)')"""

after = """# ── Drive 還原 ──
if SYNC_TO_DRIVE and drive_data:
    drive_db = drive_data / 'status.db'
    if drive_db.exists():
        try:
            shutil.copy2(drive_db, DB_PATH)
            log('📋', '已從 Drive 還原狀態庫 (status.db)')
        except Exception as e:
            log('⚠️', f'Drive 還原失敗: {e}')

# ── SQLite 佇列日誌執行緒 (移至 Drive 還原之後) ──
log_queue = queue.Queue()
_init_conn = sqlite3.connect(DB_PATH)
_init_conn.execute("CREATE TABLE IF NOT EXISTS execution_logs (timestamp TEXT, icon TEXT, message TEXT)")
_init_conn.commit()
_init_conn.close()

def sqlite_log_worker():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    while True:
        item = log_queue.get()
        if item is None: break
        try:
            conn.execute("INSERT INTO execution_logs VALUES (?, ?, ?)", item)
            conn.commit()
        except Exception:
            pass
        log_queue.task_done()

threading.Thread(target=sqlite_log_worker, daemon=True).start()
log('📝', 'SQLite 日誌系統啟動完畢 (佇列模式)')"""
source = source.replace(before, after)

# Replace everything after sys.exit(0)
old_end_marker = "if not all_dates:\n    sys.exit(0)\n\n"
parts = source.split(old_end_marker)

if len(parts) > 1:
    new_end = r"""try:
    log('🏃', '正在呼叫內部管線 run_all.py ...')
    cmd = [
        sys.executable, 'run_all.py',
        '--start', start_date,
        '--end', end_date,
        '--env', 'colab'
    ]
    if SKIP_GREEKS:
        cmd.append('--skip-phase2')
    
    env_vars = os.environ.copy()
    env_vars['DOWNLOAD_WORKERS'] = str(DOWNLOAD_WORKERS)
    env_vars['GREEKS_WORKERS'] = str(GREEKS_WORKERS)
    env_vars['SYNC_TO_DRIVE'] = str(SYNC_TO_DRIVE)
    env_vars['RESTORE_FROM_DRIVE'] = str(RESTORE_FROM_DRIVE)
    env_vars['CLEANUP_AFTER_SYNC'] = str(CLEANUP_AFTER_SYNC)

    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env_vars)
    for line in iter(process.stdout.readline, ''):
        line = line.strip()
        if line: log('⚙️', line)
    
    process.stdout.close()
    ret = process.wait()
    if ret != 0:
        log('❌', f'run_all.py 執行失敗，回傳碼 {ret}')
    else:
        elapsed_min = round((time.time() - pipeline_start_time) / 60, 1)
        header('🏁 管線執行完畢')
        log('📊', f'總耗時:~{elapsed_min}m')
        
    if 'log_queue' in globals(): log_queue.put(None)

except KeyboardInterrupt:
    emergency_backup()
except Exception as e:
    log('💥', f'管線異常中斷: {e}')
    emergency_backup()"""

    source = parts[0] + old_end_marker + new_end

# update token
new_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0yMSAxNzoxMjowNCIsInVzZXJfaWQiOiJmaW5taW5kaHNwIiwiaXAiOiIxMTQuNDcuMTk0LjEzIiwiZXhwIjoxNzcyMjY5OTI0fQ.JifzBdSLguuJHBRgEe2VIdl9DhjImReqYx0yJVlnHd0"
source = re.sub(r"FINMIND_API_TOKEN = '.*?'", f"FINMIND_API_TOKEN = '{new_token}'", source)

lines = source.splitlines(True)
cell["source"] = lines

with open("colab_launcher.ipynb", "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=4, ensure_ascii=False)
    f.write('\n')
