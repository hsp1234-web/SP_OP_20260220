import json
import re

with open("colab_launcher.ipynb", "r", encoding="utf-8") as f:
    nb = json.load(f)

for cell in nb["cells"]:
    if cell["cell_type"] == "code":
        source = "".join(cell["source"])

        # 1. Update Parameters (Remove SKIP_GREEKS, add PIPELINE_MODE and MAX_LOG_LINES)
        old_params = """#@markdown ### ⚙️ 管線參數
LOOKBACK_DAYS = 0 #@param {type:"integer"}
SKIP_GREEKS = False #@param {type:"boolean"}"""
        new_params = """#@markdown ### 🚀 執行模式
PIPELINE_MODE = 'Full' #@param ["Full", "Download Only", "Compute Only"]
#@markdown ---
#@markdown ### ⚙️ 管線參數
LOOKBACK_DAYS = 0 #@param {type:"integer"}"""
        source = source.replace(old_params, new_params)

        old_ui = """#@markdown ### 👁️ 介面設定
LOG_FONT_SIZE = '10px' #@param {type:"string"}"""
        new_ui = """#@markdown ### 👁️ 介面設定
LOG_FONT_SIZE = '10px' #@param {type:"string"}
MAX_LOG_LINES = 15 #@param {type:"integer"}"""
        source = source.replace(old_ui, new_ui)

        # 2. Update push_ui_log
        old_push = """MAX_LINES = 15

def push_ui_log(icon, msg):
    \"\"\"內部推播函式：維持 15 行，原子替換 DOM 內容防閃爍\"\"\"
    ts = datetime.now(TZ_TPE).strftime('%H:%M:%S')

    # 單行日誌：Flexbox 左側固定時間圖示，右側自動換行對齊
    log_line = f\"\"\"
    <div style='display: flex; gap: 4px; padding: 2px 0; line-height: 1.4;'>
        <span style='flex-shrink: 0; white-space: nowrap; color: #888;'>[{ts}] {icon}</span>
        <span style='word-break: break-word;'>{msg}</span>
    </div>
    \"\"\"

    LOG_BUFFER.append(log_line)
    if len(LOG_BUFFER) > MAX_LINES:
        LOG_BUFFER.pop(0)

    # 外層容器：透明背景、6px 字體、無邊框陰影
    html_content = f\"\"\"
    <div style=\"width: 100%; height: auto; background-color: transparent; color: #00FF00;
                font-family: 'Fira Code', monospace; font-size: {LOG_FONT_SIZE}; overflow: hidden;
                padding: 6px; box-sizing: border-box;\">
        {''.join(LOG_BUFFER)}
    </div>
    \"\"\"
    dash_widget.value = html_content"""
        
        new_push = """def push_ui_log(icon, msg):
    \"\"\"內部推播函式：支援垂直分行與動態行數，過濾前綴\"\"\"
    import re
    # 移除標準日誌中的長冗長前綴 (例如: 2026-02-21 09:52:05,340 - pipeline.run_all - INFO - )
    cleaned_msg = re.sub(r'\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d+ - .*? - .*? - ', '', msg)
    
    ts = datetime.now(TZ_TPE).strftime('%H:%M:%S')

    # 手機友善垂直佈局：時間與圖示第一行，內容第二行
    log_line = f\"\"\"
    <div style='padding: 4px 0; border-bottom: 1px dashed #333;'>
        <div style='color: #888; font-size: 0.9em;'>[{ts}] {icon}</div>
        <div style='color: #00FF00; word-break: break-all; line-height: 1.5;'>{cleaned_msg}</div>
    </div>
    \"\"\"

    LOG_BUFFER.append(log_line)
    if len(LOG_BUFFER) > MAX_LOG_LINES:
        LOG_BUFFER.pop(0)

    html_content = f\"\"\"
    <div style=\"width: 100%; background-color: transparent; 
                font-family: 'Fira Code', monospace; font-size: {LOG_FONT_SIZE}; overflow: hidden;
                padding: 6px; box-sizing: border-box;\">
        {''.join(LOG_BUFFER)}
    </div>
    \"\"\"
    dash_widget.value = html_content"""
        source = source.replace(old_push, new_push)

        # 3. Handle old SKIP_GREEKS log reference
        old_log = 'log(\'🔧\', f\'Greeks: {"開" if not SKIP_GREEKS else "關"} | 分支: {BRANCH}\')'
        new_log = 'log(\'🔧\', f\'模式: {PIPELINE_MODE} | 分支: {BRANCH}\')'
        source = source.replace(old_log, new_log)
        
        old_cmd_import = 'if not SKIP_GREEKS:'
        new_cmd_import = 'if PIPELINE_MODE != "Download Only":'
        source = source.replace(old_cmd_import, new_cmd_import)

        # 4. Update PIPELINE_MODE args passing
        old_cmd = """    cmd = [
        sys.executable, 'run_all.py',
        '--start', start_date,
        '--end', end_date,
        '--env', 'colab'
    ]
    if SKIP_GREEKS:
        cmd.append('--skip-phase2')"""
        new_cmd = """    cmd = [
        sys.executable, 'run_all.py',
        '--start', start_date,
        '--end', end_date,
        '--env', 'colab'
    ]
    if PIPELINE_MODE == 'Download Only':
        cmd.append('--skip-phase2')
    elif PIPELINE_MODE == 'Compute Only':
        cmd.append('--skip-phase1')"""
        source = source.replace(old_cmd, new_cmd)
        
        # update token
        new_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0yMSAxNzoxMjowNCIsInVzZXJfaWQiOiJmaW5taW5kaHNwIiwiaXAiOiIxMTQuNDcuMTk0LjEzIiwiZXhwIjoxNzcyMjY5OTI0fQ.JifzBdSLguuJHBRgEe2VIdl9DhjImReqYx0yJVlnHd0"
        source = re.sub(r"FINMIND_API_TOKEN = '.*?'", f"FINMIND_API_TOKEN = '{new_token}'", source)

        cell["source"] = [line + '\n' for line in source.split('\n')]
        if cell["source"][-1] == '\n':
            cell["source"] = cell["source"][:-1]
        else:
            cell["source"][-1] = cell["source"][-1].rstrip('\n')

with open("colab_launcher.ipynb", "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=4, ensure_ascii=False)
    f.write('\n')
