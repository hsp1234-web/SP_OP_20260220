import json

with open("colab_launcher.ipynb", "r") as f:
    nb = json.load(f)

for cell in nb["cells"]:
    if cell["cell_type"] == "code":
        src = cell["source"]
        new_src = []
        for line in src:
            new_src.append(line)
        
        # We need to find the definition of push_ui_log and replace it
        s = "".join(src)
        if "def push_ui_log(icon, msg):" in s:
            new_s = s.replace(
                "LOG_BUFFER = []\ndef push_ui_log(icon, msg):\n",
                "LOG_BUFFER = []\nPROGRESS_BAR_HTML = ''\ndef push_ui_log(icon, msg):\n    global PROGRESS_BAR_HTML\n"
            ).replace(
                "    ts = datetime.now(TZ_TPE).strftime('%H:%M:%S')\n\n    # 手機友善垂直佈局：時間與圖示第一行，內容第二行\n",
                "    ts = datetime.now(TZ_TPE).strftime('%H:%M:%S')\n\n    if '[P1 下載進度]' in cleaned_msg:\n        PROGRESS_BAR_HTML = f\"\"\"<div style='margin-top: 10px; padding: 6px; border-top: 2px dashed #00FF00; background: #001100;'><div style='color: #00FF00; font-weight: bold; line-height: 1.5;'>🚀 {cleaned_msg}</div></div>\"\"\"\n    else:\n        # 普通日誌\n"
            ).replace(
                "        <div style='color: #00FF00; word-break: break-all; line-height: 1.5;'>{cleaned_msg}</div>\n    </div>\n    \"\"\"\n\n    LOG_BUFFER.append(log_line)\n",
                "        <div style='color: #00FF00; word-break: break-all; line-height: 1.5;'>{cleaned_msg}</div>\n        </div>\n        \"\"\"\n        LOG_BUFFER.append(log_line)\n"
            ).replace(
                "        {''.join(LOG_BUFFER)}\n    </div>\n",
                "        {''.join(LOG_BUFFER)}\n        {PROGRESS_BAR_HTML}\n    </div>\n"
            )
            
            # Reconstruct lines
            cell["source"] = [l + "\n" for l in new_s.split("\n")][:-1]
            if not cell["source"][-1].endswith("\n"):
                pass

with open("colab_launcher.ipynb", "w") as f:
    json.dump(nb, f, indent=4, ensure_ascii=False)
