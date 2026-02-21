import json

with open("colab_launcher.ipynb", "r") as f:
    nb = json.load(f)

for cell in nb["cells"]:
    if cell["cell_type"] == "code":
        src = cell["source"]
        s = "".join(src)
        if "def push_ui_log(icon, msg):" in s:
            new_s = s.replace(
                "    else:\n        # 普通日誌\n    log_line = f\"\"\"\n",
                "    else:\n        # 普通日誌\n        log_line = f\"\"\"\n"
            ).replace(
                "    <div style='padding: 4px 0; border-bottom: 1px dashed #333;'>\n",
                "        <div style='padding: 4px 0; border-bottom: 1px dashed #333;'>\n"
            ).replace(
                "        <div style='color: #888; font-size: 0.9em;'>[{ts}] {icon}</div>\n",
                "            <div style='color: #888; font-size: 0.9em;'>[{ts}] {icon}</div>\n"
            ).replace(
                "        <div style='color: #00FF00; word-break: break-all; line-height: 1.5;'>{cleaned_msg}</div>\n",
                "            <div style='color: #00FF00; word-break: break-all; line-height: 1.5;'>{cleaned_msg}</div>\n"
            )
            
            cell["source"] = [l + "\n" for l in new_s.split("\n")][:-1]

with open("colab_launcher.ipynb", "w") as f:
    json.dump(nb, f, indent=4, ensure_ascii=False)
