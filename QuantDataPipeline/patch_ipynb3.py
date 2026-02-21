import json

file_path = '/media/sp/D/SP_OP_20260220/QuantDataPipeline/colab_launcher.ipynb'
try:
    with open(file_path, 'r', encoding='utf-8') as f:
        nb = json.load(f)

    for cell in nb.get('cells', []):
        if cell.get('cell_type') == 'code':
            source = cell.get('source', [])
            
            # Change branch from 7 to 8
            for i in range(len(source)):
                if "BRANCH = '7'" in source[i]:
                    source[i] = source[i].replace("BRANCH = '7'", "BRANCH = '8'")
            
            # Remove Drive DB logic, we no longer need status.db restoration
            start_idx, end_idx = -1, -1
            for i, line in enumerate(source):
                if "# ── Drive 還原 ──" in line:
                    start_idx = i
                # Look for the end of the block
                if start_idx != -1 and i > start_idx and ("# ── SQLite 佇列日誌執行緒" in line or "# ── SQLite" in line):
                    end_idx = i
                    break
            
            if start_idx != -1 and end_idx != -1:
                del source[start_idx:end_idx]

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(nb, f, indent=4, ensure_ascii=False)
    
    print("Patch OK")
except Exception as e:
    print(f"Error: {e}")
