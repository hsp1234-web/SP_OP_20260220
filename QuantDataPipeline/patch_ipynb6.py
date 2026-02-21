import json

file_path = '/media/sp/D/SP_OP_20260220/QuantDataPipeline/colab_launcher.ipynb'
try:
    with open(file_path, 'r', encoding='utf-8') as f:
        nb = json.load(f)

    for cell in nb.get('cells', []):
        if cell.get('cell_type') == 'code':
            source = cell.get('source', [])
            if source and source[-1].strip() == "else:":
                source[-1] = "    else:\n"
                source.append("        emergency_backup()")

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(nb, f, indent=4, ensure_ascii=False)
    
    print("Patch OK")
except Exception as e:
    print(f"Error: {e}")
