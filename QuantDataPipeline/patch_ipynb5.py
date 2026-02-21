import json

file_path = '/media/sp/D/SP_OP_20260220/QuantDataPipeline/colab_launcher.ipynb'
try:
    with open(file_path, 'r', encoding='utf-8') as f:
        nb = json.load(f)

    for cell in nb.get('cells', []):
        if cell.get('cell_type') == 'code':
            source = cell.get('source', [])
            
            joined = "".join(source)
            bad_block = '''    if proc is not None:
        try:
            proc.kill()
            proc.wait(timeout=1)
            log('🔪', '已強行終止底層管線程序')
        except Exception:
            pass'''
            
            new_block = '''    if proc is not None:
        try:
            proc.kill()
            proc.wait(timeout=1)
            log('🔪', '已強行終止底層管線程序')
        except Exception:
            pass

    try:
        import shutil
        local_dir = '/content/local_data'
        if os.path.exists(local_dir):
            shutil.rmtree(local_dir, ignore_errors=True)
            log('🗑️', '已清除本地暫存的中斷/未完成檔案')
    except Exception:
        pass'''
            
            if bad_block in joined:
                joined = joined.replace(bad_block, new_block)
            
            cell['source'] = [line + '\n' for line in joined.split('\n')][:-1]
            if len(cell['source']) > 0:
                cell['source'][-1] = cell['source'][-1].rstrip('\n')

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(nb, f, indent=4, ensure_ascii=False)
    
    print("Patch OK")
except Exception as e:
    print(f"Error: {e}")
