import json

file_path = '/media/sp/D/SP_OP_20260220/QuantDataPipeline/colab_launcher.ipynb'
try:
    with open(file_path, 'r', encoding='utf-8') as f:
        nb = json.load(f)

    for cell in nb.get('cells', []):
        if cell.get('cell_type') == 'code':
            source = cell.get('source', [])
            
            # Remove the ghost DB backup from watchdog emergency backup
            clean_source = []
            skip = False
            for line in source:
                if "if SYNC_TO_DRIVE and drive_data:" in line and "src_conn.backup(dst_conn)" in "".join(source):
                    # Start checking closely if this is the backup block
                    pass
                if "import sqlite3" in line and "src_conn.backup" in "".join(source):
                    skip = True
                    # Let's cleanly just delete lines if they contain these specific backup terms
                if "src_conn.backup(dst_conn)" in line or "sqlite3.connect(drive_data / 'status.db')" in line or "進度 (status.db) 已安全備份" in line:
                    continue
                clean_source.append(line)
            
            # A more robust regex/replace way for exactly the block:
            joined = "".join(source)
            bad_block = '''    if SYNC_TO_DRIVE and drive_data:
        try:
            import sqlite3
            with sqlite3.connect(DB_PATH) as src_conn, sqlite3.connect(drive_data / 'status.db') as dst_conn:
                src_conn.backup(dst_conn)
            log('💾', '進度 (status.db) 已安全備份至 Drive')
            log('🏁', '程式已秒速中斷。下次重跑將從斷點繼續。')
        except Exception as e:
            log('⛔', f'備份進度失敗: {e}')'''
            bad_block2 = '''    if SYNC_TO_DRIVE and drive_data:\n        try:\n            import sqlite3\n            with sqlite3.connect(DB_PATH) as src_conn, sqlite3.connect(drive_data / 'status.db') as dst_conn:\n                src_conn.backup(dst_conn)\n            log('💾', '進度 (status.db) 已安全備份至 Drive')\n            log('🏁', '程式已秒速中斷。下次重跑將從斷點繼續。')\n        except Exception as e:\n            log('⛔', f'備份進度失敗: {e}')'''
            
            if bad_block in joined:
                joined = joined.replace(bad_block, "    log('🏁', '程式已秒速中斷。下次重跑將從斷點繼續。')")
            elif bad_block2 in joined:
                joined = joined.replace(bad_block2, "    log('🏁', '程式已秒速中斷。下次重跑將從斷點繼續。')")
            else:
                # Manual lines remove
                new_source = []
                for line in source:
                    if "import sqlite3" in line and "status.db" in line: continue
                    if "src_conn.backup" in line: continue
                    if "進度 (status.db) 已安全備份" in line: continue
                    if "備份進度失敗" in line: continue
                    new_source.append(line)
                joined = "".join(new_source)

            # Split back
            cell['source'] = [line + '\n' for line in joined.split('\n')][:-1]
            if len(cell['source']) > 0:
                cell['source'][-1] = cell['source'][-1].rstrip('\n')

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(nb, f, indent=4, ensure_ascii=False)
    
    print("Patch OK")
except Exception as e:
    print(f"Error: {e}")
