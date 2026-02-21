import json

with open("colab_launcher.ipynb", "r", encoding="utf-8") as f:
    nb = json.load(f)

for cell in nb["cells"]:
    if cell["cell_type"] == "code":
        source = "".join(cell["source"])

        # Fail-Fast Process Kill & Backup mechanism
        # Find the emergency_backup def and execution mechanism
        
        old_backup = """def emergency_backup():
    log('⚠️', '收到停止指令！準備安全關機...')
    if SYNC_TO_DRIVE and drive_data:
        try:
            shutil.copy2(DB_PATH, drive_data / 'status.db')
            log('💾', '已將最新進度 (status.db) 安全備份至 Drive')
            log('🏁', '程式已安全中斷。下次重跑將從斷點繼續。')
        except Exception as e:
            log('⛔', f'備份進度失敗: {e}')
    if 'log_queue' in globals(): log_queue.put(None)"""

        new_backup = """def emergency_backup(proc=None):
    log('🚨', '<span style=\"color:#FF5252; font-weight:bold;\">收到中止指令！正在強制斬斷下載任務...</span>')
    # 光速斬斷底層子程序 (Fail-Fast)
    if proc is not None:
        try:
            proc.kill()
            proc.wait(timeout=1)
            log('🔪', '已強行終止底層管線程序')
        except Exception:
            pass

    if SYNC_TO_DRIVE and drive_data:
        try:
            shutil.copy2(DB_PATH, drive_data / 'status.db')
            log('💾', '進度 (status.db) 已安全備份至 Drive')
            log('🏁', '程式已秒速中斷。下次重跑將從斷點繼續。')
        except Exception as e:
            log('⛔', f'備份進度失敗: {e}')
    if 'log_queue' in globals(): log_queue.put(None)"""
        
        source = source.replace(old_backup, new_backup)

        old_exec = """    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env_vars)
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

        new_exec = """    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env_vars)
    try:
        for line in iter(process.stdout.readline, ''):
            line = line.strip()
            if line: log('⚙️', line)
        
        process.stdout.close()
        ret = process.wait()
        if ret != 0 and ret != -9 and ret != -15:  # -9 is SIGKILL, -15 is SIGTERM
            log('❌', f'run_all.py 執行失敗，回傳碼 {ret}')
        elif ret == 0:
            elapsed_min = round((time.time() - pipeline_start_time) / 60, 1)
            header('🏁 管線執行完畢')
            log('📊', f'總耗時:~{elapsed_min}m')
            
        if 'log_queue' in globals(): log_queue.put(None)

    except KeyboardInterrupt:
        # Colab 按下停止時，觸發第一道中斷
        emergency_backup(proc=process)
        
except Exception as e:
    log('💥', f'管線異常中斷: {e}')
    # 若連 Popen 都沒建立就發生錯誤
    if 'process' in locals():
        emergency_backup(proc=process)
    else:
        emergency_backup()"""

        source = source.replace(old_exec, new_exec)
        
        cell["source"] = [line + '\n' for line in source.split('\n')]
        if cell["source"][-1] == '\n':
            cell["source"] = cell["source"][:-1]
        else:
            cell["source"][-1] = cell["source"][-1].rstrip('\n')

with open("colab_launcher.ipynb", "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=4, ensure_ascii=False)
    f.write('\n')
