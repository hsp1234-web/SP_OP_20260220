import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# 將自己加入 sys.path 確保能找到專案模組
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.pipeline_logger import setup_logger

logger = setup_logger("test_mock", "/tmp/mock_pipeline.log")

def mock_download_task(task_id: int):
    """模擬一個無速限的下載任務 (0.05 秒完成)"""
    try:
        # 延遲 0.05 秒模擬抓取時間
        time.sleep(0.05)
        # 不拋錯，直接當成功
        return True
    except KeyboardInterrupt:
        # 強制斬斷時，拋出不處理以中斷線程
        raise
    except Exception as e:
        logger.error(f"任務 {task_id} 發生未知錯誤: {e}")
        return False

def simulate_pipeline():
    """模擬我們目前在 run_all.py 裡的 Phase 1 邏輯"""
    total_pending = 3000
    download_workers = 30
    
    logger.info(f"【測試開始】使用 {download_workers} 個執行緒進行「無速限」併發下載 (共 {total_pending} 個任務)")
    
    start_time = time.time()
    completed_p1 = 0
    
    try:
        with ThreadPoolExecutor(max_workers=download_workers) as executor:
            # 提交三千個模擬任務
            future_to_task = {
                executor.submit(mock_download_task, i): i
                for i in range(total_pending)
            }
            
            for i, future in enumerate(as_completed(future_to_task), 1):
                task_id = future_to_task[future]
                
                # 若被中斷，這裡的 result() 會拋出被我們砍斷的例外
                future.result()
                completed_p1 += 1
                
                # 測試我們的動態 ETA 進度條 (每 10 次推播一次，這就是剛才引發 TypeError 的地方)
                if i % 10 == 0 or i == total_pending:
                    elapsed = time.time() - start_time
                    avg_time = elapsed / i
                    eta_sec = avg_time * (total_pending - i)
                    
                    eta_minutes = int(eta_sec // 60)
                    eta_seconds = int(eta_sec % 60)
                    eta_hours = int(eta_sec // 3600)
                    eta_m_rem = int((eta_sec % 3600) // 60)
                    
                    if eta_sec < 3600:
                        eta_str = f"{eta_minutes}m {eta_seconds}s"
                    else:
                        eta_str = f"{eta_hours}h {eta_m_rem}m"
                    
                    pct = (i / total_pending)
                    blocks = int(pct * 20)
                    bar = "🟩" * blocks + "⬛" * (20 - blocks)
                    
                    logger.info(f"[P1 下載進度] {bar} {i}/{total_pending} ({pct:.1%}) | 預估剩餘: {eta_str}")
                
                # 在第 500 次時，我們主動假裝按下 Colab 的 [停止鍵] = 觸發 KeyboardInterrupt
                if i == 500:
                    logger.warning(">>> 模擬使用者按下停止鍵 (KeyboardInterrupt) <<<")
                    raise KeyboardInterrupt("USER STOP")

        logger.info(f"Phase 1 完成: {completed_p1}/{total_pending} 個任務處理完畢")
        
    except KeyboardInterrupt:
        # 這就是 colab_launcher 裡抓到中斷指令時的應對方案
        logger.error("🚨 收到中止指令！正在強制斬斷下載任務...")
        executor.shutdown(wait=False, cancel_futures=True)
        # 強制關閉並離開
        logger.error("🔪 已強行終止底層管線程序")
        sys.exit(1)

if __name__ == "__main__":
    simulate_pipeline()
