# AI 報告按鈕優化任務日誌

## 任務概述
- **任務**: 將「產生/更新 AI 報告」按鈕改為背景執行
- **需求**: 按下按鈕後顯示「報告產生中，請稍待」，在背景執行，使用者可關閉手機螢幕

## 執行記錄

### 2026-03-15 10:17 任務開始

#### 現狀分析
1. 系統架構:
   - 按鈕點擊後呼叫 `enqueue_report_job()` 建立任務
   - 使用 `launch_report_worker()` 啟動背景 worker (subprocess.Popen)
   - Worker 執行 `report_job_worker.py` 處理報告生成
   - 報告生成使用 `generate_report_bundle()` 包含市場資訊 (`get_market_intel`)

2. 現有程式碼問題:
   - 按下按鈕後呼叫 `st.rerun()` 強制頁面重新整理
   - 在手機上可能導致需要在螢幕開啟狀態才能順利運作

3. 模組確認:
   - [x] 按鈕和排程報告使用相同模組 `generate_report_bundle` (from report_payloads.py)
   - [x] 該模組已包含最新產業資訊和證券交易所公告 (透過 `get_market_intel`)

#### 修改方案
- 移除 `st.rerun()` 讓使用者可以關閉手機螢幕
- Job 會在背景執行 (subprocess)
- 使用者回到頁面時會自動顯示最新狀態

### 2026-03-15 10:25 修改完成
- 移除 `app.py` 中的 `st.rerun()` 調用
- 保持其他邏輯不變：
  - Job 仍使用 `enqueue_report_job()` + `launch_report_worker()` 在背景執行
  - 使用 `generate_report_bundle()` 包含最新產業資訊和交易所公告
- 使用者可關閉手機螢幕，報告會在背景產生
- 回到頁面時會自動顯示最新狀態（包含「報告產生中，請稍待」或最終報告）

### 2026-03-15 10:30 驗證
- 確認程式碼邏輯正確
- 背景 worker 透過 subprocess.Popen 啟動，獨立於網頁請求
- 報告產生模組與排程共用同一模組 `generate_report_bundle`

