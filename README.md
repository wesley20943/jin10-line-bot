# 我的金十快訊 → LINE｜Railway 雲端版 2.5

這份專案延續已測通的 Colab 篩選器，改成雲端常駐程式，並附上有密碼保護的關鍵字設定網頁。

## 已上線：從舊版更新到這一版

1. 下載並解壓縮 **`Jin10_LINE_Railway_v2.5.zip`**。
2. 打開其中的 `jin10-line-bot` 資料夾，把裡面的檔案與 `web`、`tests` 子資料夾上傳至原本 GitHub 儲存庫，覆蓋原檔並 Commit changes。尤其要一併更新 `engine.py`、`service.py`、`app.py` 及 `web` 裡的三個檔案。不要只上傳 ZIP，也不要只手動改版本字串。
3. 等 Railway 部署這次 GitHub 更新。原有 Variables 與 `/data` Volume 沿用。
4. 登入 Railway 的管理頁，頂端應顯示 **`2.5-websocket · skip-unparsed-20260930`**。最新 Deploy Logs 也會顯示同一版本。若沒有，仍未執行本更新包。
5. 在管理頁確認 LINE 是「通知中」；若不是，按「啟用 LINE」。這個開關在我們的管理頁，不是手機 LINE 或 LINE Developers 後台。
6. 按 **「傳送測試訊息」**。下方會顯示排隊、HTTP 回應或錯誤，然後核對手機的測試訊息。

測試訊息不經關鍵字篩選，仍使用同一個 LINE 收件人與傳送流程。只會在你按按鈕後建立，啟用通知或刷新頁面都不會自動建立測試。未完成的測試再次點擊會沿用原筆，每次新測試至少間隔 60 秒。測試也計入 LINE 訊息額度與今日接受數。

每則最近快訊現在會分開顯示「篩選結果」與「LINE 傳送狀態」。候選只代表通過篩選；若收到時通知暫停，不會在後來啟用時補發。看到「LINE API 已接受（HTTP 200）」後仍需核對手機；若沒收到，先核對畫面上的官方帳號、收件人與好友／封鎖狀態。HTTP 409 且已確認接受，表示先前已受理同一筆，不會另發一筆。

## 你會得到什麼

- 即時接收金十 WebSocket；一般斷線會以 5～60 秒間隔重連。
- 原本十個關注主題、彙總／專題排除規則，以及金十「重要」標記。
- 用兩個文字框調整「額外排除／一定收錄」，一行一個詞。繁簡自動比對，英文不分大小寫。
- 新規則即時套用，不需要重新部署，也不必停掉接收程式。
- 新聞試算、JSON 備份／匯入、LINE 暫停／啟用、測試訊息、實際傳送狀態與運作紀錄。
- SQLite 保存設定、已接收內容與傳送紀錄；Railway 必須掛載 Volume。

## 1｜上傳 GitHub

1. 解壓縮 `Jin10_LINE_Railway_v2.5.zip`，打開裡面的 `jin10-line-bot` 資料夾。
2. 前往 <https://github.com/new>，Repository name 填 `jin10-line-bot`，選 **Private**，建立儲存庫。
3. 用儲存庫的 **uploading an existing file** 或 **Add file → Upload files** 上傳資料夾裡的檔案與子資料夾，然後按 **Commit changes**。
4. 上傳後，儲存庫最上層應直接看得到 `app.py`、`Dockerfile`、`railway.json`、`requirements.txt`、`web` 資料夾。不要只上傳 ZIP，也不要在最上層多包一層 `jin10-line-bot`。
5. LINE Token 與管理密碼直接填入下一節的 Railway Variables。專案中的 `.env.example` 只是一份空白欄位範例。

## 2｜連接 Railway

1. 前往 <https://railway.com/> 登入／建立帳號，選擇適合常駐服務的方案。
2. 選 **New Project → Deploy from GitHub repo**，授權 Railway 存取剛才的私人儲存庫，選 `jin10-line-bot`。
3. 點開建立的服務，在 **Variables** 新增下表四項。

| 變數名稱 | 填什麼 |
| --- | --- |
| `LINE_CHANNEL_ACCESS_TOKEN` | 你在 Colab 已測通的 Channel access token，只填 Token 本身 |
| `LINE_USER_ID` | 你自己的 Your user ID，U 開頭，不是 Channel ID |
| `ADMIN_PASSWORD` | 自訂 16～256 字元的管理頁密碼；建議用密碼管理器產生 |
| `DATA_DIR` | `/data` |

這版不用 Channel secret，也不用在 LINE 後台填 Webhook 網址。

4. 為**同一個服務**新增 **Volume**，Mount path 填 **`/data`**。Volume 是保存資料用的磁碟，不是額外的 PostgreSQL 服務。可在服務 Settings → Volumes → Add Volume 新增，或使用專案畫布的新增選單搜尋 Volume。
5. 在服務 **Settings** 確認 **Replicas = 1**，並關閉 **Serverless／休眠**。只啟動這一份雲端服務。
6. 按 **Deploy** 套用。若選儲存庫後已自動部署、且在變數或 Volume 尚未完成時失敗，完成以上設定後 **Redeploy** 一次即可。
7. 建置方式與啟動命令已放在 Dockerfile／railway.json；不必另外填 Start Command。`PORT` 由 Railway 自動提供。
8. 部署成功後，在 **Settings → Networking → Public Networking → Generate Domain** 產生 HTTPS 網址，目標連接埠使用程式的 `PORT`（Railway 一般會自動偵測；日誌會顯示實際值）。

## 3｜第一次啟用

1. 打開 Railway 產生的 HTTPS 網址，以 `ADMIN_PASSWORD` 登入。
2. 確認頁面顯示你的 LINE 官方帳號與收件人，金十狀態為「已連線」。
3. 如有 Colab 的 `user_filters.json` 備份，按「載入 JSON 備份」，檢查後按「儲存並套用」。沒有備份時會沿用原本詞庫，額外排除預填「持倉報告」「主題」。
4. **先停止 Colab 或其他分頁正在執行的通知程式**，避免兩個服務各發一次。
5. 在管理頁按 **「啟用 LINE」**，再按 **「傳送測試訊息」** 核對手機。之後便會通知符合條件的新快訊。
6. 電腦與管理頁可以關閉；程式由 Railway 執行。通知開關會保存，之後服務重新啟動也會沿用。

首次部署預設暫停 LINE，但仍接收快訊供查看。這份雲端版沒有 Colab 的 5 分鐘／10 則測試上限。LINE 額度仍依你的官方帳號方案計算。

## 4｜以後怎麼改關鍵字

| 想做的事 | 操作 |
| --- | --- |
| 多排除一種新聞 | 在「額外排除」新增一行，例如「持倉報告」 |
| 強制納入一個關注詞 | 在「一定收錄」新增一行，例如「台積電」 |
| 刪除條件 | 刪掉對應的那一行 |
| 同時命中兩邊 | 下拉選擇排除優先或收錄優先，預設排除優先 |
| 先看會不會被收錄 | 貼新聞原文到「試算一則新聞」；使用目前尚未儲存的草稿 |
| 正式套用修改 | 按「儲存並套用」，立即生效 |
| 留一份自己的備份 | 儲存後按「下載目前設定」 |

- 「一定收錄」命中任一詞就能通過原本的主題／事件詞門檻。同時命中內建彙總或額外排除時，仍由優先順序決定。
- 「主題」只比對新聞原文。
- 金十重要標記會顯示，但不會自動繞過你的規則。
- 英文使用詞界線，例如 `ARM` 不會命中 `farm`；中文使用字串比對。這是詞面篩選，不會自動推導公司別名或同義詞。
- 新規則會重新檢查尚未送出的候選；不會回頭掃描已略過的歷史消息。
- 已送出但結果不明的訊息，重試時保留原文與編號；修改關鍵字或按暫停，無法撤回已被 LINE 接受的訊息。
- 暫停期間不新排入 LINE；恢復後只通知後續新消息。先前結果不明的傳送仍會沿用原編號確認。

## 5｜連線中斷與錯誤

2.5 更新：單筆缺少有效 ID／時間、欄位不完整或無法還原的資料，會寫入運作紀錄後略過，不排入 LINE。異常資料不會寫入正常快取，也不會猜測其發布時間；後續正常快訊繼續接收。

LINE 先顯示新聞內容，下方再放時間、金十重要程度與來源。不加「我的關注快訊」、時區標籤或主題欄；時間仍按台灣時間顯示。

- 無法辨識資料不套用關鍵字規則，因為它不會進入 LINE 待送流程。管理頁和 Railway Logs 仍可看到略過次數與診斷摘要。
- 完全相同的異常資料使用相同去重識別，重連時不重複處理；去重依 Volume 中保留的紀錄。
- 升級至 2.5 時，舊資料庫中尚未嘗試傳送的異常通知會改為略過。若某筆已經呼叫過 LINE、結果仍待確認，會保留原重試編號，避免重複傳送。
- 已知控制封包、廣告、鎖定內容與未公布數據維持原本處理方式；連線／協定本身的錯誤仍依運作紀錄處理。

- **一般金十斷線**：持續自動重連，最多約每分鐘一次；不會因為達到 120 分鐘而停止。
- **服務重新啟動**：利用 Volume 中的接收記錄核對快照，補回快照內尚未接收、且最近 15 分鐘的候選；不重發已被 LINE 接受的相同內容。
- **長時間斷線**：來源快照數量有限，無法保證完整補漏。仍只處理最近 15 分鐘的候選。
- **來源政策／協定錯誤**：管理頁顯示「需要處理」。確認接入方式後，再按「重新連線」。本專案沿用目前已測通的訪客 WebSocket，並不提供或保證付費通道權限。
- **LINE 逾時或 5xx**：採退避重試，沿用原始內容與 `X-Line-Retry-Key`，避免因回應遺失重送。
- **LINE 400／401／403／429**：暫停傳送並保存記錄，確認訊息格式、Token、權限或額度後，按「重試 LINE」。接收與設定頁仍然運作。
- **結果不明超過 23 小時**：停止 LINE，保留記錄供檢查；不要刪除資料庫重跑，避免失去防重送依據。
- **頁面顯示初始化錯誤**：先檢查 Railway Variables 與 LINE 好友狀態。錯誤改好後可 Redeploy；初始化也會每分鐘再確認一次。
- **頁面有候選但手機沒收到**：先看 LINE 開關、錯誤提示與運作紀錄。HTTP 200 表示 LINE API 接受，並不保證對方裝置確實收到，例如封鎖 Bot 時。

## 6｜保存、更新與運作限制

- `DATA_DIR` 必須對應掛載的 Volume。資料檔是 `/data/state.sqlite3`，包含關鍵字設定、開關與近期傳送紀錄。重新部署程式不會覆蓋這些設定。
- 更換／刪除 Volume 會失去保存資料；JSON 備份只含關鍵字，不含訊息傳送紀錄。可另設定 Railway Volume 備份。
- GitHub 更新連接的分支後，Railway 可自動重新部署。附掛 Volume 的部署可能有短暫中斷；程式會用快照核對，但不能保證所有斷線資料都補齊。
- 固定 1 個 replica、1 個 Python worker。程式也會鎖定同一個資料夾，避免同一 Volume 被重複使用。不同服務／不同 Volume／Colab 各自有獨立紀錄，無法彼此防重送。
- 已接受或取消的 outbox 記錄保留約 7 天；短期接收記錄及原文快取有數量／時間限制。
- `/healthz` 只表示管理服務可回應；金十與 LINE 狀態請看管理頁和 Railway Logs。Railway 的部署 healthcheck 不是持續監控器。
- 管理頁只管理單一 LINE 收件人，HTTPS Cookie 有效時間為 12 小時。變更 `ADMIN_PASSWORD` 並重新部署後，需要重新登入。

## 專案檔案

| 檔案 | 用途 |
| --- | --- |
| `app.py` | 管理頁、登入與設定 API |
| `service.py` | 常駐接收、重連、LINE 傳送及資料保存 |
| `engine.py` | 沿用 Colab 的協定、詞庫及篩選器 |
| `web/` | 簡易管理頁 |
| `Dockerfile`、`requirements.txt`、`railway.json` | 建置與 Railway 部署設定 |
| `tests/test_cloud.py` | 離線測試，使用模擬新聞與 LINE 回應 |

## 本機驗證（選用）

使用 Python 3.12：

```bash
python -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m unittest discover -s tests -v
```

本次雲端版已通過 23 項離線整合測試，涵蓋重啟、斷線補回、防重送、關鍵字、無法辨識資料略過、舊佇列升級、LINE 錯誤、登入／CSRF，以及測試按鈕的授權、暫停、排隊、逾時重試和 HTTP 429 顯示。JavaScript 語法檢查已通過。

測試不呼叫真正的金十或 LINE。若要在本機實際執行，請在環境變數填入四項設定，將 `DATA_DIR` 改為本機資料夾，另設 `LOCAL_HTTP=1`，執行 `python app.py`，開啟 `http://localhost:8080`。程式使用 Linux 檔案鎖；Windows 請用 WSL 或 Docker。程式不會自行讀取 `.env` 檔。

## 官方資料

- Railway 服務與 GitHub 部署：<https://docs.railway.com/services>
- Railway 持久儲存：<https://docs.railway.com/volumes>
- Railway 部署設定：<https://docs.railway.com/config-as-code/reference>
- Railway healthcheck：<https://docs.railway.com/deployments/healthchecks>
- LINE 重試與防重送：<https://developers.line.biz/en/docs/messaging-api/retrying-api-request/>

本更新包尚未由我上傳至你的 GitHub／Railway，也沒有使用你的 Token 實際傳送 LINE。部署是否套用，請以管理頁與最新啟動紀錄顯示的版本為準。
