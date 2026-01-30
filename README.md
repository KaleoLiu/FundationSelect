# FundationSelect

台灣基金排名與持股分析工具：從富邦取得基金排名、從 SITCA 取得持股明細，產出 Excel 並做統計比較。

## 功能

- **基金排名**：從富邦網站抓取國內股票型基金排名（科技類、中小型、一般股票型）
- **持股明細**：從 SITCA「基金投資明細-月前十大」抓取各基金前十大持股（國內上市／上櫃）
- **統計比較**：比對前後月份持股，產出增持／新進／刪除等統計

## 環境

- Python 3.11+
- 依賴：`pandas`、`openpyxl`、`requests`、`beautifulsoup4`、`lxml`、`certifi`（可選，用於 SSL）

```bash
pip install pandas openpyxl requests beautifulsoup4 lxml certifi
```

## 使用方式

1. 建立虛擬環境（建議）並安裝依賴。
2. 執行主流程（會依當日日期產生 `FundRanking_YYYY-MM-DD.xlsx`）：

```bash
python FundationSearch.py
```

流程為：**FundationTaiwan**（抓排名）→ **FundationHolding**（抓持股）→ **FundationCompare**（統計比較）→ 寫入 Excel 並刪除空白工作表。

## 專案檔案說明

| 檔案 | 說明 |
|------|------|
| `FundationSearch.py` | 主入口：依序執行 Taiwan → Holding → Compare，產出當日 Excel |
| `FundationTaiwan.py` | 從富邦抓取基金排名（requests，多執行緒） |
| `FundationHolding.py` | 從 SITCA 抓取基金前十大持股（requests，多執行緒，可調 `MAX_FUNDS_PER_RUN`、`MAX_CONCURRENT_REQUESTS`） |
| `FundationCompare.py` | 讀取持股 Excel，產出增持／新進／刪除統計 |
| `FoundationSelect.py` | 讀取 Excel、提取基金名稱等（供 Holding 使用） |
| `FundationTrack.py` | 獨立工具：從公開資訊觀測站取得月度營收等 |

## 產出

- 執行後會在專案目錄產生 **`FundRanking_YYYY-MM-DD.xlsx`**（依執行當日日期）。
- 此 Excel 不納入版控（由 `.gitignore` 忽略）。

## 授權

本專案僅供學習與個人使用；請遵守富邦、SITCA 等網站之使用條款與爬取禮節。
