# StockLab

使用 FinMind 的 `TaiwanStockPrice` 下載台股日資料，寫入 SQLite，並用 Streamlit + Plotly 顯示日線與 20/60 MA。

## 1) 安裝

```bash
source venv/bin/activate
pip install -r requirements.txt
```

## 2) 抓資料（以 2330 為例）

```bash
python fetcher.py --stock-id 2330 --start-date 2020-01-01
```

可選：

```bash
export FINMIND_TOKEN=你的token
python fetcher.py --stock-id 2317 --start-date 2020-01-01
```

## 3) 啟動頁面

```bash
streamlit run app.py
```

頁面可選股票代碼，顯示：
- 日K
- MA20
- MA60

## 補資料範例

```bash
python fetcher.py --stock-id 2330 --start-date 2018-01-01
python fetcher.py --stock-id 2317 --start-date 2018-01-01
python fetcher.py --stock-id 2454 --start-date 2018-01-01
```
