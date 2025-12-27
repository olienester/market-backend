import yfinance as yf
from cachetools import TTLCache

cache = TTLCache(maxsize=100, ttl=300)

def get_stock_data(symbol: str, interval: str = "1d", period: str = "1y"):
    key = f"{symbol}-{interval}-{period}"

    if key in cache:
        return cache[key]

    ticker = yf.Ticker(symbol)
    hist = ticker.history(period=period, interval=interval)

    if hist.empty:
        return None

    data = []

    for index, row in hist.iterrows():
        data.append({
            "timestamp": int(index.timestamp() * 1000),
            "open": round(row["Open"], 2),
            "high": round(row["High"], 2),
            "low": round(row["Low"], 2),
            "close": round(row["Close"], 2),
            "volume": int(row["Volume"])
        })

    cache[key] = data
    return data
