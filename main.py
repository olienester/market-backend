from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import requests
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import pytz
import os
import io
import time
import numpy as np
import cloudscraper

# Importações dos seus serviços (garanta que esses arquivos existem)
from services.market_data import get_stock_data
from services.strategy import calculate_probability
from services.larry_williams import calculate_lw91

app = FastAPI(title="Market Data API")

# ===============================
# CONFIG CORS (Permite acesso do React Native)
# ===============================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===============================
# CONFIG RapidAPI
# ===============================
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")

CALENDAR_URL = "https://economic-events-calendar.p.rapidapi.com/economic-events/tradingview"
CALENDAR_HEADERS = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": "economic-events-calendar.p.rapidapi.com"
}

# ===============================
# CONFIG FoundamentsAPI (Cache & Headers)
# ===============================
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}
CACHE_TIMEOUT = 3600  # 1 hora de cache
_cache_data = {"timestamp": 0, "data": None}

def fetch_fundamentus_data():
    """Baixa e trata os dados do Fundamentus usando Cloudscraper para evitar 403"""
    global _cache_data
    
    # Verifica cache
    if _cache_data["data"] is not None and (time.time() - _cache_data["timestamp"] < CACHE_TIMEOUT):
        return _cache_data["data"]

    url = 'https://www.fundamentus.com.br/fii_resultado.php'
    
    try:
        # CRIA UM SCRAPER QUE SIMULA UM NAVEGADOR REAL
        scraper = cloudscraper.create_scraper() 
        
        # Faz a requisição (o scraper já injeta os headers e cookies corretos)
        response = scraper.get(url)
        
        if response.status_code != 200:
            raise Exception(f"Status Code: {response.status_code}")
        
        # O resto do tratamento segue igual
        df = pd.read_html(io.BytesIO(response.content), decimal=',', thousands='.')[0]
        
        # Renomear colunas
        df.rename(columns={
            'Papel': 'ticker',
            'Segmento': 'setor',
            'Dividend Yield': 'dy',
            'P/VP': 'pvp',
            'Liquidez': 'liquidez',
            'Qtd de imoveis': 'qtd_imoveis',
            'Cap Rate': 'cap_rate',
            'Vacância Média': 'vacancia'
        }, inplace=True)

        # Limpeza
        for col in ['dy', 'cap_rate', 'vacancia']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace('%', '').str.replace(',', '.').replace('nan', '0')
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        df['pvp'] = pd.to_numeric(df['pvp'], errors='coerce').fillna(0)
        df['liquidez'] = pd.to_numeric(df['liquidez'], errors='coerce').fillna(0)
        
        df = df[df['liquidez'] > 0]
        
        _cache_data = {"timestamp": time.time(), "data": df}
        
        return df
        
    except Exception as e:
        print(f"Erro no scraping: {e}")
        # Se falhar e tiver cache, usa o cache
        if _cache_data["data"] is not None:
            return _cache_data["data"]
        raise e

# ===============================
# Mock fallback
# ===============================
mock_events = [
    {
        "id": "1",
        "time": "09:00",
        "country": "BR",
        "impact": "high",
        "title": "IPCA (Mensal)",
        "actual": "-",
        "forecast": "0,30%"
    }
]

# ===============================
# Endpoints
# ===============================
@app.get("/")
def root():
    return {"status": "API funcionando"}


@app.get("/stock/{symbol}")
def stock(symbol: str, interval: str = "1d", period: str = "1y"):
    data = get_stock_data(symbol, interval, period)
    if not data:
        raise HTTPException(status_code=404, detail="Dados não encontrados")
    return {
        "symbol": symbol,
        "interval": interval,
        "period": period,
        "data": data
    }

@app.get("/strategy/{symbol}")
def get_strategy(symbol: str):
    data = calculate_probability(symbol)
    if not data:
        return {"error": "Não foi possível analisar o ativo"}
    return data

@app.get("/strategy/91/{symbol}")
def get_strategy_lw91(symbol: str, interval: str = "1d"):
    result = calculate_lw91(symbol, interval)
    if result is None:
        raise HTTPException(status_code=404, detail="Dados insuficientes ou erro no cálculo")
    return result

@app.get("/market/dividends/{ticker}")
def get_dividends(ticker: str):
    symbol = ticker.upper() if ticker.upper().endswith(".SA") else f"{ticker.upper()}.SA"
    try:
        asset = yf.Ticker(symbol)
        divs = asset.dividends
        if divs.empty:
            return []

        start_date = pd.Timestamp.now(tz=divs.index.tz) - pd.DateOffset(months=12)
        recent_divs = divs[divs.index >= start_date]
        
        results = []
        for date, value in recent_divs.items():
            date_str = date.strftime("%d/%m/%Y")
            div_type = "Provento"
            results.append({
                "dataPagamento": date_str,
                "valor": float(value),
                "tipo": div_type
            })
        results.reverse()
        return results
    except Exception as e:
        print(f"Erro ao buscar dividendos de {symbol}: {e}")
        return []

@app.get("/market/quote/{ticker}")
def get_quote(ticker: str):
    try:
        t = yf.Ticker(ticker + ".SA")
        info = t.info
        return {
            "symbol": ticker,
            "price": info.get('currentPrice'),
            "regularMarketChangePercent": info.get('regularMarketChangePercent') * 100
        }
    except:
        return {"error": "not found"}
    
@app.get("/calendar")
def get_calendar():
    try:
        response = requests.get(CALENDAR_URL, headers=CALENDAR_HEADERS, timeout=15)
        if response.status_code != 200:
            return mock_events

        payload = response.json()
        data = payload.get("result", [])
        events = []

        for item in data:
            country = item.get("country")
            if country not in ("BR", "US"):
                continue

            importance = item.get("importance", 0)
            if importance >= 1: impact = "high"
            elif importance == 0: impact = "medium"
            else: impact = "low"

            if impact == "low": continue

            try:
                dt = datetime.fromisoformat(item["date"].replace("Z", "+00:00")).astimezone(pytz.timezone("America/Sao_Paulo"))
                time_str = dt.strftime("%H:%M")
            except:
                time_str = "--:--"

            events.append({
                "id": item.get("id"),
                "time": time_str,
                "country": country,
                "impact": impact,
                "title": item.get("title"),
                "actual": item.get("actual") or "-",
                "forecast": item.get("forecast") or "-"
            })
        return events or mock_events
    except Exception as e:
        print("Erro calendário:", e)
        return mock_events


@app.get("/api/ranking")
def get_ranking(type: str = Query("shank", enum=["shank", "smart"])):
    try:
        df = fetch_fundamentus_data().copy()
        
        if type == 'shank':
            # Lógica Shank
            df['rank_pvp'] = df['pvp'].rank(ascending=False) # Nota: No Excel original você usava ordem decrescente pra P/VP? Geralmente menor P/VP é melhor (ascending=True). Mantive sua lógica original do Excel.
            df['rank_dy'] = df['dy'].rank(ascending=True)    # Nota: Geralmente maior DY é melhor (ascending=False).
            df['shank_score'] = df['rank_pvp'] + df['rank_dy']
            df = df.sort_values('shank_score', ascending=False)
            df['ranking_pos'] = range(1, len(df) + 1)
            result_df = df[['ticker', 'setor', 'dy', 'pvp', 'ranking_pos']]

        elif type == 'smart':
            # Lógica Smart
            rentab_acum = 0 
            volatilidade = 0 
            
            liq_score = np.log1p(df['liquidez']) 
            
            df['smart_score'] = (
                (df['dy'] * 25 + rentab_acum * 25) * 0.5 + 
                ((1 - df['pvp']) * 15 + (1 / (1 + volatilidade)) * 15) * 0.3 + 
                (liq_score * 10) * 0.2
            )
            df = df.sort_values('smart_score', ascending=False)
            df['ranking_pos'] = range(1, len(df) + 1)
            result_df = df[['ticker', 'setor', 'dy', 'pvp', 'ranking_pos', 'smart_score']]

        return result_df.head(50).to_dict(orient='records')

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
