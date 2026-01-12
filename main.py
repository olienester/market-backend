from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import requests
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import pytz
import os

# --- IMPORTAÇÕES DOS SERVIÇOS ---
from services.market_data import get_stock_data
from services.strategy import calculate_probability
from services.larry_williams import calculate_lw91
# Importa o novo serviço de ranking
from services.ranking_service import calculate_ranking 
from services.ranking_acoes_service import get_relatorio_geral_acoes
from services.ranking_usa_service import get_relatorio_geral_usa

app = FastAPI(title="Market Data API")

# ===============================
# CONFIG CORS
# ===============================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===============================
# CONFIG RapidAPI (Calendário)
# ===============================
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
CALENDAR_URL = "https://economic-events-calendar.p.rapidapi.com/economic-events/tradingview"
CALENDAR_HEADERS = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": "economic-events-calendar.p.rapidapi.com"
}

# ===============================
# MOCK FALLBACK (Calendário)
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
# ENDPOINTS
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
        if divs.empty: return []

        start_date = pd.Timestamp.now(tz=divs.index.tz) - pd.DateOffset(months=12)
        recent_divs = divs[divs.index >= start_date]
        
        results = []
        for date, value in recent_divs.items():
            date_str = date.strftime("%d/%m/%Y")
            results.append({
                "dataPagamento": date_str,
                "valor": float(value),
                "tipo": "Provento"
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
        if response.status_code != 200: return mock_events
        payload = response.json()
        data = payload.get("result", [])
        events = []
        for item in data:
            country = item.get("country")
            if country not in ("BR", "US"): continue
            importance = item.get("importance", 0)
            if importance >= 1: impact = "high"
            elif importance == 0: impact = "medium"
            else: impact = "low"
            if impact == "low": continue
            try:
                dt = datetime.fromisoformat(item["date"].replace("Z", "+00:00")).astimezone(pytz.timezone("America/Sao_Paulo"))
                time_str = dt.strftime("%H:%M")
            except: time_str = "--:--"
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

# =========================================================
# RANKING FIIs (Agora muito mais limpo!)
# =========================================================
@app.get("/api/ranking")
def get_ranking_endpoint(sort_by: str = Query("shank", enum=["shank", "smart"])):
    try:
        # Chama a função que está no services/ranking_service.py
        result = calculate_ranking(sort_by)
        return result
    except Exception as e:
        print(f"Erro no endpoint de ranking: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# =========================================================
# RANKING AÇOES
# =========================================================
@app.get("/api/ranking/acoes/geral")
def get_ranking_geral():
    try:
        return get_relatorio_geral_acoes()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =========================================================
# ROTA: RANKING USA (TODAS AS STOCKS / SCANNER)
# =========================================================
@app.get("/api/ranking/usa/geral")
def get_ranking_usa_endpoint():
    """
    Retorna o Ranking de Ações Americanas (Scanner Completo).
    Fonte: Lista NASDAQ + Yahoo Finance.
    Estratégias: Greenblatt, Graham, Bazin, Barsi (Adaptados).
    
    Nota: A primeira execução pode levar alguns minutos para 
    baixar e processar os dados. As próximas serão instantâneas (Cache).
    """
    try:
        print("Recebida solicitação de Ranking USA...")
        resultado = get_relatorio_geral_usa()
        
        if not resultado:
            return {"message": "Nenhum ativo encontrado ou erro no scanner.", "data": []}
            
        return resultado
        
    except Exception as e:
        print(f"Erro Crítico no Endpoint USA: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno ao gerar ranking USA: {str(e)}")
