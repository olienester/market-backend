from fastapi import FastAPI, HTTPException
from services.market_data import get_stock_data
import requests
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import pytz
import os
from services.strategy import calculate_probability
from services.larry_williams import calculate_lw91

app = FastAPI(title="Market Data API")

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
def stock(
    symbol: str,
    interval: str = "1d",
    period: str = "1y"
):
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
    """
    Ex: /strategy/PETR4
    Retorna a probabilidade baseada na abertura de hoje.
    """
    data = calculate_probability(symbol)
    if not data:
        return {"error": "Não foi possível analisar o ativo"}
    return data

@app.get("/strategy/91/{symbol}")
def get_strategy_lw91(symbol: str, interval: str = "1d"):
    """
    Rota para verificar o Setup 9.1.
    
    Exemplos de chamada:
    - Diário (Padrão): /strategy/91/PETR4
    - 60 Minutos:      /strategy/91/PETR4?interval=60m
    """
    
    # Passamos o 'symbol' E o 'interval' para a função de cálculo
    result = calculate_lw91(symbol, interval)
    
    if result is None:
        raise HTTPException(status_code=404, detail="Dados insuficientes ou erro no cálculo")
        
    return result


@app.get("/market/dividends/{ticker}")
def get_dividends(ticker: str):
    """
    Retorna os proventos (Dividendos/JCP) dos últimos 12 meses e futuros.
    """
    # Garante o sufixo .SA
    symbol = ticker.upper() if ticker.upper().endswith(".SA") else f"{ticker.upper()}.SA"
    
    try:
        asset = yf.Ticker(symbol)
        
        # Pega histórico de dividendos
        divs = asset.dividends
        
        if divs.empty:
            return []

        # Filtra: Queremos apenas de 1 ano atrás até o futuro
        start_date = pd.Timestamp.now(tz=divs.index.tz) - pd.DateOffset(months=12)
        recent_divs = divs[divs.index >= start_date]
        
        results = []
        for date, value in recent_divs.items():
            # Formata a data para string "DD/MM/YYYY"
            date_str = date.strftime("%d/%m/%Y")
            
            # Tenta inferir o tipo (O yfinance básico mistura tudo como Dividends, 
            # mas para FIIs geralmente é Rendimento)
            div_type = "Provento"
            
            results.append({
                "dataPagamento": date_str, # O yfinance retorna a data Ex ou Pagamento dependendo do ativo, geralmente usamos como referência
                "valor": float(value),
                "tipo": div_type
            })
            
        # Ordena do mais recente para o mais antigo
        results.reverse()
        
        return results

    except Exception as e:
        print(f"Erro ao buscar dividendos de {symbol}: {e}")
        return []
        
    
@app.get("/calendar")
def get_calendar():
    try:
        response = requests.get(
            CALENDAR_URL,
            headers=CALENDAR_HEADERS,
            timeout=15
        )

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
            if importance >= 1:
                impact = "high"
            elif importance == 0:
                impact = "medium"
            else:
                impact = "low"

            if impact == "low":
                continue

            try:
                dt = datetime.fromisoformat(
                    item["date"].replace("Z", "+00:00")
                ).astimezone(
                    pytz.timezone("America/Sao_Paulo")
                )
                time = dt.strftime("%H:%M")
            except:
                time = "--:--"

            events.append({
                "id": item.get("id"),
                "time": time,
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
