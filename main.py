from fastapi import FastAPI, HTTPException
from services.market_data import get_stock_data
import requests
from datetime import datetime
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
def get_strategy_lw91(symbol: str):
    """
    Rota chamada pelo App React Native para verificar o Setup 9.1
    """
    result = calculate_lw91(symbol)
    
    if result is None:
        # Retorna 404 se não tiver dados ou o ticker for inválido
        # O seu app já tem um try/catch para lidar com isso silenciosamente
        raise HTTPException(status_code=404, detail="Dados insuficientes ou erro no cálculo")
        
    return result
    
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
