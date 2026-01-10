from fastapi import FastAPI, HTTPException
from services.market_data import get_stock_data
import requests
from googletrans import Translator

app = FastAPI(title="Market Data API")

translator = Translator()

# ===============================
# CONFIG RapidAPI
# ===============================
RAPIDAPI_KEY = "6953dbd60amshff67959b8365976p187260jsn89454cfdec94"

CALENDAR_URL = "https://economic-events-calendar.p.rapidapi.com/economic-events/tradingview"

CALENDAR_HEADERS = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": "economic-events-calendar.p.rapidapi.com"
}

CALENDAR_PARAMS = {
    "countries": "BR,US",
    "importance": "HIGH"
}


# ===============================
# Utils
# ===============================
def traduzir(texto: str):
    if not texto:
        return texto
    try:
        return translator.translate(texto, dest="pt").text
    except:
        return texto


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


@app.get("/calendar")
def get_calendar():
    # ===============================
    # Fallback de segurança
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

    try:
        response = requests.get(
            CALENDAR_URL,
            headers=CALENDAR_HEADERS,
            params=CALENDAR_PARAMS,
            timeout=15
        )

        if response.status_code != 200:
            print(f"Erro HTTP Calendar: {response.status_code}")
            return mock_events

        data = response.json()
        events = []

        for item in data:
            country = item.get("country")

            # Segurança extra
            if country not in ["BR", "US"]:
                continue

            events.append({
                "id": str(item.get("id")),
                "time": item.get("time"),
                "country": country,
                "impact": item.get("importance", "medium").lower(),
                "title": traduzir(item.get("title")),
                "actual": item.get("actual", "-"),
                "forecast": item.get("forecast", "-"),
                "previous": item.get("previous", "-"),
                "date": item.get("date")
            })

        if not events:
            return mock_events

        return {
            "source": "rapidapi-economic-calendar",
            "language": "pt-BR",
            "total": len(events),
            "events": events
        }

    except Exception as e:
        print(f"Erro Calendar API: {e}")
        return mock_events
