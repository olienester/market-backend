from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import requests
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import pytz
import os
import json

# --- IMPORTAÇÕES FIREBASE ---
import firebase_admin
from firebase_admin import credentials, firestore

# --- IMPORTAÇÕES DOS SERVIÇOS ---
from services.market_data import get_stock_data
from services.strategy import calculate_probability
from services.larry_williams import calculate_lw91
from services.ranking_service import calculate_ranking
from services.ranking_acoes_service import get_relatorio_geral_acoes
from services.ranking_usa_service import get_relatorio_geral_usa

app = FastAPI(title="Market Data API")

# ===============================
# CONFIGURAÇÃO DO FIREBASE
# ===============================
# Verifica se o Firebase já não foi inicializado para evitar erros no reload
if not firebase_admin._apps:
    try:
        # O arquivo 'firebase_credentials.json' deve existir na raiz (criado pelo Render Secret Files)
        if os.path.exists("firebase_credentials.json"):
            cred = credentials.Certificate("firebase_credentials.json")
            firebase_admin.initialize_app(cred)
            print("✅ Firebase inicializado com sucesso!")
        else:
            print("⚠️ Aviso: Arquivo firebase_credentials.json não encontrado.")
    except Exception as e:
        print(f"❌ Erro ao inicializar Firebase: {e}")

# Cliente do Banco de Dados
try:
    db = firestore.client()
except:
    db = None
    print("⚠️ Firestore não disponível (verifique as credenciais).")

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
# CACHE LOCAL (Backup)
# ===============================
CACHE_FILE = "calendar_cache.json"
CACHE_TTL_HOURS = 24

def load_cache():
    if not os.path.exists(CACHE_FILE):
        return None

    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)

        cache_time = datetime.fromisoformat(cache["timestamp"])
        if datetime.utcnow() - cache_time < timedelta(hours=CACHE_TTL_HOURS):
            return cache["data"]

    except Exception as e:
        print("Erro ao ler cache:", e)

    return None

def save_cache(data):
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "timestamp": datetime.utcnow().isoformat(),
                    "data": data
                },
                f,
                ensure_ascii=False,
                indent=2
            )
    except Exception as e:
        print("Erro ao salvar cache:", e)

# ===============================
# FUNÇÃO PARA SALVAR NO FIREBASE
# ===============================
def save_to_firestore(events):
    if db is None:
        return
    
    try:
        collection_ref = db.collection("economic_calendar")
        batch = db.batch()
        
        count = 0
        for event in events:
            # Usa o ID do evento como chave do documento para evitar duplicatas
            doc_id = str(event['id'])
            doc_ref = collection_ref.document(doc_id)
            
            # Adiciona o timestamp de quando foi salvo
            event_copy = event.copy()
            event_copy['saved_at'] = firestore.SERVER_TIMESTAMP
            
            batch.set(doc_ref, event_copy)
            count += 1
            
            # O Firestore tem limite de 500 operações por batch, mas seu calendário é menor
            if count >= 400:
                batch.commit()
                batch = db.batch()
                count = 0
                
        if count > 0:
            batch.commit()
            
        print(f"✅ {len(events)} eventos salvos/atualizados no Firebase.")
        
    except Exception as e:
        print(f"❌ Erro ao salvar no Firestore: {e}")

# ===============================
# MOCK (fallback)
# ===============================
mock_events = [
    {
        "id": "mock-1",
        "date": "2026-01-12",
        "time": "09:00",
        "country": "US",
        "impact": "high",
        "title": "Fed Interest Rate Decision",
        "actual": "-",
        "forecast": "-"
    }
]

# ===============================
# ENDPOINTS BÁSICOS
# ===============================
@app.get("/")
def root():
    return {"status": "API funcionando", "firebase": "Conectado" if db else "Desconectado"}

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
        raise HTTPException(status_code=404, detail="Erro no cálculo")
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
            results.append({
                "dataPagamento": date.strftime("%d/%m/%Y"),
                "valor": float(value),
                "tipo": "Provento"
            })

        return list(reversed(results))
    except Exception as e:
        print(f"Erro dividendos {symbol}:", e)
        return []

@app.get("/market/quote/{ticker}")
def get_quote(ticker: str):
    try:
        t = yf.Ticker(ticker + ".SA")
        info = t.info
        return {
            "symbol": ticker,
            "price": info.get("currentPrice"),
            "regularMarketChangePercent": info.get("regularMarketChangePercent", 0) * 100
        }
    except Exception:
        return {"error": "not found"}

# ===============================
# ENDPOINT CALENDAR
# ===============================
@app.get("/calendar")
def get_calendar(force_refresh: bool = False):
    # 1️⃣ Tenta cache primeiro (se não forçado refresh)
    if not force_refresh:
        cached = load_cache()
        if cached:
            return cached

    try:
        querystring = {"countries": "US,BR"} 

        response = requests.get(
            CALENDAR_URL,
            headers=CALENDAR_HEADERS,
            params=querystring,
            timeout=15
        )

        if response.status_code != 200:
            print("Status RapidAPI:", response.status_code)
            return mock_events

        payload = response.json()
        data = payload.get("result", [])

        events = []

        for item in data:
            country = item.get("country")

            # Filtra importância 
            importance = item.get("importance", 0)
            if importance >= 1:
                impact = "high"
            elif importance == 0:
                impact = "medium"
            else:
                impact = "low"

            if impact == "low":
                continue

            # Formata Data e Hora para Brasil (UTC-3)
            try:
                dt = datetime.fromisoformat(
                    item["date"].replace("Z", "+00:00")
                ).astimezone(
                    pytz.timezone("America/Sao_Paulo")
                )
                
                date = dt.strftime("%Y-%m-%d")
                time = dt.strftime("%H:%M")
                
            except:
                date = None
                time = "--:--"

            events.append({
                "id": item.get("id"),
                "date": date,
                "time": time,
                "country": country,
                "impact": impact,
                "title": item.get("title"),
                "actual": item.get("actual") or "-",
                "forecast": item.get("forecast") or "-"
            })

        # 2️⃣ Salva cache se vier dado real
        if events:
            save_cache(events)       # Salva no arquivo local (temporário)
            save_to_firestore(events) # SALVA NO FIREBASE (PERSISTENTE)
            return events

        return mock_events

    except Exception as e:
        print("Erro calendário:", e)
        return mock_events

# =========================================================
# RANKINGS
# =========================================================
@app.get("/api/ranking")
def get_ranking_endpoint(sort_by: str = Query("shank", enum=["shank", "smart"])):
    try:
        return calculate_ranking(sort_by)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ranking/acoes/geral")
def get_ranking_geral():
    return get_relatorio_geral_acoes()

@app.get("/api/ranking/usa/geral")
def get_ranking_usa_endpoint():
    try:
        return get_relatorio_geral_usa()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
