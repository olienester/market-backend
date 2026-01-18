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
from services.wyckoff_service import analyze_wyckoff

app = FastAPI(title="Market Data API")

# ===============================
# CONFIGURAÇÃO DO FIREBASE
# ===============================
if not firebase_admin._apps:
    try:
        if os.path.exists("firebase_credentials.json"):
            cred = credentials.Certificate("firebase_credentials.json")
            firebase_admin.initialize_app(cred)
            print("✅ Firebase inicializado com sucesso!")
        else:
            print("⚠️ Aviso: Arquivo firebase_credentials.json não encontrado.")
    except Exception as e:
        print(f"❌ Erro ao inicializar Firebase: {e}")

try:
    db = firestore.client()
except:
    db = None
    print("⚠️ Firestore não disponível.")

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
# CONFIG RapidAPI
# ===============================
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
CALENDAR_URL = "https://economic-events-calendar.p.rapidapi.com/economic-events/tradingview"
CALENDAR_HEADERS = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": "economic-events-calendar.p.rapidapi.com"
}

# ===============================
# FUNÇÕES DE BANCO DE DADOS
# ===============================

def check_daily_log(today_str):
    """Verifica se já atualizamos a lista COMPLETA hoje."""
    if db is None: return False
    try:
        doc = db.collection("system_control").document("calendar_sync").get()
        if doc.exists:
            data = doc.to_dict()
            if data.get("last_checked_date") == today_str:
                return True 
        return False
    except Exception as e:
        print(f"Erro check log: {e}")
        return False

def update_daily_log(today_str):
    """Marca que hoje já atualizamos."""
    if db is None: return
    try:
        db.collection("system_control").document("calendar_sync").set({
            "last_checked_date": today_str,
            "updated_at": firestore.SERVER_TIMESTAMP
        })
    except Exception as e:
        print(f"Erro update log: {e}")

def load_events_from_db(today_str):
    """
    Busca eventos DO FUTURO (Hoje em diante).
    Não filtra só hoje, pega tudo que é >= hoje.
    """
    if db is None: return []
    try:
        # Pega eventos onde a data é maior ou igual a hoje
        docs = db.collection("economic_calendar")\
                 .where("date", ">=", today_str)\
                 .stream()
        
        events = []
        for doc in docs:
            events.append(doc.to_dict())
        
        # Ordena primeiro por data, depois por hora
        events.sort(key=lambda x: (x['date'], x['time']))
        return events
    except Exception as e:
        print(f"Erro leitura DB: {e}")
        return []

def save_events_to_db(events):
    """Salva a lista no banco com proteção contra IDs vazios"""
    if db is None or not events: return
    try:
        batch = db.batch()
        collection = db.collection("economic_calendar")
        
        count = 0
        for event in events:
            # --- CORREÇÃO DO ERRO DE CAMINHO ---
            raw_id = event.get('id')
            
            # Se o ID vier vazio ou None da API, criamos um ID artificial
            if not raw_id or raw_id == "None":
                # Ex: "2026-01-13_US_Fed_Interest"
                safe_title = str(event.get('title', 'unknown'))[:20].replace(" ", "_").replace("/", "-")
                safe_id = f"{event.get('date')}_{event.get('country')}_{safe_title}"
            else:
                safe_id = str(raw_id)
            
            # Garante que não tem barras '/' no ID (o que também quebra o path)
            safe_id = safe_id.replace("/", "_")

            # Agora é seguro criar a referência
            doc_ref = collection.document(safe_id)
            batch.set(doc_ref, event)
            
            count += 1
            if count >= 400:
                batch.commit()
                batch = db.batch()
                count = 0
        
        if count > 0: batch.commit()
        print(f"✅ {len(events)} eventos salvos no banco com sucesso.")
        
    except Exception as e:
        print(f"❌ Erro ao salvar eventos: {e}")

# ===============================
# ENDPOINT CALENDAR
# ===============================
@app.get("/calendar")
def get_calendar(force_refresh: bool = False):
    # Data de referência (Hoje)
    tz_sp = pytz.timezone("America/Sao_Paulo")
    today_str = datetime.now(tz_sp).strftime("%Y-%m-%d")

    # 1. VERIFICAÇÃO DE ECONOMIA (Trava Diária)
    # Se já atualizamos hoje, não chama a API, apenas lê o banco.
    if not force_refresh:
        if check_daily_log(today_str):
            print("🛑 Cache do dia válido. Retornando dados do banco.")
            return load_events_from_db(today_str)

    # 2. CHAMADA API (Atualiza a lista completa)
    print("🌍 Buscando TODAS as notícias na RapidAPI...")
    
    try:
        # Removemos 'from' e 'to' para pegar tudo o que a API mandar
        querystring = {"countries": "US,BR"}

        response = requests.get(
            CALENDAR_URL,
            headers=CALENDAR_HEADERS,
            params=querystring,
            timeout=15
        )

        events = []
        
        if response.status_code == 200:
            payload = response.json()
            data = payload.get("result", [])

            for item in data:
                country = item.get("country")
                importance = item.get("importance", 0)
                
                # Filtros de Importância
                if importance >= 1: impact = "high"
                elif importance == 0: impact = "medium"
                else: impact = "low"

                # Se quiser ignorar as "low", mantenha isso. 
                # Se quiser tudo, comente as duas linhas abaixo.
                if impact == "low": continue

                try:
                    dt = datetime.fromisoformat(item["date"].replace("Z", "+00:00")).astimezone(tz_sp)
                    date_fmt = dt.strftime("%Y-%m-%d")
                    time_fmt = dt.strftime("%H:%M")
                    
                    # AQUI MUDOU: Não filtramos mais "if date != today".
                    # Aceitamos todas as datas que vierem.
                    
                except:
                    date_fmt = None
                    time_fmt = "--:--"

                events.append({
                    "id": item.get("id"),
                    "date": date_fmt,
                    "time": time_fmt,
                    "country": country,
                    "impact": impact,
                    "title": item.get("title"),
                    "actual": item.get("actual") or "-",
                    "forecast": item.get("forecast") or "-"
                })
            
            # Salva tudo o que veio
            if events:
                save_events_to_db(events)
            
            # Ativa a trava: "Hoje já busquei as notícias da semana"
            update_daily_log(today_str)
            
            # Ordena para retorno imediato
            events.sort(key=lambda x: (x['date'], x['time']))
            return events

        else:
            print(f"❌ Erro API: {response.status_code}")
            return load_events_from_db(today_str) # Fallback

    except Exception as e:
        print(f"❌ Erro fatal: {e}")
        return load_events_from_db(today_str)

# ===============================
# OUTROS ENDPOINTS (MANTIDOS)
# ===============================
@app.get("/")
def root():
    return {"status": "Online"}

@app.get("/stock/{symbol}")
def stock(symbol: str, interval: str = "1d", period: str = "1y"):
    data = get_stock_data(symbol, interval, period)
    if not data: raise HTTPException(status_code=404, detail="N/A")
    return {"symbol": symbol, "data": data}
    
@app.get("/strategy/{symbol}")
def get_strategy(symbol: str):
    return calculate_probability(symbol) or {"error": "Erro"}

@app.get("/strategy/91/{symbol}")
def get_strategy_lw91(symbol: str, interval: str = "1d"):
    return calculate_lw91(symbol, interval)

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
            results.append({
                "dataPagamento": date.strftime("%d/%m/%Y"),
                "valor": float(value),
                "tipo": "Provento"
            })
        return list(reversed(results))
    except: return []

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
    except: return {"error": "not found"}

@app.get("/api/ranking")
def get_ranking_endpoint(sort_by: str = Query("shank")):
    return calculate_ranking(sort_by)

@app.get("/api/ranking/acoes/geral")
def get_ranking_geral(): return get_relatorio_geral_acoes()

@app.get("/api/ranking/usa/geral")
def get_ranking_usa_endpoint(): return get_relatorio_geral_usa()

@app.get("/analysis/wyckoff/{symbol}")
def get_wyckoff_analysis(
    symbol: str, 
    period: str = Query("6mo", description="Período de análise (ex: 1mo, 6mo, 1y)"),
    interval: str = Query("1d", description="Intervalo dos candles (ex: 60m, 1d, 1wk)")
):
    """
    Retorna a análise Wyckoff.
    Para 60m, use: /analysis/wyckoff/PETR4?period=1mo&interval=60m
    """
    
    # Validação simples para intraday (yfinance pede period curto para dados intraday)
    if interval == "60m" and period not in ["1mo", "5d", "1wk"]:
        period = "1mo" # Força 1 mês se pedir 60m, senão o yfinance pode falhar

    result = analyze_wyckoff(symbol, period=period, interval=interval)
    
    if not result:
        return {"error": "Não foi possível analisar o ativo. Histórico insuficiente."}
    
    return result
