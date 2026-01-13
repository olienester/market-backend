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
# Mantenha seus arquivos na pasta services como estão
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
# MOCK (Fallback em caso de erro grave)
# ===============================
mock_events = [
    {
        "id": "mock-1",
        "date": datetime.now().strftime("%Y-%m-%d"),
        "time": "09:00",
        "country": "US",
        "impact": "high",
        "title": "Dados Indisponíveis (Mock)",
        "actual": "-",
        "forecast": "-"
    }
]

# ===============================
# FUNÇÕES DE CONTROLE (O Segredo da Economia)
# ===============================

def check_daily_log(today_str):
    """
    Verifica no 'system_control' se já rodamos a API hoje.
    Retorna True se JÁ ATUALIZOU hoje.
    """
    if db is None: return False
    try:
        # Busca o documento de controle
        doc = db.collection("system_control").document("calendar_sync").get()
        if doc.exists:
            data = doc.to_dict()
            last_check = data.get("last_checked_date")
            # Se a data salva for igual a hoje, retorna True (Já gastamos a cota)
            if last_check == today_str:
                return True 
        return False
    except Exception as e:
        print(f"Erro ao ler log diário: {e}")
        return False

def update_daily_log(today_str):
    """
    Marca no banco que hoje já foi verificado.
    Isso impede novas chamadas à API até amanhã.
    """
    if db is None: return
    try:
        db.collection("system_control").document("calendar_sync").set({
            "last_checked_date": today_str,
            "updated_at": firestore.SERVER_TIMESTAMP
        })
        print(f"🔒 Trava diária ativada para: {today_str}")
    except Exception as e:
        print(f"Erro ao atualizar log diário: {e}")

def load_events_from_db(date_str):
    """Busca eventos salvos no banco para a data especificada"""
    if db is None: return []
    try:
        # Filtra apenas eventos da data solicitada
        docs = db.collection("economic_calendar")\
                 .where("date", "==", date_str)\
                 .stream()
        
        events = []
        for doc in docs:
            events.append(doc.to_dict())
        
        # Ordena por horário
        events.sort(key=lambda x: x['time'])
        return events
    except Exception:
        return []

def save_events_to_db(events):
    """Salva a lista de eventos no Firestore usando Batch (Lote)"""
    if db is None or not events: return
    try:
        batch = db.batch()
        collection = db.collection("economic_calendar")
        
        count = 0
        for event in events:
            # Usa o ID do evento como ID do documento
            doc_ref = collection.document(str(event['id']))
            batch.set(doc_ref, event)
            count += 1
            # Firestore limita batch a 500 operações
            if count >= 400:
                batch.commit()
                batch = db.batch()
                count = 0
        
        if count > 0: batch.commit()
        print(f"✅ {len(events)} eventos salvos no banco.")
    except Exception as e:
        print(f"Erro ao salvar eventos: {e}")

# ===============================
# ENDPOINT INTELIGENTE (CALENDÁRIO)
# ===============================
@app.get("/calendar")
def get_calendar(force_refresh: bool = False):
    # 1. Define a data de HOJE no fuso horário do Brasil
    tz_sp = pytz.timezone("America/Sao_Paulo")
    today_str = datetime.now(tz_sp).strftime("%Y-%m-%d")

    print(f"📅 Requisição para data: {today_str}")

    # 2. VERIFICAÇÃO DE ECONOMIA (Check-in no Banco)
    if not force_refresh:
        already_checked = check_daily_log(today_str)
        
        if already_checked:
            print("🛑 Cota diária já utilizada. Retornando dados do Banco (Cache).")
            db_events = load_events_from_db(today_str)
            return db_events # Retorna lista (pode ser vazia se hoje não tiver notícias)

    # 3. CHAMADA À API (Só acontece 1x por dia)
    print("🌍 Iniciando chamada para RapidAPI (Gasto de Crédito)...")
    
    try:
        # Pede apenas os dados de HOJE (from/to)
        querystring = {
            "countries": "US,BR",
            "from": today_str,
            "to": today_str
        }

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

                if impact == "low": continue

                # Tratamento de Data/Hora
                try:
                    dt = datetime.fromisoformat(item["date"].replace("Z", "+00:00")).astimezone(tz_sp)
                    date_fmt = dt.strftime("%Y-%m-%d")
                    time_fmt = dt.strftime("%H:%M")
                    
                    # Segurança extra: Garante que é evento de hoje
                    if date_fmt != today_str: continue
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
            
            # 4. SALVA OS DADOS (Se houver eventos)
            if events:
                save_events_to_db(events)
            else:
                print("⚠️ API retornou 0 eventos relevantes para hoje.")

        else:
            print(f"❌ Erro API: {response.status_code}")
        
        # 5. O PASSO MAIS IMPORTANTE: ATIVA A TRAVA
        # Marcamos que hoje já foi verificado, independente se veio dado ou não.
        # Isso impede que o sistema fique tentando de novo e gastando crédito.
        update_daily_log(today_str)
        
        return events

    except Exception as e:
        print(f"❌ Erro fatal no calendário: {e}")
        # Se deu erro, tenta ler o que tem no banco por garantia
        return load_events_from_db(today_str)

# ===============================
# OUTROS ENDPOINTS (MANTIDOS)
# ===============================
@app.get("/")
def root():
    return {"status": "API Online", "firebase": "Conectado" if db else "Offline"}

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
