import yfinance as yf
import pandas as pd
import numpy as np
import requests
import io
import time
from datetime import datetime
import pytz
import os

# --- FIREBASE ---
import firebase_admin
from firebase_admin import credentials, firestore

# ===============================
# Firebase: inicialização segura
# ===============================
# Se o app já estiver inicializado (via main.py), só pega o client; senão, tenta inicializar localmente.
if not firebase_admin._apps:
    try:
        if os.path.exists("firebase_credentials.json"):
            cred = credentials.Certificate("firebase_credentials.json")
            firebase_admin.initialize_app(cred)
            print("✅ Firebase (ranking_usa) inicializado com sucesso!")
        else:
            print("⚠️ Aviso: firebase_credentials.json não encontrado (ranking_usa). Usando ambiente atual se houver.")
    except Exception as e:
        print(f"❌ Erro ao inicializar Firebase em ranking_usa_service: {e}")

try:
    db = firestore.client()
except Exception as e:
    db = None
    print(f"⚠️ Firestore não disponível em ranking_usa_service: {e}")

# ===============================
# Cache em memória (1 dia)
# ===============================
CACHE_TIMEOUT = 86400  # 1 dia
_cache_usa_full = {"timestamp": 0, "data": None}

# ===============================
# Utilitários de data (Brasil)
# ===============================
def _today_sp_str():
    tz_sp = pytz.timezone("America/Sao_Paulo")
    return datetime.now(tz_sp).strftime("%Y-%m-%d")

# ===============================
# Funções de banco (cache diário)
# ===============================
def check_daily_log(today_str: str) -> bool:
    """Verifica se o ranking USA já foi gerado e marcado hoje."""
    if db is None:
        return False
    try:
        doc = db.collection("system_control").document("ranking_usa_sync").get()
        if doc.exists:
            data = doc.to_dict()
            return data.get("last_checked_date") == today_str
        return False
    except Exception as e:
        print(f"Erro check_daily_log(ranking_usa): {e}")
        return False

def update_daily_log(today_str: str):
    """Marca no system_control que o ranking USA do dia foi gerado."""
    if db is None:
        return
    try:
        db.collection("system_control").document("ranking_usa_sync").set({
            "last_checked_date": today_str,
            "updated_at": firestore.SERVER_TIMESTAMP
        })
    except Exception as e:
        print(f"Erro update_daily_log(ranking_usa): {e}")

def load_ranking_from_db(today_str: str):
    """Carrega todos os registros do ranking USA (apenas do dia informado)."""
    if db is None:
        return []
    try:
        docs = db.collection("ranking_usa").where("date", "==", today_str).stream()
        items = [d.to_dict() for d in docs]
        # Ordena por ticker (ativo) para retorno consistente
        items.sort(key=lambda x: x.get("ativo", ""))
        # Remove o campo 'date' da resposta (frontend compatível)
        for it in items:
            if "date" in it:
                it.pop("date")
        return items
    except Exception as e:
        print(f"Erro load_ranking_from_db(usa): {e}")
        return []

def delete_old_ranking(today_str: str):
    """Apaga dados antigos (date != hoje) antes de salvar o dia atual."""
    if db is None:
        return
    try:
        col = db.collection("ranking_usa")
        deleted = 0
        batch = db.batch()
        count = 0

        # Tenta usar query desigualdade; se não suportado, faz fallback com stream completo.
        try:
            docs = col.where("date", "!=", today_str).stream()
        except Exception as qerr:
            print(f"ℹ️ Fallback delete: desigualdade não suportada ({qerr}). Listando todos e filtrando...")
            docs = col.stream()

        for d in docs:
            data = d.to_dict()
            if data.get("date") != today_str:
                batch.delete(d.reference)
                deleted += 1
                count += 1
                if count >= 400:
                    batch.commit()
                    batch = db.batch()
                    count = 0

        if count > 0:
            batch.commit()

        print(f"🗑️ Removidos {deleted} documentos antigos de ranking_usa.")
    except Exception as e:
        print(f"❌ Erro delete_old_ranking(usa): {e}")

def save_ranking_to_db(records: list, today_str: str):
    """Salva o ranking no Firestore (um doc por ativo) para o dia atual."""
    if db is None or not records:
        return
    try:
        batch = db.batch()
        col = db.collection("ranking_usa")
        count = 0

        for rec in records:
            data_to_save = dict(rec)
            data_to_save["date"] = today_str

            ativo = str(rec.get("ativo", "UNKNOWN")).replace("/", "_")
            doc_id = f"{today_str}_{ativo}"

            doc_ref = col.document(doc_id)
            batch.set(doc_ref, data_to_save)

            count += 1
            if count >= 400:  # abaixo do limite 500
                batch.commit()
                batch = db.batch()
                count = 0

        if count > 0:
            batch.commit()

        print(f"✅ {len(records)} registros do ranking USA salvos para {today_str}.")
    except Exception as e:
        print(f"❌ Erro save_ranking_to_db(usa): {e}")

# ===============================
# Coletas de dados (sua lógica)
# ===============================
def get_all_usa_tickers():
    """
    Baixa a lista OFICIAL completa de todos os ativos negociados na NASDAQ, NYSE e AMEX.
    Fonte: NASDAQ Trader
    """
    try:
        print("📥 Baixando lista completa de tickers dos EUA...")
        url = "http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
        s = requests.get(url, timeout=20).content
        df = pd.read_csv(io.BytesIO(s), sep='|')

        # Limpeza: Remove ETFs e Test Issue
        df = df[df['ETF'] == 'N']
        df = df[df['Test Issue'] == 'N']

        tickers = df['Symbol'].tolist()
        print(f"✅ Encontrados {len(tickers)} ativos brutos.")
        return tickers
    except Exception as e:
        print(f"Erro ao baixar lista NASDAQ: {e}")
        # Fallback para S&P 500 se der erro
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META']

def fetch_bulk_fundamentals(tickers_list):
    """
    Baixa dados em LOTES para ser mais rápido.
    """
    dados = []

    batch_size = 200
    total = len(tickers_list)
    print(f"🚀 Iniciando Scanner em {total} ativos...")

    for i in range(0, total, batch_size):
        lote = tickers_list[i:i+batch_size]
        try:
            str_lote = " ".join(lote)
            tickers_data = yf.download(str_lote, period="1d", progress=False)['Close']

            if tickers_data.empty:
                continue

            last_prices = tickers_data.iloc[-1]
            valid_tickers = last_prices[last_prices > 5.00].index.tolist()
            if not valid_tickers:
                continue

            # Análise detalhada somente nos válidos
            for ticker in valid_tickers:
                try:
                    t = yf.Ticker(ticker)
                    price = getattr(t.fast_info, "last_price", None)
                    mkt_cap = getattr(t.fast_info, "market_cap", None)

                    # Se não tiver fast_info válido, tenta via .info
                    if price is None or mkt_cap is None:
                        info_probe = t.info or {}
                        price = info_probe.get("currentPrice", 0)
                        mkt_cap = info_probe.get("marketCap", 0)

                    # Filtra por market cap (opcional)
                    if mkt_cap and mkt_cap < 500_000_000:
                        continue

                    info = t.info or {}

                    dado = {
                        'ativo': ticker,
                        'setor': info.get('sector', 'Outros'),
                        'preco': float(price) if price else 0.0,
                        'dy': float((info.get('dividendYield', 0) or 0) * 100),
                        'p_l': float(info.get('trailingPE', 0) or 0),
                        'p_vp': float(info.get('priceToBook', 0) or 0),
                        'roe': float((info.get('returnOnEquity', 0) or 0) * 100),
                        'margem_liquida': float((info.get('profitMargins', 0) or 0) * 100),
                        'div_liq_patrimonio': float(info.get('debtToEquity', 0) / 100) if info.get('debtToEquity') else 0.0,
                        'ev_ebit': float(info.get('enterpriseToEbitda', 0) or 0),
                        'lpa': float(info.get('trailingEps', 0) or 0),
                        'roic': float((info.get('returnOnAssets', 0) or 0) * 100),  # Proxy
                        'cagr_lucros_5a': float((info.get('earningsGrowth', 0) or 0) * 100)
                    }
                    dados.append(dado)
                except Exception:
                    continue

        except Exception as e:
            print(f"Erro no lote {i}: {e}")
            continue

    return pd.DataFrame(dados)

# ===============================
# Função principal (cache diário)
# ===============================
def get_relatorio_geral_usa():
    global _cache_usa_full

    today_str = _today_sp_str()

    # 1) Se já foi gerado hoje, retorna do Firestore
    if check_daily_log(today_str):
        cached = load_ranking_from_db(today_str)
        if cached:
            print("🛑 Cache diário (ranking USA) válido. Retornando dados do Firestore.")
            _cache_usa_full = {"timestamp": time.time(), "data": cached}  # reforça cache memória
            return cached
        else:
            print("⚠️ Log diário marcado, mas sem dados no Firestore. Recalculando...")

    # 2) Cache em memória (1 dia) como fallback para reduzir recomputações
    if _cache_usa_full["data"] is not None and (time.time() - _cache_usa_full["timestamp"] < CACHE_TIMEOUT):
        print("--- Usando Cache USA Full (memória) ---")
        return _cache_usa_full["data"]

    # 3) Coleta completa (demorada na primeira vez do dia)
    all_tickers = get_all_usa_tickers()
    df = fetch_bulk_fundamentals(all_tickers[:500])  # ajuste aqui conforme desempenho desejado

    if df.empty:
        return []

    # --- Estratégias (mesma lógica) ---
    # JOEL GREENBLATT
    df['earning_yield'] = df['ev_ebit'].apply(lambda x: 1/x if x > 0 else 0)
    df['score_joel'] = df['earning_yield'].rank(ascending=False) + df['roic'].rank(ascending=False)
    df['RANKING_JOEL'] = df['score_joel'].rank(ascending=True)

    # BENJAMIN GRAHAM
    df['vpa'] = df.apply(lambda x: x['preco'] / x['p_vp'] if x['p_vp'] > 0 else 0, axis=1)
    def calc_graham(row):
        if row['lpa'] > 0 and row['vpa'] > 0:
            return np.sqrt(22.5 * row['lpa'] * row['vpa'])
        return 0
    df['valor_intrinseco'] = df.apply(calc_graham, axis=1)
    df['margem_seg'] = df.apply(
        lambda x: (x['valor_intrinseco'] - x['preco'])/x['valor_intrinseco'] if x['valor_intrinseco'] > 0 else -10,
        axis=1
    )
    df['RANKING_GRAHAM'] = df['margem_seg'].rank(ascending=False)

    # DÉCIO BAZIN
    df['preco_teto_bazin'] = (df['preco'] * (df['dy']/100)) / 0.04  # 4% dólar
    df['upside_bazin'] = (df['preco_teto_bazin'] / df['preco']) - 1
    df['RANKING_BAZIN'] = (df['upside_bazin'].rank(ascending=False) + df['dy'].rank(ascending=False)).rank(ascending=True)

    # LUIZ BARSI
    SETORES_BEST_EN = ['Financial', 'Utilities', 'Energy', 'Insurance', 'Communication']
    df['score_barsi'] = (df['dy'].rank(ascending=False)*2) + df['p_vp'].rank(ascending=True) + df['roe'].rank(ascending=False)
    def ajuste_setor(row):
        setor = str(row.get('setor', ''))
        if any(s in setor for s in SETORES_BEST_EN):
            return row['score_barsi'] * 0.8
        return row['score_barsi']
    df['RANKING_BARSI'] = df.apply(ajuste_setor, axis=1).rank(ascending=True)

    # Finalização
    cols_float = ['preco', 'dy', 'p_l', 'p_vp', 'valor_intrinseco', 'preco_teto_bazin']
    for col in cols_float:
        if col in df.columns:
            df[col] = df[col].round(2)

    cols_export = [
        'ativo', 'setor', 'preco', 'dy', 'p_l', 'p_vp',
        'RANKING_JOEL', 'RANKING_GRAHAM', 'RANKING_BAZIN', 'RANKING_BARSI',
        'valor_intrinseco', 'preco_teto_bazin'
    ]
    final_data = df[cols_export].to_dict(orient='records')

    # 4) Persistência no Firestore: apaga antigos, salva atuais e marca log
    if db is not None:
        try:
            delete_old_ranking(today_str)           # apaga tudo que não seja hoje
            save_ranking_to_db(final_data, today_str)
            update_daily_log(today_str)
        except Exception as e:
            print(f"⚠️ Falha ao salvar/atualizar log do ranking USA: {e}")

    # 5) Atualiza cache em memória e retorna
    _cache_usa_full = {"timestamp": time.time(), "data": final_data}
    return final_data
