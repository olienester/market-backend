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
from google.cloud.firestore_v1 import FieldFilter  # sintaxe moderna de filtros

# ===============================
# Firebase: inicialização segura
# ===============================
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
# Sanitização JSON-safe (evita NaN/Inf no response)
# ===============================
def _df_json_safe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Torna o DataFrame seguro para serialização JSON:
    - Substitui ±inf por NaN
    - Converte NaN para None (null)
    """
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.where(pd.notnull(df), None)
    return df

def _records_json_safe(records: list) -> list:
    """
    Garantia extra: converte floats não finitos para None.
    """
    safe = []
    for rec in records:
        out = {}
        for k, v in rec.items():
            if isinstance(v, float):
                if not np.isfinite(v):
                    out[k] = None
                else:
                    out[k] = v
            else:
                out[k] = v
        safe.append(out)
    return safe

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
        query = db.collection("ranking_usa").where(filter=FieldFilter("date", "==", today_str))
        docs = query.stream()
        items = [d.to_dict() for d in docs]
        items.sort(key=lambda x: x.get("ativo", ""))
        for it in items:
            it.pop("date", None)
        items = _records_json_safe(items)
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

        # Tenta consulta com '!='; se não suportar no projeto/index, cai no fallback.
        try:
            query = col.where(filter=FieldFilter("date", "!=", today_str))
            docs_iter = query.stream()
        except Exception as qerr:
            print(f"ℹ️ Fallback delete: desigualdade não suportada ({qerr}). Listando todos e filtrando...")
            docs_iter = col.stream()

        for d in docs_iter:
            data = d.to_dict()
            if data.get("date") != today_str:
                batch.delete(d.reference)
                deleted += 1
                count += 1
                if count >= 400:  # abaixo do limite de 500
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
# Coletas de dados (sua lógica original)
# ===============================
def get_all_usa_tickers():
    """
    Baixa a lista OFICIAL completa de todos os ativos negociados na NASDAQ, NYSE e AMEX.
    Fonte: NASDAQ Trader
    """
    try:
        print("📥 Baixando lista completa de tickers dos EUA...")
        url = "http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
        s = requests.get(url).content
        df = pd.read_csv(io.BytesIO(s), sep='|')

        # Limpeza: Remove ETFs e Warrants (focando em empresas reais)
        df = df[df['ETF'] == 'N']
        df = df[df['Test Issue'] == 'N']

        tickers = df['Symbol'].tolist()

        print(f"✅ Encontrados {len(tickers)} ativos brutos.")
        return tickers
    except Exception as e:
        print(f"Erro ao baixar lista NASDAQ: {e}")
        # Fallback para S&P 500 se der erro na NASDAQ
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META']

def fetch_bulk_fundamentals(tickers_list):
    """
    Baixa dados em LOTES para ser mais rápido.
    """
    dados = []

    # Processa em lotes de 200 para não estourar a memória/API
    batch_size = 200
    total = len(tickers_list)

    print(f"🚀 Iniciando Scanner em {total} ativos...")

    for i in range(0, total, batch_size):
        lote = tickers_list[i:i+batch_size]
        try:
            # O yfinance permite baixar vários tickers de uma vez (apenas preço)
            # Mas para 'info' detalhada (P/L, Margens) precisamos instanciar.
            # O truque de performance: Baixar apenas preço e volume primeiro para filtrar "Micos"

            # 1. Filtro Rápido de Liquidez (Download em Massa)
            str_lote = " ".join(lote)
            tickers_data = yf.download(str_lote, period="1d", progress=False)['Close']

            # Se só retornou 1 linha ou erro, pula
            if tickers_data.empty:
                continue

            # Pega o último preço
            last_prices = tickers_data.iloc[-1]

            # Filtra o lote: Só analisa quem custa mais de $5.00 (Evita Penny Stocks perigosas)
            valid_tickers = last_prices[last_prices > 5.00].index.tolist()

            if not valid_tickers:
                continue

            # 2. Análise Profunda (Um por um apenas nos válidos)
            # Aqui é onde demora, mas é necessário para pegar P/L, ROE, Dívida
            for ticker in valid_tickers:
                try:
                    t = yf.Ticker(ticker)
                    # fast_info é MUITO mais rápido que .info no yfinance novo
                    price = t.fast_info.last_price
                    mkt_cap = t.fast_info.market_cap

                    # Se for muito pequeno (< 500M market cap), pula (Opcional)
                    if mkt_cap < 500_000_000:
                        continue

                    # Acessar .info é lento (requisição HTTP), faça só se passou nos filtros acima
                    info = t.info

                    dado = {
                        'ativo': ticker,
                        'setor': info.get('sector', 'Outros'),
                        'preco': price,
                        'dy': (info.get('dividendYield', 0) or 0),
                        'p_l': info.get('trailingPE', 0),
                        'p_vp': info.get('priceToBook', 0),
                        'roe': (info.get('returnOnEquity', 0) or 0),
                        'margem_liquida': (info.get('profitMargins', 0) or 0),
                        'div_liq_patrimonio': info.get('debtToEquity', 0) / 100 if info.get('debtToEquity') else 0,
                        'ev_ebit': info.get('enterpriseToEbitda', 0),
                        'lpa': info.get('trailingEps', 0),
                        'roic': (info.get('returnOnAssets', 0) or 0),  # Proxy
                        'cagr_lucros_5a': (info.get('earningsGrowth', 0) or 0) 
                    }
                    dados.append(dado)
                except:
                    continue

        except Exception as e:
            print(f"Erro no lote {i}: {e}")
            continue

    return pd.DataFrame(dados)

# ===============================
# Função principal (com cache diário no Firebase)
# ===============================
def get_relatorio_geral_usa():
    global _cache_usa_full

    today_str = _today_sp_str()

    # 1) Se já foi gerado hoje, retorna do Firestore
    if check_daily_log(today_str):
        cached = load_ranking_from_db(today_str)
        if cached:
            print("🛑 Cache diário (ranking USA) válido. Retornando dados do Firestore.")
            _cache_usa_full = {"timestamp": time.time(), "data": cached}
            return cached
        else:
            print("⚠️ Log diário marcado, mas sem dados no Firestore. Recalculando...")

    # 2) Cache em memória (1 dia) como fallback
    if _cache_usa_full["data"] is not None and (time.time() - _cache_usa_full["timestamp"] < CACHE_TIMEOUT):
        print("--- Usando Cache USA Full ---")
        return _cache_usa_full["data"]

    # 3) Fluxo original: coleta, análise e preparação
    # 1. Pega TODOS os tickers do mercado
    all_tickers = get_all_usa_tickers()

    # 2. Filtra e Baixa Fundamentos (Isso vai demorar na 1ª vez)
    # DICA: Para teste rápido, limite a lista: all_tickers[:1000]
    df = fetch_bulk_fundamentals(all_tickers[:500])  # Comece com 500 para testar a velocidade

    if df.empty:
        return []

    # 🔽 FILTRO: remove ações sem dividendos
    df = df[df['dy'] > 0]
    
    if df.empty:
        return []

    
    # --- APLICAÇÃO DAS ESTRATÉGIAS (Mesma lógica anterior) ---

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
    df['margem_seg'] = df.apply(lambda x: (x['valor_intrinseco'] - x['preco'])/x['valor_intrinseco'] if x['valor_intrinseco'] > 0 else -10, axis=1)
    df['RANKING_GRAHAM'] = df['margem_seg'].rank(ascending=False)

    # DÉCIO BAZIN
    df['preco_teto_bazin'] = (df['preco'] * (df['dy'])) / 0.04  # 4% Dólar
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

    # *** SANITIZAÇÃO JSON-SAFE ***
    df = _df_json_safe(df)

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

    # 5) Atualiza cache em memória e retorna (JSON-safe por garantia)
    final_data = _records_json_safe(final_data)
    _cache_usa_full = {"timestamp": time.time(), "data": final_data}

    return final_data
