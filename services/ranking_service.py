import pandas as pd
import numpy as np
import cloudscraper
import io
import time

# Configurações de Cache
CACHE_TIMEOUT = 3600  # 1 hora
_cache_data = {"timestamp": 0, "data": None}

def fetch_fundamentus_data():
    """Baixa e trata os dados do Fundamentus usando Cloudscraper"""
    global _cache_data
    
    # Verifica cache
    if _cache_data["data"] is not None and (time.time() - _cache_data["timestamp"] < CACHE_TIMEOUT):
        return _cache_data["data"]

    url = 'https://www.fundamentus.com.br/fii_resultado.php'
    
    try:
        # CRIA UM SCRAPER PARA FINGIR SER UM NAVEGADOR
        scraper = cloudscraper.create_scraper()
        response = scraper.get(url)
        
        if response.status_code != 200:
            raise Exception(f"Status Code: {response.status_code}")
        
        # Lê a tabela HTML
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

        # Limpeza de dados
        for col in ['dy', 'cap_rate', 'vacancia']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace('%', '').str.replace(',', '.').replace('nan', '0')
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        df['pvp'] = pd.to_numeric(df['pvp'], errors='coerce').fillna(0)
        df['liquidez'] = pd.to_numeric(df['liquidez'], errors='coerce').fillna(0)
        
        # Atualiza cache
        _cache_data = {"timestamp": time.time(), "data": df}
        
        return df
        
    except Exception as e:
        print(f"Erro no scraping: {e}")
        if _cache_data["data"] is not None:
            return _cache_data["data"]
        raise e

def calculate_ranking(sort_by: str):
    """Aplica filtros, calcula Shank/Smart e retorna a lista ordenada"""
    
    # 1. Baixa os dados brutos (ou pega do cache)
    df = fetch_fundamentus_data().copy()
    
    # =========================================================
    # APLICAÇÃO DOS FILTROS
    # =========================================================
    # Filtro de Liquidez Diária (R$ 200.000,00)
    df = df[df['liquidez'] >= 200000]

    # Filtro de P/VP e DY
    df = df[df['pvp'] > 0]
    df = df[df['dy'] > 0]

    # Filtro de Setor (Remove Desenvolvimento)
    df = df[~df['setor'].astype(str).str.contains("Desenvolvimento", case=False, na=False)]

    if df.empty:
        return []

    # =========================================================
    # CÁLCULO 1: SHANK
    # =========================================================
    df['rank_pvp'] = df['pvp'].rank(ascending=True)
    df['rank_dy'] = df['dy'].rank(ascending=False)
    df['shank_valor'] = df['rank_pvp'] + df['rank_dy']
    df['shank_pos'] = df['shank_valor'].rank(ascending=True)

    # =========================================================
    # CÁLCULO 2: SMART FIIS
    # =========================================================
    rentab_acum = 0 
    volatilidade = 0 
    liq_log = np.log1p(df['liquidez'])
    
    df['smart_valor'] = (
        (df['dy'] * 25 + rentab_acum * 25) * 0.5 + 
        ((1 - df['pvp']) * 15 + (1 / (1 + volatilidade)) * 15) * 0.3 + 
        (liq_log * 10) * 0.2
    )
    df['smart_pos'] = df['smart_valor'].rank(ascending=False)

    # =========================================================
    # ORDENAÇÃO E FORMATAÇÃO
    # =========================================================
    if sort_by == 'smart':
        df = df.sort_values('smart_pos', ascending=True)
    else:
        df = df.sort_values('shank_pos', ascending=True)
        
    # Arredondamentos
    df['shank_valor'] = df['shank_valor'].round(2)
    df['smart_valor'] = df['smart_valor'].round(2)
    df['shank_pos'] = df['shank_pos'].astype(int)
    df['smart_pos'] = df['smart_pos'].astype(int)

    # Seleção de colunas
    cols = ['ticker', 'setor', 'dy', 'pvp', 'liquidez', 'shank_pos', 'shank_valor', 'smart_pos', 'smart_valor']
    
    return df[cols].to_dict(orient='records')
