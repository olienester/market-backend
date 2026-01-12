import pandas as pd
import numpy as np
import cloudscraper
import io
import time

# Configurações de Cache
CACHE_TIMEOUT = 3600  # 1 hora
_cache_acoes = {"timestamp": 0, "data": None}

# ==============================================================================
# 1. MAPA DE SETORES (Baseado na sua lista)
# ==============================================================================
MAPA_SETORES = {
    'ABCB4': 'Financeiro', 'BBAS3': 'Financeiro', 'BBDC3': 'Financeiro', 'BBDC4': 'Financeiro',
    'BBSE3': 'Seguros', 'BMEB3': 'Financeiro', 'BMEB4': 'Financeiro', 'BMGB4': 'Financeiro',
    'BPAC11': 'Financeiro', 'BPAN4': 'Financeiro', 'BRAP3': 'Materiais Básicos', 'BRAP4': 'Materiais Básicos',
    'BRSR6': 'Financeiro', 'CXSE3': 'Seguros', 'ITUB3': 'Financeiro', 'ITUB4': 'Financeiro',
    'ITSA3': 'Financeiro', 'ITSA4': 'Financeiro', 'PSSA3': 'Seguros', 'SANB11': 'Financeiro',
    'SANB3': 'Financeiro', 'SANB4': 'Financeiro', 'SULA11': 'Seguros', 'IRBR3': 'Seguros',
    # Energia e Utilidade Pública (E do BEST)
    'AESB3': 'Energia', 'ALUP11': 'Energia', 'AURE3': 'Energia', 'CMIG3': 'Energia',
    'CMIG4': 'Energia', 'CPFE3': 'Energia', 'CPLE3': 'Energia', 'CPLE6': 'Energia',
    'EGIE3': 'Energia', 'ELET3': 'Energia', 'ELET6': 'Energia', 'ENBR3': 'Energia',
    'ENGI11': 'Energia', 'EQTL3': 'Energia', 'LIGT3': 'Energia', 'NEOE3': 'Energia',
    'TAEE11': 'Energia', 'TAEE3': 'Energia', 'TAEE4': 'Energia', 'TRPL4': 'Energia',
    # Saneamento (S do BEST)
    'CSMG3': 'Saneamento', 'SAPR11': 'Saneamento', 'SAPR3': 'Saneamento', 'SAPR4': 'Saneamento',
    'SBSP3': 'Saneamento', 'AMBP3': 'Saneamento',
    # Telecom (T do BEST)
    'VIVT3': 'Telecomunicações', 'TIMS3': 'Telecomunicações', 'OIBR3': 'Telecomunicações',
    'OIBR4': 'Telecomunicações', 'TELB3': 'Telecomunicações', 'TELB4': 'Telecomunicações',
    # Outros Setores (Para preencher o resto)
    'VALE3': 'Mineração', 'PETR3': 'Petróleo', 'PETR4': 'Petróleo', 'WEGE3': 'Indústria',
    'MGLU3': 'Varejo', 'VIIA3': 'Varejo', 'LREN3': 'Varejo', 'JBSS3': 'Alimentos',
    'MRFG3': 'Alimentos', 'BEEF3': 'Alimentos', 'CSNA3': 'Siderurgia', 'GGBR4': 'Siderurgia',
    'GOAU4': 'Siderurgia', 'USIM5': 'Siderurgia', 'SUZB3': 'Papel e Celulose', 'KLBN11': 'Papel e Celulose',
    'RAIL3': 'Logística', 'CCRO3': 'Logística', 'ECOR3': 'Logística', 'RENT3': 'Logística',
    'HAPV3': 'Saúde', 'RDOR3': 'Saúde', 'FLRY3': 'Saúde', 'RADL3': 'Saúde',
    'CYRE3': 'Construção', 'EZTC3': 'Construção', 'MRVE3': 'Construção', 'JHSF3': 'Imobiliário'
    # ... O código tentará buscar no seu Excel, mas aqui coloquei os principais para garantir
}

# Lista de Setores que o Barsi gosta (BEST)
SETORES_BARSI_BEST = [
    'Bancário', 'Financeiro', # B
    'Energia', 'Utilidade Pública', 'Petróleo', # E (Barsi considera Energia e Utilities)
    'Saneamento', 'Água', # S
    'Seguros', 'Previdência', # S
    'Telecomunicações', 'Telecom' # T
]

def fetch_fundamentus_acoes():
    """Baixa e trata os dados de AÇÕES do Fundamentus"""
    global _cache_acoes
    
    if _cache_acoes["data"] is not None and (time.time() - _cache_acoes["timestamp"] < CACHE_TIMEOUT):
        return _cache_acoes["data"]

    url = 'https://www.fundamentus.com.br/resultado.php'
    
    try:
        scraper = cloudscraper.create_scraper()
        response = scraper.get(url)
        
        if response.status_code != 200:
            raise Exception(f"Status Code: {response.status_code}")
        
        # O Fundamentus usa table[0] para ações
        df = pd.read_html(io.BytesIO(response.content), decimal=',', thousands='.')[0]

        # Renomear colunas
        rename_map = {
            'Papel': 'ativo',
            'Cotação': 'preco',
            'P/L': 'p_l',
            'P/VP': 'p_vp',
            'Div.Yield': 'dy',
            'EV/EBIT': 'ev_ebit',
            'ROIC': 'roic',
            'ROE': 'roe',
            'Liq.2meses': 'liq_media_diaria',
            'Mrg. Líq.': 'margem_liquida',
            'Cresc. Rec.5a': 'cagr_lucros_5a',
            'Dív.Brut/ Patrim.': 'div_liq_patrimonio'
        }
        df.rename(columns=rename_map, inplace=True)

        # === AQUI APLICAMOS O SEU MAPA DE SETORES ===
        # Se o ativo estiver no dicionário, pega o setor. Se não, coloca "Outros".
        df['setor'] = df['ativo'].map(MAPA_SETORES).fillna('Outros')

        # Limpeza de Strings (% e pontos)
        cols_percent = ['dy', 'roic', 'roe', 'margem_liquida', 'cagr_lucros_5a']
        for col in cols_percent:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace('%', '').str.replace(',', '.').replace('.', '')
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        cols_num = ['preco', 'p_l', 'p_vp', 'ev_ebit', 'liq_media_diaria', 'div_liq_patrimonio']
        for col in cols_num:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # LPA (Lucro por Ação) = Preço / P_L
        df['lpa'] = df.apply(lambda x: x['preco'] / x['p_l'] if x['p_l'] > 0 else 0, axis=1)

        _cache_acoes = {"timestamp": time.time(), "data": df}
        return df

    except Exception as e:
        print(f"Erro scraping ações: {e}")
        if _cache_acoes["data"] is not None: return _cache_acoes["data"]
        return pd.DataFrame()

def calculate_acoes_ranking(sort_by: str = 'joel'):
    """
    Calcula Rankings com base no setor mapeado.
    """
    df = fetch_fundamentus_acoes().copy()

    if df.empty: return []

    # 1. FILTROS (VBA)
    df = df[df['liq_media_diaria'] >= 200000] # Liquidez
    df = df[df['p_l'] > 0] # Lucro positivo
    df = df[(df['dy'] > 0) & (df['dy'] < 50)] # Tem dividendos
    
    # 2. JOEL GREENBLATT
    df['earning_yield'] = df['ev_ebit'].apply(lambda x: 1/x if x > 0 else 0)
    df['rank_ey'] = df['earning_yield'].rank(ascending=False)
    df['rank_roic'] = df['roic'].rank(ascending=False)
    df['score_joel'] = df['rank_ey'] + df['rank_roic']
    df['ranking_joel'] = df['score_joel'].rank(ascending=True) 

    # 3. BENJAMIN GRAHAM
    def calc_graham_vi(row):
        lpa = row['lpa']
        cagr = row['cagr_lucros_5a']
        if lpa > 0:
            return (lpa * (8.5 + 2 * cagr) * 4.4) / 22.5
        return 0

    df['valor_intrinseco'] = df.apply(calc_graham_vi, axis=1)
    df['margem_seguranca'] = df.apply(lambda x: (x['valor_intrinseco'] - x['preco']) / x['valor_intrinseco'] if x['valor_intrinseco'] > 0 else -10, axis=1)
    
    df['ranking_graham'] = (df['margem_seguranca'].rank(ascending=False) + df['p_vp'].rank(ascending=True)).rank(ascending=True)

    # 4. DÉCIO BAZIN
    df['preco_teto_bazin'] = (df['preco'] * df['dy']) / 6
    df['upside_bazin'] = (df['preco_teto_bazin'] / df['preco']) - 1
    df['ranking_bazin'] = (df['upside_bazin'].rank(ascending=False) + df['dy'].rank(ascending=False)).rank(ascending=True)

    # 5. LUIZ BARSI (Agora usando a coluna 'setor' mapeada)
    r_dy = df['dy'].rank(ascending=False) * 2
    r_pvp = df['p_vp'].rank(ascending=True)
    r_roe = df['roe'].rank(ascending=False)
    r_margem = df['margem_liquida'].rank(ascending=False)
    r_divida = df['div_liq_patrimonio'].rank(ascending=True)
    
    df['score_barsi_raw'] = r_dy + r_pvp + r_roe + r_margem + r_divida
    
    # Função para verificar se o setor é BEST
    def ajustar_setor(row):
        score = row['score_barsi_raw']
        setor_atual = str(row['setor'])
        
        # Verifica se o setor da ação está na lista BEST
        if setor_atual in SETORES_BARSI_BEST:
            return score * 0.8 # Bônus de 20%
        return score

    df['score_barsi_final'] = df.apply(ajustar_setor, axis=1)
    df['ranking_barsi'] = df['score_barsi_final'].rank(ascending=True)

    # Ordenação Final
    sort_column = 'ranking_joel'
    if sort_by == 'graham': sort_column = 'ranking_graham'
    elif sort_by == 'bazin': sort_column = 'ranking_bazin'
    elif sort_by == 'barsi': sort_column = 'ranking_barsi'

    df = df.sort_values(sort_column, ascending=True)
    
    cols_float = ['preco', 'dy', 'p_l', 'valor_intrinseco', 'preco_teto_bazin']
    for col in cols_float:
        if col in df.columns:
            df[col] = df[col].round(2)

    # Adicionei 'setor' na exportação para o App mostrar
    cols_export = [
        'ativo', 'setor', 'preco', 'dy', 'p_l', 'p_vp', 
        'ranking_joel', 'ranking_graham', 'ranking_bazin', 'ranking_barsi',
        'valor_intrinseco', 'preco_teto_bazin'
    ]
    
    return df[cols_export].head(100).to_dict(orient='records')
