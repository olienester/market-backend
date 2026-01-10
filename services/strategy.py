import yfinance as yf
import pandas as pd
import numpy as np

def calculate_probability(symbol: str, period: str = "1y"):
    # Garante o sufixo .SA para ações brasileiras
    ticker = symbol.upper()
    if not ticker.endswith('.SA') and not ticker.endswith(('BTC-USD', 'ETH-USD')):
        ticker = f"{ticker}.SA"

    try:
        # 1. Baixar dados históricos
        df = yf.download(ticker, period=period, progress=False)
        
        if df.empty or len(df) < 20:
            return None

        # Ajuste para novas versões do Pandas/Yfinance (remove MultiIndex nas colunas)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # 2. Cálculos
        # Gap: Diferença entre Abertura de Hoje e Fechamento de Ontem
        df['PrevClose'] = df['Close'].shift(1)
        df['Gap_Percent'] = ((df['Open'] - df['PrevClose']) / df['PrevClose']) * 100
        
        # Resultado: Se fechou positivo (Verde) ou Negativo (Vermelho)
        df['Is_Green'] = df['Close'] > df['Open']

        # 3. Pegar o cenário de HOJE (O último candle ainda em formação ou recém aberto)
        # Como o mercado pode estar aberto, pegamos o último dado disponível
        last_gap = df['Gap_Percent'].iloc[-1]
        last_price = df['Open'].iloc[-1]

        # 4. Encontrar dias no passado com Gap parecido (margem de tolerância de 0.2%)
        # Exemplo: Se hoje abriu com +0.5%, buscamos dias entre +0.3% e +0.7%
        tolerance = 0.2
        similar_days = df[
            (df['Gap_Percent'] >= (last_gap - tolerance)) & 
            (df['Gap_Percent'] <= (last_gap + tolerance))
        ]

        # Remove o dia atual da estatística histórica para não enviesar
        similar_days = similar_days[:-1]

        total_matches = len(similar_days)
        if total_matches == 0:
            return {
                "symbol": symbol,
                "msg": "Padrão inédito. Sem dados históricos suficientes."
            }

        # 5. Calcular Probabilidade
        green_days = similar_days['Is_Green'].sum()
        win_rate = (green_days / total_matches) * 100

        # Define a recomendação
        trend = "NEUTRO"
        if win_rate >= 60: trend = "COMPRA"
        elif win_rate <= 40: trend = "VENDA"

        return {
            "symbol": symbol,
            "current_open": round(last_price, 2),
            "current_gap": round(last_gap, 2),
            "similar_scenarios_found": int(total_matches),
            "win_rate_call": round(win_rate, 1), # Chance de ser Alta
            "trend": trend
        }

    except Exception as e:
        print(f"Erro na estratégia: {e}")
        return None
