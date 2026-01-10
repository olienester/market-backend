import yfinance as yf
import pandas as pd
import numpy as np

def calculate_lw91(symbol: str):
    # Garante o sufixo .SA e normaliza
    ticker_clean = symbol.replace('.SA', '').upper()
    ticker = f"{ticker_clean}.SA"
    
    try:
        # CORREÇÃO PRINCIPAL: Usar yf.Ticker() cria uma instância isolada para cada thread.
        # Isso evita que o dado da PETR4 misture com o da VALE3 quando chamados juntos.
        asset = yf.Ticker(ticker)
        
        # Baixa histórico recente (3 meses é suficiente para MME9)
        # auto_adjust=True já traz os preços ajustados por dividendos/splits
        df = asset.history(period="3mo", interval="1d", auto_adjust=True)
        
        if df.empty or len(df) < 15:
            return None
            
        # Limpeza de dados básica
        df = df[['Close', 'High', 'Low']].copy()
        
        # Cálculo da MME9 (Média Móvel Exponencial de 9)
        df['EMA9'] = df['Close'].ewm(span=9, adjust=False).mean()
        
        # Pega os 3 últimos candles para ver a "virada"
        last_rows = df.tail(3)
        
        # Garante que temos 3 linhas para comparar
        if len(last_rows) < 3:
            return None

        # Definição dos candles (Posição relativa: -1 é atual, -2 ontem, -3 anteontem)
        current = last_rows.iloc[-1]
        prev = last_rows.iloc[-2]
        prev_2 = last_rows.iloc[-3]
        
        # LÓGICA DO 9.1 DE COMPRA:
        # 1. A média de ontem (prev) era MENOR que a de anteontem (prev_2) -> Média caindo
        ema_was_falling = prev['EMA9'] < prev_2['EMA9']
        
        # 2. A média de hoje (current) é MAIOR que a de ontem (prev) -> Média virou pra cima
        ema_is_rising = current['EMA9'] > prev['EMA9']
        
        status = "NEUTRO"
        trigger_price = 0.0
        stop_price = 0.0
        
        if ema_was_falling and ema_is_rising:
            status = "ARMADO"
            trigger_price = current['High'] + 0.01 # Compra na superação da máxima
            stop_price = current['Low'] - 0.01     # Stop na mínima
            
        elif ema_was_falling:
            status = "CAINDO"
        elif ema_is_rising:
            status = "SUBINDO"

        return {
            "symbol": ticker_clean, # Retorna o nome limpo para exibir no App
            "price": round(float(current['Close']), 2),
            "ema9": round(float(current['EMA9']), 2),
            "status": status,
            "trigger": round(float(trigger_price), 2) if status == "ARMADO" else 0,
            "stop": round(float(stop_price), 2) if status == "ARMADO" else 0
        }

    except Exception as e:
        print(f"Erro LW9.1 {ticker}: {e}")
        return None
