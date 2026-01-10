import yfinance as yf
import pandas as pd
import numpy as np

def calculate_lw91(symbol: str):
    # Garante o sufixo .SA
    ticker = symbol if symbol.endswith('.SA') else f"{symbol}.SA"
    
    try:
        # Baixa dados suficientes para calcular MME9
        df = yf.download(ticker, period="3mo", interval="1d", progress=False)
        if df.empty or len(df) < 15:
            return None
            
        # Remove MultiIndex se existir
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # Cálculo da MME9 (Média Móvel Exponencial de 9)
        df['EMA9'] = df['Close'].ewm(span=9, adjust=False).mean()
        
        # Pega os 3 últimos candles para ver a "virada"
        # Candle Atual (em formação ou fechado), Ontem, Anteontem
        last_rows = df.tail(3)
        
        current = last_rows.iloc[-1]
        prev = last_rows.iloc[-2]
        prev_2 = last_rows.iloc[-3]
        
        # LÓGICA DO 9.1 DE COMPRA:
        # 1. A média de ontem era MENOR que a de anteontem (Média vinha caindo)
        ema_was_falling = prev['EMA9'] < prev_2['EMA9']
        
        # 2. A média de hoje é MAIOR que a de ontem (Média virou pra cima)
        ema_is_rising = current['EMA9'] > prev['EMA9']
        
        status = "NEUTRO"
        trigger_price = 0.0
        stop_price = 0.0
        
        if ema_was_falling and ema_is_rising:
            status = "ARMADO" # O setup está pronto
            trigger_price = current['High'] + 0.01 # Compra se superar a máxima
            stop_price = current['Low'] - 0.01     # Stop na mínima
            
        elif ema_was_falling:
            status = "CAINDO"
        elif ema_is_rising:
            status = "SUBINDO"

        return {
            "symbol": symbol,
            "price": round(current['Close'], 2),
            "ema9": round(current['EMA9'], 2),
            "status": status,
            "trigger": round(trigger_price, 2) if status == "ARMADO" else 0,
            "stop": round(stop_price, 2) if status == "ARMADO" else 0
        }

    except Exception as e:
        print(f"Erro LW9.1 {symbol}: {e}")
        return None
