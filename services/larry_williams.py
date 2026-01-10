import yfinance as yf
import pandas as pd

# Adicionamos o parâmetro interval com valor padrão "1d"
def calculate_lw91(symbol: str, interval: str = "1d"):
    ticker_clean = symbol.replace('.SA', '').upper()
    ticker = f"{ticker_clean}.SA"
    
    # Ajusta o período de busca baseado no intervalo
    # 60m precisa de um periodo menor para ser rápido (ex: 1 mês)
    period_lookup = "1mo" if interval == "60m" else "3mo"
    
    try:
        asset = yf.Ticker(ticker)
        
        # Baixa com o intervalo dinâmico
        df = asset.history(period=period_lookup, interval=interval, auto_adjust=True)
        
        if df.empty or len(df) < 15:
            return None
            
        df = df[['Close', 'High', 'Low']].copy()
        
        # Cálculo da MME9
        df['EMA9'] = df['Close'].ewm(span=9, adjust=False).mean()
        
        last_rows = df.tail(3)
        if len(last_rows) < 3:
            return None

        current = last_rows.iloc[-1]
        prev = last_rows.iloc[-2]
        prev_2 = last_rows.iloc[-3]
        
        # Lógica do Setup 9.1
        ema_was_falling = prev['EMA9'] < prev_2['EMA9']
        ema_is_rising = current['EMA9'] > prev['EMA9']
        
        status = "NEUTRO"
        trigger_price = 0.0
        stop_price = 0.0
        
        if ema_was_falling and ema_is_rising:
            status = "ARMADO"
            trigger_price = current['High'] + 0.01 
            stop_price = current['Low'] - 0.01     
            
        elif ema_was_falling:
            status = "CAINDO"
        elif ema_is_rising:
            status = "SUBINDO"

        return {
            "symbol": ticker_clean,
            "price": round(float(current['Close']), 2),
            "ema9": round(float(current['EMA9']), 2),
            "status": status,
            "trigger": round(float(trigger_price), 2) if status == "ARMADO" else 0,
            "stop": round(float(stop_price), 2) if status == "ARMADO" else 0,
            "interval": interval # Retornamos o intervalo para confirmação
        }

    except Exception as e:
        print(f"Erro LW9.1 {ticker}: {e}")
        return None
