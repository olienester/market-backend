import yfinance as yf
import pandas as pd
import numpy as np

def analyze_wyckoff(symbol, period="6mo", interval="1d"):
    try:
        # Garante o sufixo .SA se for numérico (B3)
        ticker = symbol.upper()
        if any(char.isdigit() for char in ticker) and not ticker.endswith(".SA"):
            ticker = f"{ticker}.SA"

        # 1. Busca Dados
        df = yf.download(ticker, period=period, interval=interval, progress=False)
        if df.empty or len(df) < 50:
            return None

        # Limpeza de dados (MultiIndex do yfinance novo)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        # Dados mais recentes
        current_price = df['Close'].iloc[-1]
        current_volume = df['Volume'].iloc[-1]
        avg_volume = df['Volume'].rolling(window=20).mean().iloc[-1]

        # 2. Definição do Trading Range (Caixote) dos últimos 60 dias
        last_60 = df.iloc[-60:]
        resistance = last_60['High'].max()
        support = last_60['Low'].min()
        
        # Amplitude do canal (%)
        range_amplitude = ((resistance - support) / support) * 100

        # 3. Identificando a Tendência Macro (SMA 200) e Curta (SMA 50)
        df['SMA50'] = df['Close'].rolling(window=50).mean()
        df['SMA200'] = df['Close'].rolling(window=200).mean() # Pode ser NaN se histórico curto

        trend = "Lateral"
        if len(df) > 200:
            sma200 = df['SMA200'].iloc[-1]
            if current_price > sma200 * 1.05: trend = "Alta (Mark Up)"
            elif current_price < sma200 * 0.95: trend = "Baixa (Mark Down)"

        # 4. Lógica Wyckoff Simplificada
        phase = "Indefinida"
        signal = "Neutro"
        explanation = "O preço está se movendo sem padrão claro."
        sentiment = "neutral" # neutral, bullish, bearish

        # DETECÇÃO DE ACUMULAÇÃO / SPRING (Sinal de Compra)
        # Preço está próximo do suporte E volume aumentou recentemente ou houve rejeição de fundo
        dist_to_support = ((current_price - support) / support) * 100
        
        # Cenário A: SPRING (Armadilha de Fundo)
        # O preço tocou abaixo do suporte nos últimos 5 dias mas voltou a subir
        recent_lows = df['Low'].iloc[-5:].min()
        if recent_lows < support and current_price > support:
            phase = "Fase C (Spring Potential)"
            signal = "Compra Forte"
            sentiment = "bullish"
            explanation = "Detectado possível 'Spring': O preço tentou romper o suporte, falhou e voltou para dentro do canal. Isso sugere entrada de investidores institucionais."

        # Cenário B: MARK UP (Tendência Confirmada)
        elif trend == "Alta (Mark Up)" and current_price > resistance:
            phase = "Fase E (Mark Up)"
            signal = "Manter/Compra"
            sentiment = "bullish"
            explanation = "O ativo rompeu a resistência e está em tendência de alta confirmada (Mark Up)."

        # Cenário C: DISTRIBUIÇÃO / UTAD (Sinal de Venda)
        # Preço próximo da resistência, volume alto, mas não rompe
        elif current_price >= resistance * 0.98:
            phase = "Fase C (Upthrust Potential)"
            signal = "Venda/Cuidado"
            sentiment = "bearish"
            explanation = "O preço está testando o topo (Resistência). Se falhar em romper com volume, pode iniciar uma queda (Mark Down)."

        # Cenário D: MARK DOWN
        elif trend == "Baixa (Mark Down)" and current_price < support:
            phase = "Fase E (Mark Down)"
            signal = "Venda Forte"
            sentiment = "bearish"
            explanation = "Tendência de baixa primária. O ativo perdeu o suporte e a oferta está dominando."
        
        # Cenário E: LATERALIZAÇÃO
        elif range_amplitude < 15: # Canal estreito
            phase = "Fase B (Construção de Causa)"
            signal = "Aguardar"
            explanation = f"O mercado está lateral entre R$ {support:.2f} e R$ {resistance:.2f}. Aguarde definição."

        return {
            "symbol": ticker,
            "current_price": float(current_price),
            "trend": trend,
            "wyckoff_phase": phase,
            "signal": signal,
            "sentiment": sentiment,
            "explanation": explanation,
            "levels": {
                "support": float(support),
                "resistance": float(resistance)
            }
        }

    except Exception as e:
        print(f"Erro Wyckoff: {e}")
        return None
