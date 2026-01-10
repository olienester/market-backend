from fastapi import FastAPI, HTTPException
from services.market_data import get_stock_data
import pandas as pd
from datetime import datetime
import requests          # <-- Novo (substitui investpy)
from io import StringIO  # <-- Novo (necessário para o pandas ler o HTML)

app = FastAPI(title="Market Data API")

@app.get("/")
def root():
    return {"status": "API funcionando"}

@app.get("/stock/{symbol}")
def stock(
    symbol: str,
    interval: str = "1d",
    period: str = "1y"
):
    data = get_stock_data(symbol, interval, period)

    if not data:
        raise HTTPException(status_code=404, detail="Dados não encontrados")

    return {
        "symbol": symbol,
        "interval": interval,
        "period": period,
        "data": data
    }

@app.get("/calendar")
def get_calendar():
    # 1. Mock de segurança (para não quebrar o App)
    mock_events = [
        {"id": "1", "time": "09:00", "country": "BR", "impact": "high", "title": "IPCA (Mensal) - Fallback", "actual": "-", "forecast": "0.30%"}
    ]

    # 2. URL EXATA que você testou e funcionou (pegando a semana toda para garantir dados)
    url = "https://sslecal2.forexprostools.com/?columns=exc_flags,exc_currency,exc_importance,exc_actual,exc_forecast,exc_previous&features=datepicker,timezone&countries=32,5&calType=week&timeZone=12&lang=12"

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://br.investing.com/",
            "X-Requested-With": "XMLHttpRequest"
        }

        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code != 200:
            print(f"Erro HTTP: {response.status_code}")
            return mock_events

        # 3. Lê o HTML
        # O Pandas vai achar a tabela principal
        dfs = pd.read_html(StringIO(response.text))
        if not dfs:
            return mock_events
            
        df = dfs[0]
        df = df.fillna("-") # Remove NaNs

        events = []
        
        # Palavras-chave para forçar impacto ALTO (já que não conseguimos ler os touros da imagem)
        high_impact_keywords = [
            "payroll", "ipca", "selic", "fomc", "fed", "pib", "gdp", 
            "cpi", "ppc", "copom", "estoques de petróleo", "desemprego"
        ]

        # 4. Itera sobre as linhas
        for index, row in df.iterrows():
            col0 = str(row[0]) # Tempo
            col1 = str(row[1]) # Moeda
            
            # Pula linhas de Data (ex: "Segunda, 5 de Janeiro...") e Cabeçalhos
            if "Jeira" in col0 or "Deira" in col0 or "Janeiro" in col0 or "Fevereiro" in col0:
                continue
            if col0 == "Tempo":
                continue

            # Verifica País pela moeda (Coluna 1)
            if "BRL" in col1:
                country = "BR"
            elif "USD" in col1:
                country = "US"
            else:
                continue # Pula outras moedas

            # Dados do Evento
            time_val = col0
            title = str(row[3]) # Coluna 3 é o Evento
            actual = str(row[4])
            forecast = str(row[5])

            # Lógica de Impacto (Baseada no Título, pois a coluna de imagem vem vazia)
            impact = "medium" # Padrão
            title_lower = title.lower()
            
            # Se tiver palavra chave de alto impacto, marca como high
            if any(key in title_lower for key in high_impact_keywords):
                impact = "high"
            
            # Se for feriado, ignora ou marca low
            if "Feriado" in title:
                continue

            events.append({
                "id": str(index),
                "time": time_val,
                "country": country,
                "impact": impact,
                "title": title,
                "actual": actual,
                "forecast": forecast
            })

        if not events:
            # Se filtrou tudo e sobrou nada, retorna mock
            return mock_events

        return events

    except Exception as e:
        print(f"Erro ao processar: {e}")
        return mock_events
