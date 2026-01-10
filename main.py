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

# === ROTA CALENDÁRIO CORRIGIDA ===
@app.get("/calendar")
def get_calendar():
    # 1. Dados de Fallback (Sua segurança)
    mock_events = [
        {"id": "1", "time": "09:00", "country": "BR", "impact": "high", "title": "IPCA (Mensal) - Fallback", "actual": "-", "forecast": "0.30%"},
        {"id": "2", "time": "10:30", "country": "US", "impact": "high", "title": "Payroll (Empregos) - Fallback", "actual": "-", "forecast": "180k"},
        {"id": "3", "time": "15:00", "country": "BR", "impact": "medium", "title": "Balança Comercial", "actual": "-", "forecast": "-"}
    ]

    try:
        # 2. Definição de Headers para "enganar" o bloqueio do Investing.com
        # Isso finge que a requisição vem de um PC com Chrome, e não de um robô
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"
        }

        # 3. URL do Widget (Mais leve e difícil de bloquear que o site principal)
        # countries=32 (Brasil), 5 (EUA) | lang=12 (Português)
        # url = "https://sslecal2.forexprostools.com/?columns=exc_flags,exc_currency,exc_importance,exc_actual,exc_forecast,exc_previous&features=datepicker,timezone&countries=32,5&calType=day&timeZone=12&lang=12"
        url = "https://sslecal2.forexprostools.com/?columns=exc_flags,exc_currency,exc_importance,exc_actual,exc_forecast,exc_previous&features=datepicker,timezone&countries=32,5&calType=week&timeZone=12&lang=12"
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            print(f"Erro no status code: {response.status_code}")
            return mock_events

        # 4. Pandas lê a tabela HTML diretamente
        dfs = pd.read_html(StringIO(response.text))
        
        if not dfs:
            return mock_events
            
        df = dfs[0] # Pega a primeira tabela encontrada

        # Limpeza básica
        df = df.fillna("-")
        
        events = []
        
        # 5. Iterar e formatar (O nome das colunas pode variar, então acessamos por índice as vezes)
        # Estrutura comum do widget: [Hora, Moeda, Imp., Evento, Atual, Projeção, Prévio]
        for index, row in df.iterrows():
            # Pula linhas de cabeçalho repetidas ou vazias
            if str(row[0]) == "Hora" or str(row[0]) == "nan":
                continue

            time_val = str(row[0]) # Hora
            currency = str(row[1]) # Moeda (BRL, USD)
            
            # Filtro de País (Baseado na moeda)
            if "BRL" in currency:
                country = "BR"
            elif "USD" in currency:
                country = "US"
            else:
                continue # Pula outras moedas se aparecerem
            
            # Tenta pegar o título. Geralmente coluna 3 (Índice 3)
            title = str(row[3])

            # Tratamento de Impacto (O pandas lê o texto, as vezes vem vazio pois é imagem)
            # Truque: Se não conseguir ler, marcamos como medium pra não quebrar
            imp_raw = str(row[2]).lower()
            if "alta" in imp_raw or "high" in imp_raw:
                impact = "high"
            elif "baixa" in imp_raw or "low" in imp_raw:
                impact = "low"
            else:
                impact = "medium" # Default seguro

            # Remove eventos sem título relevante
            if "Feriado" in title or title == "-":
                continue

            events.append({
                "id": str(index),
                "time": time_val,
                "country": country,
                "impact": impact,
                "title": title,
                "actual": str(row[4]),
                "forecast": str(row[5])
            })

        if not events:
            return mock_events

        return events

    except Exception as e:
        print(f"Erro ao raspar calendário: {e}")
        # Retorna o mock em caso de erro (ex: Render bloqueado)
        return mock_events
