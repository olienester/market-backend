from fastapi import FastAPI, HTTPException
from services.market_data import get_stock_data
import investpy
import pandas as pd
from datetime import datetime

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

# === NOVA ROTA: CALENDÁRIO ECONÔMICO ===
@app.get("/calendar")
def get_calendar():
    # Dados de Fallback (Simulados) para caso a API falhe ou bloqueie
    # Isso garante que seu App React Native nunca fique com a tela vazia
    mock_events = [
        {"id": "1", "time": "09:00", "country": "BR", "impact": "high", "title": "IPCA (Mensal) - Fallback", "actual": "-", "forecast": "0.30%"},
        {"id": "2", "time": "10:30", "country": "US", "impact": "high", "title": "Payroll (Empregos) - Fallback", "actual": "-", "forecast": "180k"},
        {"id": "3", "time": "15:00", "country": "BR", "impact": "medium", "title": "Balança Comercial", "actual": "-", "forecast": "-"}
    ]

    try:
        # Pega a data de hoje no formato exigido pelo investpy (dd/mm/yyyy)
        today = datetime.today().strftime('%d/%m/%Y')
        
        # Busca dados reais do Investing.com
        data = investpy.economic_calendar(
            countries=['brazil', 'united states'],
            from_date=today,
            to_date=today,
            importances=['medium', 'high']
        )
        
        # Verifica se retornou dados
        if data is None or data.empty:
            return mock_events

        # Limpeza de dados (Pandas usa NaN para vazios, JSON não aceita NaN)
        data = data.fillna("")

        # Formata para o padrão que o App espera
        events = []
        for index, row in data.iterrows():
            # Padroniza código do país
            country_code = "BR" if str(row['country']).lower() == 'brazil' else "US"
            
            events.append({
                "id": str(row['id']),
                "time": str(row['time']),
                "country": country_code,
                "impact": row['importance'], # 'high', 'medium', 'low'
                "title": str(row['event']),
                "actual": str(row['actual']),
                "forecast": str(row['forecast'])
            })
            
        return events

    except Exception as e:
        print(f"Erro no calendário: {e}")
        return mock_events
