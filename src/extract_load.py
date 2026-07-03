import requests
import json
import pandas as pd
from sqlalchemy import create_engine
import datetime

print("======= INICIANDO EXTRAÇÃO DA CAMADA BRONZE (JUNDIAÍ) =======")

# Carregar configuração
with open('/opt/airflow/src/config.json', 'r') as f:
    config = json.load(f)

# Montar URL dinamicamente usando os campos do config
campos = ",".join(config['api_fields']['hourly'])
url = f"https://api.open-meteo.com/v1/forecast?latitude=-23.1853&longitude=-46.8892&hourly={campos}&timezone=America/Sao_Paulo&forecast_days=1"

print(f"Buscando dados da API Open-Meteo com os campos: {campos}...")
response = requests.get(url, timeout=10)
dados_api = response.json()

data_hoje = datetime.datetime.now().strftime('%Y-%m-%d')
dado_bronze = {
    "data_coleta": data_hoje,
    "dados_brutos": json.dumps(dados_api)
}

df_bronze = pd.DataFrame([dado_bronze])

engine = create_engine('postgresql://admin:admin@postgres_clima:5432/clima_dw')

# Verificamos se já existe registro para a data de hoje para evitar duplicatas acidentais
try:
    
    #query_check = f"SELECT data_coleta FROM bronze_clima_jundiai WHERE data_coleta = '{data_hoje}'"
    query_check = "SELECT data_coleta FROM bronze_clima_jundiai WHERE data_coleta = :data"
    #df_existente = pd.read_sql(query_check, con=engine)
    df_existente = pd.read_sql(query_check, con=engine, params={"data": data_hoje})
except:
    df_existente = pd.DataFrame()

if df_existente.empty:
    print(f"Inserindo dados novos do dia {data_hoje}...")
    df_bronze.to_sql(name='bronze_clima_jundiai', con=engine, if_exists='append', index=False)
    print("======= DADOS INSERIDOS COM SUCESSO! =======")
else:
    print(f"Dados do dia {data_hoje} já existem. Nada a fazer.")