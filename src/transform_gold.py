import pandas as pd
import json
from sqlalchemy import create_engine

print("======= INICIANDO AGREGAÇÃO DA CAMADA GOLD (JUNDIAÍ) =======")

# 1. Conexão
engine = create_engine('postgresql://admin:admin@postgres_clima:5432/clima_dw')

# 2. Carregar configuração
with open('/opt/airflow/src/config.json', 'r') as f:
    config = json.load(f)

# 3. Leitura (Lê todos os campos que foram salvos na Silver)
# Note que agora o SELECT é dinâmico baseado no que está na Silver
df_silver = pd.read_sql("SELECT * FROM silver_clima_jundiai;", con=engine)

# 4. Processamento (ETL)
df_silver['data_previsao_agrupamento'] = pd.to_datetime(df_silver['data_hora_previsao']).dt.date

# Transformamos o dicionário do JSON no formato esperado pelo .agg()
# Exemplo: {'temp_maxima': ('temperature_2m', 'max'), ...}
agregacoes = {k: tuple(v) for k, v in config['gold_aggregations'].items()}

df_gold = df_silver.groupby('data_previsao_agrupamento').agg(**agregacoes).reset_index()

# Formatação e Cálculos fixos
df_gold = df_gold.sort_values(by='data_previsao_agrupamento', ascending=True).reset_index(drop=True)
df_gold['data_previsao'] = pd.to_datetime(df_gold['data_previsao_agrupamento']).dt.strftime('%d/%m/%Y')

# Arredondamento dinâmico para todas as colunas numéricas
for col in ['temp_media', 'vento_medio_kmh', 'chuva_total_mm']:
    if col in df_gold.columns:
        df_gold[col] = df_gold[col].round(1)

# Mantendo sua lógica de alerta original
df_gold['condicao_alerta'] = df_gold['vento_medio_kmh'].apply(
    lambda x: 'Alerta de Vento Forte' if x > 12.0 else 'Dentro da Normalidade'
)

# 5. Carga Acumulativa
try:
    df_gold_existente = pd.read_sql("SELECT data_previsao FROM gold_clima_jundiai", con=engine)
    datas_existentes = df_gold_existente['data_previsao'].tolist()
    df_gold_novo = df_gold[~df_gold['data_previsao'].isin(datas_existentes)]
except Exception:
    df_gold_novo = df_gold

if not df_gold_novo.empty:
    df_gold_novo.to_sql(name='gold_clima_jundiai', con=engine, if_exists='append', index=False)
    print(f"======= CAMADA GOLD CONCLUÍDA: {len(df_gold_novo)} DIA(S) NOVO(S) ADICIONADO(S) =======")
else:
    print("======= CAMADA GOLD: NENHUM DADO NOVO PARA ADICIONAR =======")