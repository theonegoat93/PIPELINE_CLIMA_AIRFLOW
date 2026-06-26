import pandas as pd
import json
from sqlalchemy import create_engine

print("======= INICIANDO TRANSFORMAÇÃO DA CAMADA SILVER (JUNDIAÍ) =======")

# 1. Conectar ao banco de dados
engine = create_engine('postgresql://admin:admin@postgres_clima:5432/clima_dw')

# 2. Carregar configuração
with open('/opt/airflow/src/config.json', 'r') as f:
    config = json.load(f)

# 3. Ler os dados brutos da Camada Bronze
print("Lendo dados brutos da Camada Bronze...")
query_bronze = "SELECT data_coleta, dados_brutos FROM bronze_clima_jundiai;"
df_bronze = pd.read_sql(query_bronze, con=engine)

if df_bronze.empty:
    print("Nenhum dado encontrado na Camada Bronze.")
    exit()

# 4. Processar cada linha (dia) da Bronze para a Silver
lista_dfs = []
campos_selecionados = config['api_fields']['hourly']

for index, row in df_bronze.iterrows():
    timestamp_coleta = row['data_coleta']
    json_puro_texto = row['dados_brutos']
    dados_dit = json.loads(json_puro_texto)
    dados_horarios = dados_dit['hourly']

    # Dicionário dinâmico
    dados_estrutura = {
        'data_coleta': timestamp_coleta, 
        'data_hora_previsao': pd.to_datetime(dados_horarios['time'])
    }
    
    for campo in campos_selecionados:
        # A MUDANÇA: Usamos .get() com um valor padrão de lista de zeros
        # O tamanho da lista é baseado na coluna 'time' do próprio dia
        tamanho_dados = len(dados_horarios['time'])
        dados_estrutura[campo] = dados_horarios.get(campo, [0] * tamanho_dados)

    df_dia = pd.DataFrame(dados_estrutura)
    lista_dfs.append(df_dia)

df_silver_final = pd.concat(lista_dfs, ignore_index=True)

# 5. Salvar o resultado acumulado na Camada Silver
print("Salvando dados na tabela 'silver_clima_jundiai'...")
df_silver_final.to_sql(
    name='silver_clima_jundiai',
    con=engine,
    if_exists='replace', 
    index=False
)

print(f"======= CAMADA SILVER CONCLUÍDA! {len(df_silver_final)} linhas processadas. =======")