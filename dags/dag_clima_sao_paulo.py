import pendulum # type: ignore
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import timedelta
from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
import os

# Configurações
TOKEN_BOT = "8961402804:AAEAEm8O560kSpEiWWAQdJxYvNMgZtAhtJ4"
CHAT_ID = "8651690756"
GEMINI_KEY = os.getenv("GEMINI_KEY")

def retornar_data_formatada(context):
    fuso_sp = pendulum.timezone("America/Sao_Paulo")
    return context['logical_date'].in_timezone(fuso_sp).strftime('%d/%m/%Y %H:%M')

def inicia_bronze(**context):
    msg = f"🌀 *Apache Airflow | Pipelines*\n\n🚀 *DAG:* `pipeline_clima_jundiai`\n📅 *Executado em:* {retornar_data_formatada(context)} BRT\n\n*Tasks:*\n▶️ `1) Bronze` • Extraindo...\n⬜ `2) Silver` • Aguardando...\n⬜ `3) Gold` • Aguardando..."
    resp = requests.post(f"https://api.telegram.org/bot{TOKEN_BOT}/sendMessage", json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=10).json()
    context['task_instance'].xcom_push(key='telegram_message_id', value=resp['result']['message_id'])

def atualizar_telegram(status, **context):
    msg_id = context['task_instance'].xcom_pull(task_ids='notificar_bronze', key='telegram_message_id')
    data = retornar_data_formatada(context)
    
    if status == 'silver':
        msg = f"🌀 *Apache Airflow | Pipelines*\n\n🚀 *DAG:* `pipeline_clima_jundiai`\n📅 *Executado em:* {data} BRT\n\n*Tasks:*\n✅ `1) Bronze` • Sucesso!\n▶️ `2) Silver` • Transformando...\n⬜ `3) Gold` • Aguardando..."
    elif status == 'gold_start':
        msg = f"🌀 *Apache Airflow | Pipelines*\n\n🚀 *DAG:* `pipeline_clima_jundiai`\n📅 *Executado em:* {data} BRT\n\n*Tasks:*\n✅ `1) Bronze` • Sucesso!\n✅ `2) Silver` • Sucesso!\n▶️ `3) Gold` • Gerando indicadores..."
    
    requests.post(f"https://api.telegram.org/bot{TOKEN_BOT}/editMessageText", json={"chat_id": CHAT_ID, "message_id": msg_id, "text": msg, "parse_mode": "Markdown"}, timeout=10)

# 🏆 A GRANDE FUNÇÃO FINAL
def notificar_fim_com_gemini(**context):
    print("Iniciando a função notificar_fim_com_gemini...") 
    # 1. Primeiro: Finaliza o checklist com sucesso
    msg_id = context['task_instance'].xcom_pull(task_ids='notificar_bronze', key='telegram_message_id')
    data_brt = retornar_data_formatada(context)
    msg_sucesso = f"🌀 *Apache Airflow | Pipelines*\n\n🚀 *DAG:* `pipeline_clima_jundiai`\n📅 *Executado em:* {data_brt} BRT\n\n*Tasks:*\n✅ `1) Bronze` • Sucesso!\n✅ `2) Silver` • Sucesso!\n✅ `3) Gold` • Sucesso!\n\n✨ *Pipeline concluído com sucesso!* 🏁"
    requests.post(f"https://api.telegram.org/bot{TOKEN_BOT}/editMessageText", json={"chat_id": CHAT_ID, "message_id": msg_id, "text": msg_sucesso, "parse_mode": "Markdown"}, timeout=10)
    print("Checklist atualizado no Telegram.")

    # 2. Segundo: Busca dados para a IA
    try:
        print("Conectando ao banco para ler a tabela Gold...")
        engine = create_engine('postgresql://admin:admin@postgres_clima:5432/clima_dw')
        
        # CORREÇÃO: Usamos TO_DATE para ordenar corretamente a coluna de texto DD/MM/YYYY
        query = "SELECT * FROM gold_clima_jundiai ORDER BY TO_DATE(data_previsao, 'DD/MM/YYYY') DESC LIMIT 1"
        df = pd.read_sql(query, con=engine)
        
        dados = df.iloc[0]
        print(f"Dados lidos com sucesso: {dados['data_previsao']}")

        prompt = (
            f"Bom dia, Lucas! Aqui está a previsão para {dados['data_previsao']}: "
            f"Mínima de *{dados['temp_minima']}°C* e Máxima de *{dados['temp_maxima']}°C*. "
            f"Condição de vento: {dados['condicao_alerta']}. "
            "Diretrizes: 1. A temperatura mínima deve sempre aparecer antes da máxima."
            "2. Sempre mantenha o número e o símbolo °C em negrito usando asteriscos antes do número e depois do C de Celsius. "
            "3. Seja simpática, dê algum insight, mas direta. "
            "4. Use emojis distribuídos."
            "5. A estrutura da mensagem deve ser: a saudação, pule uma linha, temperaturas minima e maxima, pule uma linha."
            "6. No final do texto, pule uma linha e traga uma frase filosófica dos grandes pensadores clássicos, em itálico, mencionando o nome do pensador, a obra e sua data."
        )
        
        # 3. Chama Gemini
        url_g = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_KEY}"
        payload = {"contents": [{"parts": [{"text": prompt}]}]}
        
        print(f"Enviando prompt para o Gemini: {prompt}")
        res_g = requests.post(url_g, json=payload, timeout=10)
        
        if res_g.status_code == 200:
            texto_ia = res_g.json()['candidates'][0]['content']['parts'][0]['text']
            print(f"Resposta recebida: {texto_ia}")
            requests.post(f"https://api.telegram.org/bot{TOKEN_BOT}/sendMessage", json={"chat_id": CHAT_ID, "text": texto_ia, "parse_mode": "Markdown"}, timeout=10)
        else:
            print(f"Erro na API do Gemini! Status: {res_g.status_code}, Resposta: {res_g.text}")
            raise Exception(f"Erro {res_g.status_code}")
            
    except Exception as e:
        print(f"Erro crítico no bloco da IA: {str(e)}")
        raise e

with DAG('pipeline_clima_jundiai', default_args={'owner': 'lucas', 'retries': 1}, schedule='0 7 * * *', start_date=pendulum.datetime(2026, 6, 13, tz="America/Sao_Paulo"), catchup=False) as dag:

    t1 = PythonOperator(task_id='notificar_bronze', python_callable=inicia_bronze)
    t2 = BashOperator(task_id='extrair_e_carregar_bronze', bash_command='python /opt/airflow/src/extract_load.py')
    t3 = PythonOperator(task_id='notificar_silver', python_callable=atualizar_telegram, op_args=['silver'])
    t4 = BashOperator(task_id='transformar_silver', bash_command='python /opt/airflow/src/transform_silver.py')
    t5 = PythonOperator(task_id='notificar_gold_start', python_callable=atualizar_telegram, op_args=['gold_start'])
    t6 = BashOperator(task_id='transformar_gold', bash_command='python /opt/airflow/src/transform_gold.py')
    t7 = PythonOperator(
            task_id='notificar_fim', 
            python_callable=notificar_fim_com_gemini,
            retries=5,                       # Tenta 3 vezes
            retry_delay=timedelta(minutes=1) # Espera 1 minutos entre as tentativas
        )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7