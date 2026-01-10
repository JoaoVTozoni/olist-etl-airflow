from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from sqlalchemy import create_engine

# 1. Função de Extração e Transformação
def extrair_e_transformar_dados():
    # Caminho interno do container conforme mapeado no docker-compose.yaml
    caminho = "/opt/airflow/datasets/olist_orders_dataset.csv"
    
    if not os.path.exists(caminho):
        raise FileNotFoundError(f"Arquivo {caminho} não encontrado.")
    
    # Extração: Leitura do CSV (Olist usa vírgula como padrão)
    df = pd.read_csv(caminho)
    
    # Transformação: Correção de tipos de dados para Timestamp
    colunas_data = [
        'order_purchase_timestamp', 
        'order_approved_at', 
        'order_delivered_carrier_date', 
        'order_delivered_customer_date', 
        'order_estimated_delivery_date'
    ]
    
    for col in colunas_data:
        df[col] = pd.to_datetime(df[col])
    
    # Limpeza: Remoção de registros sem ID de pedido
    df.dropna(subset=['order_id'], inplace=True)
    
    # Retorno em JSON para o XCom (comunicação entre tarefas)
    return df.to_json()

# 2. Função de Carga no PostgreSQL
def carregar_dados_no_postgres(**kwargs):
    ti = kwargs["ti"]
    json_data = ti.xcom_pull(task_ids="extrair_e_transformar")
    
    if not json_data:
        raise ValueError("Nenhum dado recebido do XCom.")
    
    # Ao ler o JSON, o Pandas traz as datas como números
    df = pd.read_json(json_data)
    
    # LISTA DE COLUNAS PARA RE-CONVERTER
    colunas_data = [
        'order_purchase_timestamp', 'order_approved_at', 
        'order_delivered_carrier_date', 'order_delivered_customer_date', 
        'order_estimated_delivery_date'
    ]
    
    # Converte os números de volta para o formato de data que o Postgres entende
    for col in colunas_data:
        df[col] = pd.to_datetime(df[col], unit='ms')

    # Conexão interna do Docker
    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
    
    # Carga dos dados
    df.to_sql("olist_orders", engine, if_exists="append", index=False)

# 3. Definição da DAG
with DAG(
    dag_id="olist_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["olist", "ecommerce"]
) as dag:

    extrair = PythonOperator(
        task_id="extrair_e_transformar",
        python_callable=extrair_e_transformar_dados
    )

    carregar = PythonOperator(
        task_id="carregar_no_postgres",
        python_callable=carregar_dados_no_postgres
    )
