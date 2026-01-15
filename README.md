# Pipeline de ETL - Dataset Olist E-commerce
Este projeto consiste na criação de um fluxo de dados automatizado para processar informações do dataset público da Olist. O objetivo central foi implementar a ingestão de arquivos brutos em um ambiente de Data Warehouse estruturado no PostgreSQL, garantindo a padronização e a integridade dos registros.

# Ferramentas utilizadas
Python para a lógica de processamento e manipulação de dados.
Pandas para as etapas de limpeza e transformação.
Apache Airflow para a orquestração e agendamento das tarefas do pipeline.
Docker e Docker Compose para a criação do ambiente isolado (containers).
PostgreSQL como banco de dados relacional de destino.
SQLAlchemy para a interface de conexão e carga de dados.

# Estrutura do pipeline
O fluxo de dados foi dividido em duas etapas principais para facilitar a manutenção e o monitoramento:

1. Extração e Transformação: O processo inicia com a leitura do arquivo olist_orders_dataset.csv. Nesta fase, os dados nulos são removidos e os campos de data, originalmente em formato de texto, são convertidos para objetos Timestamp, permitindo cálculos temporais precisos no banco de dados.

2. Carga no Banco de Dados: Os dados transformados são enviados ao PostgreSQL. Para garantir a compatibilidade após a serialização via XCom, foi aplicada uma lógica de reconversão de tipos antes da inserção final na tabela olist_orders.

# Instruções para execução
Para rodar o projeto localmente, é necessário ter o Docker instalado e seguir estes passos:

1. Configuração do ambiente:
```
git clone https://github.com/seu-usuario/projeto-de-olist-airflow.git
cd projeto-de-olist-airflow
docker compose up -d
```
2. Inicialização do Airflow (necessário apenas na primeira vez):
```
docker compose run --rm airflow-webserver airflow db init
docker compose run --rm airflow-webserver airflow users create --username airflow --firstname Admin --lastname User --role Admin --email admin@example.com --password airflow
```
3. Execução: Acesse o painel do Airflow em http://localhost:8080 com as credenciais criadas. Ative a DAG olist_etl_pipeline e inicie o processamento manual.

4. Verificação dos resultados
Após a conclusão das tarefas, os dados inseridos podem ser validados diretamente no container do banco de dados:
```
docker exec -it projeto_de1-postgres-1 psql -U airflow -c "SELECT * FROM olist_orders LIMIT 5;"
```


