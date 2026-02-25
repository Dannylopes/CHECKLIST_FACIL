# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### *Notebook de Ingestão da tabela '**checklistfacil_countries**'*
# MAGIC | Camada      | Domínio     | Entidade    |
# MAGIC | ----------  | ----------  | ----------  |
# MAGIC | Corp     | cvc_corp_work (Bronze)     | checklistfacil_countries     |
# MAGIC
# MAGIC
# MAGIC > *Histórico de Alterações*
# MAGIC
# MAGIC | Time      | Projeto               | Autor                                   | Data               | Descrição                                                              |
# MAGIC |-----------|-----------------------|-----------------------------------------|--------------------|------------------------------------------------------------------------|
# MAGIC | InfoStrategy - Dados | Campanhas| Daniel Mota Lopes |   05/01/2026 | Desenvolvimento inicial do notebook                                    |
# MAGIC |  |  |  |    | 
# MAGIC |  |  |  |    | 
# MAGIC |  |  |  |    | 

# COMMAND ----------

# DBTITLE 1,Importa as Libs necessárias
import requests
import time
from pyspark.sql.types import StructType, StructField, StringType
import logging

# COMMAND ----------

# DBTITLE 1,Instancia o notebook de funções do Projeto
# MAGIC %run ../0_UTILS/UTILS_CHECKLIST_FACIL

# COMMAND ----------

# DBTITLE 1,Parâmetros de execução
BASE_URL = "https://api-analytics.checklistfacil.com.br/v1/"
endpoint = "countries"

TABLE_NAME = f"cvc_corp_work.checklistfacil_{endpoint}"
LOAD_TYPE = "FULL"
PAGE_SIZE = 100

# COMMAND ----------

# DBTITLE 1,Obtém o token de acesso à API
headers = {
    "Authorization": f"Bearer {dbutils.secrets.get('dp-scope', 'api-checklistfacil-key')}",
    "Accept-Language": "pt-br"
}

# COMMAND ----------

# DBTITLE 1,Determina tipo de carga e valor da coluna de carga incremental
LOAD_TYPE, watermark = determina_tipo_carga(
    table_name = TABLE_NAME,
    incremental_column = "" # PARA ESSA TABELA NÃO HÁ COLUNA DE WATERMARK
)

# COMMAND ----------

# DBTITLE 1,Avalia parâmetros
params = {
    "limit": PAGE_SIZE
}
if LOAD_TYPE == "INCREMENTAL":
    params["updatedAt"] = watermark
    
logger.info(f"Parâmetros a serem utilizados: {params}")

# COMMAND ----------

# DBTITLE 1,Função de ingestão API de paginada
raw_data = ingestao_api_paginada(
    BASE_URL,
    endpoint,
    headers,
    params
)

# COMMAND ----------

# DBTITLE 1,Obtém o schema da tabela a ser criada/ingearida
schema = gera_bronze_schema(raw_data[0])

# COMMAND ----------

# DBTITLE 1,Grava o result em um dataframe Spark com Timestamp
df = spark.createDataFrame(raw_data, schema=schema)
df = df.withColumn("DT_HR_CARGA", from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"))

# COMMAND ----------

# DBTITLE 1,Executa a carga
executar_carga(df, TABLE_NAME, LOAD_TYPE)