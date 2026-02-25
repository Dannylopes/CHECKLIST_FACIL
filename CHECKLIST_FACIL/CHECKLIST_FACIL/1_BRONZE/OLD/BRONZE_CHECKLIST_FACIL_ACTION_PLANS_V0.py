# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### *Notebook de Ingestão da tabela '**checklistfacil_action_plans**'*
# MAGIC | Camada      | Domínio     | Entidade    |
# MAGIC | ----------  | ----------  | ----------  |
# MAGIC | Corp     | cvc_corp_work (Bronze)     | checklistfacil_action_plans     |
# MAGIC
# MAGIC
# MAGIC > *Histórico de Alterações*
# MAGIC
# MAGIC | Time      | Projeto               | Autor                                   | Data               | Descrição                                                              |
# MAGIC |-----------|-----------------------|-----------------------------------------|--------------------|------------------------------------------------------------------------|
# MAGIC | InfoStrategy - Dados | Campanhas| Daniel Mota Lopes |   09/01/2026 | Desenvolvimento inicial do notebook                                    |
# MAGIC |  |  |  |    | 
# MAGIC |  |  |  |    | 
# MAGIC |  |  |  |    | 

# COMMAND ----------

# DBTITLE 1,Importa as Libs necessárias
import requests
import time
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

# MAGIC %run ../0_UTILS/UTILS_CHECKLIST_FACIL

# COMMAND ----------

# BASE_URL = "https://api-analytics.checklistfacil.com.br/v1/"
# endpoint = "action_plans"

# TABLE_NAME = f"cvc_corp_work.checklistfacil_{endpoint}"
# LOAD_TYPE = "FULL"
# PAGE_SIZE = 100

# logger.info(f"Tabela a ser criada/ingerida: '{TABLE_NAME}'")


https://api-analytics.checklistfacil.com.br/v1/action-plans?evaluationId&checklistId&unitId&userId&updatedAt&deletedAt&pivot&page&limit

evaluationId = 0
BASE_URL = "https://api-analytics.checklistfacil.com.br/v1/"
endpoint = f"evaluations/{evaluationId}/results"

SCHEMA =  "cvc_corp_work"
TABLE   = "checklistfacil_evaluations_results_v1"
TABLE_NAME = f"{SCHEMA}.{TABLE}"

LOAD_TYPE = "FULL"
PAGE_SIZE = 100
UPDATED_COLUMN = "date"

logger.info(f"Tabela a ser criada/ingerida: {TABLE_NAME}")
logger.info(f"Full Path: {BASE_URL}{endpoint}")


# COMMAND ----------

headers = {
    "Authorization": f"Bearer {dbutils.secrets.get('dp-scope', 'api-checklistfacil-key')}",
    "Accept-Language": "pt-br"
}

# COMMAND ----------

LOAD_TYPE, watermark = determina_tipo_carga(
    table_name = TABLE_NAME,
    incremental_column = "updatedAt"
)


# COMMAND ----------

params = {
    "limit": PAGE_SIZE
}

if LOAD_TYPE == "INCREMENTAL":
    params["updatedAt"] = watermark

logger.info(f"Parâmetros a serem utilizados: {params}")

# COMMAND ----------

raw_data = ingestao_api_paginada(
    BASE_URL,
    endpoint,
    headers,
    params
)

# COMMAND ----------

schema = gera_bronze_schema(raw_data[0])

# COMMAND ----------

df = spark.createDataFrame(raw_data, schema=schema)
df = df.withColumn("DT_HR_CARGA", from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"))

# COMMAND ----------

executar_carga(df, TABLE_NAME, LOAD_TYPE)