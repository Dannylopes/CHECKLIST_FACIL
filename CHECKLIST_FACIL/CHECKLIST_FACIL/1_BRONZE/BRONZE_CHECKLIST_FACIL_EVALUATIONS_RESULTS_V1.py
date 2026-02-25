# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### *Notebook de Ingestão Camada Bronze da tabela '**checklistfacil_evaluations_results**'*
# MAGIC | Camada      | Domínio     | Entidade    |
# MAGIC | ----------  | ----------  | ----------  |
# MAGIC | Corp     | cvc_corp_work (Bronze)     | checklistfacil_evaluations_results     |
# MAGIC
# MAGIC
# MAGIC > *Histórico de Alterações*
# MAGIC
# MAGIC | Time      | Projeto               | Autor                                   | Data               | Descrição                                                              |
# MAGIC |-----------|-----------------------|-----------------------------------------|--------------------|------------------------------------------------------------------------|
# MAGIC | InfoStrategy - Dados | Campanhas| Daniel Mota Lopes |   10/01/2026 | Desenvolvimento inicial do notebook                                    |
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

evaluationId = 0
BASE_URL = "https://api-analytics.checklistfacil.com.br/v1/"
endpoint = f"evaluations/{evaluationId}/results"

SCHEMA =  "cvc_corp_work"
TABLE   = "checklistfacil_evaluations_results"
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
    incremental_column = "answeredAt"
)

# COMMAND ----------

params = {
    "limit": PAGE_SIZE
}

if LOAD_TYPE == "INCREMENTAL":
    params["answeredAt"] = watermark

# COMMAND ----------

df_evaluations = spark.table("cvc_corp_work.checklistfacil_evaluations_history")

evaluation_ids = [row.evaluationId for row in df_evaluations.select("evaluationId").distinct().collect()]

# display(evaluation_ids)

# COMMAND ----------

raw_data = []

for evaluation_id in evaluation_ids:

    endpoint = f"evaluations/{evaluation_id}/results"

    try:
        data = ingestao_api_simples(
            BASE_URL,
            endpoint,
            headers
        )

        if data:
            for row in data:
                row["evaluationId"] = evaluation_id

            raw_data.extend(data)

    except Exception:
        logger.warning(
            f"[RESULTS] Sem dados para evaluation_id={evaluation_id}"
        )

# COMMAND ----------

schema = gera_bronze_schema(raw_data[0])

# COMMAND ----------

# schema

# COMMAND ----------

df = spark.createDataFrame(raw_data, schema=schema)

df = df.withColumn("DT_HR_CARGA", from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"))

if LOAD_TYPE == "INCREMENTAL" and watermark is not None:
    df = df.filter(col("answeredAt") > watermark)

# COMMAND ----------

executar_carga(df, TABLE_NAME, LOAD_TYPE)

# COMMAND ----------

df = spark.sql(f"SELECT * FROM {TABLE_NAME}")
display(df)