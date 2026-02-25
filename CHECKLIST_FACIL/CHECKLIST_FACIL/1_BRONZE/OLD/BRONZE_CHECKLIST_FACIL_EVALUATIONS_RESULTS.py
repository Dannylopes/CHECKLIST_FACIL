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

BASE_URL = "https://api-analytics.checklistfacil.com.br/v1/"
endpoint = f"evaluations/{evaluationId}/results"

SCHEMA =  "cvc_corp_work"
# TABLE = f"checklistfacil_{endpoint.lstrip('/').replace('/', '_')}"
TABLE   = "checklistfacil_evaluations_results"
TABLE_NAME = f"{SCHEMA}.{TABLE}"

LOAD_TYPE = "FULL"
PAGE_SIZE = 100
UPDATED_COLUMN = "date"

logger.info(f"Tabela a ser criada/ingerida: {TABLE_NAME}")

# COMMAND ----------

headers = {
    "Authorization": f"Bearer {dbutils.secrets.get('dp-scope', 'api-checklistfacil-key')}",
    "Accept-Language": "pt-br"
}

# COMMAND ----------

LOAD_TYPE, watermark = determina_tipo_carga(
    table_name=TABLE_NAME,
    incremental_column= UPDATED_COLUMN
)

# COMMAND ----------

params = {
    "limit": PAGE_SIZE
}

if LOAD_TYPE == "INCREMENTAL":
    params["updatedAt[gte]"] = watermark

# COMMAND ----------

df_evaluations = spark.table("cvc_corp_work.checklistfacil_evaluations_history")

evaluation_ids = [row.evaluationId for row in df_evaluations.select("evaluationId").distinct().collect()]

# display(evaluation_ids)

# COMMAND ----------

all_data = []

for evaluation_id in evaluation_ids:
    
    endpoint = f"/evaluations/{evaluation_id}/results"

    try:
        data = ingestao_api_paginada(
            BASE_URL,
            endpoint,
            headers,
            {"limit": PAGE_SIZE}
        )

        if data:
            # opcional: guardar o ID pai
            for row in data:
                row["evaluationId"] = evaluation_id

            all_data.extend(data)

    except Exception as e:
        logger.warning(
            f"[RESULTS] Sem dados para evaluation_id={evaluation_id} → ignorado"
        )


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

# COMMAND ----------

if LOAD_TYPE == "FULL":
    mode = "overwrite"
else:
    mode = "append"
df.write.mode(mode).saveAsTable(TABLE_NAME)