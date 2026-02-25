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
BASE_URL = "https://api-analytics.checklistfacil.com.br/v2/"
endpoint = f"evaluations/{evaluationId}/results"

SCHEMA =  "cvc_corp_work"
TABLE   = "checklistfacil_evaluations_results_v2"
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

# LOAD_TYPE, watermark = determina_tipo_carga(
#     table_name=TABLE_NAME,
#     incremental_column= UPDATED_COLUMN
# )

# COMMAND ----------

params = {
    "limit": PAGE_SIZE
}

if LOAD_TYPE == "INCREMENTAL":
    params["updatedAt[gte]"] = watermark

# COMMAND ----------

df_evaluations = spark.table("cvc_corp_work.checklistfacil_evaluations_history")

evaluation_ids = [row.evaluationId for row in df_evaluations.select("evaluationId").distinct().collect()]

display(evaluation_ids)

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

schema

# COMMAND ----------

df = spark.createDataFrame(raw_data, schema=schema)

# COMMAND ----------

if LOAD_TYPE == "FULL":
    mode = "overwrite"
else:
    mode = "append"
df.write.mode(mode).saveAsTable(TABLE_NAME)

logger.info(f"Dados gravados na Tabela: ''{TABLE_NAME}'' no modo {mode} ")

# COMMAND ----------

# import requests
# import json

# evaluation_id = 176336886  # escolha um que você sabe que tem dados

# url = f"{BASE_URL}evaluations/departments"

# params = {
#     "evaluationId": evaluation_id,
#     "limit": 100
# }

# response = requests.get(url, headers=headers, params=params)

# print("STATUS:", response.status_code)
# print("RESPONSE:")
# print(json.dumps(response.json(), indent=2))
# print(f'{BASE_URL}{endpoint}')
# print(url)

# COMMAND ----------

# import requests

# evaluation_id = 12345678  # troque por um ID que você queira testar

# url = f"{BASE_URL}evaluations/departments"

# params = {
#     "evaluationId": evaluation_id
# }

# response = requests.get(url, headers=headers, params=params)

# print("Status:", response.status_code)

# json_resp = response.json()
# departments = json_resp if isinstance(json_resp, list) else json_resp.get("data", [])

# if departments:
#     print(f"✅ Existem {len(departments)} departamentos.")
# else:
#     print("❌ Não existem departamentos para essa evaluation.")
# print(url)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table cvc_metastore.cvc_corp_work.checklistfacil_evaluations_results_v2

# COMMAND ----------

import requests

url = "https://api-analytics.checklistfacil.com.br/v1/evaluations/182004119/results"

r = requests.get(url, headers=headers)

print(r.status_code)
print(r.text)
