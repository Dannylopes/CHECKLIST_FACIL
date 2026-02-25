# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### *Notebook da tabela Gold '**checklistfacil_dim_plataforma**'*
# MAGIC | Camada      | Domínio     | Entidade    |
# MAGIC | ----------  | ----------  | ----------  |
# MAGIC | Corp     | cvc_corp       | checklistfacil_dim_plataforma  |
# MAGIC
# MAGIC
# MAGIC > *Histórico de Alterações*
# MAGIC
# MAGIC | Time      | Projeto               | Autor                                   | Data               | Descrição                                                              |
# MAGIC |-----------|-----------------------|-----------------------------------------|--------------------|------------------------------------------------------------------------|
# MAGIC | InfoStrategy - Dados | Checklist Fácil| Daniel Mota Lopes |   18/01/2026 | Desenvolvimento inicial do notebook                                    |
# MAGIC |  |  |  |    | 
# MAGIC |  |  |  |    | 
# MAGIC |  |  |  |    | 
# MAGIC
# MAGIC > *Observações:*
# MAGIC 1. 

# COMMAND ----------

# DBTITLE 1,Importa as Libs necessárias
import requests
import time
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import regexp_extract, col, sha2, concat_ws, lit, when, current_timestamp, lower, regexp_replace
from pyspark.sql.functions import hex, crc32, col, row_number, trim, upper
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../0_UTILS/UTILS_CHECKLIST_FACIL

# COMMAND ----------

SCHEMA_SILVER = 'cvc_corp_silver'
SCHEMA_GOLD   = 'cvc_corp'

TABLE_SILVER = 'checklistfacil_evaluations'
TABLE_GOLD   = 'checklistfacil_dim_plataforma'
TABLE_PK = 'evaluationId'

TABLE_NAME = f"{SCHEMA_GOLD}.{TABLE_GOLD}"

logger.info(f"TABELA A SER CRIADA/INGERIDA: {TABLE_NAME}")

# COMMAND ----------

df              = spark.read.table(f"{SCHEMA_SILVER}.{TABLE_SILVER}")
df_count        = df.count()
logger.info(f"TABELA SILVER:{SCHEMA_SILVER}.{TABLE_SILVER}  |  Quantidade de registros: {df_count}")

# COMMAND ----------

df_without_dt_hr_carga = df.drop("DT_HR_CARGA")
# display(df_without_dt_hr_carga)

# COMMAND ----------

df_gold = df_without_dt_hr_carga.select(
        crc32(col("platform")     .cast("string")).alias("sk_plataforma"),
        col("platform")           .alias("id_original_plataforma")
    ).withColumn(
    "nome_plataforma",
    trim(upper(
    when(col("id_original_plataforma") == 1, "Web")
    .when(col("id_original_plataforma") == 2, "Android")
    .when(col("id_original_plataforma") == 3, "IOS")
    .otherwise("Desconhecida")
    ))).withColumn("DT_HR_CARGA", from_utc_timestamp(current_timestamp(), "America/Sao_Paulo")
    ).distinct()

# display(df_gold)

# COMMAND ----------

# DBTITLE 1,Executa a carga
executar_carga(df_gold, TABLE_NAME, 'FULL')

# COMMAND ----------

visualizar_dados = spark.sql(f"""SELECT * FROM {TABLE_NAME}""")
logger.info(f"TABELA GOLD CRIADA/INGERIDA: {TABLE_NAME}")
display(visualizar_dados)