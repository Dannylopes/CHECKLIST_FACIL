# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### *Notebook da tabela Silver '**checklistfacil_checklists**'*
# MAGIC | Camada      | Domínio     | Entidade    |
# MAGIC | ----------  | ----------  | ----------  |
# MAGIC | Corp     | cvc_corp_silver| checklistfacil_checklists     |
# MAGIC
# MAGIC
# MAGIC > *Histórico de Alterações*
# MAGIC
# MAGIC | Time      | Projeto               | Autor                                   | Data               | Descrição                                                              |
# MAGIC |-----------|-----------------------|-----------------------------------------|--------------------|------------------------------------------------------------------------|
# MAGIC | InfoStrategy - Dados | Checklist Fácil| Daniel Mota Lopes |   13/01/2026 | Desenvolvimento inicial do notebook                                    |
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

SCHEMA = 'cvc_corp_silver'
TABLE = 'checklistfacil_checklists'
TABLE_NAME = f"{SCHEMA}.{TABLE}"
logger.info(f"TABLE_NAME: {TABLE_NAME}")

# COMMAND ----------

df = spark.read.table("cvc_corp_work.checklistfacil_checklists")

# COMMAND ----------

df_silver = df.drop("DT_HR_CARGA")
df_silver = df_silver.dropDuplicates(["checklistId"])

# COMMAND ----------

df_silver = df_silver.select(
    col("checklistId").cast("long"),
    col("type").cast("string"),
    col("name").cast("string"),
    col("subject").cast("string"),
    col("description").cast("string"),
    col("active").cast("boolean"),
    col("createdAt").cast("timestamp"),
    col("updatedAt").cast("timestamp"),
    col("deletedAt").cast("timestamp")
)

# COMMAND ----------

df_silver.createOrReplaceTempView("silver_temp")

df_resultado = spark.sql("""
    SELECT 
        checklistId,
        type,
        TRIM(UPPER(name))                             AS name,
        TRIM(UPPER(ifnull(subject,'NÃO INFORMADO')))  AS subject,
        CASE WHEN TRIM(UPPER(description)) = '' THEN  'NÃO INFORMADO' ELSE TRIM(UPPER(description)) END AS description,
        active,
        createdAt,
        updatedAt,
        deletedAt
    FROM silver_temp
""")

# display(df_resultado)

# COMMAND ----------

# DBTITLE 1,Grava o result em um dataframe Spark com Timestamp
df_resultado = df_resultado.withColumn("DT_HR_CARGA", from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"))

# COMMAND ----------

# DBTITLE 1,Executa a carga
executar_carga(df_resultado, TABLE_NAME, 'FULL')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cvc_corp_silver.checklistfacil_checklists