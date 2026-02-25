# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### *Notebook da tabela Silver '**checklistfacil_users**'*
# MAGIC | Camada      | Domínio     | Entidade    |
# MAGIC | ----------  | ----------  | ----------  |
# MAGIC | Corp     | cvc_corp_silver| checklistfacil_users    |
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
from pyspark.sql.functions import regexp_extract

# COMMAND ----------

# MAGIC %run ../0_UTILS/UTILS_CHECKLIST_FACIL

# COMMAND ----------

SCHEMA_BRONZE = 'cvc_corp_work'
SCHEMA_SILVER = 'cvc_corp_silver'

TABLE = 'checklistfacil_users'
TABLE_PK = 'userId'

TABLE_NAME = f"{SCHEMA_SILVER}.{TABLE}"

logger.info(f"TABELA A SER CRIADA/INGERIDA: {TABLE_NAME}")

# COMMAND ----------

df = spark.read.table(f"{SCHEMA_BRONZE}.{TABLE}")
df_count = df.count()
logger.info(f"TABELA BRONZE:{SCHEMA_BRONZE}.{TABLE}  |  Quantidade de registros: {df_count}")

# display(df)

# COMMAND ----------

df_silver_without_dt_hr_carga = df.drop("DT_HR_CARGA")
# df_silver_without_duplicates = df_silver_without_dt_hr_carga.dropDuplicates([TABLE_PK])
df_silver_without_duplicates = df_silver_without_dt_hr_carga 

# COMMAND ----------

df_silver = df_silver_without_duplicates.select(
    col("userId")       .cast("long"),
    col("name")         .cast("string"),
    col("email")        .cast("string"),
    col("active")       .cast("boolean"),
    col("userTypeId")   .cast("long"),
    col("phone")        .cast("string"),
    col("languageId")   .cast("integer"),
    col("countryId")    .cast("integer"),
    col("stateId")      .cast("integer"),
    col("createdAt")    .cast("timestamp"),
    col("updatedAt")    .cast("timestamp"),
    col("deletedAt")    .cast("timestamp")
)

# display(df_silver)

# COMMAND ----------

df_silver.createOrReplaceTempView("silver_temp")

df_resultado = spark.sql("""
    SELECT
        userId,
        TRIM(UPPER(name))   AS name,
        TRIM(LOWER(email))  AS email,
        active,
        userTypeId,
        regexp_replace(phone, '[^0-9]', '') as phone,
        languageId,
        countryId,
        stateId,
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

visualizar_dados = spark.sql(f"""SELECT * FROM {TABLE_NAME}""")
logger.info(f"TABELA SILVER CRIADA/INGERIDA: {TABLE_NAME}")
display(visualizar_dados)