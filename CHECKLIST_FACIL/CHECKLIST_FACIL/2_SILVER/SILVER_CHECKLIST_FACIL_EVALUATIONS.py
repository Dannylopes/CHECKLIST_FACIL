# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### *Notebook da tabela Silver '**checklistfacil_evaluations**'*
# MAGIC | Camada      | Domínio     | Entidade    |
# MAGIC | ----------  | ----------  | ----------  |
# MAGIC | Corp     | cvc_corp_silver| checklistfacil_evaluations     |
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
TABLE = 'checklistfacil_evaluations'
TABLE_NAME = f"{SCHEMA}.{TABLE}"
logger.info(f"TABLE_NAME: {TABLE_NAME}")

# COMMAND ----------

df = spark.read.table("cvc_corp_work.checklistfacil_evaluations")

# COMMAND ----------

df_silver = df.drop("DT_HR_CARGA")
df_silver = df_silver.dropDuplicates(["evaluationId"])

# COMMAND ----------

df_silver = df_silver.select(
    col("evaluationId")         .cast("long"),
    col("status")               .cast("integer"),
    col("score")                .cast("decimal(10,2)"),
    col("checklistId")          .cast("long"),
    col("unitId")               .cast("long"),
    col("userId")               .cast("long"),
    col("startedAt")            .cast("timestamp"),
    col("concludedAt")          .cast("timestamp"),
    col("approvedAt")           .cast("timestamp"),
    col("platform")             .cast("string"),
    col("scheduled")            .cast("boolean"),
    col("scheduleStartDate")    .cast("timestamp"),
    col("scheduleEndDate")      .cast("timestamp"),
    col("finalComment")         .cast("string"),
    col("sharedTo")             .cast("string"),
    col("countAttachments")     .cast("integer"),
    col("initialLatitude")      .cast("double"),
    col("initialLongitude")     .cast("double"),
    col("finalLatitude")        .cast("double"),
    col("finalLongitude")       .cast("double"),
    col("createdAt")            .cast("timestamp"),
    col("updatedAt")            .cast("timestamp"),
    col("deletedAt")            .cast("timestamp")
)
# display(df_silver)
# df_silver.columns

# COMMAND ----------

df_silver.createOrReplaceTempView("silver_temp")

df_resultado = spark.sql("""
    SELECT
        evaluationId,
        status,
        score,
        checklistId,
        unitId,
        userId,
        startedAt,
        concludedAt,
        approvedAt,
        platform,
        scheduled,
        scheduleStartDate,
        scheduleEndDate,
        TRIM(UPPER(COALESCE(NULLIF(finalComment, ''), 'NÃO INFORMADO'))) AS finalComment,
        TRIM(UPPER(COALESCE(NULLIF(sharedTo, ''), 'NÃO INFORMADO'))) AS sharedTo,
        countAttachments,
        initialLatitude,
        initialLongitude,
        finalLatitude,
        finalLongitude,
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

# MAGIC %sql
# MAGIC DROP TABLE cvc_corp_silver.checklistfacil_evaluations

# COMMAND ----------

# DBTITLE 1,Executa a carga
executar_carga(df_resultado, TABLE_NAME, 'FULL')

# COMMAND ----------

visualizar_dados = spark.sql(f"""SELECT * FROM {TABLE_NAME}""")
logger.info(f"TABELA SILVER CRIADA/INGERIDA: {TABLE_NAME}")
display(visualizar_dados)