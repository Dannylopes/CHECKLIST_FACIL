# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### *Notebook da tabela Silver '**checklistfacil_evaluations_results**'*
# MAGIC | Camada      | Domínio     | Entidade    |
# MAGIC | ----------  | ----------  | ----------  |
# MAGIC | Corp     | cvc_corp_silver| checklistfacil_evaluations_results     |
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

SCHEMA_BRONZE = 'cvc_corp_work'
SCHEMA_SILVER = 'cvc_corp_silver'

TABLE = 'checklistfacil_evaluations_results'

TABLE_NAME = f"{SCHEMA_SILVER}.{TABLE}"

logger.info(f"TABELA A SER CRIADA/INGERIDA: {TABLE_NAME}")

# COMMAND ----------

df = spark.read.table(f"{SCHEMA_BRONZE}.{TABLE}")
df_count = df.count()
logger.info(f"TABELA BRONZE:{SCHEMA_BRONZE}.{TABLE}  |  Quantidade de registros: {df_count}")

# display(df)

# COMMAND ----------

df_silver_without_dt_hr_carga = df.drop("DT_HR_CARGA")
df_silver_without_duplicates = df_silver_without_dt_hr_carga.dropDuplicates(["resultId"])
# display(df_silver)

# COMMAND ----------

df_silver = df_silver_without_duplicates.select(
    col("resultId")             .cast("long"),
    col("evaluationId")         .cast("long"),
    col("categoryId")           .cast("long"),
    col("itemId")               .cast("long"),
    col("scaleId")              .cast("long"),
    col("answeredAt")           .cast("timestamp"),
    col("evaluative")           .try_cast("integer"),
    col("sharedTo")             .cast("string"),
    col("text")                 .cast("string"),
    col("number")               .cast("double"), 
    col("stateId")              .cast("integer"),
    col("cityId")               .cast("integer"),
    col("product")              .cast("string"),
    col("competencePeriodId")   .cast("long"),
    col("selectedOptions")      .cast("string"),
    col("index")                .cast("integer"),
    col("originalWeight")       .cast("decimal(10,2)"),
    col("maximumWeight")        .cast("decimal(10,2)"),
    col("obtainedWeight")       .cast("decimal(10,2)"),
    col("comment")              .cast("string"),
    col("countAttachments")     .cast("integer"),
    col("countSignatures")      .cast("integer"),
    col("itemOrder")            .cast("integer"),
    col("categoryOrder")        .cast("integer")
)
# display(df_silver)

# COMMAND ----------

df_silver.createOrReplaceTempView("silver_temp")

df_resultado = spark.sql("""
    SELECT

        resultId,
        evaluationId,
        categoryId,
        itemId,
        scaleId,
         answeredAt,
        COALESCE(evaluative, 0)                             AS evaluative,
        COALESCE(NULLIF(sharedTo, ''), 'NÃO COMPARTILHADO') AS sharedTo,
         COALESCE(NULLIF(text, ''), 'SEM RESPOSTA')         AS text,
        COALESCE(number, 0) AS number,
        stateId,
        cityId,
        product,
        competencePeriodId,
        CASE 
            WHEN selectedOptions = '[]' OR selectedOptions = '' THEN 'NÃO INFORMADO'
            WHEN selectedOptions IS NULL THEN 'NÃO INFORMADO'
            ELSE selectedOptions 
        END AS selectedOptions,
        COALESCE(index, 0)                                  AS index,
        COALESCE(originalWeight, 0)                         AS originalWeight,
        COALESCE(maximumWeight, 0)                          AS maximumWeight,
        COALESCE(obtainedWeight, 0)                         AS obtainedWeight,
        COALESCE(NULLIF(comment, ''), 'SEM COMENTÁRIO')     AS comment,
        COALESCE(countAttachments, 0)                       AS countAttachments,
        COALESCE(countSignatures, 0)                        AS countSignatures,
        itemOrder,
        categoryOrder
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