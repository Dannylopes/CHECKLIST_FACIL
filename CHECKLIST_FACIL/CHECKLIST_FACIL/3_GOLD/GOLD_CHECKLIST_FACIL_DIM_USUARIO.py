# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### *Notebook da tabela Gold '**checklistfacil_dim_usuario**'*
# MAGIC | Camada      | Domínio     | Entidade    |
# MAGIC | ----------  | ----------  | ----------  |
# MAGIC | Corp     | cvc_corp       | checklistfacil_dim_usuario  |
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
# MAGIC 1. A Tabela Dimensão é do Tipo 1: Sempre reflete o estado atual. Por exemplo, se o usuário mudar de e-mail ou telefone, a nova informação sobreescreve a antiga.

# COMMAND ----------

# DBTITLE 1,Importa as Libs necessárias
import requests
import time
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import regexp_extract, col, sha2, concat_ws, lit, when, current_timestamp, lower, regexp_replace
from pyspark.sql.functions import hex, crc32, col, row_number
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../0_UTILS/UTILS_CHECKLIST_FACIL

# COMMAND ----------

SCHEMA_SILVER = 'cvc_corp_silver'
SCHEMA_GOLD   = 'cvc_corp'

TABLE_SILVER = 'checklistfacil_users'
TABLE_GOLD   = 'checklistfacil_dim_usuario'
TABLE_PK = 'userId'

TABLE_NAME = f"{SCHEMA_GOLD}.{TABLE_GOLD}"

logger.info(f"TABELA A SER CRIADA/INGERIDA: {TABLE_NAME}")

# COMMAND ----------

df              = spark.read.table(f"{SCHEMA_SILVER}.{TABLE_SILVER}")
df_user_types   = spark.read.table(f"cvc_corp_silver.checklistfacil_user_types")
df_count        = df.count()
logger.info(f"TABELA SILVER:{SCHEMA_SILVER}.{TABLE_SILVER}  |  Quantidade de registros: {df_count}")

# COMMAND ----------

df_without_dt_hr_carga = df.drop("DT_HR_CARGA")
# df_without_duplicates = df_without_dt_hr_carga.dropDuplicates([TABLE_PK])

# COMMAND ----------

# IDENTIFICA O REGISTRO MAIS RECENTE DE CADA userId
windowSpec = Window.partitionBy("userId").orderBy(col("updatedAt").desc())

df_gold = df.withColumn("row_num", row_number().over(windowSpec))

df_gold = df_gold.alias("a").join(
    df_user_types.alias("b"),
    on="userTypeId",
    how="inner"
    ).select(
        # Surrogate Key numérica (padrão da empresa)
        crc32(col("userId")   .cast("string")).alias("sk_usuario"),
        col("userId")         .alias("id_original_usuario"),
        col("a.name")         .alias("nome_usuario"),
        col("email")          .alias("email_usuario"),
        when(col("a.deletedAt").isNotNull(), "Inativo")
        .otherwise("Ativo")
        .alias("status_usuario"),
        col("b.name")       .alias("tipo_usuario"),
        col("a.phone")      .alias("telefone"),
        col("a.createdAt")  .alias("dt_criacao"),       
        col("a.updatedAt")  .alias("dt_ultima_atualizacao"),
        col("a.deletedAt")  .alias("dt_delecao")
    ).withColumn("DT_HR_CARGA", from_utc_timestamp(current_timestamp(), "America/Sao_Paulo")
).filter(col("row_num") == 1).drop("row_num")

display(df_gold)

# COMMAND ----------

# DBTITLE 1,Executa a carga
executar_carga(df_gold, TABLE_NAME, 'FULL')

# COMMAND ----------

visualizar_dados = spark.sql(f"""SELECT * FROM {TABLE_NAME}""")
logger.info(f"TABELA GOLD CRIADA/INGERIDA: {TABLE_NAME}")
display(visualizar_dados)