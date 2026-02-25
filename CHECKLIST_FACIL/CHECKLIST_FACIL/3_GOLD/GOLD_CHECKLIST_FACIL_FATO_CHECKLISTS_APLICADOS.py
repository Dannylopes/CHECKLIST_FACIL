# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### *Notebook da tabela Gold '**checklistfacil_fato_checklists_aplicados**'*
# MAGIC | Camada      | Domínio     | Entidade    |
# MAGIC | ----------  | ----------  | ----------  |
# MAGIC | Corp     | cvc_corp       | checklistfacil_fato_checklists_aplicados  |
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
# MAGIC 1. Tabela com Checklists Aplicados

# COMMAND ----------

# DBTITLE 1,Importa as Libs necessárias
import requests
import time
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import regexp_extract, col, sha2, concat_ws, lit, when, current_timestamp, lower, regexp_replace, coalesce
from pyspark.sql.functions import hex, crc32, col, row_number
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../0_UTILS/UTILS_CHECKLIST_FACIL

# COMMAND ----------

SCHEMA_SILVER = 'cvc_corp_silver'
SCHEMA_GOLD   = 'cvc_corp'

TABLE_SILVER = 'checklistfacil_evaluations'
TABLE_GOLD   = 'checklistfacil_fato_checklists_aplicados'
TABLE_PK = 'evaluationId'

TABLE_NAME = f"{SCHEMA_GOLD}.{TABLE_GOLD}"

logger.info(f"TABELA A SER CRIADA/INGERIDA: {TABLE_NAME}")

# COMMAND ----------

df              = spark.read.table(f"{SCHEMA_SILVER}.{TABLE_SILVER}")
df_count        = df.count()
logger.info(f"TABELA SILVER:{SCHEMA_SILVER}.{TABLE_SILVER}  |  Quantidade de registros: {df_count}")

# COMMAND ----------

df_dim_checklist    = spark.read.table(f"{SCHEMA_GOLD}.checklistfacil_dim_checklists")
df_dim_status       = spark.read.table(f"{SCHEMA_GOLD}.checklistfacil_dim_status")
df_dim_plataforma   = spark.read.table(f"{SCHEMA_GOLD}.checklistfacil_dim_plataforma")
df_dim_unidades     = spark.read.table(f"{SCHEMA_GOLD}.checklistfacil_dim_unidades")
df_dim_usuario      = spark.read.table(f"{SCHEMA_GOLD}.checklistfacil_dim_usuario")

# COMMAND ----------

df_without_dt_hr_carga = df.drop("DT_HR_CARGA")
# display(df_without_dt_hr_carga)
# df_without_dt_hr_carga.columns

# COMMAND ----------

df_gold = df_without_dt_hr_carga.join

df_gold = df_without_dt_hr_carga.alias("a").join(
    df_dim_status.alias("b"),
    on=col("a.status") == col("b.id_original_status"),
    how="left"
    
    ).join(
    df_dim_plataforma.alias("c"),
    on=col("a.platform") == col("c.id_original_plataforma"),
    how="left"
    
    ).join(
    df_dim_checklist.alias("d"),
    on=col("a.checklistId") == col("d.id_original_checklist"),
    how="left"
    
    ).join(
    df_dim_unidades.alias("e"),
    on=col("a.unitId") == col("e.id_original_unidade"),
    how="left"
    
    ).join(
    df_dim_usuario.alias("f"),
    on=col("a.userId") == col("f.id_original_usuario"),
    how="left"

    ).select(
        crc32(col("evaluationId")     .cast("string")).alias("sk_checklist_aplicado"),
        col("evaluationId")           .alias("id_original_checklist_aplicado"),
        col("b.sk_status"),
        col("c.sk_plataforma"),
        col("d.sk_checklist"),
        col("e.sk_unidade"),
        col("f.sk_usuario"),
        coalesce(col("score"), lit(0))  .alias("pontuacao"),
        # col("score")                    .alias("pontuacao"),
        col("finalComment")             .alias("comentarios_finais"),
        col("sharedTo")                 .alias("compartilhado_com"),
        col("countAttachments")         .alias("qtd_anexos"),
        when(col("scheduled"), "SIM").otherwise("NÃO").alias("agendado"),
        col("startedAt")                .alias("iniciado_em"),
        col("concludedAt")              .alias("concluido_em"),
        col("approvedAt")               .alias("aprovado_em"),
        col("scheduleStartDate")        .alias("dt_inicio_agendamento"),
        col("scheduleEndDate")          .alias("dt_fim_agendamento"),
        col("initialLatitude")          .alias("latitude_inicial"),
        col("initialLongitude")         .alias("longitude_inicial"),
        col("finalLatitude")            .alias("latitude_final"),
        col("finalLongitude")           .alias("longitude_final"),
        col("createdAt")                .alias("criado_em"),
        col("updatedAt")                .alias("atualizado_em"),
        col("deletedAt")                .alias("deletado_em")
        ).withColumn("DT_HR_CARGA", from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"))

# display(df_gold)

# COMMAND ----------

# DBTITLE 1,Executa a carga
executar_carga(df_gold, TABLE_NAME, 'FULL')

# COMMAND ----------

visualizar_dados = spark.sql(f"""SELECT * FROM {TABLE_NAME}""")
logger.info(f"TABELA GOLD CRIADA/INGERIDA: {TABLE_NAME}")
display(visualizar_dados)