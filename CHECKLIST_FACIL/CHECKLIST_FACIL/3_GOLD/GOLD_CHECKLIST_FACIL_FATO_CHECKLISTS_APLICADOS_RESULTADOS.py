# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### *Notebook da tabela Gold '**checklistfacil_fato_checklists_aplicados_resultados**'*
# MAGIC | Camada      | Domínio     | Entidade    |
# MAGIC | ----------  | ----------  | ----------  |
# MAGIC | Corp     | cvc_corp       | checklistfacil_fato_checklists_aplicados_resultados  |
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

TABLE_SILVER = 'checklistfacil_evaluations_results'
TABLE_GOLD   = 'checklistfacil_fato_checklists_aplicados_resultados'
TABLE_PK = 'evaluationId'

TABLE_NAME = f"{SCHEMA_GOLD}.{TABLE_GOLD}"

logger.info(f"TABELA A SER CRIADA/INGERIDA: {TABLE_NAME}")

# COMMAND ----------

df              = spark.read.table(f"{SCHEMA_SILVER}.{TABLE_SILVER}")
df_count        = df.count()
logger.info(f"TABELA SILVER:{SCHEMA_SILVER}.{TABLE_SILVER}  |  Quantidade de registros: {df_count}")

# COMMAND ----------

df_dim_itens      = spark.read.table(f"{SCHEMA_GOLD}.checklistfacil_dim_itens")
df_dim_categorias = spark.read.table(f"{SCHEMA_GOLD}.checklistfacil_dim_categorias")
df_fato_checklists_aplicados = spark.read.table(f"{SCHEMA_GOLD}.checklistfacil_fato_checklists_aplicados")

# display(df_dim_itens.limit(15))
# display(df_dim_categorias.limit(15))
# display(df_fato_checklists_aplicados.limit(15))

# COMMAND ----------

df_without_dt_hr_carga = df.drop("DT_HR_CARGA")
# display(df_without_dt_hr_carga.limit(15))

# COMMAND ----------

df_gold = df_without_dt_hr_carga.alias("a"
    ).join(
    df_dim_itens.alias("b"),
    on=col("a.itemId") == col("b.id_original_item"),
    how="left"
    
    ).join(
    df_dim_categorias.alias("c"),
    on=col("a.categoryId") == col("c.id_original_categoria"),
    how="left"

    ).join(
    df_fato_checklists_aplicados.alias("d"),
    on=col("a.evaluationId") == col("d.id_original_checklist_aplicado"),
    how="left"
    ).select(
        crc32(col("resultId").cast("string"))     .alias("sk_resultado"),
        col("resultId")                           .alias("id_original_resultado"),
        col("d.sk_checklist_aplicado"),
        col("c.sk_categoria"),
        col("b.sk_item"),
        col("scaleId")                          .alias("id_escala"),
        col("answeredAt")                       .alias("respondido_em"),
        col("evaluative")                       .alias("avaliativo"),
        col("sharedTo")                         .alias("compartilhado_com"),
        col("text")                             .alias("texto_avaliacao"),
        col("number")                           .alias("numero"),
        col("stateId")                          .alias("id_estado"),
        col("cityId")                           .alias("id_cidade"),
        col("product")                          .alias("produto"),
        col("competencePeriodId")               .alias("id_competencia"),
        col("selectedOptions")                  .alias("opcoes_selecionadas"),
        col("index")                            .alias("indice"),
        col("originalWeight")                   .alias("peso_original"),
        col("maximumWeight")                    .alias("peso_maximo"),
        col("obtainedWeight")                   .alias("peso_obtido"),
        col("comment")                          .alias("comentario"),
        col("countAttachments")                 .alias("qtd_anexos"),
        col("countSignatures")                  .alias("qtd_assinaturas"),
        col("itemOrder")                        .alias("ordem_item"),
        col("categoryOrder")                    .alias("ordem_categoria")
      ).withColumn("DT_HR_CARGA", from_utc_timestamp(current_timestamp(), "America/Sao_Paulo"))

# display(df_gold)

# COMMAND ----------

# DBTITLE 1,Executa a carga
executar_carga(df_gold, TABLE_NAME, 'FULL')

# COMMAND ----------

visualizar_dados = spark.sql(f"""SELECT * FROM {TABLE_NAME}""")
logger.info(f"TABELA GOLD CRIADA/INGERIDA: {TABLE_NAME}")
display(visualizar_dados)