# Databricks notebook source
# DBTITLE 1,Importa as Libs necessárias
import requests
import time
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import logging
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, col
# from pyspark.sql.functions import col

# COMMAND ----------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LOG")

# COMMAND ----------

def determina_tipo_carga(
    table_name,
    incremental_column=None,
    force_full=False
):
    # SE O PARÂMETRO FORCE_FULL FOR TRUE, RETORNA FULL, O QUE DETERMINA A CARGA COMO FULL
    if force_full:
        logger.info(
            f"[LOAD_TYPE] FULL forçado para a tabela {table_name}"
        )
        return "FULL", None
    # SE O PARÂMETRO FORCE_FULL NÃO FOR TRUE, VERIFICA SE A TABELA EXISTE, CASO ELA NÃO EXISTA, RETORNA FULL
    if not spark.catalog.tableExists(table_name):
        logger.info(
            f"[LOAD_TYPE] Tabela {table_name} não existe → FULL"
        )
        return "FULL", None
    # SE A TABELA EXISTE, VERIFICA SE A COLUNA INCREMENTAL FOI INFORMADA, CASO NÃO TENHA, RETORNA FULL
    if not incremental_column:
        logger.info(
            f"[LOAD_TYPE] Coluna incremental não informada → FULL ({table_name})"
        )
        return "FULL", None
    
    # SE A COLUNA INCREMENTAL FOI INFORMADA, RETORNA O VALOR MÁXIMO DESSA COLUNA PARA CARGA INCREMENTAL
    last_value = spark.sql(f"""
        SELECT max({incremental_column}) AS max_value
        FROM {table_name}
    """).collect()[0]["max_value"]

    # SE last_value for None, A CARGA SERÁ FULL
    if last_value is None:
        logger.info(
            f"[LOAD_TYPE] {table_name} sem valor de {incremental_column} → FULL"
        )       
        return "FULL", None
    # LOG COM TIPO DE CARGA E WATERMARK
    logger.info(
        f"[LOAD_TYPE] será do tipo INCREMENTAL para a tabela:{table_name}"
        f" usando a coluna '{incremental_column}' como watermark, com o valor > '{last_value}' "
    )

    return "INCREMENTAL", last_value


# COMMAND ----------

# LOAD_TYPE, watermark = determina_tipo_carga(
#     table_name='cvc_metastore.cvc_corp_work.checklistfacil_checklists',
#     incremental_column="DT_HR_CARGA" # NÃO HÁ COLUNA DE WATERMARK
# )

# COMMAND ----------

def ingestao_api_paginada(
    base_url,
    endpoint,
    headers,
    params,
    sleep_seconds=61
):
    page = 1
    all_data = []

    while True:
        request_params = params.copy()
        request_params["page"] = page

        response = requests.get(
            f"{base_url}{endpoint}",
            headers=headers,
            params=request_params
        )

        if response.status_code != 200:
            raise Exception(response.text)

        json_resp = response.json()
        data = json_resp.get("data", [])
        meta = json_resp.get("meta", {})

        if not data:
            break

        all_data.extend(data)

        if not meta.get("hasMore", False):
            break

        page += 1
        time.sleep(sleep_seconds)

    return all_data


# COMMAND ----------

def ingestao_api_simples(base_url, endpoint, headers):
    response = requests.get(
        f"{base_url}{endpoint}",
        headers=headers
    )

    if response.status_code != 200:
        raise Exception(response.text)

    return response.json().get("data", [])

# COMMAND ----------

# def gera_bronze_schema(sample_record):
#     return StructType([
#         StructField(col, StringType(), True)
#         for col in sample_record.keys()
#     ])

def gera_bronze_schema(sample_record):
    fields = [
        StructField(col, StringType(), True)
        for col in sample_record.keys()
    ]
    
    # adiciona a coluna de data/hora da carga
    fields.append(StructField("DT_HR_CARGA", TimestampType(), True))

    return StructType(fields)


# COMMAND ----------

def executar_carga(df, table_name, load_type):
    """
    Executa a carga de um DataFrame em uma tabela Delta/Hive.
    
    Args:
        df: Spark DataFrame a ser persistido.
        table_name: Nome da tabela de destino.
        load_type: Tipo de carga ("FULL" para overwrite, qualquer outro para append).

        Exemplo de uso:
        executar_carga(meu_df, "stg_vendas", "FULL")
        
    """
    # Definindo o modo com base no parâmetro
    mode = "overwrite" if load_type == "FULL" else "append"
    
    logger.info(f"Iniciando carga na tabela {table_name} com modo: {mode}")
    
    # Executando a escrita
    df.write.mode(mode).saveAsTable(table_name)
    
    logger.info("Carga finalizada com sucesso.")

# Exemplo de uso:
# executar_carga(df, "stg_vendas", "FULL")