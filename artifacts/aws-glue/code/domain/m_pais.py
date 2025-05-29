import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")
    m_compania = spark_controller.read_table(data_paths.BIG_MAGIC, "m_compania", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.BIG_MAGIC, "m_pais", cod_pais=cod_pais, have_principal = True)

    target_table_name = "m_pais"

    
except Exception as e:
    logger.error(e)
    raise


else:
    try:
        list_cod_pais = m_compania.select("cod_pais").rdd.flatMap(lambda x: x).collect()

        tmp_dominio_m_pais = m_pais\
            .where(
                col("cod_pais").isin(list_cod_pais) & 
                col("id_pais").isin(cod_pais) \
            ).select(
                col("id_pais").cast(StringType()),
                col("cod_pais").cast(StringType()),
                col("desc_pais").cast(StringType()),
                col("desc_pais_comercial").cast(StringType()),
                col("continente").cast(StringType()).alias("desc_continente")
            )
        
        spark_controller.upsert(tmp_dominio_m_pais, data_paths.DOMAIN, target_table_name, ['id_pais'])

       
    except Exception as e:
        logger.error(str(e))
        raise
