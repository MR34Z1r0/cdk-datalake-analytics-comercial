import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
spark_controller = SPARK_CONTROLLER()
target_table_name = "m_pais"
try:
    m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal = True)
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")  
try:
    list_cod_pais = m_compania.select("cod_pais").rdd.flatMap(lambda x: x).collect()

    df_dom_m_pais = m_pais\
        .where(
            col("cod_pais").isin(list_cod_pais)
        ).select(
            col("id_pais").cast(StringType()),
            col("cod_pais").cast(StringType()),
            col("desc_pais").cast(StringType()),
            col("continente").cast(StringType()).alias("desc_continente")
        )
    
    id_columns = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dom_m_pais, data_paths.DOMAIN, target_table_name, id_columns)
    logger.info(f"Upsert de {target_table_name} success completed")    
except Exception as e:
    logger.error(f"Error processing df_dom_m_pais: {e}")
    raise ValueError(f"Error processing df_dom_m_pais: {e}") 
