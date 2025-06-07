import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, coalesce, when, trim, row_number,current_date,upper
from pyspark.sql.types import StringType

spark_controller = SPARK_CONTROLLER()
target_table_name = "m_fuerza_venta"
try:
    df_m_fuerza_venta = spark_controller.read_table(data_paths.BIGMAGIC, "m_fuerza_venta")
    df_m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais",have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")

except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("Starting creation of df_m_fuerza_venta")
    
    df_dom_m_fuerza_venta = (
    df_m_fuerza_venta.alias("mfv")
    .join(df_m_compania.alias("mc"),
        col("mfv.cod_compania") == col("mc.cod_compania"), "inner")
    .join(df_m_pais.alias("mp"),
        col("mc.cod_pais") == col("mp.cod_pais"), "inner")
    .select(
        col("mfv.id_fuerza_venta").cast(StringType()).alias("id_fuerza_venta"),
        col("mp.id_pais").cast(StringType()).alias("id_pais"),
        trim(col("mfv.cod_fuerza_venta")).cast(StringType()).alias("cod_fuerza_venta"),
        col("mfv.desc_fuerza_venta").cast(StringType()).alias("desc_fuerza_venta")
    )
    )

    id_columns = ["id_fuerza_venta"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dom_m_fuerza_venta, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert of {target_table_name} finished")
except Exception as e:
    logger.error(f"Error processing df_m_fuerza_venta: {e}")
    raise ValueError(f"Error processing df_m_fuerza_venta: {e}")