import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import StringType, DateType

spark_controller = SPARK_CONTROLLER()
target_table_name = "m_forma_pago"
try:
    df_m_condicion_pago = spark_controller.read_table(data_paths.BIGMAGIC, "m_forma_pago")
    df_m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    df_m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais",have_principal = True)
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("Starting creation of df_m_forma_pago")
    
    df_dom_m_forma_pago = (
        df_m_condicion_pago.alias("mfp")
        .join(df_m_compania.alias("mc"),
            col("mfp.cod_compania") == col("mc.cod_compania"), "inner")
        .join(df_m_pais.alias("mp"), 
              col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .select(
            col("mfp.id_forma_pago").cast(StringType()).alias("id_forma_pago"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            col("mfp.cod_forma_pago").cast(StringType()).alias("cod_forma_pago"),
            col("mfp.desc_forma_pago").cast(StringType()).alias("nomb_forma_pago"),
            col("mfp.fecha_creacion").cast(DateType()).alias("fecha_creacion"),
            col("mfp.fecha_modificacion").cast(DateType()).alias("fecha_modificacion"),
        )
    )

    id_columns = ["id_forma_pago"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dom_m_forma_pago, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
except Exception as e:
    logger.error(f"Error processing df_dom_m_forma_pago: {e}")
    raise ValueError(f"Error processing df_dom_m_forma_pago: {e}") 