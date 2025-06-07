import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_medio_transporte" 
try:
    df_m_medio_transporte = spark_controller.read_table(data_paths.DOMAIN, "m_medio_transporte")
    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("Starting creation of df_dim_medio_transporte")
    df_dim_medio_transporte = (
        df_m_medio_transporte
        .select(
            col("id_medio_transporte").cast("string").alias("id_medio_transporte"),
            col("id_pais").cast("string").alias("id_pais"),
            col("cod_medio_transporte").cast("string").alias("cod_medio_transporte"),
            col("cod_tipo_medio_transporte").cast("string").alias("cod_tipo_medio_transporte"),
            col("desc_tipo_medio_transporte").cast("string").alias("desc_tipo_medio_transporte"),
            col("desc_marca_medio_transporte").cast("string").alias("desc_marca_medio_transporte"),
            col("cod_tipo_capacidad").cast("string").alias("cod_tipo_capacidad"),
            col("cant_peso_maximo").cast("integer").alias("cant_peso_maximo"),
            col("cant_tarimas_camion").cast("numeric(38,12)").alias("cant_tarimas"),
            col("fecha_creacion").cast("timestamp"),
            col("fecha_modificacion").cast("timestamp")
        )
    )

    id_columns = ["id_medio_transporte"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_medio_transporte, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")     
except Exception as e:
    logger.error(f"Error processing df_dim_medio_transporte: {e}")
    raise ValueError(f"Error processing df_dim_medio_transporte: {e}") 