import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_lista_precio" 
try:
    df_m_lista_precio = spark_controller.read_table(data_paths.DOMAIN, "m_lista_precio")
    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("Starting creation of df_dim_lista_precio")
    df_dim_lista_precio = (
        df_m_lista_precio
        .select(
            col('id_lista_precio'),
            col('id_pais'),
            col('cod_lista_precio'),
            col('nomb_lista_precio').alias('desc_lista_precio')
        )
    )

    id_columns = ["id_lista_precio"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_lista_precio, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")    
except Exception as e:
    logger.error(f"Error processing df_dim_lista_precio: {e}")
    raise ValueError(f"Error processing df_dim_lista_precio: {e}") 