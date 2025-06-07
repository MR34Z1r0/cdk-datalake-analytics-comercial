import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_tipo_pedido"
try:
    df_m_tipo_pedido = spark_controller.read_table(data_paths.DOMAIN, "m_tipo_pedido")
    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("Starting creation of df_dim_tipo_pedido")
    df_dim_tipo_pedido = (
        df_m_tipo_pedido
        .select(
            col('id_tipo_pedido').cast("string"),
            col('id_pais').cast("string"),
            col('cod_tipo_pedido').cast("string"),
            col('nomb_tipo_pedido').cast("string").alias('desc_tipo_pedido')
        )
    )

    id_columns = ["id_tipo_pedido"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_tipo_pedido, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array) 
    logger.info(f"Upsert de {target_table_name} success completed")     
except Exception as e:
    logger.error(f"Error processing df_dim_tipo_pedido: {e}")
    raise ValueError(f"Error processing df_dim_tipo_pedido: {e}") 