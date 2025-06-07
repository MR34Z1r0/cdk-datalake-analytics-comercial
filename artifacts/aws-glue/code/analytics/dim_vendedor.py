import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_vendedor"
try:
    df_m_responsable_comercial = spark_controller.read_table(data_paths.DOMAIN, "m_responsable_comercial")
    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    df_dim_vendedor = (
        df_m_responsable_comercial
        .select(
            col('id_responsable_comercial').cast("string").alias('id_vendedor'),
            col('id_pais').cast("string"),
            col('cod_responsable_comercial').cast("string").alias('cod_vendedor'),
            col('nomb_responsable_comercial').cast("string").alias('nombre_vendedor')
        )
    )

    id_columns = ["id_vendedor"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_vendedor, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")     
except Exception as e:
    logger.error(f"Error processing df_dim_vendedor: {e}")
    raise ValueError(f"Error processing df_dim_vendedor: {e}") 