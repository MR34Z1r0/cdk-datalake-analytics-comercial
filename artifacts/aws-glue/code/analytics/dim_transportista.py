import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_transportista"
try:
    df_m_transportista = spark_controller.read_table(data_paths.DOMAIN, "m_transportista")
    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("Starting creation of df_dim_transportista")
    df_dim_transportista = (
        df_m_transportista
        .select(
            col("id_transportista").cast("string"),
            col("id_pais").cast("string"),
            col("cod_transportista").cast("string"),
            col("nomb_transportista").cast("string"),
            col("cod_tipo_transportista").cast("string"),
            col("desc_tipo_transportista").cast("string"),
            col("ruc_transportista").cast("string"),
        )
    )

    id_columns = ["id_transportista"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_transportista, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")     
except Exception as e:
    logger.error(f"Error processing df_dim_transportista: {e}")
    raise ValueError(f"Error processing df_dim_transportista: {e}") 