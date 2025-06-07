import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_forma_pago"
try:
    df_m_forma_pago = spark_controller.read_table(data_paths.DOMAIN, "m_forma_pago")
    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("Starting creation of df_dim_forma_pago")
    df_dim_forma_pago = (
        df_m_forma_pago
        .select(
            col('id_forma_pago').cast("string"),
            col('id_pais').cast("string"),
            col('cod_forma_pago').cast("string"),
            col('nomb_forma_pago').cast("string").alias('desc_forma_pago')
        )
    )

    id_columns = ["id_forma_pago"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_forma_pago, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")    
except Exception as e:
    logger.error(f"Error processing df_dim_forma_pago: {e}")
    raise ValueError(f"Error processing df_dim_forma_pago: {e}") 