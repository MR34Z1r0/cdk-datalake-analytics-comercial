import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_forma_pago"
try:
    cod_pais = COD_PAIS.split(",")
    df_m_forma_pago = spark_controller.read_table(data_paths.DOMINIO, "m_forma_pago", cod_pais=cod_pais)

    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(e)
    raise
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
    spark_controller.upsert(df_dim_forma_pago, data_paths.COMERCIAL, target_table_name, id_columns, partition_columns_array)    
except Exception as e:
    logger.error(e)
    raise