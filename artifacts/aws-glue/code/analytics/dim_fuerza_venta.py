import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_fuerza_venta"
try:
    cod_pais = COD_PAIS.split(",")
    df_m_fuerza_venta = spark_controller.read_table(data_paths.DOMINIO, "m_fuerza_venta", cod_pais=cod_pais)

    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(e)
    raise
try:
    logger.info("Starting creation of df_dim_fuerza_venta")
    df_dim_fuerza_venta = (
        df_m_fuerza_venta
        .select(
            col('id_fuerza_venta').cast("string"),
            col('id_pais').cast("string"),
            col('cod_fuerza_venta').cast("string"),
            col('desc_fuerza_venta').cast("string").alias('desc_fuerza_venta')
        )
    )

    id_columns = ["id_fuerza_venta"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(df_dim_fuerza_venta, data_paths.COMERCIAL, target_table_name, id_columns, partition_columns_array)    
except Exception as e:
    logger.error(e)
    raise