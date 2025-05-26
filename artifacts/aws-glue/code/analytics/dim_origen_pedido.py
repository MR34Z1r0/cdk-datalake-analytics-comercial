import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_origen_pedido"
try:
    cod_pais = COD_PAIS.split(",")
    df_m_origen_pedido = spark_controller.read_table(data_paths.DOMINIO, "m_origen_pedido", cod_pais=cod_pais)

    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(e)
    raise
try:
    logger.info("Starting creation of df_dim_origen_pedido")
    df_dim_origen_pedido = (
        df_m_origen_pedido
        .select(
            col('id_origen_pedido').cast("string"),
            col('id_pais').cast("string"),
            col('cod_origen_pedido').cast("string"),
            col('nomb_origen_pedido').cast("string").alias('desc_origen_pedido')
        )
    )

    id_columns = ["id_origen_pedido"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(df_dim_origen_pedido, data_paths.COMERCIAL, target_table_name, id_columns, partition_columns_array)    
except Exception as e:
    logger.error(e)
    raise