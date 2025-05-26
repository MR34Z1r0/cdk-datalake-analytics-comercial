import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_tipo_venta"
try:
    cod_pais = COD_PAIS.split(",")
    df_m_tipo_venta = spark_controller.read_table(data_paths.DOMINIO, "m_tipo_venta", cod_pais=cod_pais)

    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(e)
    raise
try:
    logger.info("Starting creation of df_dim_tipo_venta")
    df_dim_tipo_venta = (
        df_m_tipo_venta
        .select(
            col('id_tipo_venta'),
            col('id_pais'),
            col('cod_tipo_venta'),
            lit(None).cast("string").alias('cod_subtipo_venta'),
            col('nomb_tipo_venta').alias('desc_tipo_venta'),
            lit(None).cast("string").alias('desc_subtipo_venta'),
            col('cod_tipo_operacion').alias('cod_tipo_operacion'),
        )
    )

    id_columns = ["id_tipo_venta"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(df_dim_tipo_venta, data_paths.COMERCIAL, target_table_name, id_columns, partition_columns_array)    
except Exception as e:
    logger.error(e)
    raise