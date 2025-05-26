import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_transportista"
try:
    cod_pais = COD_PAIS.split(",")
    df_m_transportista = spark_controller.read_table(data_paths.DOMINIO, "m_transportista", cod_pais=cod_pais) 

    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(e)
    raise
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
    spark_controller.upsert(df_dim_transportista, data_paths.COMERCIAL, target_table_name, id_columns, partition_columns_array)    
except Exception as e:
    logger.error(e)
    raise