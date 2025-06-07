import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, upper, lit, cast

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_clasificacion_cliente" 
try:
    df_m_clasificacion_cliente = spark_controller.read_table(data_paths.DOMAIN, "m_clasificacion_cliente").cache()
    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("Starting creation of df_m_clasificacion_cliente_subgiro")
    df_m_clasificacion_cliente_subgiro = (
        df_m_clasificacion_cliente.alias("mcc")
        .where(
            (upper(col('cod_tipo_clasificacion_cliente')) == 'SUBGIRO')
        )
        .select(
            col('mcc.id_pais'),
            col('mcc.id_clasificacion_cliente').alias('id_subgiro'),
            col('mcc.id_clasificacion_cliente_padre').alias('id_giro'),
            col('mcc.cod_clasificacion_cliente').alias('cod_subgiro'),
            col('mcc.nomb_clasificacion_cliente').alias('desc_subgiro')
        )
    )
    
    df_m_clasificacion_cliente_giro = (
        df_m_clasificacion_cliente.alias("mcc")
        .where(
            (upper(col('cod_tipo_clasificacion_cliente')) == 'GIRO')
        )
        .select(
            col('mcc.id_pais'),
            col('mcc.id_clasificacion_cliente').alias('id_giro'),
            col('mcc.id_clasificacion_cliente_padre').alias('id_canal'),
            col('mcc.cod_clasificacion_cliente').alias('cod_giro'),
            col('mcc.nomb_clasificacion_cliente').alias('desc_giro')
        )
    )
     
    df_m_clasificacion_cliente_canal = (
        df_m_clasificacion_cliente.alias("mcc")
        .where(
            (upper(col('cod_tipo_clasificacion_cliente')) == 'CANAL')
        )
        .select(
            col('mcc.id_pais'),
            col('mcc.id_clasificacion_cliente').alias('id_canal'),
            col('mcc.cod_clasificacion_cliente').alias('cod_canal'),
            col('mcc.nomb_clasificacion_cliente').alias('desc_canal')
        )
    )
    
    df_dim_clasificacion_cliente = (
        df_m_clasificacion_cliente_subgiro.alias("su")
        .join(
            df_m_clasificacion_cliente_giro.alias("gi"),
            (col("gi.id_giro") == col("su.id_giro")),
            "left"
        )
        .join(
            df_m_clasificacion_cliente_canal.alias("ca"),
            (col("ca.id_canal") == col("gi.id_canal")),
            "left",
        )
        .select(
            col('su.id_subgiro').cast("string").alias('id_clasificacion_cliente'),
            col('su.id_pais').cast("string").alias('id_pais'),
            col('su.cod_subgiro').cast("string").alias('cod_subgiro'),
            col('su.desc_subgiro').cast("string").alias('desc_subgiro'),
            lit(None).cast("string").alias('cod_ocasion_consumo'),
            lit(None).cast("string").alias('desc_ocasion_consumo'),
            col('gi.cod_giro').cast("string").alias('cod_giro'),
            col('gi.desc_giro').cast("string").alias('desc_giro'),
            col('ca.cod_canal').cast("string").alias('cod_canal'),
            col('ca.desc_canal').cast("string").alias('desc_canal')
        )
    )

    column_keys = ["id_clasificacion_cliente"]
    partition_keys = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_clasificacion_cliente, data_paths.ANALYTICS, target_table_name, column_keys, partition_keys)
    logger.info(f"Upsert de {target_table_name} success completed")
except Exception as e:
    logger.error(f"Error processing df_dim_clasificacion_cliente: {e}")
    raise ValueError(f"Error processing df_dim_clasificacion_cliente: {e}") 