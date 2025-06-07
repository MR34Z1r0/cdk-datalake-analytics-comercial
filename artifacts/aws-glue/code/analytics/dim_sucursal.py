import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_sucursal"
try: 
    df_m_sucursal = spark_controller.read_table(data_paths.DOMAIN, "m_sucursal") 
    df_m_pais = spark_controller.read_table(data_paths.DOMAIN, "m_pais") 
    df_m_compania = spark_controller.read_table(data_paths.DOMAIN, "m_compania")    
    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("Starting creation of df_dim_sucursal")
    df_dim_sucursal = (
        df_m_sucursal.alias("ms")
        .join(
            df_m_pais.alias("mp"),
            (col("mp.id_pais") == col("ms.id_pais")),
            "inner",
        )
        .join(
            df_m_compania.alias("mc"),
            (col("ms.id_compania") == col("mc.id_compania"))
            & (col("ms.id_pais") == col("mc.id_pais")),
            "inner",
        )
        .select(
            col('ms.id_sucursal').cast("string"),
            col('ms.id_pais').cast("string"),
            col('mc.cod_compania').cast("string"),
            col('mc.nomb_compania').cast("string"),
            col('mc.cod_tipo_compania').cast("string"),
            col('ms.cod_sucursal').cast("string"),
            col('ms.nomb_sucursal').cast("string"),
            col('ms.cod_tipo_sucursal').cast("string")
        )
    )

    id_columns = ["id_sucursal"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_sucursal, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array) 
    logger.info(f"Upsert de {target_table_name} success completed")     
except Exception as e:
    logger.error(f"Error processing df_dim_sucursal: {e}")
    raise ValueError(f"Error processing df_dim_sucursal: {e}") 