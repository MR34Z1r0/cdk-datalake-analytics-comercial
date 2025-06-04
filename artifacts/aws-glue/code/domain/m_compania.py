import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, DateType

spark_controller = SPARK_CONTROLLER()
target_table_name = "m_compania"
try:
    m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal = True)
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")  
try:
    df_dom_m_compania = (
        m_compania.alias("mc")
        .join(m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .select(
            col("mc.cod_compania").cast(StringType()).alias("id_compania"),
            #col("sc.id").cast(StringType()).alias("id_compania_ref"),
            lit(None).cast(StringType()).alias("id_compania_ref"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            col("cod_compania").cast(StringType()).alias("cod_compania"),
            col("desc_compania").cast(StringType()).alias("nomb_compania"),
            lit(None).cast(StringType()).alias("cod_tipo_compania"), 
            col("es_activo").cast(StringType()).alias("estado"),
            col("mc.fecha_creacion").cast(DateType()).alias("fecha_creacion"),
            col("mc.fecha_modificacion").cast(DateType()).alias("fecha_modificacion"),
        )
    )

    id_columns = ["id_compania"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dom_m_compania, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
except Exception as e:
    logger.error(f"Error processing df_dom_m_compania: {e}")
    raise ValueError(f"Error processing df_dom_m_compania: {e}") 