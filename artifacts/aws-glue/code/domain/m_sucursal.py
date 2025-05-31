import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.types import StringType, DateType
spark_controller = SPARK_CONTROLLER()
target_table_name = "m_sucursal"
try:
    df_m_sucursal = spark_controller.read_table(data_paths.APDAYC, "m_sucursal")
    df_m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")  
try:
    df_dom_m_sucursal = (
        df_m_sucursal.alias("ms")
        .join(
            df_m_compania.alias("mc"),
            col("ms.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .select(
            concat_ws("|", col("ms.cod_compania"), col("ms.cod_sucursal")).cast(StringType()).alias("id_sucursal"),
            #col("ss.id").cast(StringType()).alias("id_sucursal_ref"),
            lit(None).cast(StringType()).alias("id_sucursal_ref"),
            col("ms.cod_compania").cast(StringType()).alias("id_compania"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            col("ms.cod_sucursal").cast(StringType()).alias("cod_sucursal"),
            col("ms.desc_sucursal").cast(StringType()).alias("nomb_sucursal"),
            #col("ss.tipo_de_sucursal__c").cast(StringType()).alias("cod_tipo_sucursal"),
            lit(None).cast(StringType()).alias("cod_tipo_sucursal"), 
            col("ms.es_activo").cast(StringType()).alias("estado"),
            col("ms.fecha_creacion").cast(DateType()).alias("fecha_creacion"),
            col("ms.fecha_modificacion").cast(DateType()).alias("fecha_modificacion"),
        )
    )

    id_columns = ["id_sucursal"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dom_m_sucursal, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} completado exitosamente")
except Exception as e:
    logger.error(f"Error processing df_dom_m_sucursal: {e}")
    raise ValueError(f"Error processing df_dom_m_sucursal: {e}") 