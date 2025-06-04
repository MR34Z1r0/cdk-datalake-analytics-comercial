import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, concat, concat_ws, lit, coalesce, when, trim, row_number,current_date,upper,split
from pyspark.sql.types import StringType, DateType 

spark_controller = SPARK_CONTROLLER()
target_table_name = "m_modelo_atencion" 
try:
    df_m_modelo_atencion = spark_controller.read_table(data_paths.APDAYC, "m_modelo_atencion")
    df_m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais",have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    df_dom_m_modelo_atencion = (
        df_m_modelo_atencion.alias("mma")
        .join(
            df_m_compania.alias("mc"),
            (col("mma.id_compania") == col("mc.cod_compania")),
            "inner",
        )
        .join(
            df_m_pais.alias("mp"),
            (col("mc.cod_pais") == col("mp.cod_pais")),
            "inner",
        )
        .select(
            concat_ws("|",
                trim(col("mma.id_compania")),
                trim(col("mma.cod_modelo_atencion").cast("string")),
            ).alias("id_modelo_atencion"),
            col("mp.id_pais").alias("id_pais"),
            col("mma.cod_modelo_atencion"),
            col("mma.desc_modelo_atencion").alias("desc_modelo_atencion"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
        .distinct()
        .select(
            col("id_modelo_atencion").cast(StringType()),
            col("id_pais").cast(StringType()),
            col("cod_modelo_atencion").cast(StringType()),
            col("desc_modelo_atencion").cast(StringType()),
            col("fecha_creacion").cast(DateType()),
            col("fecha_modificacion").cast(DateType()),
        )
    )
    
    id_columns = ["id_modelo_atencion"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dom_m_modelo_atencion, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert of {target_table_name} finished")
except Exception as e:
    logger.error(f"Error processing df_dom_m_modelo_atencion: {e}")
    raise ValueError(f"Error processing df_dom_m_modelo_atencion: {e}")