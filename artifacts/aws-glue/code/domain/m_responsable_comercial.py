import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, concat, concat_ws, lit, coalesce, when , current_date, trim
from pyspark.sql.types import StringType, IntegerType, DecimalType, TimestampType, DateType

spark_controller = SPARK_CONTROLLER()
target_table_name = "m_responsable_comercial"
try: 
    df_m_vendedor = spark_controller.read_table(data_paths.APDAYC, "m_vendedor")
    df_m_persona = spark_controller.read_table(data_paths.APDAYC, "m_persona")
    df_m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais",have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")  
try:
    df_dom_m_responsable_comercial = (
        df_m_vendedor.alias("mv")
        .join(
            df_m_persona.alias("mpe"),
            (col("mv.cod_vendedor") == col("mpe.cod_persona")) & (col("mv.cod_compania") == col("mpe.cod_compania")),
            "inner",
        )
        .join(
            df_m_compania.alias("mc"),
            col("mv.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .select(
            concat_ws("|",
                trim(col("mv.cod_compania")),
                trim(col("mv.cod_vendedor"))
            ).cast(StringType()).alias("id_responsable_comercial"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            trim(col("mv.cod_vendedor")).cast(StringType()).alias("cod_responsable_comercial"),
            col("mpe.nomb_persona").cast(StringType()).alias("nomb_responsable_comercial"),
            col("mv.cod_tipo_vendedor").cast(StringType()).alias("cod_tipo_responsable_comercial"),
            lit(None).cast(StringType()).alias("estado"), 
            current_date().cast(TimestampType()).alias("fecha_creacion"),
            current_date().cast(TimestampType()).alias("fecha_modificacion")
        )
    )
    
    id_columns = ["id_responsable_comercial"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dom_m_responsable_comercial, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} completado exitosamente")
except Exception as e:
    logger.error(f"Error processing df_dom_m_responsable_comercial: {e}")
    raise ValueError(f"Error processing df_dom_m_responsable_comercial: {e}")