import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat, coalesce , lit
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType,DecimalType

spark_controller = SPARK_CONTROLLER() 
try:
    cod_pais = COD_PAIS.split(",") 
    df_m_compania = spark_controller.read_table(data_paths.BIG_MAGIC, "m_compania", cod_pais=cod_pais)
    df_m_pais = spark_controller.read_table(data_paths.BIG_MAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)
    df_m_familia_equipo = spark_controller.read_table(data_paths.BIG_MAGIC, "m_familia_equipo", cod_pais=cod_pais) 
    target_table_name = "m_familia_equipo" 
except Exception as e:
    logger.error(e)
    raise 
 
try:
    logger.info("Starting creation of tmp_dominio_m_familia_equipo")
    tmp_dominio_m_familia_equipo = (
        df_m_familia_equipo.alias("mfe")
        .join(df_m_compania.alias("mc"), col("mc.cod_compania") == col("mfe.cod_compania"), "inner")
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            concat(
                coalesce(col("mfe.cod_compania"), lit("")),
                lit("|"),
                coalesce(col("mfe.cod_sucursal"), lit("")),
                lit("|"),
                coalesce(col("mfe.cod_familia_equipo"), lit("")),
            ).cast(StringType()).alias("id_familia_equipo"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            col("mfe.cod_compania").cast(StringType()).alias("id_compania"),
            concat(
                coalesce(col("mfe.cod_compania"), lit("")),
                lit("|"),
                coalesce(col("mfe.cod_sucursal"), lit("")),
            ).cast(StringType()).alias("id_sucursal"),
            col("mfe.cod_familia_equipo").cast(StringType()).alias("cod_familia_equipo"),
            col("mfe.cod_area").cast(StringType()).alias("cod_area"),
            col("mfe.descripcion").cast(StringType()).alias("desc_familia_equipo"),
            col("mfe.nivel_costo").cast(DecimalType(38, 12)).alias("nivel_costo"),
            col("mfe.estado").cast(StringType()).alias("estado")
        )
    )

    id_columns = ["id_familia_equipo"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(tmp_dominio_m_familia_equipo, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(str(e))
    raise