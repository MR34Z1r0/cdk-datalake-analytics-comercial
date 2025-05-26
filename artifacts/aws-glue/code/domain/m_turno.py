import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat, lit, coalesce, when, trim, row_number,current_date,upper
from pyspark.sql.types import StringType, DateType

spark_controller = SPARK_CONTROLLER() 
try: 
    cod_pais = COD_PAIS.split(",")
    logger.info(f"Databases: {cod_pais}")
 
    df_m_compania = spark_controller.read_table(data_paths.BIG_BAGIC, "m_compania", cod_pais=cod_pais)
    df_m_pais = spark_controller.read_table(data_paths.BIG_BAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)
    df_m_turno = spark_controller.read_table(data_paths.BIG_BAGIC, "m_turno", cod_pais=cod_pais)
 
    logger.info("Dataframes cargados correctamente")

    target_table_name = "m_turno"

except Exception as e:
    logger.error(e)
    raise 
 
try:
    logger.info("Starting creation of tmp_dominio_m_turno")
    tmp_dominio_m_turno = (
        df_m_turno.alias("mt")
        .join(df_m_compania.alias("mc"), col("mc.cod_compania") == col("mt.cod_compania"), "inner")
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            concat(
                trim(coalesce(col("mt.cod_compania"), lit(""))),
                lit("|"),
                trim(coalesce(col("mt.cod_sucursal"), lit(""))),
                lit("|"),
                trim(coalesce(col("mt.cod_turno"), lit(""))),
            ).cast(StringType()).alias("id_turno"),
            col("mt.cod_compania").cast(StringType()).alias("id_compania"),
            concat(
                trim(coalesce(col("mt.cod_compania"), lit(""))),
                lit("|"),
                trim(coalesce(col("mt.cod_sucursal"), lit(""))),
            ).cast(StringType()).alias("id_sucursal"),
            col("mt.cod_turno").cast(StringType()).alias("cod_turno"),
            col("mt.desc_turno").cast(StringType()).alias("desc_turno")
        )
    )

    id_columns = ["id_turno"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(tmp_dominio_m_turno, data_paths.DOMINIO, target_table_name, id_columns, partition_columns_array) 

except Exception as e:
    logger.error(e)
    raise