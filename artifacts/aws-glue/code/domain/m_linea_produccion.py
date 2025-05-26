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
    df_m_equipo_insumo = spark_controller.read_table(data_paths.BIG_BAGIC, "m_equipo_insumo", cod_pais=cod_pais)

    target_table_name = "m_linea_produccion"

    logger.info("Dataframes cargados correctamente")

except Exception as e:
    logger.error(e)
    raise 

try:
    logger.info("Starting creation of tmp_dominio_m_linea_produccion")
    tmp_dominio_m_linea_produccion = (
        df_m_equipo_insumo.alias("mei")
        .join(df_m_compania.alias("mc"), col("mc.cod_compania") == col("mei.cod_compania"), "inner")
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            concat(
                trim(coalesce(col("mei.cod_compania"), lit(""))),
                lit("|"),
                trim(coalesce(col("mei.cod_sucursal"), lit(""))),
                lit("|"),
                trim(coalesce(col("mei.cod_familia_equipo"), lit(""))),
                lit("|"),
                trim(coalesce(col("mei.cod_equipo_insumo"), lit("")))
            ).cast(StringType()).alias("id_linea_produccion"),
            col("mei.cod_compania").cast(StringType()).alias("id_compania"),
            concat(
                trim(coalesce(col("mei.cod_compania"), lit(""))),
                lit("|"),
                trim(coalesce(col("mei.cod_sucursal"), lit(""))),
            ).cast(StringType()).alias("id_sucursal"),
            concat(
                trim(coalesce(col("mei.cod_compania"), lit(""))),
                lit("|"),
                trim(coalesce(col("mei.cod_sucursal"), lit(""))),
                lit("|"),
                trim(coalesce(col("mei.cod_familia_equipo"), lit(""))),
            ).cast(StringType()).alias("id_familia_equipo"),
            col("mei.cod_equipo_insumo").cast(StringType()).alias("cod_linea_produccion"),
            col("mei.desc_equipo_insumo").cast(StringType()).alias("desc_linea_produccion"),
            col("mei.estado").cast(StringType()).alias("estado")
        )
    )
    
    id_columns = ["id_linea_produccion"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(tmp_dominio_m_linea_produccion, data_paths.DOMINIO, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise