import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat, lit, coalesce, when, trim, row_number,current_date,upper
from pyspark.sql.types import StringType, DecimalType

spark_controller = SPARK_CONTROLLER() 
try:
    cod_pais = COD_PAIS.split(",") 
    df_m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania", cod_pais=cod_pais)
    df_m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", cod_pais=cod_pais,have_principal = True)
    df_m_formula_fabricacion = spark_controller.read_table(data_paths.APDAYC, "m_formula_fabricacion", cod_pais=cod_pais) 
    target_table_name = "m_formula_fabricacion" 
except Exception as e:
    logger.error(e)
    raise 

try:
    logger.info("Starting creation of tmp_dominio_m_formula_fabricacion")
    tmp_dominio_m_formula_fabricacion = (
        df_m_formula_fabricacion.alias("ff")
        .join(df_m_compania.alias("mc"), col("mc.cod_compania") == col("ff.cod_compania"), "inner")
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            col("ff.cod_compania").cast(StringType()).alias("id_compania"),
            concat(
                trim(coalesce(col("ff.cod_compania"), lit(""))),
                lit("|"),
                trim(coalesce(col("ff.cod_sucursal"), lit(""))),
                lit("|"),
                trim(coalesce(col("ff.cod_articulo"), lit(""))),
                lit("|"),
                trim(coalesce(col("ff.cod_material"), lit("")))
            ).cast(StringType()).alias("id_formula_fabricacion"),
            concat(
                trim(coalesce(col("ff.cod_compania"), lit(""))),
                lit("|"),
                trim(coalesce(col("ff.cod_sucursal"), lit(""))),
            ).cast(StringType()).alias("id_sucursal"),
            concat(
                trim(coalesce(col("ff.cod_compania"), lit(""))),
                lit("|"),
                trim(coalesce(col("ff.cod_articulo"), lit(""))),
            ).cast(StringType()).alias("id_articulo"),
            concat(
                trim(coalesce(col("ff.cod_compania"), lit(""))),
                lit("|"),
                trim(coalesce(col("ff.cod_material"), lit(""))),
            ).cast(StringType()).alias("id_material"),
            col("ff.cod_material").cast(StringType()).alias("cod_material"),
            col("ff.porcentaje").cast(DecimalType(38, 12)).alias("porcentaje"),
            col("ff.factor_conversion").cast(DecimalType(38, 12)).alias("factor_conversion"),
            col("ff.cant_fabricacion").cast(DecimalType(38, 12)).alias("cant_fabricacion"),
            col("ff.stock_disponible").cast(DecimalType(38, 12)).alias("stock_disponible"),
            col("ff.tipo_distribucion").cast(StringType()).alias("tipo_distribucion")
        )
    ) 
    id_columns = ["id_formula_fabricacion"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(tmp_dominio_m_formula_fabricacion, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array) 
except Exception as e:
    logger.error(str(e))
    raise