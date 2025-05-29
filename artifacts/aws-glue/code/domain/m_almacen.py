from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col

spark_controller = SPARK_CONTROLLER()
target_table_name = "m_almacen"
try:
    cod_pais = COD_PAIS.split(",")
    df_m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania", cod_pais=cod_pais)
    df_m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", cod_pais=cod_pais,have_principal = True)
    df_m_almacen = spark_controller.read_table(data_paths.APDAYC, "m_almacen", cod_pais=cod_pais)
except Exception as e:
    logger.error(e)
    raise

try:
    logger.info("Starting creation of tmp_dominio_m_almacen")
    tmp_dominio_m_almacen = (
        df_m_almacen.alias("ma")
        .join(df_m_compania.alias("mc"), col("mc.cod_compania") == col("ma.cod_compania"), "inner")
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            col("ma.id_almacen").cast("string").alias("id_almacen"),
            col("mp.id_pais").cast("string").alias("id_pais"),
            col("ma.cod_compania").cast("string").alias("id_compania"),
            col("ma.id_sucursal").cast("string").alias("id_sucursal"),
            col("ma.cod_almacen").cast("string").alias("cod_almacen"),
            col("ma.desc_almacen").cast("string").alias("desc_almacen"),
            col("ma.tipo_almacen").cast("string").alias("desc_tipo_almacen")
        )
    )

    id_columns = ["id_almacen"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(tmp_dominio_m_almacen, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)
except Exception as e:
    logger.error(str(e))
    raise