from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat_ws, lit, when

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")

    m_pais = spark_controller.read_table(data_paths.BIG_MAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)
    m_compania = spark_controller.read_table(data_paths.BIG_MAGIC, "m_compania", cod_pais=cod_pais)
    m_termino = spark_controller.read_table(data_paths.BIG_MAGIC, "m_termino", cod_pais=cod_pais)

    target_table_name = "m_terminos"

except Exception as e:
    logger.error(e)
    raise

try:
    m_pais = m_pais.filter(col("id_pais").isin(cod_pais))

    tmp_m_terminos = (
        m_termino.alias("mt")
        .join(m_compania.alias("mc"), col("mt.cod_compania") == col("mc.cod_compania"), "inner")
        .join(m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .select(
            col("mp.id_pais"),
            col("mt.cod_compania").alias("id_compania"),
            concat_ws("|", col("mt.cod_compania"), col("mt.cod_terminos")).alias("id_terminos"),
            col("mt.cod_terminos"),
            col("mt.descripcion").alias("desc_termino"),
            col("mt.estado").alias("cod_estado"),
            col("mt.fecha_creacion"),
            col("mt.fecha_modificacion"),
            when(col("mt.es_seguro") == lit("T"), lit(1)).otherwise(lit(0)).alias("es_seguro")
        )
    )

    tmp = tmp_m_terminos.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_compania").cast("string").alias("id_compania"),
        col("id_terminos").cast("string").alias("id_terminos"),
        col("cod_terminos").cast("string").alias("cod_terminos"),
        col("desc_termino").cast("string").alias("desc_termino"),
        col("cod_estado").cast("string").alias("cod_estado"),
        col("fecha_creacion").cast("timestamp").alias("fecha_creacion"),
        col("fecha_modificacion").cast("timestamp").alias("fecha_modificacion"),
        col("es_seguro").cast("int").alias("es_seguro"),
    )

    id_columns = ["id_terminos"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(tmp, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise
