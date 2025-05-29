from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat_ws

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")

    m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", cod_pais=cod_pais,have_principal = True)
    m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania", cod_pais=cod_pais)
    m_tipo_embarque = spark_controller.read_table(data_paths.APDAYC, "m_tipo_embarque", cod_pais=cod_pais)

    target_table_name = "m_via_embarque"

except Exception as e:
    logger.error(e)
    raise

try:
    m_pais = m_pais.filter(col("id_pais").isin(cod_pais))

    tmp_m_via_embarque = (
        m_tipo_embarque.alias("mte")
        .join(m_compania.alias("mc"), col("mte.cod_compania") == col("mc.cod_compania"), "inner")
        .join(m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .select(
            col("mp.id_pais"),
            col("mte.cod_compania").alias("id_compania"),
            concat_ws("|", col("mte.cod_compania"), col("mte.cod_via_embarque")).alias("id_via_embarque"),
            col("mte.cod_via_embarque"),
            col("mte.descripcion_larga").alias("desc_via_embarque1"),
            col("mte.descripcion_corta").alias("desc_via_embarque2"),
            col("mte.estado").alias("cod_estado"),
            col("mte.fecha_creacion"),
            col("mte.fecha_modificacion"),
        )
    )

    tmp = tmp_m_via_embarque.select(
      col("id_pais").cast("string").alias("id_pais"),
      col("id_compania").cast("string").alias("id_compania"),
      col("id_via_embarque").cast("string").alias("id_via_embarque"),
      col("cod_via_embarque").cast("string").alias("cod_via_embarque"),
      col("desc_via_embarque1").cast("string").alias("desc_via_embarque1"),
      col("desc_via_embarque2").cast("string").alias("desc_via_embarque2"),
      col("cod_estado").cast("string").alias("cod_estado"),
      col("fecha_creacion").cast("date").alias("fecha_creacion"),
      col("fecha_modificacion").cast("date").alias("fecha_modificacion"),
    )

    id_columns = ["id_via_embarque"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(tmp, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise
