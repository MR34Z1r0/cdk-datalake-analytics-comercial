from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat_ws

spark_controller = SPARK_CONTROLLER()
target_table_name = "m_empleado"

try:
    cod_pais = COD_PAIS.split(",")

    m_pais = spark_controller.read_table(data_paths.BIG_MAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)
    m_compania = spark_controller.read_table(data_paths.BIG_MAGIC, "m_compania", cod_pais=cod_pais)
    m_empleado = spark_controller.read_table(data_paths.BIG_MAGIC, "m_empleado", cod_pais=cod_pais)


except Exception as e:
    logger.error(str(e))
    raise

try:
    m_pais = m_pais.filter(col("id_pais").isin(cod_pais))

    tmp_m_empleado = (
      m_empleado.alias("me")
      .join(m_compania.alias("mc"), col("me.cod_compania") == col("mc.cod_compania"), "inner")
      .join(m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
      .select(
      col("mp.id_pais"),
      col("me.cod_empleado"),
      concat_ws("|", col("me.cod_compania"), col("me.cod_empleado")).alias("id_empleado"),
      col("me.nombres").alias("nomb_empleado"),
      col("me.estado"),
      col("me.fecha_creacion"),
      col("me.fecha_modificacion"),
      )
    )

    tmp = tmp_m_empleado.select(
      col("id_pais").cast("string").alias("id_pais"),
      col("id_empleado").cast("string").alias("id_empleado"),
      col("cod_empleado").cast("string").alias("cod_empleado"),
      col("nomb_empleado").cast("string").alias("nomb_empleado"),
      col("estado").cast("string").alias("estado"),
      col("fecha_creacion").cast("date").alias("fecha_creacion"),
      col("fecha_modificacion").cast("date").alias("fecha_modificacion"),
    )

    id_columns = ["id_empleado", "id_pais"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(tmp, data_paths.DOMINIO, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(str(e))
    raise
