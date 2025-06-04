from common_jobs_functions import data_paths, logger, SPARK_CONTROLLER
from pyspark.sql.functions import col

spark_controller = SPARK_CONTROLLER()
target_table_name = "m_medio_transporte"

try:
    m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal = True)
    m_vehiculo = spark_controller.read_table(data_paths.BIGMAGIC, "m_vehiculo")
    m_tipo_vehiculo = spark_controller.read_table(data_paths.BIGMAGIC, "m_tipo_vehiculo")
    m_capacidad_vehiculo = spark_controller.read_table(data_paths.BIGMAGIC, "m_capacidad_vehiculo")

except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")

try:
    logger.info("Starting creation of df_m_medio_transporte")

    m_vehiculo = m_vehiculo.orderBy(col("fecha_modificacion").desc())
    m_vehiculo = m_vehiculo.dropDuplicates(["id_medio_transporte"])

    df_dom_m_medio_transporte = (
        m_vehiculo.alias("mv")
        .join(m_compania.alias("mc"), col("mc.cod_compania") == col("mv.cod_compania"), "inner")
        .join(m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .join(m_tipo_vehiculo.alias("mtv"), ((col("mv.cod_tipo_vehiculo") == col("mtv.cod_tipo_vehiculo")) & (col("mv.cod_compania") == col("mtv.cod_compania"))), "inner")
        .join(m_capacidad_vehiculo.alias("mcv"), ((col("mv.cod_tipo_vehiculo") == col("mcv.cod_tipo_capacidad_vehiculo")) & (col("mv.cod_compania") == col("mcv.cod_compania"))), "left")
        .select(
            col("mp.id_pais").alias("id_pais"),
            col("mv.id_medio_transporte"),
            col("mv.cod_vehiculo").alias("cod_medio_transporte"),
            col("mv.cod_tipo_vehiculo").alias("cod_tipo_medio_transporte"),
            col("mtv.descripcion").alias("desc_tipo_medio_transporte"),
            col("mv.marca").alias("desc_marca_medio_transporte"),
            col("mcv.desc_tipo_vehiculo").alias("desc_tipo_capacidad"),
            col("mcv.cod_tipo_capacidad_vehiculo").alias("cod_tipo_capacidad"),
            col("mv.capacidad_max_kg").alias("cant_peso_maximo"),
            col("mcv.pesoxcamion").alias("cant_peso_camion"),
            col("mcv.tarimasxcamion").alias("cant_tarimas_camion"),
            col("mv.fecha_creacion").alias("fecha_creacion"),
            col("mv.fecha_modificacion").alias("fecha_modificacion"),
        )
        .distinct()
    )

    tmp = df_dom_m_medio_transporte.select(
        col("id_medio_transporte").cast("string").alias("id_medio_transporte"),
        col("id_pais").cast("string").alias("id_pais"),
        col("cod_medio_transporte").cast("string").alias("cod_medio_transporte"),
        col("cod_tipo_medio_transporte").cast("string").alias("cod_tipo_medio_transporte"),
        col("desc_tipo_medio_transporte").cast("string").alias("desc_tipo_medio_transporte"),
        col("desc_marca_medio_transporte").cast("string").alias("desc_marca_medio_transporte"),
        col("desc_tipo_capacidad").cast("string").alias("desc_tipo_capacidad"),
        col("cod_tipo_capacidad").cast("string").alias("cod_tipo_capacidad"),
        col("cant_peso_maximo").cast("integer").alias("cant_peso_maximo"),
        col("cant_peso_camion").cast("integer").alias("cant_peso_camion"),
        col("cant_tarimas_camion").cast("integer").alias("cant_tarimas_camion"),
        col("fecha_creacion").cast("date").alias("fecha_creacion"),
        col("fecha_modificacion").cast("date").alias("fecha_modificacion"),
    )

    id_columns = ["id_medio_transporte"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dom_m_medio_transporte, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert of {target_table_name} finished")
except Exception as e:
    logger.error(f"Error processing df_dom_m_medio_transporte: {e}")
    raise ValueError(f"Error processing df_dom_m_medio_transporte: {e}")