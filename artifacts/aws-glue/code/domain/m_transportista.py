from common_jobs_functions import data_paths, logger, SPARK_CONTROLLER
from pyspark.sql.functions import col

spark_controller = SPARK_CONTROLLER()
target_table_name = "m_transportista"

try:
    m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal = True)
    m_transportista = spark_controller.read_table(data_paths.BIGMAGIC, "m_transportista")
    m_persona = spark_controller.read_table(data_paths.BIGMAGIC, "m_persona")
    m_tipo_transportista = spark_controller.read_table(data_paths.BIGMAGIC, "m_tipo_transportista")

except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")

try:

    tmp_dominio_m_transportista = (
        m_transportista.alias("mt")
        .join(m_compania.alias("mc"), col("mc.cod_compania") == col("mt.cod_compania"), "inner")
        .join(m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .join(m_persona.alias("mpers"), col("mpers.id_persona") == col("mt.id_transportista"), "inner")
        .join(m_tipo_transportista.alias("mtt"), 
              (col("mt.cod_tipo_transportista") == col("mtt.cod_tipo_transportista")) 
              & (col("mt.cod_compania") == col("mtt.cod_compania"))
              , "left"
        )
        .select(
            col("mp.id_pais"),
            # col("mt.id_compania").alias("id_compania"), # confirmar
            col("mt.id_transportista"),
            col("mt.cod_transportista"),
            col("mpers.nomb_persona").alias("nomb_transportista"),
            col("mt.cod_tipo_transportista").alias("cod_tipo_transportista"),
            col("mtt.descripcion1").alias("desc_tipo_transportista"),
            col("mpers.nro_documento").alias("ruc_transportista"),
            col("mt.fecha_creacion").alias("fecha_creacion"),
            col("mt.fecha_modificacion").alias("fecha_modificacion")
        )
    )

    df_dom_m_transportista = tmp_dominio_m_transportista.select(
        col("id_transportista").cast("string").alias("id_transportista"),
        col("id_pais").cast("string").alias("id_pais"),
        col("cod_transportista").cast("integer").alias("cod_transportista"),
        col("nomb_transportista").cast("string").alias("nomb_transportista"),
        col("cod_tipo_transportista").cast("string").alias("cod_tipo_transportista"),
        col("desc_tipo_transportista").cast("string").alias("desc_tipo_transportista"),
        col("ruc_transportista").cast("string").alias("ruc_transportista"),
        col("fecha_creacion").cast("date").alias("fecha_creacion"),
        col("fecha_modificacion").cast("date").alias("fecha_modificacion"),
    )

    id_columns = ["id_transportista"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dom_m_transportista, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert of {target_table_name} finished")
except Exception as e:
    logger.error(f"Error processing df_dom_m_transportista: {e}")
    raise ValueError(f"Error processing df_dom_m_transportista: {e}")