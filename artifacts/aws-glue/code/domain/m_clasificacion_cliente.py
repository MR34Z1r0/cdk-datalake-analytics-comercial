import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, lit, current_date, concat, trim, max
from pyspark.sql.types import StringType, DateType

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")

    i_relacion_consumo = spark_controller.read_table(data_paths.APDAYC, "i_relacion_consumo", cod_pais=cod_pais)
    m_canal_visibilidad = spark_controller.read_table(data_paths.APDAYC, "m_canal", cod_pais=cod_pais)
    m_subgiro_visibilidad = spark_controller.read_table(data_paths.APDAYC, "m_subgiro", cod_pais=cod_pais)
    m_giro_visibilidad = spark_controller.read_table(data_paths.APDAYC, "m_giro", cod_pais=cod_pais)
    m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", cod_pais=cod_pais, have_principal=True) 

    target_table_name = "m_clasificacion_cliente"

except Exception as e:
    raise

try:
    df_subgiro = (
        i_relacion_consumo.alias("irc")
        .join(m_compania.alias("mc"), col("mc.cod_compania") == col("irc.cod_compania"), "inner")
        .join(m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .join(
            m_subgiro_visibilidad.alias("mgv"),
            (col("mgv.cod_subgiro") == col("irc.cod_subgiro")) & (col("mgv.cod_compania") == col("irc.cod_compania")),
            "inner",
        )
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            col("mp.id_pais").alias("id_pais"), 
            concat(trim(col("irc.cod_compania")), lit("|"), lit("SG"), lit("|"), trim(col("irc.cod_subgiro"))).alias("id_clasificacion_cliente"),
            concat(trim(col("irc.cod_compania")), lit("|"), lit("GR"), lit("|"), trim(col("irc.cod_giro"))).alias("id_clasificacion_cliente_padre"),
            col("irc.cod_subgiro").alias("cod_clasificacion_cliente"),
            col("mgv.desc_subgiro").alias("nomb_clasificacion_cliente"),
            lit("Subgiro").alias("cod_tipo_clasificacion_cliente"),
            col("mgv.es_activo").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    df_giro = (
        i_relacion_consumo.alias("irc")
        .join(m_compania.alias("mc"), col("mc.cod_compania") == col("irc.cod_compania"), "inner")
        .join(m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .join(
            m_giro_visibilidad.alias("mgv"),
            (col("mgv.cod_giro") == col("irc.cod_giro")) & (col("mgv.cod_compania") == col("irc.cod_compania")),
            "inner",
        )
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            col("mp.id_pais").alias("id_pais"), 
            concat(trim(col("irc.cod_compania")), lit("|"), lit("GR"), lit("|"), trim(col("irc.cod_giro"))).alias("id_clasificacion_cliente"),
            concat(trim(col("irc.cod_compania")), lit("|"), lit("CN"), lit("|"), trim(col("irc.cod_canal"))).alias("id_clasificacion_cliente_padre"),
            col("irc.cod_giro").alias("cod_clasificacion_cliente"),
            col("mgv.desc_giro").alias("nomb_clasificacion_cliente"),
            lit("Giro").alias("cod_tipo_clasificacion_cliente"),
            col("mgv.es_activo").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    df_canal = (
        i_relacion_consumo.alias("irc")
        .join(m_compania.alias("mc"), col("mc.cod_compania") == col("irc.cod_compania"), "inner")
        .join(m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .join(
            m_canal_visibilidad.alias("mcv"),
            (col("mcv.cod_canal") == col("irc.cod_canal")) & (col("mcv.cod_compania") == col("irc.cod_compania")),
            "inner",
        )
        .select(
            col("mp.id_pais").alias("id_pais"),
            concat(trim(col("irc.cod_compania")), lit("|"), lit("CN"), lit("|"), trim(col("irc.cod_canal"))).alias("id_clasificacion_cliente"),
            lit(None).alias("id_clasificacion_cliente_padre"),
            col("irc.cod_canal").alias("cod_clasificacion_cliente"),
            col("mcv.desc_canal").alias("nomb_clasificacion_cliente"),
            lit("Canal").alias("cod_tipo_clasificacion_cliente"),
            col("mcv.es_activo").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    df_subgiro = df_subgiro.distinct()
    df_giro = df_giro.distinct()
    df_canal = df_canal.distinct()

    df_m_clasificacion_cliente = (
        df_subgiro.union(df_giro).union(df_canal)
        .select(
            col("id_pais").cast(StringType()).alias("id_pais"),
            col("id_clasificacion_cliente").cast(StringType()).alias("id_clasificacion_cliente"),
            col("id_clasificacion_cliente_padre").cast(StringType()).alias("id_clasificacion_cliente_padre"),
            col("cod_clasificacion_cliente").cast(StringType()).alias("cod_clasificacion_cliente"),
            col("nomb_clasificacion_cliente").cast(StringType()).alias("nomb_clasificacion_cliente"),
            col("cod_tipo_clasificacion_cliente").cast(StringType()).alias("cod_tipo_clasificacion_cliente"),
            col("estado").cast(StringType()).alias("estado"),
            col("fecha_creacion").cast(DateType()).alias("fecha_creacion"),
            col("fecha_modificacion").cast(DateType()).alias("fecha_modificacion"),
        )
    )

    id_columns = ["id_clasificacion_cliente"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_m_clasificacion_cliente, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise
