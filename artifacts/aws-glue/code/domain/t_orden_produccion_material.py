from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import coalesce, col, concat_ws, date_format, lit

spark_controller = SPARK_CONTROLLER()


try:
    cod_pais = COD_PAIS.split(",")
    periodos= spark_controller.get_periods()
    logger.info(periodos)

    m_articulo = spark_controller.read_table(data_paths.APDAYC, "m_articulo", cod_pais=cod_pais)
    m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", cod_pais=cod_pais, have_principal=True)
    t_orden_produccion_material = spark_controller.read_table(data_paths.APDAYC, "t_orden_produccion_material", cod_pais=cod_pais)

    target_table_name = "t_orden_produccion_material"

except Exception as e:
    logger.error(e)
    raise
try:
    m_pais = m_pais.filter(col("id_pais").isin(cod_pais))
    t_orden_produccion_material = t_orden_produccion_material.filter(date_format(col("fecha_creacion"), "yyyyMM").isin(periodos))

    tmp_dominio_t_orden_produccion_material = (
        t_orden_produccion_material.alias("topm")
        .join(m_compania.alias("mc"), col("topm.cod_compania") == col("mc.cod_compania"), "inner")
        .join(m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .join(m_articulo.alias("ma"), col("ma.id_articulo") == col("topm.id_material"), "inner")

        .select(
            col("mp.id_pais"),
            date_format(col("topm.fecha_creacion"), "yyyyMM").alias("id_periodo"),
            col("topm.id_compania"),
            col("topm.id_sucursal"),
            concat_ws("|",
                coalesce(col("topm.cod_compania"), lit("")),
                coalesce(col("topm.cod_sucursal"), lit("")),
                coalesce(col("topm.nro_operacion"), lit("")),
            ).alias("id_orden_produccion"),
            col("topm.nro_operacion"),
            col("topm.id_material").alias("id_articulo"),
            col("topm.cant_teorica").alias("cant_unidades_teorica"),
            col("topm.cant_entregada").alias("cant_unidades_entregada"),
            col("topm.cant_teorica_fabricada").alias("cant_unidades_teorica_fabricada"),
            col("topm.factor_conversion"),
            col("topm.usuario_creacion"),
            col("topm.fecha_creacion"),
            col("topm.usuario_modificacion"),
            col("topm.fecha_modificacion"),
            lit("1").alias("es_eliminado")
        )
    )

    tmp = tmp_dominio_t_orden_produccion_material.alias("tmp").select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_periodo").cast("string").alias("id_periodo"),
        col("id_compania").cast("string").alias("id_compania"),
        col("id_sucursal").cast("string").alias("id_sucursal"),
        col("id_orden_produccion").cast("string").alias("id_orden_produccion"),
        col("id_articulo").cast("string").alias("id_articulo"),
        col("nro_operacion").cast("string").alias("nro_operacion"),
        col("cant_unidades_teorica").cast("numeric(38,12)").alias("cant_unidades_teorica"),
        col("cant_unidades_entregada").cast("numeric(38,12)").alias("cant_unidades_entregada"),
        col("cant_unidades_teorica_fabricada").cast("numeric(38,12)").alias("cant_unidades_teorica_fabricada"),
        col("factor_conversion").cast("numeric(38,12)").alias("factor_conversion"),
        col("usuario_creacion").cast("string").alias("usuario_creacion"),
        col("fecha_creacion").cast("timestamp").alias("fecha_creacion"),
        col("usuario_modificacion").cast("string").alias("usuario_modificacion"),
        col("fecha_modificacion").cast("timestamp").alias("fecha_modificacion"),
        col("es_eliminado").cast("int").alias("es_eliminado"),
        )

    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(tmp, data_paths.DOMAIN, target_table_name, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise