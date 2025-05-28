from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import coalesce, col, concat_ws, date_format, lit

spark_controller = SPARK_CONTROLLER()
target_table_name = "t_orden_compra_cronograma_entrega"

try:
    cod_pais = COD_PAIS.split(",")
    periodos = spark_controller.get_periods()
    logger.info(periodos)

    m_compania = spark_controller.read_table(data_paths.BIG_MAGIC, "m_compania", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.BIG_MAGIC, "m_pais", cod_pais=cod_pais, have_principal=True)
    t_orden_compra_cronograma_entrega = spark_controller.read_table(data_paths.BIG_MAGIC, "t_orden_compra_cronograma_entrega", cod_pais=cod_pais)

except Exception as e:
    logger.error(e)
    raise

try:
    m_pais = m_pais.filter(col("id_pais").isin(cod_pais))
    t_orden_compra_cronograma_entrega = t_orden_compra_cronograma_entrega.filter(date_format(col("fecha_creacion"), "yyyyMM").isin(periodos))

    tmp_dominio_t_orden_compra_cronograma_entrega = (
        t_orden_compra_cronograma_entrega.alias("tocce")
        .join(m_compania.alias("mc"), col("tocce.cod_compania") == col("mc.cod_compania"), "left")
        .join(m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .select(
            col("mp.id_pais").alias("id_pais"),
            date_format(col("tocce.fecha_inicio"), "yyyyMM").alias("id_periodo"),
            col("tocce.cod_compania").alias("id_compania"),
            concat_ws("|",
                coalesce(col("tocce.cod_compania"), lit("")),
                coalesce(col("tocce.cod_sucursal"), lit("")),
            ).alias("id_sucursal"),
            col("tocce.cod_transaccion").alias("cod_transaccion"),
            col("tocce.nro_documento").alias("nro_documento"),
            concat_ws("|",
                coalesce(col("tocce.cod_compania"), lit("")),
                coalesce(col("tocce.cod_articulo"), lit("")),
            ).alias("id_articulo"),
            concat_ws("|",
                coalesce(col("tocce.cod_compania"), lit("")),
                coalesce(col("tocce.cod_proveedor"), lit("")),
            ).alias("id_proveedor"),
            col("tocce.nro_secuencia").alias("nro_secuencia"),
            col("tocce.desc_tipo_detalle").alias("desc_tipo_detalle"),
            col("tocce.cantidad_perdida").alias("cant_perdida"),
            col("tocce.cantidad_atendida").alias("cant_atendida"),
            col("tocce.fecha_inicio").alias("fecha_inicio"),
            col("tocce.fecha_final").alias("fecha_final"),
            col("tocce.usuario_creacion").alias("usuario_creacion"),
            col("tocce.fecha_creacion").alias("fecha_creacion"),
            col("tocce.usuario_modificacion").alias("usuario_modificacion"),
            col("tocce.fecha_modificacion").alias("fecha_modificacion"),
            lit("1").alias("es_eliminado"),
        )
    )

    tmp = tmp_dominio_t_orden_compra_cronograma_entrega.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_periodo").cast("string").alias("id_periodo"),
        col("id_compania").cast("string").alias("id_compania"),
        col("id_sucursal").cast("string").alias("id_sucursal"),
        col("cod_transaccion").cast("string").alias("cod_transaccion"),
        col("nro_documento").cast("string").alias("nro_documento"),
        col("id_articulo").cast("string").alias("id_articulo"),
        col("id_proveedor").cast("string").alias("id_proveedor"),
        col("nro_secuencia").cast("string").alias("nro_secuencia"),
        col("desc_tipo_detalle").cast("string").alias("desc_tipo_detalle"),
        col("cant_perdida").cast("numeric(38, 12)").alias("cant_perdida"),
        col("cant_atendida").cast("numeric(38, 12)").alias("cant_atendida"),
        col("fecha_inicio").cast("date").alias("fecha_inicio"),
        col("fecha_final").cast("date").alias("fecha_final"),
        col("usuario_creacion").cast("string").alias("usuario_creacion"),
        col("fecha_creacion").cast("timestamp").alias("fecha_creacion"),
        col("usuario_modificacion").cast("string").alias("usuario_modificacion"),
        col("fecha_modificacion").cast("timestamp").alias("fecha_modificacion"),
        col("es_eliminado").cast("int").alias("es_eliminado"),
    )

    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(tmp, data_paths.DOMINIO, target_table_name, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise
