from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import coalesce, col, concat_ws, date_format, lit, when

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")
    periodos= spark_controller.get_periods()
    logger.info(periodos)

    m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", cod_pais=cod_pais, have_principal=True)
    m_tipo_cambio = spark_controller.read_table(data_paths.APDAYC, "m_tipo_cambio", cod_pais=cod_pais, have_principal=True)
    t_orden_compra_detalle = spark_controller.read_table(data_paths.APDAYC, "t_orden_compra_detalle", cod_pais=cod_pais)
    t_orden_compra_cabecera_dom = spark_controller.read_table(data_paths.DOMAIN, "t_orden_compra_cabecera", cod_pais=cod_pais)
   
    target_table_name = "t_orden_compra_detalle"

except Exception as e:
    logger.error(e)
    raise

try:
    m_pais = m_pais.filter(col("id_pais").isin(cod_pais))
    m_tipo_cambio = m_tipo_cambio.filter((col("fecha").isNotNull()) & (col("tc_compra") > 0) & (col("tc_venta") > 0))
    t_orden_compra_detalle = t_orden_compra_detalle.filter(date_format(col("fecha_emision"), "yyyyMM").isin(periodos))
    t_orden_compra_cabecera_dom = t_orden_compra_cabecera_dom.filter(col("id_pais").isin(cod_pais))

    tmp_dominio_t_orden_compra_detalle = (
        t_orden_compra_detalle.alias("tocd")
        .join(m_compania.alias("mc"), col("tocd.cod_compania") == col("mc.cod_compania"), "left")
        .join(m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .join(t_orden_compra_cabecera_dom.alias("tocc"), (col("tocc.id_sucursal") == col("tocd.id_sucursal")) & (col("tocd.cod_transaccion") == col("tocc.cod_tipo_documento")) & (col("tocd.nro_orden_compra") == col("tocc.nro_orden_compra")), "left")
        .join(m_tipo_cambio.alias("mtc"), (col("mtc.cod_compania") == col("tocd.cod_compania")) & (col("mtc.cod_moneda") == col("tocc.cod_moneda_mo")) & (col("mtc.fecha") == col("tocd.fecha_emision")), "left")
        .select(
            col("mp.id_pais"),
            date_format(col("tocd.fecha_emision"), "yyyyMM").alias("id_periodo"),
            concat_ws("|",
                coalesce(col("tocd.cod_compania"), lit("")),
                coalesce(col("tocd.cod_sucursal"), lit("")),
                coalesce(col("tocd.cod_transaccion"), lit("")),
                coalesce(col("tocd.nro_orden_compra"), lit("")),
            ).alias("id_orden_compra"),
            concat_ws("|",
                coalesce(col("tocd.cod_compania"), lit("")),
                coalesce(col("tocd.cod_sucursal"), lit("")),
                coalesce(col("tocd.cod_transaccion"), lit("")),
                coalesce(col("tocd.nro_orden_compra"), lit("")),
                coalesce(col("tocd.cod_articulo"), lit("")),
                coalesce(col("tocd.nro_secuencia"), lit("")),
            ).alias("id_orden_ingreso"),
            col("tocd.cod_compania").alias("id_compania"),
            concat_ws("|",
                coalesce(col("tocd.cod_compania"), lit("")),
                coalesce(col("tocd.cod_sucursal"), lit("")),
            ).alias("id_sucursal"),
            concat_ws("|",
                coalesce(col("tocd.cod_compania"), lit("")),
                coalesce(col("tocd.cod_articulo"), lit("")),
            ).alias("id_articulo"),
            concat_ws("|",
                coalesce(col("tocd.cod_compania"), lit("")),
                coalesce(col("tocd.cod_centro_costo"), lit("")),
            ).alias("id_centro_costo"),
            col("tocd.fecha_emision"),
            col("tocd.desc_tipo_detalle"),
            col("tocd.cod_transaccion").alias("cod_tipo_documento"),
            col("tocd.nro_orden_compra"),
            col("tocd.nro_secuencia"),
            when(col("tocd.flg_atendido") == 'T', 1).otherwise(0).alias("es_atendido"),
            col("tocd.cod_unidad_compra"),
            col("tocd.precio_compra").alias("precio_compra_mo"),
            when(col("tocc.cod_moneda_mo") == "EUR", col("tocd.precio_compra") * col("mtc.tc_venta"))
            .otherwise(col("tocd.precio_compra") / col("mtc.tc_venta"))
            .alias("precio_compra_me"),
            (col("tocd.precio_compra") * col("mtc.tc_venta")).alias("precio_compra_mn"),
            col("tocd.cantidad_compra").alias("cant_comprada"),
            col("tocd.cantidad_atendida").alias("cant_antendida"),
            col("tocd.cantidad_facturada").alias("cant_facturada"),
            col("tocd.cantidad_cancelada").alias("cant_cancelada"),
            col("tocd.precio_std_local").alias("precio_std_mn"),
            col("tocd.precio_std_global").alias("precio_std_me"),
            col("tocd.total").alias("imp_compra_mo"),
            (col("tocd.precio_compra") * col("mtc.tc_venta") * col("tocd.cantidad_compra")).alias("imp_compra_mn"),
            when(col("tocc.cod_moneda_mo") == "EUR", (col("tocd.precio_compra") * col("mtc.tc_venta") * col("tocd.cantidad_compra")))
            .otherwise((col("tocd.precio_compra") / col("mtc.tc_venta")) * col("tocd.cantidad_compra"))
            .alias("imp_compra_me"),
            col("tocd.usuario_creacion"),
            col("tocd.fecha_creacion"),
            col("tocd.usuario_modificacion"),
            col("tocd.fecha_modificacion"),
            lit("1").alias("es_eliminado"),
        )
    )

    tmp = (
        tmp_dominio_t_orden_compra_detalle.alias("tmp")
        .select(
            col("id_pais").cast("string").alias("id_pais"),
            col("id_periodo").cast("string").alias("id_periodo"),
            col("id_orden_compra").cast("string").alias("id_orden_compra"),
            col("id_orden_ingreso").cast("string").alias("id_orden_ingreso"),
            col("id_compania").cast("string").alias("id_compania"),
            col("id_sucursal").cast("string").alias("id_sucursal"),
            col("id_articulo").cast("string").alias("id_articulo"),
            col("id_centro_costo").cast("string").alias("id_centro_costo"),
            col("fecha_emision").cast("date").alias("fecha_emision"),
            col("desc_tipo_detalle").cast("string").alias("desc_tipo_detalle"),
            col("cod_tipo_documento").cast("string").alias("cod_tipo_documento"),
            col("nro_orden_compra").cast("string").alias("nro_orden_compra"),
            col("nro_secuencia").cast("string").alias("nro_secuencia"),
            col("es_atendido").cast("int").alias("es_atendido"),
            col("cod_unidad_compra").cast("string").alias("cod_unidad_compra"),
            col("precio_compra_mo").cast("numeric(38, 12)").alias("precio_compra_mo"),
            col("precio_compra_me").cast("numeric(38, 12)").alias("precio_compra_me"),
            col("precio_compra_mn").cast("numeric(38, 12)").alias("precio_compra_mn"),
            col("cant_comprada").cast("numeric(38, 12)").alias("cant_comprada"),
            col("cant_antendida").cast("numeric(38, 12)").alias("cant_antendida"),
            col("cant_facturada").cast("numeric(38, 12)").alias("cant_facturada"),
            col("cant_cancelada").cast("numeric(38, 12)").alias("cant_cancelada"),
            col("precio_std_mn").cast("numeric(38, 12)").alias("precio_std_mn"),
            col("precio_std_me").cast("numeric(38, 12)").alias("precio_std_me"),
            col("imp_compra_mo").cast("numeric(38, 12)").alias("imp_compra_mo"),
            col("imp_compra_mn").cast("numeric(38, 12)").alias("imp_compra_mn"),
            col("imp_compra_me").cast("numeric(38, 12)").alias("imp_compra_me"),
            col("usuario_creacion").cast("string").alias("usuario_creacion"),
            col("fecha_creacion").cast("timestamp").alias("fecha_creacion"),
            col("usuario_modificacion").cast("string").alias("usuario_modificacion"),
            col("fecha_modificacion").cast("timestamp").alias("fecha_modificacion"),
            col("es_eliminado").cast("int").alias("es_eliminado"),
        )
    )

    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(tmp, data_paths.DOMAIN, target_table_name, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise