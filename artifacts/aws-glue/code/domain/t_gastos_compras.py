from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import coalesce, col, concat_ws, date_format, lit, when
from pyspark.sql.types import StructType, StructField, StringType


spark_controller = SPARK_CONTROLLER()
target_table_name = "t_gastos_compras"

try:
    cod_pais = COD_PAIS.split(",")
    periodos = spark_controller.get_periods()
    logger.info(periodos)

    m_compania = spark_controller.read_table(data_paths.BIG_MAGIC, "m_compania", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.BIG_MAGIC, "m_pais", cod_pais=cod_pais, have_principal=True)
    m_tipo_embarque = spark_controller.read_table(data_paths.BIG_MAGIC, "m_tipo_embarque", cod_pais=cod_pais)
    m_termino = spark_controller.read_table(data_paths.BIG_MAGIC, "m_termino", cod_pais=cod_pais)
    m_proveedor = spark_controller.read_table(data_paths.BIG_MAGIC, "m_proveedor", cod_pais=cod_pais)
    m_unidad = spark_controller.read_table(data_paths.BIG_MAGIC, "m_unidad", cod_pais=cod_pais)
    t_cotizacion_compra = spark_controller.read_table(data_paths.BIG_MAGIC, "t_cotizacion_compra", cod_pais=cod_pais)
    t_orden_compra_cabecera = spark_controller.read_table(data_paths.BIG_MAGIC, "t_orden_compra_cabecera", cod_pais=cod_pais)
    t_orden_compra_detalle = spark_controller.read_table(data_paths.BIG_MAGIC, "t_orden_compra_detalle", cod_pais=cod_pais)
    t_requerimiento_compra_cabecera = spark_controller.read_table(data_paths.BIG_MAGIC, "t_requerimiento_compra_cabecera", cod_pais=cod_pais)

except Exception as e:
    logger.error(e)
    raise

try:
    # create default m_proveedor
    companies = m_compania.alias("mc").join(m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
    companies = companies.select("cod_compania").distinct()
    schema = StructType([
        StructField("cod_proveedor", StringType(), True),
        StructField("nombre", StringType(), True),
        StructField("ruc", StringType(), True),
    ])
    default_m_proveedor = spark_controller.spark.createDataFrame(
        [("0", "PROVEEDOR DEFAULT", "")], 
        ["cod_proveedor", "nombre", "ruc"]
    )
    default_m_proveedor = default_m_proveedor.crossJoin(companies)
    m_proveedor = m_proveedor.select(
        col("cod_proveedor"),
        col("nombre"),
        col("ruc"),
        col("cod_compania")
    ).unionByName(default_m_proveedor)

    m_pais_filtered = m_pais.filter(col("id_pais").isin(cod_pais))
    t_requerimiento_compra_cabecera = t_requerimiento_compra_cabecera.filter(col("cod_transaccion") == "REQ")
    t_orden_compra_cabecera = t_orden_compra_cabecera.filter(
        (date_format(col("fecha_emision"), "yyyyMM").isin(periodos))
        & (col("flg_anulado") == "F")
    )

    t_cotizacion_compra = (
        t_cotizacion_compra.alias("tcc")
        .join(m_proveedor.alias("mp"), (col("mp.cod_compania") == col("tcc.cod_compania_cotizacion")) & (col("mp.cod_proveedor") == col("tcc.cod_proveedor_cotizacion")), "inner")
        .join(m_unidad.alias("mu"), (col("mu.cod_compania") == col("tcc.cod_compania_cotizacion")) & (col("mu.cod_unidad") == col("tcc.unidad_requerida")), "inner")
        .select (col("tcc.*"))
    )

    tmp_gastos_compras = (
        t_requerimiento_compra_cabecera.alias("trcc")
        .join(
            t_cotizacion_compra.alias("tcc"),
            (col("trcc.cod_compania") == col("tcc.cod_compania_cotizacion"))
            & (col("trcc.cod_sucursal") == col("tcc.cod_sucursal_cotizacion"))
            & (col("trcc.nro_documento") == col("tcc.nro_documento_cotizacion"))
            & (col("trcc.cod_transaccion") == col("tcc.cod_transaccion_cotizacion")),
            "inner",
        )
        .join(
            t_orden_compra_detalle.alias("tocd"),
            (col("tcc.cod_compania") == col("tocd.cod_compania"))
            & (col("tcc.cod_sucursal") == col("tocd.cod_sucursal"))
            & (col("tcc.cod_transaccion") == col("tocd.cod_transaccion"))
            & (col("tcc.nro_orden_compra") == col("tocd.nro_orden_compra"))
            & (col("tcc.tipo_detalle_ord_compra") == col("tocd.desc_tipo_detalle"))
            & (col("tcc.cod_articulo") == col("tocd.cod_articulo"))
            & (col("tcc.secuencia") == col("tocd.nro_secuencia")),
            "inner",
        )
        .join(
            t_orden_compra_cabecera.alias("tocc"),
            (col("tocd.cod_compania") == col("tocc.cod_compania"))
            & (col("tocd.cod_sucursal") == col("tocc.cod_sucursal"))
            & (col("tocd.cod_transaccion") == col("tocc.cod_transaccion"))
            & (col("tocd.nro_orden_compra") == col("tocc.nro_orden_compra")),
            "inner",
        )
        .join(m_compania.alias("mc"), col("tocc.cod_compania") == col("mc.cod_compania"), "left")
        .join(m_pais_filtered.alias("mp1"), col("mc.cod_pais") == col("mp1.cod_pais"), "left")
        .join(m_pais.alias("mp2"), col("tocc.cod_pais_embarque") == col("mp2.cod_pais"), "left")
        .join(
            m_tipo_embarque.alias("mte"),
            (col("tocc.cod_compania") == col("mte.cod_compania"))
            & (col("tocc.cod_via_embarque") == col("mte.cod_via_embarque")),
            "left",
        )
        .join(
            m_termino.alias("mt"),
            (col("tocc.cod_terminos") == col("mt.cod_terminos"))
            & (col("tocc.cod_compania") == col("mt.cod_compania")),
            "left",
        )
        .select(
            col("mp1.id_pais").alias("id_pais"),
            date_format(col("tocc.fecha_emision"), "yyyyMM").alias("id_periodo"),
            concat_ws("|", col("tocc.cod_compania"), col("tocc.cod_sucursal")).alias("id_sucursal"),
            concat_ws("|", col("tocc.cod_compania"), col("tocc.cod_proveedor")).alias("id_proveedor"),
            concat_ws("|", col("tocc.cod_compania"), col("tocc.cod_condicion_pago")).alias("id_forma_pago"),
            concat_ws("|", col("tocc.cod_compania"), col("tocd.cod_articulo")).alias("id_articulo"),
            concat_ws("|", col("tocc.cod_compania"), col("tocc.cod_terminos")).alias("id_terminos"),
            concat_ws("|", col("tocc.cod_compania"), col("trcc.cod_empleado_preparado")).alias("id_solicitante"),
            concat_ws("|", col("tocc.cod_compania"), col("tocc.cod_comprador")).alias("id_empleado"),
            concat_ws("|", col("tocc.cod_compania"), col("tocc.cod_via_embarque")).alias("id_via_embarque"),
            col("tocc.nro_orden_compra"),
            col("tocc.cod_area"),
            col("tocc.estado_oc").alias("cod_estado"),
            col("tocc.cod_via_embarque"),
            col("tocc.cod_terminos").alias("cod_termino"),
            col("mp2.id_pais").alias("desc_pais_embarque"),
            col("tocc.cod_pais_embarque").alias("cod_pais_embarque"),
            col("tocc.flg_tipo_orden").alias("cod_tipo_orden"),
            col("tocc.cod_proveedor"),
            col("tocc.cod_comprador"),
            col("tocc.cod_condicion_pago"),
            col("tocc.cod_moneda"),
            col("tocd.unidad_proveedor").alias("cod_unidad_proveedor"),
            col("tocc.fecha_emision"),
            col("tocd.fecha_entrega"),
            col("tocd.precio_compra").alias("precio_compra_mn"),
            col("tocc.tipo_cambio_mn"),
            col("tocc.tipo_cambio_me"),
            col("tocd.cantidad_atendida").alias("cant_atendida"),
            col("tocd.cantidad_facturada").alias("cant_facturada"),
            col("tocd.cantidad_cancelada").alias("cant_cancelada"),
            col("tocd.cantidad_proveedor").alias("cant_pedida"),
            (col("tocd.precio_compra") / coalesce(when(col("tocc.tipo_cambio_me") == 0, None).otherwise(col("tocc.tipo_cambio_me")),lit(1))).alias("precio_compra_me"),
            (col("tocd.cantidad_atendida") * (col("tocd.precio_compra") / coalesce(when(col("tocc.tipo_cambio_me") == 0, None).otherwise(col("tocc.tipo_cambio_me")),lit(1)))).alias("imp_atendido_me"),
            (col("tocd.cantidad_cancelada") * (col("tocd.precio_compra") / coalesce(when(col("tocc.tipo_cambio_me") == 0, None).otherwise(col("tocc.tipo_cambio_me")),lit(1)))).alias("imp_cancelado_me"),
            (col("tocd.cantidad_facturada") * (col("tocd.precio_compra") / coalesce(when(col("tocc.tipo_cambio_me") == 0, None).otherwise(col("tocc.tipo_cambio_me")),lit(1)))).alias("imp_facturado_me"),
            (col("tocd.cantidad_proveedor") * (col("tocd.precio_compra") / coalesce(when(col("tocc.tipo_cambio_me") == 0, None).otherwise(col("tocc.tipo_cambio_me")),lit(1)))).alias("imp_pedido_me"),
            col("tocc.neto").alias("imp_neto_mn"),
            (col("tocc.neto") / coalesce(when(col("tocc.tipo_cambio_me") == 0, None).otherwise(col("tocc.tipo_cambio_me")),lit(1))).alias("imp_neto_me"),
            col("tocc.valor_venta").alias("imp_venta_mn"),
            (col("tocc.valor_venta") / coalesce(when(col("tocc.tipo_cambio_me") == 0, None).otherwise(col("tocc.tipo_cambio_me")),lit(1))).alias("imp_venta_me"),
        )
    )

    tmp = tmp_gastos_compras.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_periodo").cast("string").alias("id_periodo"),
        col("id_sucursal").cast("string").alias("id_sucursal"),
        col("id_proveedor").cast("string").alias("id_proveedor"),
        col("id_forma_pago").cast("string").alias("id_forma_pago"),
        col("id_articulo").cast("string").alias("id_articulo"),
        col("id_terminos").cast("string").alias("id_terminos"),
        col("id_solicitante").cast("string").alias("id_solicitante"),
        col("id_empleado").cast("string").alias("id_empleado"),
        col("id_via_embarque").cast("string").alias("id_via_embarque"),
        col("nro_orden_compra").cast("string").alias("nro_orden_compra"),
        col("cod_area").cast("string").alias("cod_area"),
        col("cod_estado").cast("string").alias("cod_estado"),
        col("cod_via_embarque").cast("string").alias("cod_via_embarque"),
        col("desc_pais_embarque").cast("string").alias("desc_pais_embarque"),
        col("cod_pais_embarque").cast("string").alias("cod_pais_embarque"),
        col("cod_tipo_orden").cast("string").alias("cod_tipo_orden"),
        col("cod_moneda").cast("string").alias("cod_moneda"),
        col("cod_unidad_proveedor").cast("string").alias("cod_unidad_proveedor"),
        col("fecha_emision").cast("date").alias("fecha_emision"),
        col("fecha_entrega").cast("date").alias("fecha_entrega"),
        col("precio_compra_mn").cast("numeric(38, 12)").alias("precio_compra_mn"),
        col("precio_compra_me").cast("numeric(38, 12)").alias("precio_compra_me"),
        col("tipo_cambio_mn").cast("numeric(38, 12)").alias("tipo_cambio_mn"),
        col("tipo_cambio_me").cast("numeric(38, 12)").alias("tipo_cambio_me"),
        col("cant_atendida").cast("numeric(38, 12)").alias("cant_atendida"),
        col("cant_facturada").cast("numeric(38, 12)").alias("cant_facturada"),
        col("cant_cancelada").cast("numeric(38, 12)").alias("cant_cancelada"),
        col("cant_pedida").cast("numeric(38, 12)").alias("cant_pedida"),
        col("imp_atendido_me").cast("numeric(38, 12)").alias("imp_atendido_me"),
        col("imp_cancelado_me").cast("numeric(38, 12)").alias("imp_cancelado_me"),
        col("imp_facturado_me").cast("numeric(38, 12)").alias("imp_facturado_me"),
        col("imp_pedido_me").cast("numeric(38, 12)").alias("imp_pedido_me"),
        col("imp_neto_mn").cast("numeric(38, 12)").alias("imp_neto_mn"),
        col("imp_neto_me").cast("numeric(38, 12)").alias("imp_neto_me"),
        col("imp_venta_mn").cast("numeric(38, 12)").alias("imp_venta_mn"),
        col("imp_venta_me").cast("numeric(38, 12)").alias("imp_venta_me"),
        col("cod_termino").cast("string").alias("cod_termino"),
    )

    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(tmp, data_paths.DOMINIO, target_table_name, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise
