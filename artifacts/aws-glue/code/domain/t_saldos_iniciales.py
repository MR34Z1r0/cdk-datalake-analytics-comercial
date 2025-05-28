from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import coalesce, col, concat_ws, date_format, first, lit, max, sum, when, row_number
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")
    periodos= spark_controller.get_periods()
    logger.info(periodos)

    m_compania = spark_controller.read_table(data_paths.BIG_MAGIC, "m_compania", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.BIG_MAGIC, "m_pais", cod_pais=cod_pais, have_principal=True)
    m_parametro = spark_controller.read_table(data_paths.BIG_MAGIC, "m_parametro", cod_pais=cod_pais, have_principal=True)
    m_articulo = spark_controller.read_table(data_paths.BIG_MAGIC, "m_articulo", cod_pais=cod_pais, have_principal=True)
    m_tipo_cambio = spark_controller.read_table(data_paths.BIG_MAGIC, "m_tipo_cambio", cod_pais=cod_pais, have_principal=True)
    m_empleado = spark_controller.read_table(data_paths.BIG_MAGIC, "m_empleado", cod_pais=cod_pais, have_principal=True)
    t_toma_inventario = spark_controller.read_table(data_paths.BIG_MAGIC, "t_toma_inventario", cod_pais=cod_pais, have_principal=True)
    t_toma_inventario_detalle = spark_controller.read_table(data_paths.BIG_MAGIC, "t_toma_inventario_detalle", cod_pais=cod_pais, have_principal=True)
    t_cierre_inventario_cpm = spark_controller.read_table(data_paths.BIG_MAGIC, "t_cierre_inventario_cpm", cod_pais=cod_pais, have_principal=True)

    target_table_name = "t_saldos_iniciales"

except Exception as e:
    logger.error(e)
    raise

try:
    m_pais = m_pais.filter(col("id_pais").isin(cod_pais))
    m_tipo_cambio = m_tipo_cambio.filter((col("fecha").isNotNull()) & (col("tc_compra") > 0) & (col("tc_venta") > 0))
    t_toma_inventario = (
        t_toma_inventario.alias("tti")
        .join(m_empleado.alias("me"), (col("tti.cod_compania") == col("me.cod_compania")) & (col("tti.cod_empleado_aprobador") == col("me.cod_empleado")), "inner")
        .select(col("tti.*"))
    )

    m_compania = (
        m_compania.alias("mc")
        .join(m_parametro.alias("mp"), col("mp.id_compania") == col("mc.id_compania"), "left")
        .select(
            col("mc.*"), 
            col("mp.cod_moneda_mn").alias("moneda_mn"))
    )
    # logger.info(f"m_compania count: {m_compania.count()}")
    
    logger.info("Starting creation of tmp_t_saldos_iniciales")

    tmp_t_saldos_iniciales = (
        t_toma_inventario.alias("tti")
        .filter(date_format(col("tti.fecha_inventario"), "yyyyMM").isin(periodos))
        .join(t_toma_inventario_detalle.alias("ttid"), 
            (col("tti.id_sucursal") == col("ttid.id_sucursal")) 
            & (col("tti.cod_almacen_emisor") == col("ttid.cod_almacen_emisor")) 
            & (col("tti.fecha_inventario") == col("ttid.fecha_inventario"))
            , "inner"
        )
        .join(m_articulo.alias("ma"), col("ma.id_articulo") == col("ttid.id_articulo"), "inner")
        .join(m_compania.alias("mc"), col("tti.id_compania") == col("mc.id_compania"), "inner")
        .join(m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .select(
            col("mp.id_pais"),
            date_format(col("tti.fecha_inventario"), "yyyyMM").alias("id_periodo"),
            col("tti.id_compania"),
            col("tti.id_sucursal"),
            col("ttid.id_almacen"),
            col("ttid.id_articulo"),
            col("tti.fecha_inventario"),
            col("mc.moneda_mn"),
            col("ttid.stock_final").alias("cantidad_cajas"),
            (col("ttid.stock_final") * col("ma.cant_unidad_paquete")).alias("unidades"),
            col("tti.estado"),
            col("tti.usuario_creacion").alias("usuario_creacion"),
            col("tti.fecha_creacion").alias("fecha_creacion"),
            col("tti.usuario_modificacion").alias("usuario_modificacion"),
            col("tti.fecha_modificacion").alias("fecha_modificacion"),
            lit("1").alias("es_eliminado")
        )
    )
    # logger.info(f"tmp_t_saldos_iniciales count: {tmp_t_saldos_iniciales.count()}")

    logger.info("Starting creation of tmp_t_saldos_iniciales_valorizado")
    tmp_t_saldos_iniciales_valorizado = (
        tmp_t_saldos_iniciales.alias("tmp")
        .join(t_cierre_inventario_cpm.alias("cpm"), ((col("tmp.id_sucursal") == col("cpm.id_sucursal")) & (col("tmp.id_articulo") == col("cpm.id_articulo")) & (col("tmp.id_periodo") == col("cpm.id_periodo"))), "left")
        .join(m_articulo.alias("ma"), col("tmp.id_articulo") == col("ma.id_articulo"), "left")
        .join(m_tipo_cambio.alias("mtc"), ((col("mtc.id_compania") == col("tmp.id_compania")) & (col("mtc.fecha") == col("tmp.fecha_inventario")) & (col("mtc.cod_moneda") == col("tmp.moneda_mn"))), "left")
        .select(
            col("tmp.id_pais"),
            col("tmp.id_compania"),
            col("tmp.id_sucursal"),
            col("tmp.id_almacen"),
            col("tmp.id_articulo"),
            col("tmp.id_periodo"),
            col("tmp.fecha_inventario"),
            col("tmp.cantidad_cajas").alias("cant_cajafisica_inicial"),
            col("tmp.unidades").alias("cant_unidades_inicial"),
            col("tmp.estado"),
            col("cpm.imp_cpm").alias("precio_unitario_mn"),
            (col("cpm.imp_cpm") / col("mtc.tc_compra")).alias("precio_unitario_me"),
            (col("tmp.cantidad_cajas") * col("cpm.imp_cpm")).alias("imp_valorizado_mn"),
            (col("tmp.cantidad_cajas") * col("cpm.imp_cpm") / col("mtc.tc_compra")).alias("imp_valorizado_me"),
            col("cpm.imp_saldo_inicial").alias("imp_saldo_inicial"), 
            col("cpm.imp_ingreso").alias("imp_valorizado_ingreso"), 
            col("cpm.imp_salida").alias("imp_valorizado_salida"), 
            col("cpm.imp_saldo_final").alias("imp_saldo_final"),
            col("tmp.usuario_creacion"),
            col("tmp.fecha_creacion"),
            col("tmp.usuario_modificacion"),
            col("tmp.fecha_modificacion"),
            col("es_eliminado")
        )
    )

    tmp = tmp_t_saldos_iniciales_valorizado.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_compania").cast("string").alias("id_compania"),
        col("id_sucursal").cast("string").alias("id_sucursal"),
        col("id_almacen").cast("string").alias("id_almacen"),
        col("id_articulo").cast("string").alias("id_articulo"),
        col("id_periodo").cast("string").alias("id_periodo"),
        col("fecha_inventario").cast("date").alias("fecha_inventario"),
        col("cant_cajafisica_inicial").cast("numeric(30, 4)").alias("cant_cajafisica_inicial"),
        col("cant_unidades_inicial").cast("numeric(30, 4)").alias("cant_unidades_inicial"),
        col("estado").cast("string").alias("estado"),
        col("precio_unitario_mn").cast("numeric(30, 4)").alias("precio_unitario_mn"),
        col("precio_unitario_me").cast("numeric(30, 4)").alias("precio_unitario_me"),
        col("imp_valorizado_mn").cast("numeric(30, 4)").alias("imp_valorizado_mn"),
        col("imp_valorizado_me").cast("numeric(30, 4)").alias("imp_valorizado_me"),
        col("imp_saldo_inicial").cast("numeric(30, 4)").alias("imp_saldo_inicial"),
        col("imp_valorizado_ingreso").cast("numeric(30, 4)").alias("imp_valorizado_ingreso"),
        col("imp_valorizado_salida").cast("numeric(30, 4)").alias("imp_valorizado_salida"),
        col("imp_saldo_final").cast("numeric(30, 4)").alias("imp_saldo_final"),
        col("usuario_creacion").cast("string").alias("usuario_creacion"),
        col("fecha_creacion").cast("timestamp").alias("fecha_creacion"),
        col("usuario_modificacion").cast("string").alias("usuario_modificacion"),
        col("fecha_modificacion").cast("timestamp").alias("fecha_modificacion"),
        col("es_eliminado").cast("int").alias("es_eliminado"),
    )
    # logger.info(f"tmp_t_saldos_iniciales_valorizado count: {tmp_t_saldos_iniciales_valorizado.count()}")

    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(tmp, data_paths.DOMINIO, target_table_name, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise