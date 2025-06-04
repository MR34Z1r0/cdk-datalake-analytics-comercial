from common_jobs_functions import data_paths, logger, SPARK_CONTROLLER
from pyspark.sql.functions import coalesce, col, concat_ws, date_format, first, lit, max, sum, when, row_number
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "t_saldos_iniciales"
try:
    PERIODOS= spark_controller.get_periods()
    logger.info(f"Periods to filter: {PERIODOS}")

    df_m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    df_m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal=True)
    df_m_parametro = spark_controller.read_table(data_paths.BIGMAGIC, "m_parametro", have_principal=True)
    df_m_articulo = spark_controller.read_table(data_paths.BIGMAGIC, "m_articulo", have_principal=True)
    df_m_tipo_cambio = spark_controller.read_table(data_paths.BIGMAGIC, "m_tipo_cambio", have_principal=True)
    df_m_empleado = spark_controller.read_table(data_paths.BIGMAGIC, "m_empleado", have_principal=True)

    df_t_toma_inventario = spark_controller.read_table(data_paths.BIGMAGIC, "t_toma_inventario", have_principal=True)
    df_t_toma_inventario_detalle = spark_controller.read_table(data_paths.BIGMAGIC, "t_toma_inventario_detalle", have_principal=True)
    df_t_cierre_inventario_cpm = spark_controller.read_table(data_paths.BIGMAGIC, "t_cierre_inventario_cpm", have_principal=True)

    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("starting creation of df_m_compania")
    df_m_compania = (
        df_m_compania.alias("mc")
        .join(
            df_m_parametro.alias("mpar"),
            col("mpar.id_compania") == col("mc.id_compania"),
            "left",
        )
        .join(
            df_m_pais.alias("mp"),
            col("mp.cod_pais") == col("mc.cod_pais"),
        )
        .select(col("mp.id_pais"), col("mc.cod_compania").alias("id_compania"), col("mc.cod_compania"), col("mc.cod_pais"), col("mpar.cod_moneda_mn").alias("moneda_mn"))
    ).cache()

    df_t_toma_inventario = (
        df_t_toma_inventario.alias("tti")
        .join(df_m_empleado.alias("me"), 
              (col("tti.cod_compania") == col("me.cod_compania")) & 
              (col("tti.cod_empleado_aprobador") == col("me.cod_empleado")), "inner")
        .select(col("tti.*"))
    )
    
    logger.info("Starting creation of df_tmp_toma_inventario_detalle")
    df_tmp_toma_inventario_detalle = (
        df_t_toma_inventario.alias("tti")
        .filter(date_format(col("tti.fecha_inventario"), "yyyyMM").isin(PERIODOS))
        .join(df_t_toma_inventario_detalle.alias("ttid"), 
            (col("tti.id_sucursal") == col("ttid.id_sucursal")) & 
            (col("tti.cod_almacen_emisor") == col("ttid.cod_almacen_emisor"))&
            (col("tti.fecha_inventario") == col("ttid.fecha_inventario")), "inner")
        .join(df_m_articulo.alias("ma"), 
              col("ma.id_articulo") == col("ttid.id_articulo"), "inner")
        .join(df_m_compania.alias("mc"), 
              col("tti.id_compania") == col("mc.id_compania"), "inner")
        .select(
            col("mc.id_pais"),
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
            lit("0").alias("es_eliminado")
        )
    )

    logger.info("Starting creation of df_tmp_toma_inventario_detalle_valorizado")
    df_tmp_toma_inventario_detalle_valorizado = (
        df_tmp_toma_inventario_detalle.alias("tmp")
        .join(df_t_cierre_inventario_cpm.alias("cpm"), 
              ((col("tmp.id_sucursal") == col("cpm.id_sucursal")) & 
               (col("tmp.id_articulo") == col("cpm.id_articulo")) & 
               (col("tmp.id_periodo") == col("cpm.id_periodo"))), "left")
        .join(df_m_articulo.alias("ma"), 
              col("tmp.id_articulo") == col("ma.id_articulo"), "left")
        .join(df_m_tipo_cambio.alias("mtc"), 
              ((col("mtc.id_compania") == col("tmp.id_compania")) & 
               (col("mtc.fecha") == col("tmp.fecha_inventario")) & 
               (col("mtc.cod_moneda") == col("tmp.moneda_mn"))), "left")
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

    logger.info("Starting creation of df_dom_saldos_iniciales")
    df_dom_saldos_iniciales = df_tmp_toma_inventario_detalle_valorizado.select(
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

    logger.info(f"starting upsert of {target_table_name}")
    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(df_dom_saldos_iniciales, data_paths.DOMAIN, target_table_name, partition_columns_array)
    logger.info(f"Upsert of {target_table_name} finished completed") 
except Exception as e:
    logger.error(f"Error processing df_dom_saldos_iniciales: {e}")
    raise ValueError(f"Error processing df_dom_saldos_iniciales: {e}")