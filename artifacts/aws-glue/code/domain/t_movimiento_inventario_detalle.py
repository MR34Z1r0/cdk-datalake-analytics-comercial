from common_jobs_functions import data_paths, logger, SPARK_CONTROLLER
from pyspark.sql.functions import col, concat_ws, date_format, lit, when

spark_controller = SPARK_CONTROLLER()
target_table_name = "t_movimiento_inventario_detalle"
try:
    PERIODOS= spark_controller.get_periods()
    logger.info(f"Periods to filter: {PERIODOS}")

    df_m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    df_m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal=True)
    df_m_parametro = spark_controller.read_table(data_paths.BIGMAGIC, "m_parametro")
    df_m_documento_almacen = spark_controller.read_table(data_paths.BIGMAGIC, "m_documento_almacen")
    df_m_articulo = spark_controller.read_table(data_paths.BIGMAGIC, "m_articulo")
    df_m_tipo_cambio = spark_controller.read_table(data_paths.BIGMAGIC, "m_tipo_cambio")

    df_t_cierre_inventario_cpm = spark_controller.read_table(data_paths.BIGMAGIC, "t_cierre_inventario_cpm")
    df_t_movimiento_inventario_detalle = spark_controller.read_table(data_paths.BIGMAGIC, "t_movimiento_inventario_detalle")
    df_t_movimiento_inventario_dom = spark_controller.read_table(data_paths.DOMAIN, "t_movimiento_inventario")
    
    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    df_t_movimiento_inventario_detalle = df_t_movimiento_inventario_detalle.filter(date_format(col("fecha_almacen"), "yyyyMM").isin(PERIODOS))

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

    df_tmp_movimiento_inventario_detalle = (
        df_t_movimiento_inventario_detalle.alias("tmid")
        .join(df_m_documento_almacen.alias("mda"), 
              (col("tmid.cod_compania") == col("mda.cod_compania")) & 
              (col("tmid.cod_procedimiento") == col("mda.cod_transaccion")), "inner")
        .join(df_m_compania.alias("mc"), col("tmid.cod_compania") == col("mc.cod_compania"), "inner")
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .join(df_m_articulo.alias("ma"), 
              (col("tmid.cod_compania") == col("ma.cod_compania")) & 
              (col("tmid.cod_articulo") == col("ma.cod_articulo")), "inner")
        .select(
            col("mp.id_pais"),
            date_format(col("tmid.fecha_almacen"), "yyyyMM").alias("id_periodo"),
            col("tmid.fecha_almacen"),
            col("tmid.id_compania"),
            col("tmid.id_sucursal"),
            col("tmid.id_almacen"),
            col("tmid.id_articulo"),
            col("tmid.id_movimiento_almacen"),
            col("tmid.id_centro_costo"),
            col("tmid.nro_documento_movimiento"),
            col("tmid.nro_linea_comprobante"),
            col("tmid.cod_documento_transaccion"),
            col("tmid.nro_documento_almacen"),
            col("tmid.cod_documento_transaccion_ref"),
            col("tmid.nro_documento_almacen_ref"),
            col("tmid.cod_procedimiento"),
            col("tmi.cod_estado_comprobante"),
            col("tmid.cod_unidad_articulo"),
            col("tmid.cod_motivo"),
            col("tmid.costo_unitario"),
            col("tmid.costo_total").alias("imp_total"),
            ((when(col("mda.cod_operacion_origen") == "S", 1).otherwise(-1)) * col("tmid.cant_cajas")).alias("cant_cajafisica"),
            ((when(col("mda.cod_operacion_origen") == "S", 1).otherwise(-1)) * col("tmid.cant_botellas")).alias("cant_unidades"),
            ((when(col("mda.cod_operacion_origen") == "S", 1).otherwise(-1)) * col("tmid.cant_unidades")).alias("cant_unidades_total"),
            when(col("mda.cod_operacion_origen") == 'S', col("tmid.cant_cajas")).otherwise(0).alias("cant_cajafisica_ingresada"),
            when(col("mda.cod_operacion_origen") == 'R', col("tmid.cant_cajas")).otherwise(0).alias("cant_cajafisica_salida"),
            when(col("mda.cod_operacion_origen") == 'S', col("tmid.cant_botellas")).otherwise(0).alias("cant_unidades_ingresada"),
            when(col("mda.cod_operacion_origen") == 'R', col("tmid.cant_botellas")).otherwise(0).alias("cant_unidades_salida"),
            when(col("mda.cod_operacion_origen") == 'S', col("tmid.cant_unidades")).otherwise(0).alias("cant_unidades_total_ingresada"),
            when(col("mda.cod_operacion_origen") == 'R', col("tmid.cant_unidades")).otherwise(0).alias("cant_unidades_total_salida"),
            when(col("mda.cod_operacion_origen") == 'S', col("tmid.costo_total")).otherwise(0).alias("imp_total_ingreso"),
            when(col("mda.cod_operacion_origen") == 'R', col("tmid.costo_total")).otherwise(0).alias("imp_total_salida"),
            when((col("tmid.estado") == 'PLI') & (col("tmid.cod_documento_transaccion") == 'GRA'), col("tmid.cant_unidades")).otherwise(0).alias("cant_unidades_transito"),
            when((col("tmid.estado") == 'PLI') & (col("tmid.cod_documento_transaccion") == 'GRA'), col("tmid.costo_total")).otherwise(0).alias("imp_total_transito"),
            when(col("tmid.cod_procedimiento") == 'REV', col("tmid.operacion_kardex")).otherwise(col("mda.cod_operacion_origen")).alias("cod_operacion_kardex"),
            col("ma.cant_unidad_paquete").alias("cant_unidad_paquete"),
            col("tmid.nro_secuencia_origen"),
            col("tmid.usuario_creacion"),
            col("tmid.fecha_creacion"),
            col("tmid.usuario_modificacion"),
            col("tmid.fecha_modificacion"),
        )
    )

    df_dom_t_movimiento_inventario_detalle = (
        df_tmp_movimiento_inventario_detalle.alias("thad")
        .join(df_m_compania.alias("mc"), col("thad.id_compania") == col("mc.cod_compania"), "inner")
        .join(df_t_cierre_inventario_cpm.alias("c"), 
            (col("c.id_sucursal") == col("thad.id_sucursal")) 
            & (col("c.id_articulo") == col("thad.id_articulo")) 
            & (col("c.id_periodo") == col("thad.id_periodo"))
            , "left"
        )
        .join(df_m_tipo_cambio.alias("mtc"), 
              (col("thad.id_compania") == col("mtc.cod_compania")) 
              & (col("thad.fecha_almacen") == col("mtc.fecha")) 
              & (col("mc.moneda_mn") == col("mtc.cod_moneda"))
              , "left"
        )
        .select(
            col("thad.id_pais"),
            col("thad.id_periodo"),
            col("thad.id_compania"),
            col("thad.id_sucursal"),
            col("thad.id_almacen"),
            col("thad.id_articulo"),
            col("thad.id_movimiento_almacen"),
            col("thad.id_centro_costo"),
            col("thad.fecha_almacen"),
            col("thad.nro_documento_movimiento"),
            col("thad.nro_linea_comprobante"),
            col("thad.cod_documento_transaccion"),
            col("thad.nro_documento_almacen"),
            col("thad.cod_documento_transaccion_ref").alias("cod_documento_transaccion_referencia"),
            col("thad.nro_documento_almacen_ref").alias("nro_documento_almacen_referencia"),
            col("thad.cod_procedimiento"),
            col("thad.cod_operacion_kardex"),
            col("thad.cod_estado_comprobante"),
            col("thad.cod_motivo"),
            col("thad.cod_unidad_articulo").alias("cod_unidad_almacen"),
            col("thad.nro_secuencia_origen"),
            col("thad.cant_cajafisica"),
            when(col("thad.cant_unidad_paquete") == 0, col("thad.cant_cajafisica"))
            .otherwise(col("thad.cant_cajafisica") + (col("thad.cant_unidades") / col("thad.cant_unidad_paquete")))
            .alias("cant_cajafisica_total"),
            col("thad.cant_unidades"),
            col("thad.cant_unidades_total"),
            col("thad.cant_cajafisica_ingresada"),
            when(col("thad.cant_unidad_paquete") == 0, col("thad.cant_cajafisica_ingresada"))
            .otherwise(col("thad.cant_cajafisica_ingresada") + (col("thad.cant_unidades_ingresada") / col("thad.cant_unidad_paquete")))
            .alias("cant_cajafisica_ingresada_total"),
            col("thad.cant_cajafisica_salida").alias("cant_cajafisica_salida"),
            when(col("thad.cant_unidad_paquete") == 0, col("thad.cant_cajafisica_salida"))
            .otherwise(col("thad.cant_cajafisica_salida") + (col("thad.cant_unidades_salida") / col("thad.cant_unidad_paquete")))
            .alias("cant_cajafisica_salida_total"),
            col("thad.cant_unidades_ingresada"),
            col("thad.cant_unidades_salida"),
            col("thad.cant_unidades_total_ingresada"),
            col("thad.cant_unidades_total_salida"),
            col("thad.costo_unitario").alias("imp_unitario"),
            col("thad.imp_total"),
            col("thad.imp_total_ingreso"),
            col("thad.imp_total_salida"),
            col("thad.cant_unidades_transito"),
            col("thad.imp_total_transito"),
            col("c.imp_cpm").alias("precio_unitario_mn"),
            when(col("mtc.tc_compra") == 0, 0)
            .otherwise(col("c.imp_cpm") / col("mtc.tc_compra"))
            .alias("precio_unitario_me"),
            (col("thad.cant_cajafisica") * col("c.imp_cpm")).alias("imp_valorizado_mn"),
            (col("thad.cant_cajafisica") * col("c.imp_cpm") / col("mtc.tc_compra")).alias("imp_valorizado_me"),
            col("c.imp_saldo_inicial"),
            col("c.imp_saldo_final"),
            col("c.imp_ingreso").alias("imp_valorizado_ingreso"),
            col("c.imp_salida").alias("imp_valorizado_salida"),
            col("thad.usuario_creacion"),
            col("thad.fecha_creacion"),
            col("thad.usuario_modificacion"),
            col("thad.fecha_modificacion"),
            lit(1).alias("es_eliminado"),
        )
    )

    df_dom_t_movimiento_inventario_detalle = df_dom_t_movimiento_inventario_detalle.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_periodo").cast("string").alias("id_periodo"),
        col("id_compania").cast("string").alias("id_compania"),
        col("id_sucursal").cast("string").alias("id_sucursal"),
        col("id_almacen").cast("string").alias("id_almacen"),
        col("id_articulo").cast("string").alias("id_articulo"),
        col("id_movimiento_almacen").cast("string").alias("id_movimiento_almacen"),
        col("id_centro_costo").cast("string").alias("id_centro_costo"),
        col("fecha_almacen").cast("date").alias("fecha_almacen"),
        col("nro_documento_movimiento").cast("string").alias("nro_documento_movimiento"),
        col("nro_linea_comprobante").cast("string").alias("nro_linea_comprobante"),
        col("cod_documento_transaccion").cast("string").alias("cod_documento_transaccion"),
        col("nro_documento_almacen").cast("string").alias("nro_documento_almacen"),
        col("cod_documento_transaccion_referencia").cast("string").alias("cod_documento_transaccion_referencia"),
        col("nro_documento_almacen_referencia").cast(" string").alias("nro_documento_almacen_referencia"),
        col("cod_procedimiento").cast("string").alias("cod_procedimiento"),
        col("cod_operacion_kardex").cast("string").alias("cod_operacion_kardex"),
        col("cod_estado_comprobante").cast("string").alias("cod_estado_comprobante"),
        col("cod_motivo").cast("string").alias("cod_motivo"),
        col("cod_unidad_almacen").cast("string").alias("cod_unidad_almacen"),
        col("cant_cajafisica").cast("numeric(38,12)").alias("cant_cajafisica"),
        col("cant_cajafisica_total").cast("numeric(38,12)").alias("cant_cajafisica_total"),
        col("cant_unidades").cast("numeric(38,12)").alias("cant_unidades"),
        col("cant_unidades_total").cast("numeric(38,12)").alias("cant_unidades_total"),
        col("cant_cajafisica_ingresada").cast("numeric(38,12)").alias("cant_cajafisica_ingresada"),
        col("cant_cajafisica_ingresada_total").cast("numeric(38,12)").alias("cant_cajafisica_ingresada_total"),
        col("cant_cajafisica_salida").cast("numeric(38,12)").alias("cant_cajafisica_salida"),
        col("cant_cajafisica_salida_total").cast("numeric(38,12)").alias("cant_cajafisica_salida_total"),
        col("cant_unidades_ingresada").cast("numeric(38,12)").alias("cant_unidades_ingresada"),
        col("cant_unidades_salida").cast("numeric(38,12)").alias("cant_unidades_salida"),
        col("cant_unidades_total_ingresada").cast("numeric(38,12)").alias("cant_unidades_total_ingresada"),
        col("cant_unidades_total_salida").cast("numeric(38,12)").alias("cant_unidades_total_salida"),
        col("imp_unitario").cast("numeric(38,12)").alias("imp_unitario"),
        col("imp_total").cast("numeric(38,12)").alias("imp_total"),
        col("imp_total_ingreso").cast("numeric(38,12)").alias("imp_total_ingreso"),
        col("imp_total_salida").cast("numeric(38,12)").alias("imp_total_salida"),
        col("imp_valorizado_ingreso").cast("numeric(38,12)").alias("imp_valorizado_ingreso"),
        col("imp_valorizado_salida").cast("numeric(38,12)").alias("imp_valorizado_salida"),
        col("cant_unidades_transito").cast("numeric(38,12)").alias("cant_unidades_transito"),
        col("imp_total_transito").cast("numeric(38,12)").alias("imp_total_transito"),
        col("precio_unitario_mn").cast("numeric(38,12)").alias("precio_unitario_mn"),
        col("precio_unitario_me").cast("numeric(38,12)").alias("precio_unitario_me"),
        col("imp_valorizado_mn").cast("numeric(38,12)").alias("imp_valorizado_mn"),
        col("imp_valorizado_me").cast("numeric(38,12)").alias("imp_valorizado_me"),
        col("imp_saldo_inicial").cast("numeric(38,12)").alias("imp_saldo_inicial"),
        col("imp_saldo_final").cast("numeric(38,12)").alias("imp_saldo_final"),
        col("usuario_creacion").cast("string").alias("usuario_creacion"),
        col("fecha_creacion").cast("timestamp").alias("fecha_creacion"),
        col("usuario_modificacion").cast("string").alias("usuario_modificacion"),
        col("fecha_modificacion").cast("timestamp").alias("fecha_modificacion"),
        col("es_eliminado").cast("int").alias("es_eliminado"),
    )

    logger.info(f"starting write of {target_table_name}")
    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(df_dom_t_movimiento_inventario_detalle, data_paths.DOMAIN, target_table_name, partition_columns_array)
    logger.info(f"Write de {target_table_name} success completed")
except Exception as e:
    logger.error(f"Error processing df_dom_t_movimiento_inventario_detalle: {e}")
    raise ValueError(f"Error processing df_dom_t_movimiento_inventario_detalle: {e}")