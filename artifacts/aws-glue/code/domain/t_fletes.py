from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import coalesce, col, concat_ws, date_format, first, lit, max, sum, when, row_number
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")
    periodos= spark_controller.get_periods()
    logger.info(periodos)

    m_almacen = spark_controller.read_table(data_paths.BIG_BAGIC, "m_almacen", cod_pais=cod_pais)
    m_articulo = spark_controller.read_table(data_paths.BIG_BAGIC, "m_articulo", cod_pais=cod_pais)
    m_capacidad_vehiculo = spark_controller.read_table(data_paths.BIG_BAGIC, "m_capacidad_vehiculo", cod_pais=cod_pais)
    m_cliente = spark_controller.read_table(data_paths.BIG_BAGIC, "m_cliente", cod_pais=cod_pais)
    m_compania = spark_controller.read_table(data_paths.BIG_BAGIC, "m_compania", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.BIG_BAGIC, "m_pais", cod_pais=cod_pais, have_principal=True)
    m_persona = spark_controller.read_table(data_paths.BIG_BAGIC, "m_persona", cod_pais=cod_pais)
    m_procedimiento = spark_controller.read_table(data_paths.BIG_BAGIC, "m_procedimiento", cod_pais=cod_pais)
    m_sucursal = spark_controller.read_table(data_paths.BIG_BAGIC, "m_sucursal", cod_pais=cod_pais)
    m_tarifa_flete = spark_controller.read_table(data_paths.BIG_BAGIC, "m_tarifa_flete", cod_pais=cod_pais)
    m_tipo_cambio = spark_controller.read_table(data_paths.BIG_BAGIC, "m_tipo_cambio", cod_pais=cod_pais)
    t_documento_flete = spark_controller.read_table(data_paths.BIG_BAGIC, "t_documento_flete", cod_pais=cod_pais)
    t_guia_por_flete = spark_controller.read_table(data_paths.BIG_BAGIC, "t_guia_por_flete", cod_pais=cod_pais)
    t_movimiento_inventario = spark_controller.read_table(data_paths.BIG_BAGIC, "t_movimiento_inventario", cod_pais=cod_pais)
    t_movimiento_inventario_detalle = spark_controller.read_table(data_paths.BIG_BAGIC, "t_movimiento_inventario_detalle", cod_pais=cod_pais)
    t_requerimiento_compra_cabecera = spark_controller.read_table(data_paths.BIG_BAGIC, "t_requerimiento_compra_cabecera", cod_pais=cod_pais)
    t_requerimiento_servicio_flete = spark_controller.read_table(data_paths.BIG_BAGIC, "t_requerimiento_servicio_flete", cod_pais=cod_pais)

    target_table_name = "t_fletes"

except Exception as e:
    logger.error(e)
    raise

try:
    m_pais = m_pais.filter(col("id_pais").isin(cod_pais))
    m_tipo_cambio = m_tipo_cambio.filter((col("fecha").isNotNull()) & (col("tc_compra") > 0) & (col("tc_venta") > 0))
    m_cliente = m_cliente.filter((col("nomb_cliente").isNotNull()) & (col("nomb_cliente") != ""))
    
    t_movimiento_inventario = t_movimiento_inventario \
    .filter(date_format(col("fecha_almacen"), "yyyyMM").isin(periodos)) \
    .select(
        concat_ws("|", col("cod_compania"), col("cod_sucursal"), col("cod_almacen_emisor_origen"), col("cod_documento_transaccion"), col("nro_documento_almacen")).alias("id_reparto"),
        col("cod_compania"),
        col("cod_sucursal"),
        col("nro_documento_almacen"),
        col("cod_almacen_emisor_origen"),
        col("fecha_almacen"),
        col("nro_documento_movimiento"),
        col("total_peso"),
        col("nro_serie_alm"),
        col("nropricoal"),
        col("cod_procedimiento"),
        col("fecha_liquidacion"),
        col("cod_vehiculo"),
        col("cod_transportista"),
        col("prov_clien_emp"),
        col("cod_estado_comprobante"),
        col("cod_documento_transaccion"),
    )
    # logger.info(f"t_movimiento_inventario filtered successfully: {t_movimiento_inventario.count()}")

    t_documento_flete = t_documento_flete \
    .select(
        concat_ws("|", col("cod_compania"), col("cod_sucursal"), col("cod_almacen_origen"), col("cod_documento_transaccion"), col("nro_gra_principal")).alias("id_reparto"),
        col("cod_compania"),
        col("documento_flete"),
        col("nro_flete"),
        col("documento_pago_ftm"),
        col("nro_serie"),
        col("nro_factura"),
        col("fecha_confirmacion"),
        col("cod_confirmpor"),
        col("voucher_contable"),
        col("centro_costo"),
        col("flg_pago"),
        col("cod_moneda"),
        col("flg_impuesto"),
    )
    # logger.info(f"t_documento_flete filtered successfully: {t_documento_flete.count()}")

    t_movimiento_inventario_detalle = t_movimiento_inventario_detalle \
    .select(
        concat_ws("|", col("cod_compania"), col("cod_sucursal"), col("cod_almacen_emisor_origen"), col("cod_documento_transaccion"), col("nro_documento_almacen")).alias("id_reparto"),
        col("cod_compania"),
        col("cod_articulo"),
        col("nro_documento_movimiento"),
        col("cant_cajas"),
        col("cant_botellas"),
    )
    # logger.info(f"t_movimiento_inventario_detalle filtered successfully: {t_movimiento_inventario_detalle.count()}")

    t_guia_por_flete = t_guia_por_flete \
    .select(
        concat_ws("|", col("cod_compania"), col("cod_sucursal"), col("cod_almacen"), col("cod_documento_transaccion"), col("nro_guia_destino")).alias("id_reparto"),
        col("cod_documento"),
        col("nro_guia_destino"),
        col("cod_compania"),
        col("cod_sucursal"),
        col("doc_flete"),
        col("nro_flete"),
    )
    # logger.info(f"t_guia_por_flete filtered successfully: {t_guia_por_flete.count()}")

    t_requerimiento_servicio_flete = t_requerimiento_servicio_flete \
    .select(
        concat_ws("|", col("cod_compania_ocarga"), col("cod_sucursal_ocarga"), col("cod_almacen_ocarga"), col("trans_ocarga"), col("nro_ocarga")).alias("id_reparto"),
        col("id_requerimiento_servicio_flete"),
        col("cod_compania"),
        col("cod_sucursal"),
        col("idtv"),
        col("tipo_carga"),
        col("codigo_envio_efletex"),
        col("cod_transaccion"),
        col("nro_documento"),
        col("total_negociado"),
        col("precio_envio_std"),
        col("distancia_recorrida_km"),
        col("cod_chofer"),
        col("gasto_adicional"),
        col("importe_gasto_adicional"),
        col("tipo_viaje"),
        col("estado_envio"),
        col("identifica_envio_efletex"),    
    )
    # logger.info(f"t_requerimiento_servicio_flete filtered successfully: {t_requerimiento_servicio_flete.count()}")
    # logger.info("all tables filtered succesfully")

    tmp_principal = (
        t_movimiento_inventario.alias("tmi")
        .join(t_documento_flete.alias("tdf"), col("tmi.id_reparto") == col("tdf.id_reparto"), "inner")
        .select(
            col("tmi.id_reparto"),
            col("tmi.cod_compania"),
            col("tmi.cod_sucursal"),
            col("tmi.nro_documento_almacen"),
            col("tdf.documento_flete"),
            col("tdf.nro_flete"),
        )
    )
    # logger.info(f"tmp_principal created succesfully: {tmp_principal.count()}")

    tmp_principal_almacen_group = (
        tmp_principal 
        .select(
            col("id_reparto"),
        )
        .distinct()
    )
    # logger.info(f"tmp_principal_almacen_group created succesfully: {tmp_principal_almacen_group.count()}")
    
    tmp_fletes_peso = (
        tmp_principal_almacen_group.alias("tmp")
        .join(t_movimiento_inventario_detalle.alias("tmid"), col("tmp.id_reparto") == col("tmid.id_reparto"), "inner")
        .join(t_movimiento_inventario.alias("tmi"), (col("tmid.id_reparto") == col("tmi.id_reparto")) & (col("tmi.nro_documento_movimiento") == col("tmid.nro_documento_movimiento")), "inner")
        .join(m_articulo.alias("ma"), (col("ma.cod_compania") == col("tmid.cod_compania")) & (col("ma.cod_articulo") == col("tmid.cod_articulo")), "inner")
        .groupBy(
            col("tmp.id_reparto")
        )
        .agg(
            sum(
                when(col("ma.cod_linea").isin("01", "02", "05"),
                    ((col("tmid.cant_cajas") * col("ma.cant_unidad_paquete") + col("tmid.cant_botellas")) * col("ma.cant_unidad_volumen"))
                ).otherwise(lit(0))
            ).alias("cant_cajavolumen_principal"),
            sum(
                when(col("ma.cod_linea").isin("01", "02", "05"),
                    ((col("tmid.cant_cajas") * col("ma.cant_unidad_paquete") + col("tmid.cant_botellas")) / col("ma.cant_unidad_paquete"))
                ).otherwise(lit(0))
            ).alias("cant_cajafisica_principal"),
            sum(
                when(~col("ma.cod_linea").isin("01", "02", "05"),
                    ((col("tmid.cant_cajas") * col("ma.cant_unidad_paquete") + col("tmid.cant_botellas")) / col("ma.cant_unidad_paquete"))
                ).otherwise(lit(0))
            ).alias("cant_tarimas_principal"),
            max(col("tmi.total_peso")).alias("cant_peso_principal"),
        )
        .select(
            col("tmp.id_reparto"),
            col("cant_cajavolumen_principal"),
            col("cant_cajafisica_principal"),
            col("cant_tarimas_principal"),
            col("cant_peso_principal"),
        )
    )
    # logger.info(f"tmp_fletes_peso created succesfully: {tmp_fletes_peso.count()}")

    tmp_documentos_asociados = (
        tmp_principal.alias("tmp")
        .join(
            t_guia_por_flete.alias("tgpf"),
            (col("tmp.cod_compania") == col("tgpf.cod_compania"))
            & (col("tmp.cod_sucursal") == col("tgpf.cod_sucursal"))
            & (col("tmp.documento_flete") == col("tgpf.doc_flete"))
            & (col("tmp.nro_flete") == col("tgpf.nro_flete")),
            "inner"
        )
        .filter(col("tmp.nro_documento_almacen") != col("tgpf.nro_guia_destino"))
        .join(t_movimiento_inventario.alias("tmi"),col("tmi.id_reparto") == col("tgpf.id_reparto"), "inner")
        .select(
            col("tmp.nro_documento_almacen").alias("nro_documento_almacen_pri"),
            col("tmp.documento_flete"),
            col("tmp.nro_flete"),
            concat_ws("-", col("tgpf.cod_documento"), col("tgpf.nro_guia_destino")).alias("nro_documento"),
            concat_ws(" ", col("tmi.nro_serie_alm"), col("tmi.nropricoal")).alias("nro_guia"), # why
            col("tgpf.cod_documento").alias("cod_documento_transaccion"),
            col("tgpf.nro_guia_destino").alias("nro_documento_almacen"),
            col("tmi.nro_documento_movimiento"),
            col("tmp.id_reparto"),
            col("tmp.cod_compania"),
            col("tmp.cod_sucursal"),
        )
    )
    # logger.info(f"tmp_documentos_asociados created succesfully: {tmp_documentos_asociados.count()}")

    tmp1 = (
        tmp_documentos_asociados.alias("tmp")
        .join(
            t_movimiento_inventario_detalle.alias("tmid"), (col("tmp.id_reparto") == col("tmid.id_reparto")) & (col("tmp.nro_documento_movimiento") == col("tmid.nro_documento_movimiento")), "inner")
        .join(m_articulo.alias("ma"), (col("ma.cod_compania") == col("tmid.cod_compania")) & (col("ma.cod_articulo") == col("tmid.cod_articulo")), "inner")
        .groupBy(
            col("tmp.id_reparto"),
            col("tmp.nro_documento_almacen_pri"),
            col("tmp.cod_compania"),
            col("tmp.cod_sucursal"),
            col("tmp.nro_flete"),
        )
        .agg(
            sum(
                when(col("ma.cod_linea").isin("01", "02", "05"),
                    ((col("tmid.cant_cajas") * col("ma.cant_unidad_paquete") + col("tmid.cant_botellas")) * col("ma.cant_unidad_volumen"))
                ).otherwise(lit(0))
            ).alias("cant_cajavolumen_asociada"),
            sum(
                when(col("ma.cod_linea").isin("01", "02", "05"),
                    ((col("tmid.cant_cajas") * col("ma.cant_unidad_paquete") + col("tmid.cant_botellas")) / col("ma.cant_unidad_paquete"))
                ).otherwise(lit(0))
            ).alias("cant_cajafisica_asociada"),
            sum(
                when(~col("ma.cod_linea").isin("01", "02", "05"),
                    ((col("tmid.cant_cajas") * col("ma.cant_unidad_paquete") + col("tmid.cant_botellas")) / col("ma.cant_unidad_paquete"))
                ).otherwise(lit(0))
            ).alias("cant_tarimas_asociada"),
        )
        .select(
            col("tmp.id_reparto"),
            col("tmp.nro_documento_almacen_pri"),
            col("tmp.cod_compania"),
            col("tmp.cod_sucursal"),
            col("tmp.nro_flete"),
            col("cant_cajavolumen_asociada"),
            col("cant_cajafisica_asociada"),
            col("cant_tarimas_asociada"),
            lit(0).alias("cant_peso_asociada"),
        )
    )
    # logger.info(f"tmp1 created succesfully: {tmp1.count()}")

    tmp2 = (
        tmp_principal.alias("tmp")
        .join(
            t_guia_por_flete.alias("tgpf"),
            (col("tmp.cod_compania") == col("tgpf.cod_compania"))
            & (col("tmp.cod_sucursal") == col("tgpf.cod_sucursal"))
            & (col("tmp.documento_flete") == col("tgpf.doc_flete"))
            & (col("tmp.nro_flete") == col("tgpf.nro_flete")),
            "inner"
        )
        .filter(col("tmp.nro_documento_almacen") != col("tgpf.nro_guia_destino"))
        .join(t_movimiento_inventario.alias("tmi"), col("tmi.id_reparto") == col("tgpf.id_reparto"), "inner")
        .select(
            col("tmp.nro_documento_almacen").alias("nro_documento_almacen_pri"),
            col("tmp.cod_compania"),
            col("tmp.cod_sucursal"),
            col("tmp.documento_flete"),
            col("tmp.nro_flete"),
            concat_ws("-", col("tgpf.cod_documento"), col("tgpf.nro_guia_destino")).alias("nro_documento"),
            concat_ws(" ", col("tmi.nro_serie_alm"), col("tmi.nropricoal")).alias("nro_guia"),  # why
            col("tgpf.cod_documento").alias("cod_documento_transaccion"),
            col("tgpf.nro_guia_destino").alias("nro_documento_almacen"),
            col("tmi.id_reparto"),
        )
    )
    # logger.info(f"tmp2 created succesfully: {tmp2.count()}")

    tmp3 = (
        tmp2.alias("tmp")
        .join(t_movimiento_inventario.alias("tmi"), col("tmp.id_reparto") == col("tmi.id_reparto"), "inner")
        .groupBy(
            col("tmp.nro_documento_almacen_pri"),
            col("tmp.cod_compania"),
            col("tmp.cod_sucursal"),
            col("tmp.nro_flete"),
            col("tmi.id_reparto"),
        )
        .agg(
            sum(col("tmi.total_peso")).alias("cant_peso_asociada"),
        )
        .select(
            col("tmi.id_reparto"),
            col("tmp.nro_documento_almacen_pri"),
            col("tmp.cod_compania"),
            col("tmp.cod_sucursal"),
            col("tmp.nro_flete"),
            lit(0).alias("cant_cajavolumen_asociada"),
            lit(0).alias("cant_cajafisica_asociada"),
            lit(0).alias("cant_tarimas_asociada"),
            col("cant_peso_asociada"),
        )
    )
    # logger.info(f"tmp3 created succesfully: {tmp3.count()}")

    tmp_union = tmp1.unionByName(tmp3)
    # logger.info(f"tmp_union created succesfully: {tmp_union.count()}")

    tmp_fletes_peso_asociado = (
        tmp_union.groupBy(
            col("id_reparto"),
            col("nro_documento_almacen_pri"),
            col("cod_compania"),
            col("cod_sucursal"),
            col("nro_flete"),
        )
        .agg(
            sum(col("cant_cajavolumen_asociada")).alias("cant_cajavolumen_asociada"),
            sum(col("cant_cajafisica_asociada")).alias("cant_cajafisica_asociada"),
            sum(col("cant_tarimas_asociada")).alias("cant_tarimas_asociada"),
            sum(col("cant_peso_asociada")).alias("cant_peso_asociada"),
        )
        .select(
            col("id_reparto"),
            col("nro_documento_almacen_pri"),
            col("cod_compania"),
            col("cod_sucursal"),
            col("nro_flete"),
            col("cant_cajavolumen_asociada"),
            col("cant_cajafisica_asociada"),
            col("cant_tarimas_asociada"),
            col("cant_peso_asociada"),
        )
    )
    # logger.info(f"tmp_fletes_peso_asociado created succesfully: {tmp_fletes_peso_asociado.count()}")

    m_tarifa_flete_filtered = m_tarifa_flete.filter((col("cod_tipo_persona") == lit("3")) & (col("estado") == lit("A")))
    
    # window_spec = Window.partitionBy(
    #     "mtf.cod_sucursal", 
    #     "mtf.cod_tipo_capacidad_vehiculo",
    #     "mtf.tipo_carga",
    #     "mcli.cod_zona_postal",
    # ).orderBy(lit(1)) # check

    tmp_tarifa_distancia_km = (
        t_movimiento_inventario.alias("tmi")
        .join(t_requerimiento_servicio_flete.alias("trsf"), col("tmi.id_reparto") == col("trsf.id_reparto"), "left")
        .join(m_tarifa_flete_filtered.alias("mtf"),
            (col("trsf.cod_compania") == col("mtf.cod_compania"))
            & (col("trsf.cod_sucursal") == col("mtf.cod_sucursal"))
            & (col("trsf.idtv") == col("mtf.cod_tipo_capacidad_vehiculo"))
            & (col("trsf.tipo_carga") == col("mtf.tipo_carga")),
            "left"
        )
        .join(m_cliente.alias("mcli"), 
            (col("mcli.cod_compania") == col("tmi.cod_compania"))
            & (col("mcli.cod_cliente") == col("tmi.prov_clien_emp"))
            & (col("mcli.cod_zona_postal") == col("mtf.cod_zona_postal")),
            "inner"
        )
        .groupBy(
            col("mtf.cod_compania"),
            col("mtf.cod_sucursal"),
            col("mtf.cod_tipo_capacidad_vehiculo"),
            col("mtf.tipo_carga"),
            col("mcli.cod_zona_postal"),
        )
        .agg(first(col("mtf.distancia_km")).alias("tarifa_distancia_km"))
        .select(
            col("mtf.cod_compania"),
            col("mtf.cod_sucursal"),
            col("mtf.cod_tipo_capacidad_vehiculo"),
            col("mtf.tipo_carga"),
            col("mcli.cod_zona_postal"),
            col("tarifa_distancia_km"),
        )
        .distinct()
    )
    # logger.info(f"tmp_tarifa_distancia_km created succesfully: {tmp_tarifa_distancia_km.count()}")

    t_requerimiento_compra_cabecera = (
        t_requerimiento_compra_cabecera
        .select(
            col("id_requerimiento_compra_cabecera"),
            col("prioridad"),
            when(col("prioridad") == lit("E"), lit("Si"))
            .otherwise(lit("No"))
            .alias("desc_efletex"),
            col("atencion_logistica"),
            when(col("atencion_logistica") == lit("Pe"), lit("Pendiente"))
            .when(col("atencion_logistica") == lit("En"), lit("Enviado"))
            .when(col("atencion_logistica") == lit("Ne"), lit("Negociado"))
            .when(col("atencion_logistica") == lit("At"), lit("Atendido"))
            .when(col("atencion_logistica") == lit("An"), lit("Anulado"))
            .when(col("atencion_logistica") == lit("As"), lit("Asociado"))
            .when(col("atencion_logistica") == lit("Co"), lit("Confirmado"))
            .when(col("atencion_logistica") == lit("Fa"), lit("Facturado"))
            .when(col("atencion_logistica") == lit("Ca"), lit("Cancelado"))
            .otherwise(col("atencion_logistica"))
            .alias("desc_estado_atencion"),
        )
    )
    # logger.info(f"t_requerimiento_compra_cabecera modified succesfully: {t_requerimiento_compra_cabecera.count()}")

    t_requerimiento_servicio_flete = (
        t_requerimiento_servicio_flete
        .select(
            col("id_reparto"),
            col("id_requerimiento_servicio_flete"),
            col("cod_compania"),
            col("cod_sucursal"),
            col("idtv"),
            col("tipo_carga"),
            coalesce(col("codigo_envio_efletex"), lit("")).alias("id_fletex"),
            concat_ws("-", col("cod_transaccion"), col("nro_documento")).alias("nro_documento"),
            col("total_negociado").alias("tarifa_negociacion_mn"),
            col("precio_envio_std").alias("tarifa_base_mn"),
            coalesce(col("distancia_recorrida_km"), lit(0)).alias("distancia_km"),
            col("cod_chofer"),
            col("gasto_adicional"),
            when(col("gasto_adicional") == lit("1"), lit("1.Problemas en la ruta por mal clima"))
            .when(col("gasto_adicional") == lit("2"), lit("2.Exceso de espera para descargar"))
            .when(col("gasto_adicional") == lit("3"), lit("3.Maniobras"))
            .when(col("gasto_adicional") == lit("4"), lit("4.Maniobras y movimiento"))
            .when(col("gasto_adicional") == lit("5"), lit("5.Maniobras y estadias"))
            .alias("desc_tipo_gasto_adicional"),
            col("importe_gasto_adicional").alias("imp_gasto_adicional_mn"),
            when(col("tipo_viaje") == lit("1"), lit("1. Tarifa Fija"))
            .when(col("tipo_viaje") == lit("2"), lit("2. Tarifa Peso"))
            .when(col("tipo_viaje") == lit("3"), lit("3. Tarifa Cero"))
            .when(col("tipo_viaje") == lit("4"), lit("4. Tarifa Unidad"))
            .otherwise(col("tipo_viaje"))
            .alias("desc_tipo_tarifa"),
            col("estado_envio").alias("desc_estado_envio"),
            coalesce(col("identifica_envio_efletex"), lit("")).alias("cod_envio"),
        )
    )
    # logger.info(f"t_requerimiento_servicio_flete modified succesfully: {t_requerimiento_servicio_flete.count()}")

    tmp_fletes = (
        t_movimiento_inventario.alias("tmi")
        .join(t_documento_flete.alias("tdf"), col("tmi.id_reparto") == col("tdf.id_reparto"), "inner")
        .join(t_requerimiento_servicio_flete.alias("trsf"), col("tmi.id_reparto") == col("trsf.id_reparto"), "left")
        .join(t_requerimiento_compra_cabecera.alias("trcc"), col("trcc.id_requerimiento_compra_cabecera") == col("trsf.id_requerimiento_servicio_flete"), "left")
        .join(m_cliente.alias("mcli"), (col("tmi.cod_compania") == col("mcli.cod_compania")) & (col("tmi.prov_clien_emp") == col("mcli.cod_cliente")), "inner")
        .join(m_persona.alias("mp1"), (col("tmi.cod_compania") == col("mp1.cod_compania")) & (col("tmi.cod_transportista") == col("mp1.cod_persona")), "inner")
        .join(m_persona.alias("mp2"), (col("tmi.cod_compania") == col("mp2.cod_compania")) & (col("tmi.prov_clien_emp") == col("mp2.cod_persona")), "inner")
        .join(m_persona.alias("mp3"), (col("tdf.cod_compania") == col("mp3.cod_compania")) & (col("tdf.cod_confirmpor") == col("mp3.cod_persona")), "left")
        .join(
            m_tarifa_flete.alias("mtf"),
            (col("trsf.cod_compania") == col("mtf.cod_compania"))
            & (col("trsf.cod_sucursal") == col("mtf.cod_sucursal"))
            & (col("trsf.idtv") == col("mtf.cod_tipo_capacidad_vehiculo"))
            & (col("trsf.tipo_carga") == col("mtf.tipo_carga"))
            & (col("mcli.cod_zona_postal") == col("mtf.cod_zona_postal"))
            & (col("mtf.cod_tipo_persona") == "S")
            & (col("mtf.estado") == "A"),
            "left"
        )
        .join(m_procedimiento.alias("mpr"), 
            (col("tmi.cod_compania") == col("mpr.cod_compania"))
            & (col("tmi.cod_documento_transaccion") == col("mpr.cod_documento_transaccion"))
            & (col("tmi.cod_procedimiento") == col("mpr.cod_procedimiento")),
            "left"
        )
        .join(m_compania.alias("mcomp"), col("tmi.cod_compania") == col("mcomp.cod_compania"), "left")
        .join(m_pais.alias("mpais"), col("mcomp.cod_pais") == col("mpais.cod_pais"), "left")
        .join(m_sucursal.alias("ms"), (col("tmi.cod_compania") == col("ms.cod_compania")) & (col("tmi.cod_sucursal") == col("ms.cod_sucursal")), "left")
        .join(m_almacen.alias("malm"), 
              (col("tmi.cod_compania") == col("malm.cod_compania")) 
              & (col("tmi.cod_sucursal") == col("malm.cod_sucursal")) 
              & (col("tmi.cod_almacen_emisor_origen") == col("malm.cod_almacen")), 
              "left"
        )
        .join(
            m_capacidad_vehiculo.alias("mcv"),
            (col("tmi.cod_compania") == col("mcv.cod_compania"))
            & (col("trsf.idtv") == col("mcv.cod_tipo_capacidad_vehiculo")),
            "left"
        )
        .join(
            m_tipo_cambio.alias("mtc"),
            (col("tdf.cod_moneda") == col("mtc.cod_moneda"))
            & (col("tmi.cod_compania") == col("mtc.cod_compania"))
            & (col("tmi.fecha_almacen") == col("mtc.fecha")),
            "left"
        )
        .join(
            tmp_tarifa_distancia_km.alias("tmp"),
            (col("trsf.cod_compania") == col("tmp.cod_compania"))
            & (col("trsf.cod_sucursal") == col("tmp.cod_sucursal"))
            & (col("trsf.idtv") == col("tmp.cod_tipo_capacidad_vehiculo"))
            & (col("trsf.tipo_carga") == col("tmp.tipo_carga"))
            & (col("mcli.cod_zona_postal") == col("tmp.cod_zona_postal")),
            "left"
        )
        .select(
            col("mpais.id_pais"),
            col("mcomp.desc_compania"),
            col("ms.desc_sucursal"),
            col("tmi.id_reparto"),
            col("tmi.cod_procedimiento"),
            col("tmi.cod_compania"),
            col("tmi.cod_sucursal"),
            col("tmi.cod_almacen_emisor_origen"),
            concat_ws("-", col("tmi.cod_documento_transaccion"), col("tmi.nro_documento_almacen")).alias("nro_orden_carga"),
            concat_ws("-", col("tmi.nro_serie_alm"), col("tmi.nropricoal")).alias("nro_pre_impreso"),
            col("tmi.fecha_almacen"),
            col("trsf.id_fletex"),
            concat_ws("-", col("tdf.documento_flete"), col("tdf.nro_flete")).alias("nro_documento_flete"),
            col("trsf.nro_documento"),
            col("tdf.documento_pago_ftm").alias("documento_pago"),
            col("tdf.nro_serie").alias("nro_serie_pago"),
            col("tdf.nro_factura").alias("nro_pago"),
            col("tmi.fecha_liquidacion"),
            col("tmi.cod_vehiculo"),
            col("tmi.cod_transportista"),
            col("mp1.nomb_persona").alias("nombre_transport"), # why
            col("tmi.prov_clien_emp"),
            lit(1).alias("cod_persona_direccion"),
            col("mp2.nomb_persona").alias("nombre_cliente"), # why
            col("trsf.tarifa_negociacion_mn"),
            col("trsf.tarifa_base_mn"),
            col("tmi.total_peso"),
            col("tdf.fecha_confirmacion"),
            col("tdf.cod_confirmpor"),
            col("mp3.nomb_persona").alias("confirmado_por"), # why
            col("trsf.distancia_km"),
            coalesce(col("mtf.nro_dias"), lit(0)).cast("int").alias("cant_dias"),
            coalesce(col("mtf.tviaje"), lit(0)).cast("numeric(38,12)").alias("cant_tiempo"),
            col("tdf.voucher_contable").alias("nro_comprobante"),
            col("tmi.cod_documento_transaccion"),
            col("tmi.nro_documento_almacen"),
            col("malm.desc_almacen").alias("desc_almacen_origen"),
            col("tdf.documento_flete"),
            col("tdf.nro_flete"),
            col("trcc.prioridad"),
            col("trcc.desc_efletex"),
            col("trcc.atencion_logistica"),
            col("trcc.desc_estado_atencion"),
            col("trsf.cod_chofer"),
            col("trsf.gasto_adicional"),
            col("trsf.desc_tipo_gasto_adicional"),
            col("trsf.imp_gasto_adicional_mn"),
            col("trsf.desc_tipo_tarifa"),
            col("tmi.cod_estado_comprobante"),
            col("trsf.idtv"),
            col("tdf.centro_costo"),
            when(col("tdf.flg_pago") == lit("T"), lit(1)).otherwise(lit(0)).alias("flag_pago"),
            col("trsf.desc_estado_envio"),
            col("trsf.cod_envio"),
            col("mcv.desc_tipo_vehiculo").alias("desc_tipo_vehiculo_servicio"),
            col("mpr.desc_procedimiento_corto").alias("desc_procedimiento"),
            col("tmp.tarifa_distancia_km"),
            col("tdf.cod_moneda"),
            col("mtc.tc_venta").alias("tipo_cambio"),
            when(col("tdf.flg_impuesto") == lit("T"), lit(1)).otherwise(lit(0)).alias("incluye_impuesto"),
        )
    )
    # logger.info(f"tmp_fletes created succesfully: {tmp_fletes.count()}")

    tmp_t_fletes = (
        tmp_fletes.alias("tmp1")
        .join(tmp_fletes_peso.alias("tmp2"), col("tmp1.id_reparto") == col("tmp2.id_reparto"), "inner")
        .join(tmp_fletes_peso_asociado.alias("tmp3"), col("tmp1.id_reparto") == col("tmp3.id_reparto"), "left")
        .select(
            col("tmp1.id_pais"),
            date_format(col("tmp1.fecha_almacen"), "yyyyMM").alias("id_periodo"),
            concat_ws("|", col("tmp1.cod_compania"), col("tmp1.cod_sucursal")).alias("id_sucursal"),
            concat_ws("|", col("tmp1.cod_compania"), col("tmp1.cod_sucursal"), col("tmp1.cod_almacen_emisor_origen")).alias("id_localidad_origen"),
            concat_ws("|", col("tmp1.cod_compania"), col("tmp1.cod_transportista")).alias("id_transportista"),
            concat_ws("|", col("tmp1.cod_compania"), col("tmp1.cod_vehiculo")).alias("id_medio_transporte"),
            concat_ws("|", col("tmp1.cod_compania"), col("tmp1.prov_clien_emp"), col("tmp1.cod_persona_direccion")).alias("id_localidad_destino"),
            concat_ws("|", col("tmp1.cod_compania"), col("tmp1.prov_clien_emp")).alias("id_cliente"),
            concat_ws("|", col("tmp1.cod_compania"), col("tmp1.cod_confirmpor")).alias("id_empleado"),
            col("tmp1.id_fletex"),
            col("tmp1.cod_procedimiento"),
            col("tmp1.nro_orden_carga"),
            col("tmp1.nro_pre_impreso"),
            col("tmp1.fecha_almacen"),
            col("tmp1.nro_documento_flete"),
            col("tmp1.nro_documento"),
            col("tmp1.documento_pago"),
            col("tmp1.nro_serie_pago"),
            col("tmp1.nro_pago"),
            col("tmp1.fecha_liquidacion"),
            col("tmp1.tarifa_negociacion_mn"),
            (col("tmp1.tarifa_negociacion_mn") / coalesce(col("tmp1.tipo_cambio"), lit(1))).alias("tarifa_negociacion_me"),
            col("tmp1.tarifa_base_mn"),
            (col("tmp1.tarifa_base_mn") / coalesce(col("tmp1.tipo_cambio"), lit(1))).alias("tarifa_base_me"),
            col("tmp1.total_peso"),
            col("tmp1.fecha_confirmacion"),
            col("tmp1.distancia_km"),
            col("tmp1.cant_dias"),
            col("tmp1.cant_tiempo"),
            col("tmp1.nro_comprobante"),
            col("tmp1.cod_documento_transaccion"),
            col("tmp1.nro_documento_almacen"),
            col("tmp1.documento_flete"),
            col("tmp1.nro_flete"),
            col("tmp1.prioridad").alias("cod_prioridad"),
            col("tmp1.desc_efletex"),
            col("tmp1.atencion_logistica").alias("cod_antencion_logistica"),
            col("tmp1.desc_estado_atencion"),
            col("tmp1.cod_chofer").alias("cod_chofer_servicio"),
            col("tmp1.gasto_adicional").alias("cod_tipo_gasto_adicional"),
            col("tmp1.desc_tipo_gasto_adicional"),
            col("tmp1.imp_gasto_adicional_mn"),
            (col("tmp1.imp_gasto_adicional_mn") / coalesce(col("tmp1.tipo_cambio"), lit(1))).alias("imp_gasto_adicional_me"),
            col("tmp1.desc_tipo_tarifa"),
            col("tmp1.cod_estado_comprobante"),
            col("tmp1.idtv"),
            col("tmp1.centro_costo").alias("cod_centro_costo"),
            col("tmp1.flag_pago"),
            col("tmp1.desc_estado_envio"),
            col("tmp1.cod_envio"),
            col("tmp1.desc_tipo_vehiculo_servicio"),
            col("tmp1.desc_procedimiento"),
            col("tmp1.tarifa_distancia_km").alias("tarifa_distancia_km_mn"),
            (col("tmp1.tarifa_distancia_km") / coalesce(col("tmp1.tipo_cambio"), lit(1))).alias("tarifa_distancia_km_me"),
            col("tmp1.cod_moneda"),
            col("tmp1.incluye_impuesto"),
            col("tmp2.cant_cajavolumen_principal"),
            col("tmp2.cant_cajafisica_principal"),
            col("tmp2.cant_tarimas_principal"),
            col("tmp2.cant_peso_principal"),
            col("tmp3.cant_cajavolumen_asociada"),
            col("tmp3.cant_cajafisica_asociada"),
            col("tmp3.cant_tarimas_asociada"),
            col("tmp3.cant_peso_asociada"),
        )
    )
    # logger.info(f"tmp_t_fletes created succesfully: {tmp_t_fletes.count()}")

    tmp = tmp_t_fletes.select(
            col("id_pais").cast("string").alias("id_pais"),
            col("id_periodo").cast("string").alias("id_periodo"),
            col("id_sucursal").cast("string").alias("id_sucursal"),
            col("id_localidad_origen").cast("string").alias("id_localidad_origen"),
            col("id_transportista").cast("string").alias("id_transportista"),
            col("id_medio_transporte").cast("string").alias("id_medio_transporte"),
            col("id_localidad_destino").cast("string").alias("id_localidad_destino"),
            col("id_empleado").cast("string").alias("id_empleado"),
            col("id_fletex").cast("string").alias("id_fletex"),
            col("cod_procedimiento").cast("string").alias("cod_procedimiento"),
            col("nro_orden_carga").cast("string").alias("nro_orden_carga"),
            col("nro_pre_impreso").cast("string").alias("nro_pre_impreso"),
            col("fecha_almacen").cast("date").alias("fecha_almacen"),
            col("nro_documento_flete").cast("string").alias("nro_documento_flete"),
            col("nro_documento").cast("string").alias("nro_documento"),
            col("documento_pago").cast("string").alias("documento_pago"),
            col("nro_serie_pago").cast("string").alias("nro_serie_pago"),
            col("nro_pago").cast("string").alias("nro_pago"),
            col("fecha_liquidacion").cast("date").alias("fecha_liquidacion"),
            col("tarifa_negociacion_mn").cast("numeric(38,12)").alias("tarifa_negociacion_mn"),
            col("tarifa_negociacion_me").cast("numeric(38,12)").alias("tarifa_negociacion_me"),
            col("tarifa_base_mn").cast("numeric(38,12)").alias("tarifa_base_mn"),
            col("tarifa_base_me").cast("numeric(38,12)").alias("tarifa_base_me"),
            col("total_peso").cast("numeric(38,12)").alias("total_peso"),
            col("fecha_confirmacion").cast("date").alias("fecha_confirmacion"),
            col("distancia_km").cast("numeric(38,12)").alias("distancia_km"),
            col("cant_dias").cast("numeric(38,12)").alias("cant_dias"),
            col("cant_tiempo").cast("numeric(38,12)").alias("cant_tiempo"),
            col("nro_comprobante").cast("string").alias("nro_comprobante"),
            col("cod_documento_transaccion").cast("string").alias("cod_documento_transaccion"),
            col("nro_documento_almacen").cast("string").alias("nro_documento_almacen"),
            col("documento_flete").cast("string").alias("documento_flete"),
            col("nro_flete").cast("string").alias("nro_flete"),
            col("cod_prioridad").cast("string").alias("cod_prioridad"),
            col("desc_efletex").cast("string").alias("desc_efletex"),
            col("cod_antencion_logistica").cast("string").alias("cod_antencion_logistica"),
            col("desc_estado_atencion").cast("string").alias("desc_estado_atencion"),
            col("cod_chofer_servicio").cast("string").alias("cod_chofer_servicio"),
            col("cod_tipo_gasto_adicional").cast("string").alias("cod_tipo_gasto_adicional"),
            col("desc_tipo_gasto_adicional").cast("string").alias("desc_tipo_gasto_adicional"),
            col("imp_gasto_adicional_mn").cast("numeric(38,12)").alias("imp_gasto_adicional_mn"),
            col("imp_gasto_adicional_me").cast("numeric(38,12)").alias("imp_gasto_adicional_me"),
            col("desc_tipo_tarifa").cast("string").alias("desc_tipo_tarifa"),
            col("cod_estado_comprobante").cast("string").alias("cod_estado_comprobante"),
            col("idtv").cast("string").alias("idtv"),
            col("cod_centro_costo").cast("string").alias("cod_centro_costo"),
            col("flag_pago").cast("integer").alias("flag_pago"),
            col("desc_estado_envio").cast("string").alias("desc_estado_envio"),
            col("cod_envio").cast("string").alias("cod_envio"),
            col("desc_tipo_vehiculo_servicio").cast("string").alias("desc_tipo_vehiculo_servicio"),
            col("desc_procedimiento").cast("string").alias("desc_procedimiento"),
            col("tarifa_distancia_km_mn").cast("numeric(38,12)").alias("tarifa_distancia_km_mn"),
            col("tarifa_distancia_km_me").cast("numeric(38,12)").alias("tarifa_distancia_km_me"),
            col("cod_moneda").cast("string").alias("cod_moneda"),
            col("incluye_impuesto").cast("integer").alias("incluye_impuesto"),
            col("cant_cajavolumen_principal").cast("numeric(38,12)").alias("cant_cajavolumen_principal"),
            col("cant_cajafisica_principal").cast("numeric(38,12)").alias("cant_cajafisica_principal"),
            col("cant_tarimas_principal").cast("numeric(38,12)").alias("cant_tarimas_principal"),
            col("cant_peso_principal").cast("numeric(38,12)").alias("cant_peso_principal"),
            col("cant_cajavolumen_asociada").cast("numeric(38,12)").alias("cant_cajavolumen_asociada"),
            col("cant_cajafisica_asociada").cast("numeric(38,12)").alias("cant_cajafisica_asociada"),
            col("cant_tarimas_asociada").cast("numeric(38,12)").alias("cant_tarimas_asociada"),
            col("cant_peso_asociada").cast("numeric(38,12)").alias("cant_peso_asociada"),
    )
    # logger.info("tmp_t_fletes types casted succesfully")

    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(tmp, data_paths.DOMINIO, target_table_name, partition_columns_array)


except Exception as e:
    logger.error(e)
    raise