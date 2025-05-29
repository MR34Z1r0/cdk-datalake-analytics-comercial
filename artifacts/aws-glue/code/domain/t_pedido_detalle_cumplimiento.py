from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat, concat_ws, lit, coalesce, when, date_format, round, trim, to_date, substring, lower, to_timestamp, row_number, max, sum
from pyspark.sql.window import Window


spark_controller = SPARK_CONTROLLER()
target_table_name = "t_pedido_detalle_cumplimiento"
try:
    PERIODOS= spark_controller.get_periods()
    cod_pais = COD_PAIS.split(",")
    logger.info(f"Databases: {cod_pais}")

    df_m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", cod_pais=cod_pais, have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania", cod_pais=cod_pais)
    df_i_relacion_proced_venta = spark_controller.read_table(data_paths.APDAYC, "i_relacion_proced_venta", cod_pais=cod_pais)
    df_m_procedimiento = spark_controller.read_table(data_paths.APDAYC, "m_procedimiento", cod_pais=cod_pais)
    df_m_articulo = spark_controller.read_table(data_paths.APDAYC, "m_articulo", cod_pais=cod_pais)
 
    df_t_historico_pedido = spark_controller.read_table(data_paths.APDAYC, "t_documento_pedido", cod_pais=cod_pais)
    df_t_historico_pedido_detalle = spark_controller.read_table(data_paths.APDAYC, "t_documento_pedido_detalle", cod_pais=cod_pais)
    df_t_historico_pedido_ades = spark_controller.read_table(data_paths.APDAYC, "t_documento_pedido_ades", cod_pais=cod_pais)
    df_t_historico_pedido_ades_detalle = spark_controller.read_table(data_paths.APDAYC, "t_documento_pedido_ades_detalle", cod_pais=cod_pais)
    df_t_historico_almacen = spark_controller.read_table(data_paths.APDAYC, "t_movimiento_inventario", cod_pais=cod_pais)
    df_t_historico_venta = spark_controller.read_table(data_paths.APDAYC, "t_documento_venta", cod_pais=cod_pais)
    df_t_historico_venta_detalle = spark_controller.read_table(data_paths.APDAYC, "t_documento_venta_detalle", cod_pais=cod_pais) 

    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(e)
    raise

try:
    logger.info("starting filter of pais and periodo") 
    df_t_historico_pedido = df_t_historico_pedido.filter(date_format(col("fecha_pedido"), "yyyyMM").isin(PERIODOS))
    df_t_historico_pedido_detalle = df_t_historico_pedido_detalle.filter(date_format(col("fecha_pedido"), "yyyyMM").isin(PERIODOS))
    df_t_historico_pedido_ades = df_t_historico_pedido_ades.filter(date_format(col("fecha_pedido"), "yyyyMM").isin(PERIODOS))
    df_t_historico_pedido_ades_detalle = df_t_historico_pedido_ades_detalle.filter(date_format(col("fecha_pedido"), "yyyyMM").isin(PERIODOS))

    logger.info("starting creation of df_m_compania")
    df_m_compania = (
        df_m_compania.alias("mc")
            .join(
                df_m_pais.alias("mp"),
                col("mp.cod_pais") == col("mc.cod_pais"),
            )
            .select(col("mc.cod_compania"), trim(col("mp.id_pais")).alias("id_pais"), trim(col("mc.cod_pais")).alias("cod_pais"))
    )

    logger.info("starting creation of df_i_relacion_proced_venta")
    df_i_relacion_proced_venta = (
        df_i_relacion_proced_venta.alias("irpv")
        .join(df_m_procedimiento.alias("mp"), (
            (col("irpv.cod_compania") == col("mp.cod_compania"))
            & (col("irpv.cod_documento_pedido") == col("mp.cod_documento_transaccion"))
            & (col("irpv.cod_procedimiento_pedido") == col("mp.cod_procedimiento"))
        ) , "inner")
        .select(
            col("irpv.cod_compania"),
            col("irpv.cod_documento_pedido"),
            col("irpv.cod_procedimiento_pedido"),
            col("irpv.cod_documento_venta"),
            col("irpv.cod_procedimiento_venta"),
            col("irpv.cod_operacion_venta"),
            col("irpv.cod_tipo_pedido"),
            col("mp.cod_tipo_operacion")
        )
    )

    logger.info("Starting creation of df_t_historico_almacen")
    window_spec = Window.partitionBy(
        "cod_compania",
        "cod_sucursal",
        "cod_almacen_emisor_origen",
        "cod_documento_transaccion",
        "nro_documento_almacen"
    ).orderBy(col("nro_documento_movimiento").desc())
    df_t_historico_almacen = df_t_historico_almacen.withColumn("orden", row_number().over(window_spec))

    logger.info("starting creation of df_t_historico_pedido_detalle_select")
    df_t_historico_pedido_detalle_select = (
        df_t_historico_pedido_detalle.alias("tdpd")
        .join(df_t_historico_pedido.alias("tdp"),
            (
                (col("tdpd.cod_compania") == col("tdp.cod_compania"))
                & (col("tdpd.cod_sucursal") == col("tdp.cod_sucursal"))
                & (col("tdpd.cod_almacen") == col("tdp.cod_almacen"))
                & (col("tdpd.cod_documento_pedido") == col("tdp.cod_documento_pedido"))
                & (col("tdpd.nro_documento_pedido") == col("tdp.nro_documento_pedido"))
            ), "inner")
        .join(df_m_compania.alias("mc"), col("tdpd.cod_compania") == col("mc.cod_compania"), "inner") 
        .where((col("tdpd.cod_documento_pedido") == lit('200')))
        .select(
            col("mc.id_pais"),
            date_format(col("tdp.fecha_pedido"), "yyyyMM").alias("id_periodo"),
            col("tdpd.cod_compania"),
            col("tdpd.cod_sucursal"),
            col("tdpd.cod_almacen"),
            col("tdpd.cod_documento_pedido"),
            col("tdpd.nro_documento_pedido"),
            col("tdp.cod_tipo_pedido"),
            coalesce(col("tdp.cod_documento_pedido_origen"), lit('000')).alias("cod_origen_pedido"),
            col("tdp.cod_vendedor"),
            lit(None).alias("cod_supervisor"),
            lit(None).alias("cod_jefe_venta"),
            col("tdpd.cod_lista_precio"),
            col("tdp.cod_condicion_pago").alias("cod_forma_pago"),
            lit(None).alias("desc_region"),
            lit(None).alias("desc_subregion"),
            lit(None).alias("desc_division"),
            col("tdpd.cod_zona"),
            col("tdpd.cod_ruta"),
            col("tdpd.cod_modulo"),
            #col("tdpd.id_regla"),
            col("tdp.cod_fuerza_venta"), 
            col("tdpd.cod_documento_almacen"), 
            col("tdpd.nro_documento_almacen"), 
            col("tdpd.cod_cliente"), 
            col("tdpd.id_salesforce").alias("nro_pedido_ref"), 
            col("tdpd.cod_articulo"), 
            col("tdpd.cod_procedimiento"), 
            col("tdp.fecha_pedido"), 
            col("tdp.fecha_entrega"), 
            col("tdpd.cant_paquete").alias("cantidad_cajas"),
            col("tdpd.cant_unidad").alias("cantidad_botellas"),
            col("tdpd.cant_paquete_asignado").alias("cantidad_cajas_asignada"),
            col("tdpd.cant_unidad_asignado").alias("cantidad_botellas_asignada"),
            col("tdpd.fecha_creacion"),
            col("tdpd.fecha_modificacion"),
        )
    ) 

    logger.info("starting creation of tmp_t_historico_pedido_ades_detalle")
    df_t_historico_pedido_ades_detalle_select = (
        df_t_historico_pedido_ades_detalle.alias("tdpad")
        .join(df_t_historico_pedido_ades.alias("tdpa"), 
            (
                (col("tdpad.cod_compania") == col("tdpa.cod_compania"))
                & (col("tdpad.cod_sucursal") == col("tdpa.cod_sucursal"))
                & (col("tdpad.cod_almacen_emisor") == col("tdpa.cod_almacen_emisor"))
                & (col("tdpad.cod_documento_transaccion") == col("tdpa.cod_documento_transaccion"))
                & (col("tdpad.nro_comprobante") == col("tdpa.nro_comprobante"))
            ), "inner")
        .join(df_m_compania.alias("mc"), col("tdpad.cod_compania") == col("mc.cod_compania"), "inner")
        .where((col("tdpad.cod_documento_transaccion").isin(['200','300'])))
        .select(
            col("mc.id_pais"),
            date_format(col("tdpa.fecha_pedido"), "yyyyMM").alias("id_periodo"),
            col("tdpad.cod_compania"),
            col("tdpad.cod_sucursal"),
            col("tdpad.cod_almacen_emisor").alias("cod_almacen"),
            col("tdpad.cod_documento_transaccion").alias("cod_documento_pedido"),
            col("tdpad.nro_comprobante").alias("nro_documento_pedido"),
            col("tdpa.cod_tipo_pedido"),
            coalesce(col("tdpa.cod_tipo_documento_origen"), lit('000')).alias("cod_origen_pedido"),
            col("tdpa.cod_vendedor"),
            lit(None).alias("cod_supervisor"),
            lit(None).alias("cod_jefe_venta"),
            col("tdpad.cod_lista_precios").alias("cod_lista_precio"),
            col("tdpa.cod_condicion_pago").alias("cod_forma_pago"),
            lit(None).alias("desc_region"),
            lit(None).alias("desc_subregion"),
            lit(None).alias("desc_division"),
            col("tdpad.cod_zona_distribucion").alias("cod_zona"),
            col("tdpad.cod_ruta_distribucion").alias("cod_ruta"),
            col("tdpad.cod_modulo"),
            #col("tdpad.idregla").alias("id_regla"),
            col("tdpa.cod_fuerza_venta"),
            col("tdpad.nro_documento_almacen"),
            col("tdpad.cod_tipo_documento_almacen").alias("cod_documento_almacen"),
            col("tdpad.cod_cliente"),
            col("tdpad.id_salesforce").alias("nro_pedido_ref"),
            col("tdpad.cod_articulo"),
            col("tdpad.cod_procedimiento"),
            col("tdpa.fecha_pedido"),
            col("tdpa.fecha_entrega"),
            col("tdpad.cantidad_cajas"),
            col("tdpad.cantidad_botellas"),
            col("tdpad.cantidad_cajas_asignada"),
            col("tdpad.cantidad_botellas_asignada"),
            col("tdpad.fecha_creacion"),
            col("tdpad.fecha_modificacion"),
        )
    )
    
    logger.info("Starting creation of df_t_historico_pedido_ades_detalle_left_anti")
    df_t_historico_pedido_ades_detalle_left_anti = (
        df_t_historico_pedido_ades_detalle_select.alias("a")
        .join(df_t_historico_pedido_detalle_select.alias("b"), (
                (col("a.cod_compania") == col("b.cod_compania"))
                & (col("a.cod_sucursal") == col("b.cod_sucursal"))
                & (col("a.cod_almacen") == col("b.cod_almacen"))
                & (col("a.cod_documento_pedido") == col("b.cod_documento_pedido"))
                & (col("a.nro_documento_pedido") == col("b.nro_documento_pedido"))
            ), "left_anti"
        )
    )

    logger.info("starting creation of df_t_historico_pedido_detalle_union")
    df_t_historico_pedido_detalle_union = df_t_historico_pedido_detalle_select.unionByName(df_t_historico_pedido_ades_detalle_left_anti)
 
    logger.info("Starting creation of df_t_historico_pedido_detalle_almacen")
    df_t_historico_pedido_detalle_almacen = (
        df_t_historico_pedido_detalle_union.alias("t")
        .join(df_t_historico_almacen.alias("vtha"), (
            (col("t.cod_compania") == col("vtha.cod_compania"))
            & (col("t.cod_sucursal") == col("vtha.cod_sucursal"))
            & (col("t.cod_almacen") == col("vtha.cod_almacen_emisor_origen"))
            & (col("t.cod_documento_almacen") == col("vtha.cod_documento_transaccion"))
            & (col("t.nro_documento_almacen") == col("vtha.nro_documento_almacen"))
            & (col("vtha.orden") == 1)
            & (col("vtha.cod_estado_comprobante").isin(["PLI", "LIQ"]))
            ), "left")
        .join(df_i_relacion_proced_venta.alias("irpv"), (
            (col("t.cod_compania") == col("irpv.cod_compania"))
            & (col("t.cod_documento_pedido") == col("irpv.cod_documento_pedido"))
            & (col("t.cod_tipo_pedido") == col("irpv.cod_tipo_pedido"))
            & (col("t.cod_procedimiento") == col("irpv.cod_procedimiento_pedido"))
        ) ,"left")
        .groupBy(
            col("t.id_pais"),
            col("t.cod_compania"),
            col("t.cod_sucursal"),
            col("t.cod_almacen"),
            col("t.cod_documento_pedido"),
            col("t.nro_documento_pedido"),
            col("t.cod_cliente"),
            col("t.cod_articulo"),
            col("t.cod_documento_almacen"),
            col("t.nro_documento_almacen"),
            col("irpv.cod_documento_venta"),
            col("irpv.cod_procedimiento_venta"),
            col("irpv.cod_operacion_venta")
        )
        .agg(
            max(col("t.id_periodo")).alias("id_periodo"),
            max(col("t.cod_procedimiento")).alias("cod_procedimiento"),
            max(col("t.cod_origen_pedido")).alias("cod_origen_pedido"),
            max(col("t.cod_tipo_pedido")).alias("cod_tipo_pedido"),
            max(col("t.cod_vendedor")).alias("cod_vendedor"),
            max(col("t.cod_supervisor")).alias("cod_supervisor"),
            max(col("t.cod_jefe_venta")).alias("cod_jefe_venta"),
            max(col("t.cod_lista_precio")).alias("cod_lista_precio"),
            max(col("t.cod_forma_pago")).alias("cod_forma_pago"),
            max(col("t.desc_region")).alias("desc_region"),
            max(col("t.desc_subregion")).alias("desc_subregion"),
            max(col("t.desc_division")).alias("desc_division"),
            max(col("t.cod_zona")).alias("cod_zona"),
            max(col("t.cod_ruta")).alias("cod_ruta"),
            max(col("t.cod_modulo")).alias("cod_modulo"),
            max(col("t.nro_pedido_ref")).alias("nro_pedido_ref"),
            max(col("t.cod_fuerza_venta")).alias("cod_fuerza_venta"),
            max(col("vtha.cod_transportista")).alias("cod_transportista"),
            max(col("vtha.cod_chofer")).alias("cod_chofer"),
            max(col("vtha.cod_vehiculo")).alias("cod_medio_transporte"),
            max(col("vtha.cod_estado_comprobante")).alias("estado_guia"),
            max(col("t.fecha_pedido")).alias("fecha_pedido"),
            max(col("t.fecha_entrega")).alias("fecha_entrega"),
            max(col("vtha.fecha_emision")).alias("fecha_orden_carga"),
            max(col("vtha.fecha_almacen")).alias("fecha_movimiento_inventario"),
            max(col("irpv.cod_tipo_operacion")).alias("cod_tipo_operacion"),
            max(col("irpv.cod_documento_venta")).alias("cod_documento_venta"),
            max(col("irpv.cod_procedimiento_venta")).alias("cod_procedimiento_venta"),
            max(col("irpv.cod_operacion_venta")).alias("cod_operacion_venta"),
            sum(col("t.cantidad_cajas")).alias("cantidad_cajas"),
            sum(col("t.cantidad_botellas")).alias("cantidad_botellas"),
            sum(col("t.cantidad_cajas_asignada")).alias("cantidad_cajas_asignada"),
            sum(col("t.cantidad_botellas_asignada")).alias("cantidad_botellas_asignada"),
            sum(
                when(col("vtha.cod_documento_transaccion").isNull(), 0)
                .otherwise(col("t.cantidad_cajas_asignada"))
            ).alias("cantidad_cajas_despechado"),
            sum(
                when(col("vtha.cod_documento_transaccion").isNull(), 0)
                .otherwise(col("t.cantidad_botellas_asignada"))
            ).alias("cantidad_botellas_despechado"),
            max(col("t.fecha_creacion")).alias("fecha_creacion"),
            max(col("t.fecha_modificacion")).alias("fecha_modificacion"),
        )
        .select(
            col("t.id_pais"),
            col("id_periodo"),
            col("t.cod_compania"),
            col("t.cod_sucursal"),
            col("t.cod_almacen"),
            col("t.cod_documento_pedido"),
            col("t.nro_documento_pedido"),
            col("t.cod_cliente"),
            col("t.cod_articulo"),
            col("t.cod_documento_almacen"),
            col("t.nro_documento_almacen"),
            col("cod_procedimiento"),
            col("cod_origen_pedido"),
            col("cod_tipo_pedido"),
            col("cod_vendedor"),
            col("cod_supervisor"),
            col("cod_jefe_venta"),
            col("cod_lista_precio"),
            col("cod_forma_pago"),
            col("desc_region"),
            col("desc_subregion"),
            col("desc_division"),
            col("cod_zona"),
            col("cod_ruta"),
            col("cod_modulo"),
            col("nro_pedido_ref"),
            col("cod_fuerza_venta"),
            col("cod_transportista"),
            col("cod_chofer"),
            col("cod_medio_transporte"),
            col("estado_guia"),
            col("fecha_pedido"),
            col("fecha_entrega"),
            col("fecha_orden_carga"),
            col("fecha_movimiento_inventario"),
            col("cod_tipo_operacion"),
            col("irpv.cod_documento_venta"),
            col("irpv.cod_procedimiento_venta"),
            col("irpv.cod_operacion_venta"),
            col("cantidad_cajas"),
            col("cantidad_botellas"),
            col("cantidad_cajas_asignada"),
            col("cantidad_botellas_asignada"),
            col("cantidad_cajas_despechado"),
            col("cantidad_botellas_despechado"),
            col("fecha_creacion"),
            col("fecha_modificacion"),
        )
    )
    
    logger.info("Starting creation of df_t_historico_pedido_detalle_almacen_unico")
    df_t_historico_pedido_detalle_almacen_unico = (
        df_t_historico_pedido_detalle_almacen
        .where(coalesce(col("cod_documento_almacen"), lit("")) != lit(""))
        .select(
            col("cod_compania"),
            col("cod_sucursal"),
            col("cod_almacen"),
            col("cod_documento_pedido"),
            col("nro_documento_pedido"),
            col("cod_documento_almacen"),
            col("nro_documento_almacen"),
            col("cod_documento_venta"),
            col("cod_procedimiento_venta"),
            col("cod_operacion_venta")
        )
        .distinct()
    )
    
    logger.info("Starting creation of df_t_historico_pedido_detalle_resumen_almacen_venta") 
    df_t_historico_pedido_detalle_resumen_almacen_venta = (
        df_t_historico_venta.alias("vthv") 
        .join(df_t_historico_venta_detalle.alias("vthvd"), (
            (col("vthv.cod_compania") == col("vthvd.cod_compania"))
            & (col("vthv.cod_sucursal") == col("vthvd.cod_sucursal"))
            & (col("vthv.cod_almacen") == col("vthvd.cod_almacen"))
            & (col("vthv.cod_documento_venta") == col("vthvd.cod_documento_transaccion"))
            & (col("vthv.nro_documento_venta") == col("vthvd.nro_comprobante_venta"))
        ), "inner")
        .join(df_t_historico_pedido_detalle_almacen_unico.alias("t"), (
            (col("vthv.cod_compania") == col("t.cod_compania"))
            & (col("vthv.cod_sucursal") == col("t.cod_sucursal"))
            & (col("vthv.cod_almacen") == col("t.cod_almacen"))
            & (col("vthv.cod_documento_pedido") == col("t.cod_documento_pedido"))
            & (col("vthv.nro_documento_pedido") == col("t.nro_documento_pedido"))
            & (col("vthv.cod_documento_almacen") == col("t.cod_documento_almacen"))
            & (col("vthv.nro_documento_almacen") == col("t.nro_documento_almacen"))
            & (col("vthv.cod_documento_venta") == col("t.cod_documento_venta"))
            & (col("vthv.cod_procedimiento") == col("t.cod_procedimiento_venta"))
            & (col("vthvd.cod_operacion") == col("t.cod_operacion_venta"))
        ), "inner")
        .where(~col("vthv.cod_documento_venta").isin(['RMD', 'CMD']) & (col("vthv.cod_estado_comprobante") != '002'))
        .groupBy(
            col("vthv.cod_compania"),
            col("vthv.cod_sucursal"),
            col("vthv.cod_almacen"),
            col("t.cod_documento_almacen"),
            col("t.nro_documento_almacen"),
            col("vthv.cod_documento_pedido"),
            col("vthv.nro_documento_pedido"),
            col("vthv.cod_cliente"),
            col("vthvd.cod_articulo"),
            col("vthv.cod_documento_venta"),
            col("vthv.cod_procedimiento"),
            col("vthvd.cod_operacion"),
        )
        .agg(
            max(col("vthv.fecha_liquidacion")).alias("fecha_liquidacion"),
            sum(
                ((when(col("vthv.cod_documento_venta") == lit('NCC'), lit(-1)).otherwise(lit(1)))
                * col("vthvd.cant_paquete"))
            ).alias("cantidad_cajas"),
            sum(
                ((when(col("vthv.cod_documento_venta") == lit('NCC'), lit(-1)).otherwise(lit(1)))
                * col("vthvd.cant_unidad"))
            ).alias("cantidad_botellas"),
        )
        .select(
            col("vthv.cod_compania"),
            col("vthv.cod_sucursal"),
            col("vthv.cod_almacen"),
            col("t.cod_documento_almacen"),
            col("t.nro_documento_almacen"),
            col("vthv.cod_documento_pedido"),
            col("vthv.nro_documento_pedido"),
            col("vthv.cod_cliente"),
            col("vthvd.cod_articulo"),
            col("vthv.cod_documento_venta"),
            col("vthv.cod_procedimiento"),
            col("vthvd.cod_operacion"),
            col("fecha_liquidacion"),
            col("cantidad_cajas"),
            col("cantidad_botellas"),
        )
    )

    logger.info("Starting creation of df_t_pedido_detalle_cumplimiento") 
    df_t_pedido_detalle_cumplimiento = (
        df_t_historico_pedido_detalle_almacen.alias("vthv")
        .join(df_t_historico_pedido_detalle_resumen_almacen_venta.alias("t"), (
            (col("vthv.cod_compania") == col("t.cod_compania"))
            & (col("vthv.cod_sucursal") == col("t.cod_sucursal"))
            & (col("vthv.cod_almacen") == col("t.cod_almacen"))
            & (col("vthv.cod_documento_pedido") == col("t.cod_documento_pedido"))
            & (col("vthv.nro_documento_pedido") == col("t.nro_documento_pedido"))
            & (col("vthv.cod_cliente") == col("t.cod_cliente"))
            & (col("vthv.cod_articulo") == col("t.cod_articulo"))
            & (col("vthv.cod_documento_almacen") == col("t.cod_documento_almacen"))
            & (col("vthv.nro_documento_almacen") == col("t.nro_documento_almacen"))
            & (col("vthv.cod_documento_venta") == col("t.cod_documento_venta"))
            & (col("vthv.cod_procedimiento_venta") == col("t.cod_procedimiento"))
            & (col("vthv.cod_operacion_venta") == col("t.cod_operacion"))
        ), "left")
        .join(df_m_articulo.alias("m"), (
            (col("vthv.cod_compania") == col("m.cod_compania"))
            & (col("vthv.cod_articulo") == col("m.cod_articulo"))
        ), "inner")
        .groupBy(
            col("vthv.cod_compania"),
            col("vthv.cod_sucursal"),
            col("vthv.cod_almacen"),
            col("vthv.cod_documento_pedido"),
            col("vthv.nro_documento_pedido"),
            col("vthv.cod_cliente"),
            col("vthv.cod_articulo"),
            coalesce(col("vthv.cod_documento_almacen"), lit("")).alias("cod_documento_almacen"),
            coalesce(col("vthv.nro_documento_almacen"), lit("")).alias("nro_documento_almacen"),
        )
        .agg(
            max(col("vthv.id_pais")).alias("id_pais"),
            max(col("vthv.id_periodo")).alias("id_periodo"),
            max(col("t.fecha_liquidacion")).alias("fecha_liquidacion"),
            sum(
                ((when(col("vthv.cod_tipo_operacion") == lit('Ven'), col("vthv.cantidad_cajas") + (col("vthv.cantidad_botellas") / col("m.cant_unidad_paquete")))).otherwise(lit(0))) 
            ).alias("cant_cajafisica_ped"),
            sum(
                ((when(col("vthv.cod_tipo_operacion") == lit('Ven'), (col("vthv.cantidad_cajas") * col("m.cant_unidad_paquete") + (col("vthv.cantidad_botellas")))* (col("m.cant_unidad_volumen")))).otherwise(lit(0))) 
            ).alias("cant_cajavolumen_ped"),
            sum(
                ((when(col("vthv.cod_tipo_operacion") == lit('Pro'), col("vthv.cantidad_cajas") + (col("vthv.cantidad_botellas") / col("m.cant_unidad_paquete")))).otherwise(lit(0))) 
            ).alias("cant_cajafisica_ped_pro"),
            sum(
                ((when(col("vthv.cod_tipo_operacion") == lit('Pro'), (col("vthv.cantidad_cajas") * col("m.cant_unidad_paquete") + (col("vthv.cantidad_botellas")))* (col("m.cant_unidad_volumen")))).otherwise(lit(0))) 
            ).alias("cant_cajavolumen_ped_pro"),
            sum(
                ((when(col("vthv.cod_tipo_operacion") == lit('Ven'), col("vthv.cantidad_cajas_asignada") + (col("vthv.cantidad_botellas_asignada") / col("m.cant_unidad_paquete")))).otherwise(lit(0))) 
            ).alias("cant_cajafisica_asignado_ped"),
            sum(
                ((when(col("vthv.cod_tipo_operacion") == lit('Ven'), (col("vthv.cantidad_cajas_asignada") * col("m.cant_unidad_paquete") + (col("vthv.cantidad_botellas_asignada")))* (col("m.cant_unidad_volumen")))).otherwise(lit(0))) 
            ).alias("cant_cajavolumen_asignado_ped"),
            sum(
                ((when(col("vthv.cod_tipo_operacion") == lit('Pro'), col("vthv.cantidad_cajas_asignada") + (col("vthv.cantidad_botellas_asignada") / col("m.cant_unidad_paquete")))).otherwise(lit(0))) 
            ).alias("cant_cajafisica_asignado_ped_pro"),
            sum(
                ((when(col("vthv.cod_tipo_operacion") == lit('Pro'), (col("vthv.cantidad_cajas_asignada") * col("m.cant_unidad_paquete") + (col("vthv.cantidad_botellas_asignada")))* (col("m.cant_unidad_volumen")))).otherwise(lit(0))) 
            ).alias("cant_cajavolumen_asignado_ped_pro"),
            sum(
                ((when(col("vthv.cod_tipo_operacion") == lit('Ven'), col("vthv.cantidad_cajas_despechado") + (col("vthv.cantidad_botellas_despechado") / col("m.cant_unidad_paquete")))).otherwise(lit(0))) 
            ).alias("cant_cajafisica_desp"),
            sum(
                ((when(col("vthv.cod_tipo_operacion") == lit('Ven'), (col("vthv.cantidad_cajas_despechado") * col("m.cant_unidad_paquete") + (col("vthv.cantidad_botellas_despechado")))* (col("m.cant_unidad_volumen")))).otherwise(lit(0))) 
            ).alias("cant_cajavolumen_desp"),
            sum(
                ((when(col("vthv.cod_tipo_operacion") == lit('Pro'), col("vthv.cantidad_cajas_despechado") + (col("vthv.cantidad_botellas_despechado") / col("m.cant_unidad_paquete")))).otherwise(lit(0))) 
            ).alias("cant_cajafisica_desp_pro"),
            sum(
                ((when(col("vthv.cod_tipo_operacion") == lit('Pro'), (col("vthv.cantidad_cajas_despechado") * col("m.cant_unidad_paquete") + (col("vthv.cantidad_botellas_despechado")))* (col("m.cant_unidad_volumen")))).otherwise(lit(0))) 
            ).alias("cant_cajavolumen_desp_pro"),            
            sum(
                ((when(col("vthv.cod_tipo_operacion") == lit('Ven'), col("t.cantidad_cajas") + (col("t.cantidad_botellas") / col("m.cant_unidad_paquete")))).otherwise(lit(0))) 
            ).alias("cant_caja_fisica_ven"),
            sum(
                ((when(col("vthv.cod_tipo_operacion") == lit('Ven'), (col("t.cantidad_cajas") * col("m.cant_unidad_paquete") + (col("t.cantidad_botellas")))* (col("m.cant_unidad_volumen")))).otherwise(lit(0))) 
            ).alias("cant_caja_volumen_ven"),
            sum(
                ((when(col("vthv.cod_tipo_operacion") == lit('Pro'), col("t.cantidad_cajas") + (col("t.cantidad_botellas") / col("m.cant_unidad_paquete")))).otherwise(lit(0))) 
            ).alias("cant_caja_fisica_pro"),
            sum(
                ((when(col("vthv.cod_tipo_operacion") == lit('Pro'), (col("t.cantidad_cajas") * col("m.cant_unidad_paquete") + (col("t.cantidad_botellas")))* (col("m.cant_unidad_volumen")))).otherwise(lit(0))) 
            ).alias("cant_caja_volumen_pro"),
            max(col("vthv.fecha_creacion")).alias("fecha_creacion"),
            max(col("vthv.fecha_modificacion")).alias("fecha_modificacion"),
        )
        .select(
            col("id_pais"),
            col("id_periodo"),
            concat_ws("|",col("vthv.cod_compania"),col("vthv.cod_sucursal")).alias("id_sucursal"),
            concat(col("vthv.cod_compania"), lit("|"), col("vthv.cod_sucursal"), lit("|"), col("vthv.cod_almacen"), lit("|"), col("vthv.cod_documento_pedido"), lit("|"), col("vthv.nro_documento_pedido"), lit("|"), col("vthv.cod_cliente")).alias("id_pedido"),
            concat(col("vthv.cod_compania"), lit("|"), col("vthv.cod_articulo")).alias("id_producto"),
            when(coalesce(col("cod_documento_almacen"), lit("")) != lit(""), concat(col("vthv.cod_compania"), lit("|"), col("vthv.cod_sucursal"), lit("|"), col("vthv.cod_almacen"), lit("|"), col("cod_documento_almacen"), lit("|"), col("nro_documento_almacen"))).otherwise(lit(None)).alias("id_reparto"),
            col("fecha_liquidacion").cast("date"),
            col("cant_cajafisica_ped").cast("numeric(38,12)"),
            col("cant_cajavolumen_ped").cast("numeric(38,12)"),
            col("cant_cajafisica_ped_pro").cast("numeric(38,12)"),
            col("cant_cajavolumen_ped_pro").cast("numeric(38,12)"),
            col("cant_cajafisica_asignado_ped").cast("numeric(38,12)"),
            col("cant_cajavolumen_asignado_ped").cast("numeric(38,12)"),
            col("cant_cajafisica_asignado_ped_pro").cast("numeric(38,12)"),
            col("cant_cajavolumen_asignado_ped_pro").cast("numeric(38,12)"),
            col("cant_cajafisica_desp").cast("numeric(38,12)"),
            col("cant_cajavolumen_desp").cast("numeric(38,12)"),
            col("cant_cajafisica_desp_pro").cast("numeric(38,12)"),
            col("cant_cajavolumen_desp_pro").cast("numeric(38,12)"),
            col("cant_caja_fisica_ven").cast("numeric(38,12)"),
            col("cant_caja_volumen_ven").cast("numeric(38,12)"),
            col("cant_caja_fisica_pro").cast("numeric(38,12)"),
            col("cant_caja_volumen_pro").cast("numeric(38,12)"),
            col("fecha_creacion").cast("timestamp"),
            col("fecha_modificacion").cast("timestamp"),
        )
    ) 

    logger.info(f"starting write of {target_table_name}")
    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(df_t_pedido_detalle_cumplimiento, data_paths.DOMAIN, target_table_name, partition_columns_array)
except Exception as e:
    logger.error(e)
    raise 