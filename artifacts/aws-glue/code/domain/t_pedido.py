from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, concat, concat_ws, lit, coalesce, when, date_format, round, trim, to_date, substring, lower, to_timestamp, row_number, max, sum
from pyspark.sql.window import Window
spark_controller = SPARK_CONTROLLER()
target_table_name = "t_pedido"
try:
    PERIODOS= spark_controller.get_periods()

    df_m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania")
    df_m_parametro = spark_controller.read_table(data_paths.APDAYC, "m_parametro")
    df_m_tipo_cambio = spark_controller.read_table(data_paths.APDAYC, "m_tipo_cambio")
    df_m_region = spark_controller.read_table(data_paths.APDAYC, "m_region", have_principal = True)
    df_m_subregion = spark_controller.read_table(data_paths.APDAYC, "m_subregion", have_principal = True)
    df_m_centro_distribucion = spark_controller.read_table(data_paths.APDAYC, "m_division")
    df_m_zona_distribucion = spark_controller.read_table(data_paths.APDAYC, "m_zona")

    df_t_historico_pedido = spark_controller.read_table(data_paths.APDAYC, "t_documento_pedido")
    df_t_historico_pedido_ades_cabecera = spark_controller.read_table(data_paths.APDAYC, "t_documento_pedido_ades")    
    df_t_historico_pedido_detalle = spark_controller.read_table(data_paths.APDAYC, "t_documento_pedido_detalle")
    df_t_historico_pedido_ades_detalle = spark_controller.read_table(data_paths.APDAYC, "t_documento_pedido_ades_detalle")
    
    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("starting filter of pais and periodo") 
    df_t_historico_pedido = df_t_historico_pedido.filter((date_format(col("fecha_pedido"), "yyyyMM").isin(PERIODOS)))
    df_t_historico_pedido_ades_cabecera = df_t_historico_pedido_ades_cabecera.filter((date_format(col("fecha_pedido"), "yyyyMM").isin(PERIODOS)))
    df_t_historico_pedido_ades_cabecera = df_t_historico_pedido_ades_cabecera.filter((date_format(col("fecha_pedido"), "yyyyMM").isin(PERIODOS)))
    df_t_historico_pedido_detalle = df_t_historico_pedido_detalle.filter((date_format(col("fecha_pedido"), "yyyyMM").isin(PERIODOS)))
    df_t_historico_pedido_ades_detalle = df_t_historico_pedido_ades_detalle.filter((date_format(col("fecha_pedido"), "yyyyMM").isin(PERIODOS)))

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

    logger.info("starting aplying filters and selecting columns")

    logger.info("starting creation of df_t_historico_pedido_filter")
    df_t_historico_pedido_filter = (
        df_t_historico_pedido.alias("tp").filter((col("cod_documento_pedido") == "200"))
        .join(
            df_m_compania.alias("mc"), 
            (col("tp.cod_compania") == col("mc.cod_compania")),
            "inner"
        )
        .join(
            df_m_zona_distribucion.alias("mzo"),
            (col("mzo.cod_compania") == col("tp.cod_compania"))
            & (col("mzo.cod_sucursal") == col("tp.cod_sucursal"))
            & (col("mzo.cod_zona") == col("tp.cod_zona_distribucion")),
            "left"
        )
        .join(
            df_m_centro_distribucion.alias("mcd"),
            (col("mcd.cod_division") == col("tp.cod_centro_distribucion"))
            & (col("mcd.cod_compania") == col("tp.cod_compania")),
            "left",
        )
        .join(
            df_m_region.alias("mr"),
            (col("mr.cod_pais") == col("mc.cod_pais"))
            & (col("mr.cod_region") == col("mzo.cod_region")),
            "left",
        )
        .join(
            df_m_subregion.alias("msr"),
            (col("msr.cod_pais") == col("mc.cod_pais"))
            & (col("msr.cod_region") == col("mzo.cod_region"))
            & (col("msr.cod_subregion") == col("mzo.cod_subregion")),
            "left",
        )
        .join(
            df_m_tipo_cambio.alias("mtc"),
            (col("mtc.fecha") == col("tp.fecha_pedido"))
            & (col("mtc.cod_compania") == col("mc.cod_compania"))
            & (col("mtc.cod_moneda") == col("mc.moneda_mn")),
            "left",
        )
        .select(
            col("mc.id_pais"),
            date_format(col("fecha_pedido"), "yyyyMM").alias("id_periodo"),
            col("tp.cod_compania").alias("id_compania"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_sucursal")).alias("id_sucursal"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_sucursal"), col("tp.cod_almacen")).alias("id_almacen"),  
            col("tp.cod_documento_pedido").alias("cod_tipo_documento_pedido"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_sucursal"), col("tp.cod_documento_pedido"), col("nro_documento_pedido")).alias("id_documento_pedido"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_documento_pedido_origen")).alias("id_origen_pedido"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_tipo_pedido")).alias("id_tipo_pedido"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_sucursal"), col("tp.cod_fuerza_venta")).alias("id_fuerza_venta"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_vendedor")).alias("id_vendedor"),
            lit(None).alias("id_supervisor"),
            lit(None).alias("id_jefe_venta"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_condicion_pago")).alias("id_forma_pago"),
            coalesce(col("mr.desc_region"), lit("REGION DEFAULT")).alias("desc_region"),
            coalesce(col("msr.desc_subregion"), lit("SUBREGION DEFAULT")).alias("desc_subregion"),
            col("mcd.desc_division"),
            col("tp.cod_centro_distribucion").alias("cod_division"),
            col("tp.cod_zona_distribucion").alias("cod_zona"),
            col("tp.fecha_entrega"),
            col("tp.fecha_pedido"),
            col("tp.fecha_pedido").alias("fecha_visita"),
            col("tp.fecha_creacion"),
            col("tp.fecha_modificacion"), 
            lit(0).alias("es_eliminado"),
            when(col("mtc.cod_moneda") == col("mc.moneda_mn"), 1).otherwise(col('mtc.tc_venta')).alias("tipo_cambio_mn"),
            when((col("mtc.cod_moneda") == 'DOL') | (col("mtc.cod_moneda") == 'USD'), 1).otherwise(col('mtc.tc_venta')).alias("tipo_cambio_me"))
    )

    logger.info("starting creation of df_t_historico_pedido_ades_cabecera_filter")
    df_t_historico_pedido_ades_cabecera_filter = (
        df_t_historico_pedido_ades_cabecera.alias("tp").filter((col("tp.cod_documento_transaccion").isin(["200", "300"])))
        .join(
            df_m_compania.alias("mc"), 
            (col("tp.cod_compania") == col("mc.cod_compania")),
            "inner"
        )
        .join(
            df_m_zona_distribucion.alias("mzo"),
            (col("mzo.cod_compania") == col("tp.cod_compania"))
            & (col("mzo.cod_sucursal") == col("tp.cod_sucursal"))
            & (col("mzo.cod_zona") == col("tp.cod_zona_distribucion")),
            "left"
        )
        .join(
            df_m_centro_distribucion.alias("mcd"),
            (col("mcd.cod_division") == col("tp.cod_centro_distribucion"))
            & (col("mcd.cod_compania") == col("tp.cod_compania")),
            "left",
        )
        .join(
            df_m_region.alias("mr"),
            (col("mr.cod_pais") == col("mc.cod_pais"))
            & (col("mr.cod_region") == col("mzo.cod_region")),
            "left",
        )
        .join(
            df_m_subregion.alias("msr"),
            (col("msr.cod_pais") == col("mc.cod_pais"))
            & (col("msr.cod_region") == col("mzo.cod_region"))
            & (col("msr.cod_subregion") == col("mzo.cod_subregion")),
            "left",
        )
        .join(
            df_m_tipo_cambio.alias("mtc"),
            (col("mtc.fecha") == col("tp.fecha_pedido"))
            & (col("mtc.cod_compania") == col("mc.cod_compania"))
            & (col("mtc.cod_moneda") == col("mc.moneda_mn")),
            "left",
        )
        .select(
            col("mc.id_pais"),
            date_format(col("fecha_pedido"), "yyyyMM").alias("id_periodo"),
            col("tp.cod_compania").alias("id_compania"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_sucursal")).alias("id_sucursal"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_sucursal"), col("tp.cod_almacen_emisor")).alias("id_almacen"),  
            col("tp.cod_documento_transaccion").alias("cod_tipo_documento_pedido"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_sucursal"), col("tp.cod_documento_transaccion"), col("tp.nro_comprobante")).alias("id_documento_pedido"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_tipo_documento_origen")).alias("id_origen_pedido"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_tipo_pedido")).alias("id_tipo_pedido"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_sucursal"), col("cod_fuerza_venta")).alias("id_fuerza_venta"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_vendedor")).alias("id_vendedor"),
            lit(None).alias("id_supervisor"),
            lit(None).alias("id_jefe_venta"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_condicion_pago")).alias("id_forma_pago"),
            coalesce(col("mr.desc_region"), lit("REGION DEFAULT")).alias("desc_region"),
            coalesce(col("msr.desc_subregion"), lit("SUBREGION DEFAULT")).alias("desc_subregion"),
            col("mcd.desc_division"),
            col("tp.cod_centro_distribucion").alias("cod_division"),
            col("tp.cod_zona_distribucion").alias("cod_zona"),
            col("tp.fecha_entrega"),
            col("tp.fecha_pedido"),
            col("tp.fecha_pedido").alias("fecha_visita"),
            col("tp.fecha_creacion"),
            col("tp.fecha_modificacion"), 
            lit(0).alias("es_eliminado"),
            when(col("mtc.cod_moneda") == col("mc.moneda_mn"), 1).otherwise(col('mtc.tc_venta')).alias("tipo_cambio_mn"),
            when((col("mtc.cod_moneda") == 'DOL') | (col("mtc.cod_moneda") == 'USD'), 1).otherwise(col('mtc.tc_venta')).alias("tipo_cambio_me"))
    )

    logger.info("starting creation of df_t_historico_pedido_detalle_filter")
    df_t_historico_pedido_detalle_filter = (
        df_t_historico_pedido_detalle.filter((col("cod_documento_pedido") == "200"))
        .select(
            concat_ws("|", col("cod_compania"), col("cod_sucursal"), col("cod_documento_pedido"), col("nro_documento_pedido")).alias("id_documento_pedido"),
            concat_ws("|", col("cod_compania"), col("cod_sucursal"), col("cod_documento_pedido"), col("nro_documento_pedido"), col("cod_cliente")).alias("id_pedido"),
            concat_ws("|", col("cod_compania"), col("cod_cliente")).alias("id_cliente"),
            concat_ws("|", col("cod_compania"), col("cod_lista_precio")).alias("id_lista_precio"),
            lit(None).alias("id_pedido_ref"),
            col("cod_ruta"),
            col("cod_modulo"),
            concat_ws("|", col("nro_documento_pedido"), col("cod_cliente")).alias("nro_pedido"),
        )
        .groupby(
            col("id_documento_pedido"),
            col("id_cliente"),
        )
        .agg(
            max(col("nro_pedido")).alias("nro_pedido"),
            max(col("cod_ruta")).alias("cod_ruta"),
            max(col("cod_modulo")).alias("cod_modulo"),
            max(col("id_lista_precio")).alias("id_lista_precio"),
            max(col("id_pedido")).alias("id_pedido"),
            max(col("id_pedido_ref")).alias("id_pedido_ref"),
        )
        .select(
            col("id_documento_pedido"),
            col("id_pedido"),
            col("id_cliente"),
            col("nro_pedido"),
            col("cod_ruta"),
            col("cod_modulo"),
            col("id_lista_precio"),
            col("id_pedido_ref"),
        )
    )

    logger.info("starting creation of df_t_historico_pedido_ades_detalle_filter")
    df_t_historico_pedido_ades_detalle_filter = (
        df_t_historico_pedido_ades_detalle.filter((col("cod_documento_transaccion").isin(["200", "300"])))
        .select(
            concat_ws("|", col("cod_compania"), col("cod_sucursal"), col("cod_documento_transaccion"), col("nro_comprobante")).alias("id_documento_pedido"),
            concat_ws("|", col("cod_compania"), col("cod_sucursal"), col("cod_documento_transaccion"), col("nro_comprobante"), col("cod_cliente")).alias("id_pedido"),
            concat_ws("|", col("cod_compania"), col("cod_cliente")).alias("id_cliente"),
            concat_ws("|", col("cod_compania"), col("cod_lista_precios")).alias("id_lista_precio"),
            lit(None).alias("id_pedido_ref"),
            col("cod_ruta_distribucion").alias("cod_ruta"),
            col("cod_modulo"),
            concat_ws("|", col("nro_comprobante"), col("cod_cliente")).alias("nro_pedido"),
        )
        .groupby(
            col("id_documento_pedido"),
            col("id_cliente"),
        )
        .agg(
            max(col("nro_pedido")).alias("nro_pedido"),
            max(col("cod_ruta")).alias("cod_ruta"),
            max(col("cod_modulo")).alias("cod_modulo"),
            max(col("id_lista_precio")).alias("id_lista_precio"),
            max(col("id_pedido")).alias("id_pedido"),
            max(col("id_pedido_ref")).alias("id_pedido_ref"),
        )
        .select(
            col("id_documento_pedido"),
            col("id_pedido"),
            col("id_cliente"),
            col("nro_pedido"),
            col("cod_ruta"),
            col("cod_modulo"),
            col("id_lista_precio"),
            col("id_pedido_ref"),
        )
    )
    
    logger.info("starting creation of df_t_historico_pedido_cliente")
    df_t_historico_pedido_cliente = (
        df_t_historico_pedido_filter.alias("tp")
        .join(df_t_historico_pedido_detalle_filter.alias("tpd"),
            (col("tpd.id_documento_pedido") == col("tp.id_documento_pedido")),
            "inner"
        )
        .select( 
            col("tp.id_pais"),
            col("tp.id_periodo"),
            col("tpd.id_pedido"),
            col("tp.id_compania"),
            col("tp.id_sucursal"), 
            col("tp.cod_tipo_documento_pedido"),
            col("tp.id_origen_pedido"),
            col("tp.id_tipo_pedido"),
            col("tp.id_fuerza_venta"),
            col("tp.id_vendedor"),
            col("tp.id_supervisor"),
            col("tp.id_jefe_venta"),
            col("tp.id_forma_pago"),
            col("tp.desc_region"),
            col("tp.desc_subregion"),
            col("tp.desc_division"),
            col("tp.cod_zona"),
            col("tpd.cod_ruta"),
            col("tpd.cod_modulo"),
            col("tp.fecha_pedido"),
            col("tp.fecha_entrega"),
            col("tp.fecha_visita"),
            col("tp.tipo_cambio_mn"),
            col("tp.tipo_cambio_me"),
            col("tp.fecha_creacion"),
            col("tp.fecha_modificacion"),
            col("tp.es_eliminado"), 
            col("tpd.id_lista_precio"),
            col("tpd.id_pedido_ref"), 
            col("tpd.id_cliente"),
            col("tpd.nro_pedido"),
        )
    )

    logger.info("starting creation of df_t_historico_pedido_ades_cliente")
    df_t_historico_pedido_ades_cliente = (
        df_t_historico_pedido_ades_cabecera_filter.alias("tp")
        .join(df_t_historico_pedido_ades_detalle_filter.alias("tpd"),
            (col("tpd.id_documento_pedido") == col("tp.id_documento_pedido")),
            "inner"
        )
        .select( 
            col("tp.id_pais"),
            col("tp.id_periodo"),
            col("tpd.id_pedido"),
            col("tp.id_compania"),
            col("tp.id_sucursal"), 
            col("tp.cod_tipo_documento_pedido"),
            col("tp.id_origen_pedido"),
            col("tp.id_tipo_pedido"),
            col("tp.id_fuerza_venta"),
            col("tp.id_vendedor"),
            col("tp.id_supervisor"),
            col("tp.id_jefe_venta"),
            col("tp.id_forma_pago"),
            col("tp.desc_region"),
            col("tp.desc_subregion"),
            col("tp.desc_division"),
            col("tp.cod_zona"),
            col("tpd.cod_ruta"),
            col("tpd.cod_modulo"),
            col("tp.fecha_pedido"),
            col("tp.fecha_entrega"),
            col("tp.fecha_visita"),
            col("tp.tipo_cambio_mn"),
            col("tp.tipo_cambio_me"),
            col("tp.fecha_creacion"),
            col("tp.fecha_modificacion"),
            col("tp.es_eliminado"),  
            col("tpd.id_lista_precio"),
            col("tpd.id_pedido_ref"), 
            col("tpd.id_cliente"),
            col("tpd.nro_pedido"),
        )
    )
 
    logger.info("Starting creation of df_t_historico_pedido_ades_cliente_left_anti")
    df_t_historico_pedido_ades_cliente_left_anti = (
        df_t_historico_pedido_ades_cliente.alias("a")
        .join(df_t_historico_pedido_cliente.alias("b"), (
                (col("a.id_pedido") == col("b.id_pedido"))
            ), "left_anti"
        )
    )

    logger.info("starting creation of df_t_historico_pedido_cliente_union")
    df_t_historico_pedido_cliente_union = df_t_historico_pedido_cliente.unionByName(df_t_historico_pedido_ades_cliente_left_anti)
 
    logger.info("starting creation of df_t_pedido")
    df_dom_t_pedido = (
        df_t_historico_pedido_cliente_union.alias("tp")
        .select(
            col("tp.id_pais").cast("string"),
            col("tp.id_periodo").cast("string"),
            col("tp.id_pedido").cast("string"),
            col("tp.id_pedido_ref").cast("string"),
            col("tp.id_compania").cast("string"),
            col("tp.id_sucursal").cast("string"), 
            col("tp.cod_tipo_documento_pedido").cast("string"),
            lit(None).alias("id_visita").cast("string"),
            col("tp.id_cliente").cast("string"),
            lit(None).alias("id_modelo_atencion").cast("string"),
            col("tp.id_origen_pedido").cast("string"),
            col("tp.id_tipo_pedido").cast("string"),
            col("tp.id_fuerza_venta").cast("string"),
            col("tp.id_vendedor").cast("string"),
            col("tp.id_supervisor").cast("string"),
            col("tp.id_jefe_venta").cast("string"),
            col("tp.id_lista_precio").cast("string"),
            col("tp.id_forma_pago").cast("string"),
            col("tp.desc_region").cast("string"),
            col("tp.desc_subregion").cast("string"),
            col("tp.desc_division").cast("string"),
            col("tp.cod_zona").cast("string"),
            col("tp.cod_ruta").cast("string"),
            col("tp.cod_modulo").cast("string"),
            col("tp.nro_pedido").cast("string"),
            lit(None).alias("nro_pedido_ref").cast("string"),
            lit(None).alias("cod_tipo_atencion").cast("string"),
            col("tp.fecha_pedido").cast("date"),
            col("tp.fecha_entrega").cast("date"),
            col("tp.fecha_visita").cast("date"),
            col("tp.tipo_cambio_mn").cast("numeric(38,12)"),
            col("tp.tipo_cambio_me").cast("numeric(38,12)"),
            col("tp.fecha_creacion").cast("timestamp"),
            col("tp.fecha_modificacion").cast("timestamp"),
            col("tp.es_eliminado").cast("int"),
        )
    ) 
     
    partition_columns_array = ["id_pais", "id_periodo"]
    logger.info(f"starting write of {target_table_name}")
    spark_controller.write_table(df_dom_t_pedido, data_paths.DOMAIN, target_table_name, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} completado exitosamente")
except Exception as e:
    logger.error(f"Error processing df_dom_t_pedido: {e}")
    raise ValueError(f"Error processing df_dom_t_pedido: {e}")