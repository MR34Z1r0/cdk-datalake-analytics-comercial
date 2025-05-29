from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat_ws, lit, coalesce, when ,date_format, round

spark_controller = SPARK_CONTROLLER()
try:

    PERIODOS= spark_controller.get_periods()

    cod_pais = COD_PAIS.split(",")
    logger.info(f"Databases: {cod_pais}")

    # Load Stage
    df_m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania", cod_pais=cod_pais)
    df_m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", cod_pais=cod_pais,have_principal = True)
    df_m_tipo_cambio = spark_controller.read_table(data_paths.APDAYC, "m_tipo_cambio", cod_pais=cod_pais)
    df_m_moneda = spark_controller.read_table(data_paths.APDAYC, "m_moneda", cod_pais=cod_pais)
    df_t_orden_compra_cabecera = spark_controller.read_table(data_paths.APDAYC, "t_orden_compra_cabecera", cod_pais=cod_pais)

    target_table_name = "t_orden_compra_cabecera"

    logger.info("Dataframes cargados correctamente")

except Exception as e:
    logger.error(e)
    raise

try:
    m_pais = df_m_pais.filter(col("id_pais").isin(cod_pais))
    df_m_tipo_cambio = df_m_tipo_cambio.filter((col("fecha").isNotNull()) & (col("tc_compra") > 0) & (col("tc_venta") > 0))
    
    df_t_orden_compra_cabecera = df_t_orden_compra_cabecera.filter(date_format(col("fecha_emision"), "yyyyMM").isin(PERIODOS))

    logger.info("Starting creation of df_moneda")
    df_moneda = (
        df_m_moneda.alias("mm")
        .where(
            (col("mm.estado") == "A")
            & (col("mm.flg_procedencia") == "L")
        )
        .select(
            col("mm.cod_moneda").alias("cod_moneda"),
            col("mm.cod_compania").alias("cod_compania")
        )
    )

    logger.info("Starting creation of tmp_dominio_t_orden_compra_cabecera")        
    tmp_dominio_t_orden_compra_cabecera = (
        df_t_orden_compra_cabecera.alias("tocc")
        .join(df_m_compania.alias("mc"), col("tocc.cod_compania") == col("mc.cod_compania"), "inner")
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .join(df_m_tipo_cambio.alias("mtc"), 
            (col("mtc.cod_compania") == col("tocc.cod_compania")) 
            & (col("mtc.cod_moneda") == col("tocc.cod_moneda")) 
            & (col("mtc.fecha") == col("tocc.fecha_emision"))
            , "left"
        )
        .join(df_moneda.alias("mm"), (col("mm.cod_compania") == col("tocc.cod_compania")) & (col("mm.cod_moneda") == col("tocc.cod_moneda")), "left")
        .select(
            col("mp.id_pais").alias("id_pais"),
            date_format(col("tocc.fecha_emision"), "yyyyMM").alias("id_periodo"),
            concat_ws("|",
                coalesce(col("tocc.cod_compania"), lit("")),
                coalesce(col("tocc.cod_sucursal"), lit("")),
                coalesce(col("tocc.cod_transaccion"), lit("")),
                coalesce(col("tocc.nro_orden_compra"), lit("")),

            ).alias("id_orden_compra"),
            col("tocc.cod_compania").alias("id_compania"),
            concat_ws("|",
                coalesce(col("tocc.cod_compania"), lit("")),
                coalesce(col("tocc.cod_sucursal"), lit(""))  
            ).alias("id_sucursal"),
            concat_ws("|",
                coalesce(col("tocc.cod_compania"), lit("")),
                coalesce(col("tocc.cod_proveedor"), lit(""))
            ).alias("id_proveedor"),
            concat_ws("|",
                coalesce(col("tocc.cod_compania"), lit("")),
                coalesce(col("tocc.cod_almacen_emisor"), lit(""))
            ).alias("id_almacen"),
            concat_ws("|",
                coalesce(col("tocc.cod_compania"), lit("")),
                coalesce(col("tocc.cod_empleado_aprobado"), lit(""))
            ).alias("id_empleado_aprobado"),
            concat_ws("|",
                coalesce(col("tocc.cod_compania"), lit("")),
                coalesce(col("tocc.cod_empleado_preparado"), lit(""))
            ).alias("id_empleado_preparado"),
            concat_ws("|",
                coalesce(col("tocc.cod_compania"), lit("")),
                coalesce(col("tocc.cod_centro_costo"), lit(""))
            ).alias("id_centro_costo"),
            concat_ws("|",
                coalesce(col("tocc.cod_compania"), lit("")),
                coalesce(col("tocc.cod_condicion_pago"), lit(""))
            ).alias("id_forma_pago"),
            col("tocc.fecha_emision"),
            col("tocc.fecha_atencion"),
            col("tocc.fecha_aprobacion"),
            col("tocc.fecha_entrega"),
            col("tocc.cod_transaccion").alias("cod_tipo_documento"),
            col("tocc.nro_orden_compra"),
            when(col("tocc.flg_tipo_orden") == 'L', "LOCAL")
            .when(col("tocc.flg_tipo_orden") == 'S', "SERVICIOS")
            .when(col("tocc.flg_tipo_orden") == 'I', "IMPORTACION")
            .when(col("tocc.flg_tipo_orden") == 'O', "OC MAQUILA")
            .when(col("tocc.flg_tipo_orden") == 'A', "A CONSIGNACION")
            .otherwise("NINGUNA").alias("desc_tipo_orden"),
            when(col("tocc.flg_atendido") == 'T', 1).otherwise(0).alias("es_atendido"),
            when(col("tocc.cod_empleado_aprobado") == '0', 0).otherwise(1).alias("es_aprobado"),
            when(col("tocc.flg_impuesto_incluido") == 'T', 1).otherwise(0).alias("tiene_impuesto_incluido"),
            when(col("tocc.flg_factura") == 'T', 1).otherwise(0).alias("tiene_factura"),
            when(col("tocc.flg_anulado") == 'T', 1).otherwise(0).alias("es_anulado"),
            col("tocc.estado_oc").alias("desc_estado_orden_compra"),
            col("tocc.neto").alias("imp_neto_mo"),
            when(col("mtc.tc_venta") == 0, 0)
            .otherwise(round(col("tocc.neto") * col("mtc.tc_venta"), 2))
            .alias("imp_neto_mn"),
            when(col("mtc.tc_venta") == 0, 0)
            .otherwise(round(col("tocc.neto") / col("mtc.tc_venta"), 2))
            .alias("imp_neto_me"),
            col("tocc.cod_moneda").alias("cod_moneda_mo"),
            col("mm.cod_moneda").alias("cod_moneda_mn"),
            col("tocc.tipo_cambio_me"),
            col("tocc.tipo_cambio_mn"),
            col("tocc.usuario_creacion"),
            col("tocc.fecha_creacion"),
            col("tocc.usuario_modificacion"),
            col("tocc.fecha_modificacion"),
            lit("1").alias("es_eliminado"),
        )
    )

    tmp = tmp_dominio_t_orden_compra_cabecera.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_periodo").cast("string").alias("id_periodo"),
        col("id_orden_compra").cast("string").alias("id_orden_compra"),
        col("id_compania").cast("string").alias("id_compania"),
        col("id_sucursal").cast("string").alias("id_sucursal"),
        col("id_proveedor").cast("string").alias("id_proveedor"),
        col("id_almacen").cast("string").alias("id_almacen"),
        col("id_empleado_aprobado").cast("string").alias("id_empleado_aprobado"),
        col("id_empleado_preparado").cast("string").alias("id_empleado_preparado"),
        col("id_centro_costo").cast("string").alias("id_centro_costo"),
        col("id_forma_pago").cast("string").alias("id_forma_pago"),
        col("fecha_emision").cast("date").alias("fecha_emision"),
        col("fecha_atencion").cast("date").alias("fecha_atencion"),
        col("fecha_aprobacion").cast("date").alias("fecha_aprobacion"),
        col("fecha_entrega").cast("date").alias("fecha_entrega"),
        col("cod_tipo_documento").cast("string").alias("cod_tipo_documento"),
        col("nro_orden_compra").cast("string").alias("nro_orden_compra"),
        col("desc_tipo_orden").cast("string").alias("desc_tipo_orden"),
        col("es_atendido").cast("int").alias("es_atendido"),
        col("es_aprobado").cast("int").alias("es_aprobado"),
        col("tiene_impuesto_incluido").cast("int").alias("tiene_impuesto_incluido"),
        col("tiene_factura").cast("int").alias("tiene_factura"),
        col("es_anulado").cast("int").alias("es_anulado"),
        col("desc_estado_orden_compra").cast("string").alias("desc_estado_orden_compra"),
        col("imp_neto_mo").cast("numeric(38, 12)").alias("imp_neto_mo"),
        col("imp_neto_mn").cast("numeric(38, 12)").alias("imp_neto_mn"),
        col("imp_neto_me").cast("numeric(38, 12)").alias("imp_neto_me"),
        col("cod_moneda_mo").cast("string").alias("cod_moneda_mo"),
        col("cod_moneda_mn").cast("string").alias("cod_moneda_mn"),
        col("tipo_cambio_me").cast("numeric(38, 12)").alias("tipo_cambio_me"),
        col("tipo_cambio_mn").cast("numeric(38, 12)").alias("tipo_cambio_mn"),
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
