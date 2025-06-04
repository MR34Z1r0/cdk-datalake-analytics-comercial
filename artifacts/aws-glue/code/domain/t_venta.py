from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, concat, concat_ws, lit, coalesce, when, date_format, round, trim, to_date, substring, lower, to_timestamp, row_number, max, sum
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "t_venta"
try:
    PERIODOS= spark_controller.get_periods()
    logger.info(f"Periods to filter: {PERIODOS}")
    df_m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    df_m_parametro = spark_controller.read_table(data_paths.BIGMAGIC, "m_parametro")
    df_m_tipo_cambio = spark_controller.read_table(data_paths.BIGMAGIC, "m_tipo_cambio")
    df_m_region = spark_controller.read_table(data_paths.BIGMAGIC, "m_region", have_principal = True)
    df_m_subregion = spark_controller.read_table(data_paths.BIGMAGIC, "m_subregion", have_principal = True)
    df_m_centro_distribucion = spark_controller.read_table(data_paths.BIGMAGIC, "m_division")
    df_m_zona_distribucion = spark_controller.read_table(data_paths.BIGMAGIC, "m_zona")
    df_t_historico_venta = spark_controller.read_table(data_paths.BIGMAGIC, "t_documento_venta")
    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("starting filter of pais and periodo") 
    df_t_historico_venta = df_t_historico_venta.filter(date_format(col("fecha_liquidacion"), "yyyyMM").isin(PERIODOS))

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

    logger.info("starting creation of df_t_historico_venta_filter")
    df_t_historico_venta_filter = (
        df_t_historico_venta.alias("tp").filter((~col("tp.cod_documento_venta").isin(["CMD", "RMD"]))
        & (coalesce(col("tp.flg_facglob"), lit("F")) == "F")
        & (coalesce(col("tp.flg_refact"), lit("F")) == "F")
        )
        .join(
            df_m_compania.alias("mc"), 
            (col("tp.cod_compania") == col("mc.cod_compania")),
            "inner"
        )
        .join(
            df_m_zona_distribucion.alias("mzo"),
            (col("mzo.cod_compania") == col("tp.cod_compania"))
            & (col("mzo.cod_sucursal") == col("tp.cod_sucursal"))
            & (col("mzo.cod_zona") == col("tp.cod_zona")),
            "left"
        )
        .join(
            df_m_centro_distribucion.alias("mcd"),
            (col("mcd.cod_division") == col("mzo.cod_zona"))
            & (col("mcd.cod_compania") == col("mzo.cod_compania")),
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
            (col("mtc.fecha") == col("tp.fecha_emision"))
            & (col("mtc.cod_compania") == col("mc.cod_compania"))
            & (col("mtc.cod_moneda") == col("mc.moneda_mn")),
            "left",
        )
        .select(
            col("mc.id_pais"),
            date_format(col("tp.fecha_liquidacion"), "yyyyMM").alias("id_periodo"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_sucursal"),col("cod_almacen"), col("tp.cod_documento_venta"), col("nro_documento_venta")).alias("id_venta"),
            col("tp.cod_compania").alias("id_compania"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_sucursal")).alias("id_sucursal"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_sucursal"), col("tp.cod_documento_pedido"), col("tp.nro_documento_pedido"), col("tp.cod_cliente")).alias("id_pedido"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_documento_venta"), col("tp.cod_procedimiento")).alias("id_tipo_venta"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_cliente")).alias("id_cliente"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_sucursal"), col("tp.cod_fuerza_venta")).alias("id_fuerza_venta"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_vendedor")).alias("id_vendedor"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_supervisor")).alias("id_supervisor"),
            lit(None).alias("id_jefe_venta"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_lista_precio")).alias("id_lista_precio"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_documento_pedido")).alias("id_tipo_documento"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_forma_pago")).alias("id_forma_pago"),
            concat_ws("|", col("tp.cod_compania"), col("tp.cod_motivo_rechazo")).alias("id_motivo_rechazo"), 
            lit(None).alias('id_motivo_nota_credito'),
            #concat_ws("|", col("tp.cod_compania"), col("tp.cod_documento_venta")).alias("id_tipo_documento_venta"),
            col("tp.cod_documento_venta"),
            col('nro_comprobante').alias('nro_venta'),
            lit(None).alias('nro_venta_ref'), 
            coalesce(col("mr.desc_region"), lit("REGION DEFAULT")).alias("desc_region"),
            coalesce(col("msr.desc_subregion"), lit("SUBREGION DEFAULT")).alias("desc_subregion"),
            col("mcd.desc_division"), 
            col("tp.cod_zona"),
            col("tp.cod_ruta"),
            col("tp.cod_modulo"), 
            col("tp.fecha_liquidacion"),
            col("tp.fecha_emision"),
            col("tp.fecha_pedido"),  
            when(col("tp.cod_estado_comprobante") == "002", 1).otherwise(0).alias("es_anulado"),    
            coalesce(when(col("tp.cod_moneda") == col("mc.moneda_mn"), 1).otherwise(col("mtc.tc_venta")), col("tp.tipo_cambio_mn")).alias("tipo_cambio_mn"),
            coalesce(when((col("tp.cod_moneda") == "DOL") | (col("tp.cod_moneda") == "USD"), 1).otherwise(col("mtc.tc_venta")), when(col("tp.tipo_cambio_me") == 0, 1).otherwise(col("tp.tipo_cambio_me"))).alias("tipo_cambio_me"),
            col("tp.fecha_creacion").alias("fecha_creacion"),
            col("tp.fecha_modificacion").alias("fecha_modificacion"),
            when(col("tp.cod_estado_comprobante") == "002", 1).otherwise(0).alias("es_eliminado"),
        )
    ) 

    logger.info("starting creation of df_t_venta")
    df_dom_t_venta = (
        df_t_historico_venta_filter.alias("tv")
        .select(
            col("tv.id_pais").cast("string"),
            col("tv.id_periodo").cast("string"),
            col("tv.id_venta").cast("string"),
            col("tv.id_compania").cast("string"),
            col("tv.id_sucursal").cast("string"),
            col("tv.id_pedido").cast("string"),
            col("tv.id_tipo_venta").cast("string"),
            col("tv.id_cliente").cast("string"),
            col("tv.id_fuerza_venta").cast("string"),
            col("tv.id_vendedor").cast("string"),
            col("tv.id_supervisor").cast("string"),
            col("tv.id_jefe_venta").cast("string"),
            col("tv.id_lista_precio").cast("string"),
            col("tv.id_tipo_documento").cast("string"),
            col("tv.id_forma_pago").cast("string"),
            col("tv.id_motivo_rechazo").cast("string"),
            col("tv.id_motivo_nota_credito").cast("string"),
            col("tv.cod_documento_venta").cast("string"),
            col("tv.nro_venta").cast("string"),
            col("tv.nro_venta_ref").cast("string"),
            col("tv.desc_region").cast("string"),
            col("tv.desc_subregion").cast("string"),
            col("tv.desc_division").cast("string"),
            col("tv.cod_zona").cast("string"),
            col("tv.cod_ruta").cast("string"),
            col("tv.cod_modulo").cast("string"),
            col("tv.fecha_liquidacion").cast("date"),
            col("tv.fecha_emision").cast("date"),
            col("tv.fecha_pedido").cast("date"),
            col("tv.es_anulado").cast("int"),
            col("tv.tipo_cambio_mn").cast("numeric(38,12)"),
            col("tv.tipo_cambio_me").cast("numeric(38,12)"),
            col("tv.fecha_creacion").cast("timestamp"),
            col("tv.fecha_modificacion").cast("timestamp"),
            col("tv.es_eliminado").cast("int"),
        )
    )
    
    logger.info(f"starting write of {target_table_name}")
    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(df_dom_t_venta, data_paths.DOMAIN, target_table_name, partition_columns_array)
    logger.info(f"Write de {target_table_name} success completed")
except Exception as e:
    logger.error(f"Error processing df_dom_t_venta: {e}")
    raise ValueError(f"Error processing df_dom_t_venta: {e}")