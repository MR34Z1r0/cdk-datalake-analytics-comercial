from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, concat, concat_ws, lit, coalesce, when, date_format, round, trim, to_date, substring, lower, to_timestamp, row_number, max, sum, upper 
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "t_venta_detalle"
try:
    PERIODOS= spark_controller.get_periods()

    df_m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania")
    df_m_parametro = spark_controller.read_table(data_paths.APDAYC, "m_parametro")
    df_m_tipo_cambio = spark_controller.read_table(data_paths.APDAYC, "m_tipo_cambio")
    df_m_articulo = spark_controller.read_table(data_paths.APDAYC, "m_articulo")
    df_m_linea = spark_controller.read_table(data_paths.APDAYC, "m_linea")
    df_m_operacion = spark_controller.read_table(data_paths.APDAYC, "m_operacion")
    df_t_historico_venta_detalle = spark_controller.read_table(data_paths.APDAYC, "t_documento_venta_detalle")
    df_t_historico_venta = spark_controller.read_table(data_paths.APDAYC, "t_documento_venta")

    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")  
try:
    logger.info("starting filter of pais and periodo") 
    df_t_historico_venta = df_t_historico_venta.filter(date_format(col("fecha_liquidacion"), "yyyyMM").isin(PERIODOS))
    df_t_historico_venta_detalle = df_t_historico_venta_detalle.filter(date_format(col("fecha_liquidacion"), "yyyyMM").isin(PERIODOS))

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

    logger.info("starting creation of df_m_articulo")
    df_m_articulo_filter = (
        df_m_articulo.alias("ma")
        .join(
            df_m_linea.alias("ml"),
            (col("ma.cod_compania") == col("ml.cod_compania"))
            & (col("ma.cod_linea") == col("ml.cod_linea")),
            "inner",
        )
        .where(
            (upper(col("ml.flg_linea")) == "TE")  # Manejo de nulls
            | (
                (col("ma.cod_linea") == "17") 
                & (col("ma.cod_familia").isin(["001", "002", "003"]))
            )
        )
        .select(
            concat_ws("|", col("ma.cod_compania"), col("ma.cod_articulo")).alias("id_producto"),
            col("ma.cant_unidad_volumen"),
            col("ma.cant_unidad_paquete"),
            col("ma.cant_paquete_caja"),
        )
    ).cache()

    logger.info("starting creation of df_t_historico_venta_filter")
    df_t_historico_venta_filter = (
        df_t_historico_venta.alias("tp").filter((~col("tp.cod_documento_venta").isin(["CMD", "RMD"]))
        & (coalesce(col("tp.flg_facglob"), lit("F")) == "F")
        & (coalesce(col("tp.flg_refact"), lit("F")) == "F")
        )
        .join(
            df_m_compania.alias("mc"),
            col("tp.cod_compania") == col("mc.cod_compania"),
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
            col("tp.cod_compania"),
            col("tp.cod_documento_venta"),
            col("tp.cod_procedimiento"),
            coalesce(when(col("tp.cod_moneda") == col("mc.moneda_mn"), 1).otherwise(col("mtc.tc_venta")), col("tp.tipo_cambio_mn")).alias("tipo_cambio_mn"),
            coalesce(when((col("tp.cod_moneda") == "DOL") | (col("tp.cod_moneda") == "USD"), 1).otherwise(col("mtc.tc_venta")), when(col("tp.tipo_cambio_me") == 0, 1).otherwise(col("tp.tipo_cambio_me"))).alias("tipo_cambio_me"),
        )
    )

    logger.info("starting creation of df_t_historico_venta_detalle_filter")
    df_t_historico_venta_detalle_filter = (
        df_t_historico_venta_detalle
        .select(
            concat_ws("|", col("cod_compania"), col("cod_sucursal"), col("cod_almacen"), col("cod_documento_transaccion"), col("nro_comprobante_venta")).alias("id_venta"),
            concat_ws("|", col("cod_compania"), col("cod_articulo")).alias("id_producto"),
            col("cod_compania"),
            col("cod_operacion"),
            col("cant_paquete"),
            col("cant_unidad"),
            col("imp_valorizado"),
            col("imp_cobrar"),
            col("imp_descuento"),
            col("imp_descuento_sinimp"),
            col("precio_paquete"),
            col("imp_isc"),
            col("imp_igv"),
            col("imp_im3"),
            col("imp_im4"),
            col("imp_im5"),
            col("imp_im6"),            
            col("fecha_creacion"),
            col("fecha_modificacion"),
            lit(0).alias("es_eliminado")
        )
    )

    logger.info("starting creation of df_t_historico_venta_detalle_select")
    df_t_historico_venta_detalle_select = (
        df_t_historico_venta_detalle_filter.alias("tvd")
        .join(
            df_t_historico_venta_filter.alias("tv"),
            col("tv.id_venta") == col("tvd.id_venta"),
            "inner"
        )
        .join(
            df_m_articulo_filter.alias("ma"),
            col("tvd.id_producto") == col("ma.id_producto"),
            "inner",
        )
        .join(
            df_m_operacion.alias("mo"),
            (concat_ws("|", col("tv.cod_compania"), col("tv.cod_documento_venta"), col("tv.cod_procedimiento"), col("tvd.cod_operacion"))) == (concat_ws("|", col("mo.cod_compania"), col("mo.cod_documento_transaccion"), col("mo.cod_procedimiento"), col("mo.cod_operacion"))),
            "inner",
        )        
        .select(
            col("tv.id_pais"),
            col("tv.id_periodo"),
            col("tv.id_venta"),
            col("tvd.id_producto"),
            when(col("tv.cod_documento_venta") == "NCC", -1).otherwise(1).alias("factor"),
            upper(col("mo.cod_tipo_operacion")).alias("cod_tipo_operacion"),
            col("tv.tipo_cambio_mn"),
            col("tv.tipo_cambio_me"),
            col("tvd.cant_paquete"),
            col("tvd.cant_unidad"),
            col("ma.cant_unidad_paquete"),
            col("ma.cant_paquete_caja"),
            col("ma.cant_unidad_volumen"),
            col("tvd.imp_valorizado"),
            col("tvd.imp_cobrar"),
            col("tvd.imp_descuento"),
            col("tvd.imp_descuento_sinimp"),
            col("tvd.precio_paquete"),
            lit(0).alias("imp_sugerido"),
            lit(0).alias("imp_ventafull"),
            col("tvd.imp_isc"),
            col("tvd.imp_igv"),
            col("tvd.imp_im3"),
            col("tvd.imp_im4"),
            col("tvd.imp_im5"),
            col("tvd.imp_im6"),            
            col("tvd.precio_paquete"),
            col("tvd.fecha_creacion"),
            col("tvd.fecha_modificacion"),
            col("tvd.es_eliminado")            
        )
    )
 
    logger.info("starting creation of df_dom_t_venta_detalle")
    df_dom_t_venta_detalle = (
        df_t_historico_venta_detalle_select 
        .groupby(
            col("id_venta"),
            col("id_producto")
        )
        .agg(
            max(col("id_pais")).alias('id_pais'),
            max(col("id_periodo")).alias('id_periodo'),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("cant_paquete") + (col("cant_unidad") / col("cant_unidad_paquete"))) * (col("cant_paquete_caja"))
            ).alias("cant_caja_fisica_ven"),
            sum(
                (when(upper(col("cod_tipo_operacion")) == "PRO", 1).otherwise(0)) * col("factor") * (col("cant_paquete") + (col("cant_unidad") / col("cant_unidad_paquete"))) * (col("cant_paquete_caja"))
            ).alias("cant_caja_fisica_pro"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("cant_paquete") * col("cant_unidad_paquete") + col("cant_unidad")) * (col("cant_unidad_volumen"))
            ).alias("cant_caja_volumen_ven"),
            sum(
                (when(upper(col("cod_tipo_operacion")) == "PRO", 1).otherwise(0)) * col("factor") * (col("cant_paquete") * col("cant_unidad_paquete") + col("cant_unidad")) * (col("cant_unidad_volumen"))
            ).alias("cant_caja_volumen_pro"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_valorizado") * col("tv.tipo_cambio_mn"))
            ).alias("imp_neto_vta_mn"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_valorizado") / col("tv.tipo_cambio_me"))
            ).alias("imp_neto_vta_me"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_cobrar") * col("tv.tipo_cambio_mn"))
            ).alias("imp_bruto_vta_mn"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_cobrar") / col("tv.tipo_cambio_me"))
            ).alias("imp_bruto_vta_me"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_descuento") * col("tv.tipo_cambio_mn"))
            ).alias("imp_dscto_mn"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_descuento") / col("tv.tipo_cambio_me"))
            ).alias("imp_dscto_me"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_descuento_sinimp") * col("tv.tipo_cambio_mn"))
            ).alias("imp_desnimp_mn"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_descuento_sinimp") / col("tv.tipo_cambio_me"))
            ).alias("imp_desnimp_me"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_cobrar") * col("tv.tipo_cambio_mn"))
            ).alias("imp_cobrar_vta_mn"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_cobrar") / col("tv.tipo_cambio_me"))
            ).alias("imp_cobrar_vta_me"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * ((col("cant_paquete") + (col("cant_unidad") / col("cant_unidad_paquete")) * col("cant_paquete_caja") * col("precio_paquete") * col("tv.tipo_cambio_mn")))
            ).alias("imp_paquete_vta_mn"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * ((col("cant_paquete") + (col("cant_unidad") / col("cant_unidad_paquete")) * col("cant_paquete_caja") * col("precio_paquete") / col("tv.tipo_cambio_me")))
            ).alias("imp_paquete_vta_me"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_sugerido") * col("tv.tipo_cambio_mn"))
            ).alias("imp_sugerido_mn"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_sugerido") / col("tv.tipo_cambio_me"))
            ).alias("imp_sugerido_me"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_ventafull") * col("tv.tipo_cambio_mn"))
            ).alias("imp_full_vta_mn"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_ventafull") / col("tv.tipo_cambio_me"))
            ).alias("imp_full_vta_me"),
            sum(
                (when(upper(col("cod_tipo_operacion")) == "PRO", 1).otherwise(0)) * col("factor") * (col("imp_valorizado") * col("tv.tipo_cambio_mn"))
            ).alias("imp_valorizado_pro_mn"),
            sum(
                (when(upper(col("cod_tipo_operacion")) == "PRO", 1).otherwise(0)) * col("factor") * (col("imp_valorizado") / col("tv.tipo_cambio_me"))
            ).alias("imp_valorizado_pro_me"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_isc") * col("tv.tipo_cambio_mn"))
            ).alias("imp_impuesto1_mn"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_isc") / col("tv.tipo_cambio_me"))
            ).alias("imp_impuesto1_me"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_igv") * col("tv.tipo_cambio_mn"))
            ).alias("imp_impuesto2_mn"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_igv") / col("tv.tipo_cambio_me"))
            ).alias("imp_impuesto2_me"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_im3") * col("tv.tipo_cambio_mn"))
            ).alias("imp_impuesto3_mn"),
            sum(
                (when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0)) * col("factor") * (col("imp_im3") / col("tv.tipo_cambio_me"))
            ).alias("imp_impuesto3_me"),
            sum(
                when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0) * col("factor") * (col("imp_im4") * col("tv.tipo_cambio_mn"))
            ).alias("imp_impuesto4_mn"),
            sum(
                when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0) * col("factor") * (col("imp_im4") / col("tv.tipo_cambio_me"))
            ).alias("imp_impuesto4_me"),
            sum(
                when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0) * col("factor") * (col("imp_im5") * col("tv.tipo_cambio_mn"))
            ).alias("imp_impuesto5_mn"),
            sum(
                when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0) * col("factor") * (col("imp_im5") / col("tv.tipo_cambio_me"))
            ).alias("imp_impuesto5_me"),
            sum(
                when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0) * col("factor") * (col("imp_im6") * col("tv.tipo_cambio_mn"))
            ).alias("imp_impuesto6_mn"),
            sum(
                when(upper(col("cod_tipo_operacion")) != "PRO", 1).otherwise(0) * col("factor") * (col("imp_im6") / col("tv.tipo_cambio_me"))
            ).alias("imp_impuesto6_me"),
            max(col("fecha_creacion")).alias("fecha_creacion"),
            max(col("fecha_modificacion")).alias("fecha_modificacion"),
            max(col("es_eliminado")).alias('es_eliminado')
        )
        .select(
            col("id_pais").cast("string"),
            col("id_periodo").cast("string"),
            col("id_venta").cast("string"),
            col("id_producto").cast("string"),
            col("cant_caja_fisica_ven").cast("numeric(38,12)"),
            col("cant_caja_fisica_pro").cast("numeric(38,12)"),
            col("cant_caja_volumen_ven").cast("numeric(38,12)"),
            col("cant_caja_volumen_pro").cast("numeric(38,12)"),
            col("imp_neto_vta_mn").cast("numeric(38,12)"),
            col("imp_neto_vta_me").cast("numeric(38,12)"),
            col("imp_bruto_vta_mn").cast("numeric(38,12)"),
            col("imp_bruto_vta_me").cast("numeric(38,12)"),
            col("imp_dscto_mn").cast("numeric(38,12)"),
            col("imp_dscto_me").cast("numeric(38,12)"),
            col("imp_desnimp_mn").cast("numeric(38,12)"),
            col("imp_desnimp_me").cast("numeric(38,12)"),
            col("imp_cobrar_vta_mn").cast("numeric(38,12)"),
            col("imp_cobrar_vta_me").cast("numeric(38,12)"),
            col("imp_paquete_vta_mn").cast("numeric(38,12)"),
            col("imp_paquete_vta_me").cast("numeric(38,12)"),
            col("imp_sugerido_mn").cast("numeric(38,12)"),
            col("imp_sugerido_me").cast("numeric(38,12)"),
            col("imp_full_vta_mn").cast("numeric(38,12)"),
            col("imp_full_vta_me").cast("numeric(38,12)"),
            col("imp_valorizado_pro_mn").cast("numeric(38,12)"),
            col("imp_valorizado_pro_me").cast("numeric(38,12)"),
            col("imp_impuesto1_mn").cast("numeric(38,12)"),
            col("imp_impuesto1_me").cast("numeric(38,12)"),
            col("imp_impuesto2_mn").cast("numeric(38,12)"),
            col("imp_impuesto2_me").cast("numeric(38,12)"),
            col("imp_impuesto3_mn").cast("numeric(38,12)"),
            col("imp_impuesto3_me").cast("numeric(38,12)"),
            col("imp_impuesto4_mn").cast("numeric(38,12)"),
            col("imp_impuesto4_me").cast("numeric(38,12)"),
            col("imp_impuesto5_mn").cast("numeric(38,12)"),
            col("imp_impuesto5_me").cast("numeric(38,12)"),
            col("imp_impuesto6_mn").cast("numeric(38,12)"),
            col("imp_impuesto6_me").cast("numeric(38,12)"),
            col("fecha_creacion").cast("timestamp"),
            col("fecha_modificacion").cast("timestamp"),
            col("es_eliminado").cast("int")
        )
    ) 
    partition_columns_array = ["id_pais", "id_periodo"]
    logger.info(f"starting write of {target_table_name}")
    spark_controller.write_table(df_dom_t_venta_detalle, data_paths.DOMAIN, target_table_name, partition_columns_array)
    logger.info(f"Write de {target_table_name} completado exitosamente")
except Exception as e:
    logger.error(f"Error processing df_dom_t_venta_detalle: {e}")
    raise ValueError(f"Error processing df_dom_t_venta_detalle: {e}")