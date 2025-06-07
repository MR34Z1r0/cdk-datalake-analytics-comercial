import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, upper, to_date, max, sum
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "fact_venta_cliente_historico"
try:
    PERIODOS= spark_controller.get_periods()
    logger.info(f"Periods: {PERIODOS}")
    
    df_m_tipo_venta = spark_controller.read_table(data_paths.DOMAIN, "m_tipo_venta")
    
    df_t_venta = spark_controller.read_table(data_paths.DOMAIN, "t_venta")
    df_t_venta_detalle = spark_controller.read_table(data_paths.DOMAIN, "t_venta_detalle")
    df_t_pedido = spark_controller.read_table(data_paths.DOMAIN, "t_pedido")

    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("starting filter of pais and periodo") 
    df_t_venta = df_t_venta.filter(col("id_periodo").isin(PERIODOS))
    df_t_venta_detalle = df_t_venta_detalle.filter(col("id_periodo").isin(PERIODOS))

    df_fact_venta_cliente_historico = (
        df_t_venta_detalle.alias("tvd")
        .join(
            df_t_venta.alias("tv"),
            (col("tvd.id_venta") == col("tv.id_venta")),
            "inner",
        )
        .join(
            df_t_pedido.alias("tp"),
            (col("tv.id_pedido") == col("tp.id_pedido")),
            "left",
        ) 
        .join(
            df_m_tipo_venta.alias("mtv"),
            (col("tv.id_tipo_venta") == col("mtv.id_tipo_venta"))
            & (upper(col('mtv.cod_tipo_operacion')).isin(['VEN', 'EXP'])),
            "inner",
        )
        .where((col('tv.es_eliminado') == 0))
        .groupby(
            col("tv.id_sucursal"),
            col("tv.id_cliente"),
            col("tvd.id_producto"),
            col("tv.id_forma_pago"),
            col("tv.id_lista_precio"),
            col("tv.id_periodo"),
            col("tv.id_pais"),
        )
        .agg(
            max(coalesce(col("tv.desc_region"), col("tp.desc_region"))).alias("desc_region"),
            max(coalesce(col("tv.desc_subregion"), col("tp.desc_subregion"))).alias("desc_subregion"),
            max(coalesce(col("tv.desc_division"), col("tp.desc_division"))).alias("desc_division"),
            max(coalesce(col("tv.cod_zona"), col("tp.cod_zona"))).alias("cod_zona"),
            max(coalesce(col("tv.cod_ruta"), col("tp.cod_ruta"))).alias("cod_ruta"),
            max(coalesce(col("tv.cod_modulo"), col("tp.cod_modulo"))).alias("cod_modulo"),
            sum(col("tvd.cant_caja_fisica_ven")).alias("cant_cajafisica_vta"),
            (sum(col("tvd.cant_caja_volumen_ven")) / 30).alias("cant_cajaunitaria_vta"),
            sum(col("tvd.cant_caja_fisica_pro")).alias("cant_cajafisica_pro"),
            (sum(col("tvd.cant_caja_volumen_pro")) / 30).alias("cant_cajaunitaria_pro"),
            sum(col("tvd.imp_neto_vta_mn")).alias("imp_neto_vta_mn"),
            sum(col("tvd.imp_neto_vta_me")).alias("imp_neto_vta_me"),
            sum(col("tvd.imp_bruto_vta_mn")).alias("imp_bruto_vta_mn"),
            sum(col("tvd.imp_bruto_vta_me")).alias("imp_bruto_vta_me"),
            sum(col("tvd.imp_dscto_mn")).alias("imp_dscto_mn"),
            sum(col("tvd.imp_dscto_me")).alias("imp_dscto_me"),
            sum(col("tvd.imp_desnimp_mn")).alias("imp_dscto_sinimpvta_mn"),
            sum(col("tvd.imp_desnimp_me")).alias("imp_dscto_sinimpvta_me"),
            sum(col("tvd.imp_cobrar_vta_mn")).alias("imp_cobrar_vta_mn"),
            sum(col("tvd.imp_cobrar_vta_me")).alias("imp_cobrar_vta_me"),
            sum(col("tvd.imp_paquete_vta_mn")).alias("imp_paquete_vta_mn"),
            sum(col("tvd.imp_paquete_vta_me")).alias("imp_paquete_vta_me"),
            sum(col("tvd.imp_sugerido_mn")).alias("imp_sugerido_mn"),
            sum(col("tvd.imp_sugerido_me")).alias("imp_sugerido_me"),
            sum(col("tvd.imp_full_vta_mn")).alias("imp_full_vta_mn"),
            sum(col("tvd.imp_full_vta_me")).alias("imp_full_vta_me"),
            sum(col("tvd.imp_valorizado_pro_mn")).alias("imp_valorizado_pro_mn"),
            sum(col("tvd.imp_valorizado_pro_me")).alias("imp_valorizado_pro_me"),
            sum(col("tvd.imp_impuesto1_mn")).alias("imp_impuesto1_mn"),
            sum(col("tvd.imp_impuesto1_me")).alias("imp_impuesto1_me"),
            sum(col("tvd.imp_impuesto2_mn")).alias("imp_impuesto2_mn"),
            sum(col("tvd.imp_impuesto2_me")).alias("imp_impuesto2_me"),
            sum(col("tvd.imp_impuesto3_mn")).alias("imp_impuesto3_mn"),
            sum(col("tvd.imp_impuesto3_me")).alias("imp_impuesto3_me"),
            sum(col("tvd.imp_impuesto4_mn")).alias("imp_impuesto4_mn"),
            sum(col("tvd.imp_impuesto4_me")).alias("imp_impuesto4_me"),
            sum(col("tvd.imp_impuesto5_mn")).alias("imp_impuesto5_mn"),
            sum(col("tvd.imp_impuesto5_me")).alias("imp_impuesto5_me"),
            sum(col("tvd.imp_impuesto6_mn")).alias("imp_impuesto6_mn"),
            sum(col("tvd.imp_impuesto6_me")).alias("imp_impuesto6_me"),
        )
        .select(
            col("tv.id_pais").cast("string"),
            col("tv.id_periodo").cast("string"),
            col("tv.id_sucursal").cast("string"),
            col("tv.id_cliente").cast("string"),
            col("tvd.id_producto").cast("string").alias("id_producto"),
            col("tv.id_forma_pago").cast("string"),
            col("tv.id_lista_precio").cast("string"),
            to_date(col("tv.id_periodo"), "yyyyMM").alias("fecha_liquidacion"),
            col("desc_region").cast("string"),
            col("desc_subregion").cast("string"),
            col("desc_division").cast("string"),
            col("cod_zona").cast("string"),
            col("cod_ruta").cast("string"),
            col("cod_modulo").cast("string"),
            col("cant_cajafisica_vta").cast("decimal(38,12)"),
            col("cant_cajaunitaria_vta").cast("decimal(38,12)"),
            col("cant_cajafisica_pro").cast("decimal(38,12)"),
            col("cant_cajaunitaria_pro").cast("decimal(38,12)"),
            col("imp_neto_vta_mn").cast("decimal(38,12)"),
            col("imp_neto_vta_me").cast("decimal(38,12)"),
            col("imp_bruto_vta_mn").cast("decimal(38,12)"),
            col("imp_bruto_vta_me").cast("decimal(38,12)"),
            col("imp_dscto_mn").cast("decimal(38,12)"),
            col("imp_dscto_me").cast("decimal(38,12)"),
            col("imp_dscto_sinimpvta_mn").cast("decimal(38,12)"),
            col("imp_dscto_sinimpvta_me").cast("decimal(38,12)"),
            col("imp_cobrar_vta_mn").cast("decimal(38,12)"),
            col("imp_cobrar_vta_me").cast("decimal(38,12)"),
            col("imp_paquete_vta_mn").cast("decimal(38,12)"),
            col("imp_paquete_vta_me").cast("decimal(38,12)"),
            col("imp_sugerido_mn").cast("decimal(38,12)"),
            col("imp_sugerido_me").cast("decimal(38,12)"),
            col("imp_full_vta_mn").cast("decimal(38,12)"),
            col("imp_full_vta_me").cast("decimal(38,12)"),
            col("imp_valorizado_pro_mn").cast("decimal(38,12)"),
            col("imp_valorizado_pro_me").cast("decimal(38,12)"),
            col("imp_impuesto1_mn").cast("decimal(38,12)"),
            col("imp_impuesto1_me").cast("decimal(38,12)"),
            col("imp_impuesto2_mn").cast("decimal(38,12)"),
            col("imp_impuesto2_me").cast("decimal(38,12)"),
            col("imp_impuesto3_mn").cast("decimal(38,12)"),
            col("imp_impuesto3_me").cast("decimal(38,12)"),
            col("imp_impuesto4_mn").cast("decimal(38,12)"),
            col("imp_impuesto4_me").cast("decimal(38,12)"),
            col("imp_impuesto5_mn").cast("decimal(38,12)"),
            col("imp_impuesto5_me").cast("decimal(38,12)"),
            col("imp_impuesto6_mn").cast("decimal(38,12)"),
            col("imp_impuesto6_me").cast("decimal(38,12)"),
        )
    )
    
    partition_columns_array = ["id_pais", "id_periodo"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.write_table(df_fact_venta_cliente_historico, data_paths.ANALYTICS, target_table_name, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")     
except Exception as e:
    logger.error(f"Error processing df_fact_venta_cliente_historico: {e}")
    raise ValueError(f"Error processing df_fact_venta_cliente_historico: {e}") 