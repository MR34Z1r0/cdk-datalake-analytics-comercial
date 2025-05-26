import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, upper
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "fact_venta_detalle"
try:
    PERIODOS= spark_controller.get_periods()
    cod_pais = COD_PAIS.split(",")
    logger.info(f"Databases: {cod_pais}")
    
    df_m_tipo_venta = spark_controller.read_table(data_paths.DOMINIO, "m_tipo_venta", cod_pais=cod_pais)
    df_t_venta = spark_controller.read_table(data_paths.DOMINIO, "t_venta", cod_pais=cod_pais)
    df_t_venta_detalle = spark_controller.read_table(data_paths.DOMINIO, "t_venta_detalle", cod_pais=cod_pais)
    df_t_pedido = spark_controller.read_table(data_paths.DOMINIO, "t_pedido", cod_pais=cod_pais)

    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(e)
    raise
try:
    logger.info("starting filter of pais and periodo") 
    df_t_venta = df_t_venta.filter(col("id_periodo").isin(PERIODOS))
    df_t_venta_detalle = df_t_venta_detalle.filter(col("id_periodo").isin(PERIODOS))

    df_fact_venta_detalle = (
        df_t_venta_detalle.alias("tvd")
        .join(
            df_t_venta.alias("tv"),
            (col("tvd.id_venta") == col("tv.id_venta")),
            "inner",
        )
        .join(
            df_m_tipo_venta.alias("mtv"),
            (col("tv.id_tipo_venta") == col("mtv.id_tipo_venta"))
            & (upper(col('mtv.cod_tipo_operacion')).isin(['VEN', 'EXP','OBS'])),
            "inner",
        )
        .join(
            df_t_pedido.alias("tp"),
            (col("tv.id_pedido") == col("tp.id_pedido")),
            "left",
        ) 
        .where((col('tv.es_eliminado') == 0))
        .select(
            col('tv.id_pais').cast("string"),
            col('tv.id_periodo').cast("string"),
            col('tv.id_sucursal').cast("string"),
            col('tv.id_cliente').cast("string"),
            col('tvd.id_producto').cast("string").alias('id_producto'),
            col('tv.id_vendedor').cast("string"),
            col('tv.id_supervisor').cast("string"),
            col('tv.id_forma_pago').cast("string"),
            col('tv.id_fuerza_venta').cast("string"),
            col('tp.id_modelo_atencion').cast("string"),
            col('tv.id_lista_precio').cast("string"),
            col('tp.id_origen_pedido').cast("string"),
            col('tv.id_tipo_venta').cast("string"),
            col('tv.id_venta').cast("string"),
            col('tv.id_pedido').cast("string"),
            col('tv.fecha_emision').cast("date"),
            col('tv.fecha_liquidacion').cast("date"), 
            col('tv.fecha_pedido').cast("date"),  
            col('tv.nro_venta').cast("string"),
            col('tp.nro_pedido').cast("string"),
            coalesce(col("tv.desc_region"), col("tp.desc_region")).cast("string").alias('desc_region'),
            coalesce(col("tv.desc_subregion"), col("tp.desc_subregion")).cast("string").alias('desc_subregion'),
            coalesce(col("tv.desc_division"), col("tp.desc_division")).cast("string").alias('desc_division'),
            coalesce(col("tv.cod_zona"), col("tp.cod_zona")).cast("string").alias('cod_zona'),
            coalesce(col("tv.cod_ruta"), col("tp.cod_ruta")).cast("string").alias('cod_ruta'),
            coalesce(col("tv.cod_modulo"), col("tp.cod_modulo")).cast("string").alias('cod_modulo'),
            col("tvd.cant_caja_fisica_ven").cast("numeric(38,12)").alias("cant_cajafisica_vta"),
            (col("tvd.cant_caja_volumen_ven")/30).cast("numeric(38,12)").alias("cant_cajaunitaria_vta"),
            col("tvd.cant_caja_fisica_pro").cast("numeric(38,12)").alias("cant_cajafisica_pro"),
            (col("tvd.cant_caja_volumen_pro")/30).cast("numeric(38,12)").alias("cant_cajaunitaria_pro"),
            col("tvd.imp_neto_vta_mn").cast("numeric(38,12)").alias("imp_neto_vta_mn"),
            col("tvd.imp_neto_vta_me").cast("numeric(38,12)").alias("imp_neto_vta_me"),
            col("tvd.imp_bruto_vta_mn").cast("numeric(38,12)").alias("imp_bruto_vta_mn"),
            col("tvd.imp_bruto_vta_me").cast("numeric(38,12)").alias("imp_bruto_vta_me"),
            col("tvd.imp_dscto_mn").cast("numeric(38,12)").alias("imp_dscto_mn"),
            col("tvd.imp_dscto_me").cast("numeric(38,12)").alias("imp_dscto_me"),
            col("tvd.imp_desnimp_mn").cast("numeric(38,12)").alias("imp_dscto_sinimpvta_mn"),
            col("tvd.imp_desnimp_me").cast("numeric(38,12)").alias("imp_dscto_sinimpvta_me"),
            col("tvd.imp_cobrar_vta_mn").cast("numeric(38,12)").alias("imp_cobrar_vta_mn"),
            col("tvd.imp_cobrar_vta_me").cast("numeric(38,12)").alias("imp_cobrar_vta_me"),
            col("tvd.imp_paquete_vta_mn").cast("numeric(38,12)").alias("imp_paquete_vta_mn"),
            col("tvd.imp_paquete_vta_me").cast("numeric(38,12)").alias("imp_paquete_vta_me"),
            col("tvd.imp_sugerido_mn").cast("numeric(38,12)").alias("imp_sugerido_mn"),
            col("tvd.imp_sugerido_me").cast("numeric(38,12)").alias("imp_sugerido_me"),
            col("tvd.imp_full_vta_mn").cast("numeric(38,12)").alias("imp_full_vta_mn"),
            col("tvd.imp_full_vta_me").cast("numeric(38,12)").alias("imp_full_vta_me"),
            col("tvd.imp_valorizado_pro_mn").cast("numeric(38,12)").alias("imp_valorizado_pro_mn"),
            col("tvd.imp_valorizado_pro_me").cast("numeric(38,12)").alias("imp_valorizado_pro_me"),
            col("tvd.imp_impuesto1_mn").cast("numeric(38,12)").alias("imp_impuesto1_mn"),
            col("tvd.imp_impuesto1_me").cast("numeric(38,12)").alias("imp_impuesto1_me"),
            col("tvd.imp_impuesto2_mn").cast("numeric(38,12)").alias("imp_impuesto2_mn"),
            col("tvd.imp_impuesto2_me").cast("numeric(38,12)").alias("imp_impuesto2_me"),
            col("tvd.imp_impuesto3_mn").cast("numeric(38,12)").alias("imp_impuesto3_mn"),
            col("tvd.imp_impuesto3_me").cast("numeric(38,12)").alias("imp_impuesto3_me"),
            col("tvd.imp_impuesto4_mn").cast("numeric(38,12)").alias("imp_impuesto4_mn"),
            col("tvd.imp_impuesto4_me").cast("numeric(38,12)").alias("imp_impuesto4_me"),
            col("tvd.imp_impuesto5_mn").cast("numeric(38,12)").alias("imp_impuesto5_mn"),
            col("tvd.imp_impuesto5_me").cast("numeric(38,12)").alias("imp_impuesto5_me"),
            col("tvd.imp_impuesto6_mn").cast("numeric(38,12)").alias("imp_impuesto6_mn"),
            col("tvd.imp_impuesto6_me").cast("numeric(38,12)").alias("imp_impuesto6_me"),
        )
    )
    
    logger.info(f"starting write of {target_table_name}")
    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(df_fact_venta_detalle, data_paths.COMERCIAL, target_table_name, partition_columns_array)
except Exception as e:
    logger.error(e) 
    raise