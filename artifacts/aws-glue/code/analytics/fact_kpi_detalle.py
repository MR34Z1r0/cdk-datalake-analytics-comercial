import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, upper
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "fact_kpi_detalle"
try:
    PERIODOS= spark_controller.get_periods()
    logger.info(f"Periods: {PERIODOS}")
    
    df_m_tipo_venta = spark_controller.read_table(data_paths.DOMAIN, "m_tipo_venta")
    
    df_t_pedido = spark_controller.read_table(data_paths.DOMAIN, "t_pedido")
    df_t_pedido_detalle = spark_controller.read_table(data_paths.DOMAIN, "t_pedido_detalle")
    df_t_venta = spark_controller.read_table(data_paths.DOMAIN, "t_venta")
    df_t_venta_detalle = spark_controller.read_table(data_paths.DOMAIN, "t_venta_detalle")
    df_t_visita = spark_controller.read_table(data_paths.DOMAIN, "t_visita")

    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("starting filter of pais and periodo") 
    df_t_pedido = df_t_pedido.filter(col("id_periodo").isin(PERIODOS))
    df_t_pedido_detalle = df_t_pedido_detalle.filter(col("id_periodo").isin(PERIODOS))
    
    df_fact_kpi_detalle_pedido = (
        df_t_pedido.alias("tp")
        .join(
            df_t_pedido_detalle.alias("tpd"),
            (col("tp.id_pedido") == col("tpd.id_pedido")),
            "inner",
        )
        .where((col("tp.cod_tipo_documento_pedido") == "200"))
        .select(
            col("tp.id_pais"),
            col("tp.id_pedido"),
            col("tp.id_periodo"),
            col("tp.id_sucursal"),
            col("tp.id_cliente"),
            col("tpd.id_articulo"),
            col("tp.id_vendedor"),
            col("tp.id_supervisor"),
            col("tp.id_fuerza_venta"),
            col("tp.id_modelo_atencion"),
            col("tp.id_origen_pedido"),
            col("tp.fecha_pedido"),
            col("tp.cod_tipo_atencion"),
            col("tp.id_visita").alias('id_visita_pedido'),
            col("tp.id_cliente").alias('id_cliente_pedido')
        )
    )

    df_fact_kpi_detalle_venta = (
        df_t_pedido.alias("tp")
        .join(
            df_t_venta.alias("tv"),
            (col("tp.id_pedido") == col("tv.id_pedido")),
            "inner",
        )
        .join(
            df_t_venta_detalle.alias("tvd"),
            (col("tv.id_venta") == col("tvd.id_venta")),
            "inner",
        )
        .join(
            df_m_tipo_venta.alias("mtv"),
            (col("tv.id_tipo_venta") == col("mtv.id_tipo_venta"))
            & (upper(col("mtv.cod_tipo_operacion")).isin(['VEN', 'PRO'])),
            "inner",
        )
        .where(
            (col("tp.cod_tipo_documento_pedido") == "200")
            & (col("tv.es_eliminado") == 0)
            & (~col("tv.cod_documento_venta").isin(['CMD', 'RMD']))
        )
        .select(
            col("tv.id_pais"),
            col("tv.id_sucursal"),
            col("tp.id_periodo"),
            col("tv.id_pedido"),
            col("tv.id_cliente"),
            col("tvd.id_producto"),
            col("tvd.cant_caja_fisica_ven").alias('cant_cajafisica_vta'),
            (col("tvd.cant_caja_volumen_ven")/30).alias('cant_cajaunitaria_vta'),
            col("tvd.cant_caja_fisica_pro").alias('cant_cajafisica_pro'),
            (col("tvd.cant_caja_volumen_pro")/30).alias('cant_cajaunitaria_pro'),
            col("tvd.imp_neto_vta_mn"),
            col("tvd.imp_neto_vta_me"),
            col("tvd.imp_bruto_vta_mn"),
            col("tvd.imp_bruto_vta_me"),
            col("tv.id_cliente").alias('id_cliente_venta'),
        )
    )

    df_venta_pedido_resumen = (
        df_fact_kpi_detalle_pedido.alias("tp")
        .join(
            df_fact_kpi_detalle_venta.alias("tv"),
            (col("tp.id_pedido") == col("tv.id_pedido"))
            & (col("tp.id_articulo") == col("tv.id_producto")),
            "full",
        )
        .select(
            coalesce(col('tp.id_pais'),col('tv.id_pais')).alias('id_pais'),
            coalesce(col('tp.id_periodo'),col('tv.id_periodo')).alias('id_periodo'),
            coalesce(col('tp.id_pedido'),col('tv.id_pedido')).alias('id_pedido'),
            coalesce(col('tp.id_sucursal'),col('tv.id_sucursal')).alias('id_sucursal'),
            coalesce(col('tp.id_cliente'),col('tv.id_cliente')).alias('id_cliente'),
            coalesce(col('tp.id_articulo'),col('tv.id_producto')).alias('id_articulo'),
            col('tp.id_vendedor'),
            col('tp.id_supervisor'),
            col('tp.id_fuerza_venta'),
            col('tp.id_modelo_atencion'),
            col('tp.id_origen_pedido'),
            col('tp.fecha_pedido'),
            col('tp.id_visita_pedido').alias('id_visita'),
            col('tp.id_visita_pedido'),
            col('tp.id_cliente_pedido'),
            col('tp.cod_tipo_atencion'),
            col('tp.id_visita_pedido').alias('id_visita_venta'),
            col('tv.id_cliente_venta'),
            col('tv.cant_cajafisica_vta'),
            col('tv.cant_cajaunitaria_vta'),
            col('tv.cant_cajafisica_pro'),
            col('tv.cant_cajaunitaria_pro'),
            col('tv.imp_neto_vta_mn'),
            col('tv.imp_neto_vta_me'),
            col('tv.imp_bruto_vta_mn'),
            col('tv.imp_bruto_vta_me')
        )
    )
    
    df_fact_kpi_detalle = (
        df_t_visita.alias("tv")
        .join(
            df_venta_pedido_resumen.alias("tvpr"),
            (col("tvpr.id_visita") == col("tv.id_visita")),
            "full",
        )
        .select(
            coalesce(col('tv.id_pais'),col('tvpr.id_pais')).cast("string").alias('id_pais'),
            coalesce(col('tv.id_periodo'),col('tvpr.id_periodo')).cast("string").alias('id_periodo'),
            coalesce(col('tv.id_sucursal'),col('tvpr.id_sucursal')).cast("string").alias('id_sucursal'),
            coalesce(col('tv.id_cliente'),col('tvpr.id_cliente')).cast("string").alias('id_cliente'),
            col('tvpr.id_articulo').cast("string").alias('id_producto'),
            col('tvpr.id_vendedor').cast("string"),
            col('tvpr.id_supervisor').cast("string"),
            col('tvpr.id_fuerza_venta').cast("string"),
            col('tvpr.id_modelo_atencion').cast("string"),
            col('tvpr.id_origen_pedido').cast("string"),
            coalesce(col('tvpr.fecha_pedido'),col('tv.fecha_visita')).cast("date").alias('fecha_pedido'),
            col('tvpr.cod_tipo_atencion').cast("string"),
            coalesce(col('tv.id_visita'),col('tvpr.id_pedido')).cast("string").alias('id_visita'),
            col('tvpr.id_visita_pedido').cast("string"),
            col('tvpr.id_visita_venta').cast("string"),
            col('tv.id_cliente').cast("string").alias('id_cliente_visita'),
            coalesce(col('tv.id_cliente'),col('tvpr.id_cliente_pedido')).cast("string").alias('id_cliente_visita_pedido'),
            coalesce(col('tv.id_cliente'),col('tvpr.id_cliente_venta')).cast("string").alias('id_cliente_visita_venta'),
            col('tvpr.cant_cajafisica_vta').cast("numeric(38,12)"),
            col('tvpr.cant_cajaunitaria_vta').cast("numeric(38,12)"),
            col('tvpr.cant_cajafisica_pro').cast("numeric(38,12)"),
            col('tvpr.cant_cajaunitaria_pro').cast("numeric(38,12)"),
            col('tvpr.imp_neto_vta_mn').cast("numeric(38,12)"),
            col('tvpr.imp_neto_vta_me').cast("numeric(38,12)"),
            col('tvpr.imp_bruto_vta_mn').cast("numeric(38,12)"),
            col('tvpr.imp_bruto_vta_me').cast("numeric(38,12)")
        )
    )
    
    partition_columns_array = ["id_pais", "id_periodo"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.write_table(df_fact_kpi_detalle, data_paths.ANALYTICS, target_table_name, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")     
except Exception as e:
    logger.error(f"Error processing df_fact_kpi_detalle: {e}")
    raise ValueError(f"Error processing df_fact_kpi_detalle: {e}") 