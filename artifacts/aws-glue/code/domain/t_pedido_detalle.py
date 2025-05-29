from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat, concat_ws, lit, coalesce, when, date_format, round, trim, to_date, substring, lower, to_timestamp, row_number, max, sum, upper
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "t_pedido_detalle"
try:
    PERIODOS= spark_controller.get_periods()
    cod_pais = COD_PAIS.split(",")
    logger.info(f"Databases: {cod_pais}")

    df_m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", cod_pais=cod_pais, have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania", cod_pais=cod_pais)
    df_m_parametro = spark_controller.read_table(data_paths.APDAYC, "m_parametro", cod_pais=cod_pais)
    df_m_articulo = spark_controller.read_table(data_paths.APDAYC, "m_articulo", cod_pais=cod_pais)
    df_m_procedimiento = spark_controller.read_table(data_paths.APDAYC, "m_procedimiento", cod_pais=cod_pais)
    df_t_historico_pedido_detalle = spark_controller.read_table(data_paths.APDAYC, "t_documento_pedido_detalle", cod_pais=cod_pais)
    df_t_historico_pedido_ades_detalle = spark_controller.read_table(data_paths.APDAYC, "t_documento_pedido_ades_detalle", cod_pais=cod_pais) 

    logger.info("Dataframes cargados correctamente")
except Exception as e:
    logger.error(str(e))
    raise

try:
    logger.info("starting filter of pais and periodo") 
    df_t_historico_pedido_detalle = df_t_historico_pedido_detalle.filter(date_format(col("fecha_pedido"), "yyyyMM").isin(PERIODOS))
    df_t_historico_pedido_ades_detalle = df_t_historico_pedido_ades_detalle.filter(date_format(col("fecha_pedido"), "yyyyMM").isin(PERIODOS))

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
        .select(col("mc.cod_compania"), trim(col("mp.id_pais")).alias("id_pais"), trim(col("mc.cod_pais")).alias("cod_pais"),trim(col("mpar.cod_moneda_mn")).alias("moneda_mn"))
    )

    logger.info("Starting creation of df_t_historico_pedido_detalle_filter")
    df_t_historico_pedido_detalle_filter = (
        df_t_historico_pedido_detalle.alias("thvd")
        .join(
            df_m_compania.alias("mc"),
            col("thvd.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .where((col("cod_documento_pedido") == "200"))
        .select(
            col("mc.id_pais"),
            date_format(col("thvd.fecha_pedido"), "yyyyMM").alias("id_periodo"),
            col("thvd.cod_compania"),
            col("thvd.cod_sucursal"),
            col("thvd.cod_almacen").alias("cod_almacen_emisor"),
            col("thvd.cod_documento_pedido").alias("cod_documento_transaccion"),
            col("thvd.nro_documento_pedido").alias("nro_comprobante"),
            col("thvd.cod_cliente"),
            col("thvd.cod_articulo"),
            col("thvd.cod_procedimiento"),
            col("thvd.id_salesforce"),
            col("thvd.fecha_pedido"),
            col("thvd.cant_paquete").alias("cantidad_cajas"),
            col("thvd.cant_unidad").alias("cantidad_botellas"),
            col("thvd.cant_paquete_asignado").alias("cantidad_cajas_asignada"),
            col("thvd.cant_unidad_asignado").alias("cantidad_botellas_asignada"),
            col("thvd.fecha_creacion"),
            col("thvd.fecha_modificacion"),
            lit(0).alias("es_eliminado"),
        )
    )

    logger.info("Starting creation of df_t_historico_pedido_ades_detalle_filter")
    df_t_historico_pedido_ades_detalle_filter = (
        df_t_historico_pedido_ades_detalle.alias("thvd")
        .join(
            df_m_compania.alias("mc"),
            col("thvd.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .where(col("cod_documento_transaccion").isin(["200", "300"]))
        .select(
            col("mc.id_pais"),
            date_format(col("thvd.fecha_pedido"), "yyyyMM").alias("id_periodo"),
            col("thvd.cod_compania"),
            col("thvd.cod_sucursal"),
            col("thvd.cod_almacen_emisor"),
            col("thvd.cod_documento_transaccion"),
            col("thvd.nro_comprobante"),
            col("thvd.cod_cliente"),
            col("thvd.cod_articulo"),
            col("thvd.cod_procedimiento"),
            col("thvd.id_salesforce"),
            col("thvd.fecha_pedido"),
            col("thvd.cantidad_cajas"),
            col("thvd.cantidad_botellas"),
            col("thvd.cantidad_cajas_asignada"),
            col("thvd.cantidad_botellas_asignada"),
            col("thvd.fecha_creacion").alias("fecha_creacion"),
            col("thvd.fecha_modificacion"),
            lit(0).alias("es_eliminado"),
        )
    )

    logger.info("Starting creation of df_t_historico_pedido_detalle_group")
    df_t_historico_pedido_detalle_group = (
        df_t_historico_pedido_detalle_filter.alias("thvd")
        .groupby(
            col("thvd.cod_compania"),
            col("thvd.cod_sucursal"),
            col("thvd.cod_almacen_emisor"),
            col("thvd.cod_documento_transaccion"),
            col("thvd.nro_comprobante"),
            col("thvd.cod_cliente"),
            col("thvd.cod_articulo"),
            col("thvd.cod_procedimiento"),
        )
        .agg(
            max(col("thvd.id_pais")).alias("id_pais"),
            max(col("thvd.id_periodo")).alias("id_periodo"),
            max(col("thvd.id_salesforce")).alias("id_salesforce"),
            max(col("thvd.fecha_pedido")).alias("fecha_pedido"), 
            sum(col("thvd.cantidad_cajas")).alias("cantidad_cajas"), 
            sum(col("thvd.cantidad_botellas")).alias("cantidad_botellas"), 
            sum(col("thvd.cantidad_cajas_asignada")).alias("cantidad_cajas_asignada"), 
            sum(col("thvd.cantidad_botellas_asignada")).alias("cantidad_botellas_asignada"), 
            max(col("thvd.fecha_creacion")).alias("fecha_creacion"),
            max(col("thvd.fecha_modificacion")).alias("fecha_modificacion"),
            max(col("thvd.es_eliminado")).alias("es_eliminado"),
        )
        .select(
            col("id_pais"),
            col("id_periodo"),
            col("thvd.cod_compania"),
            col("thvd.cod_sucursal"),
            col("thvd.cod_almacen_emisor"),
            col("thvd.cod_documento_transaccion"),
            col("thvd.nro_comprobante"),
            col("thvd.cod_cliente"),
            col("thvd.cod_articulo"),
            col("thvd.cod_procedimiento"),
            col("id_salesforce"),
            col("fecha_pedido"),
            col("cantidad_cajas"),
            col("cantidad_botellas"),
            col("cantidad_cajas_asignada"),
            col("cantidad_botellas_asignada"),
            col("fecha_creacion"),
            col("fecha_modificacion"),
            col("es_eliminado"),
        )
    )

    logger.info("Starting creation of df_t_historico_pedido_ades_detalle_filter")
    df_t_historico_pedido_ades_detalle_group = (
        df_t_historico_pedido_ades_detalle_filter.alias("thvd")
        .groupby(
            col("thvd.cod_compania"),
            col("thvd.cod_sucursal"),
            col("thvd.cod_almacen_emisor"),
            col("thvd.cod_documento_transaccion"),
            col("thvd.nro_comprobante"),
            col("thvd.cod_cliente"),
            col("thvd.cod_articulo"),
            col("thvd.cod_procedimiento"),
        )
        .agg(
            max(col("thvd.id_pais")).alias("id_pais"),
            max(col("thvd.id_periodo")).alias("id_periodo"),
            max(col("thvd.id_salesforce")).alias("id_salesforce"),
            max(col("thvd.fecha_pedido")).alias("fecha_pedido"), 
            sum(col("thvd.cantidad_cajas")).alias("cantidad_cajas"), 
            sum(col("thvd.cantidad_botellas")).alias("cantidad_botellas"), 
            sum(col("thvd.cantidad_cajas_asignada")).alias("cantidad_cajas_asignada"), 
            sum(col("thvd.cantidad_botellas_asignada")).alias("cantidad_botellas_asignada"), 
            max(col("thvd.fecha_creacion")).alias("fecha_creacion"),
            max(col("thvd.fecha_modificacion")).alias("fecha_modificacion"),
            max(col("thvd.es_eliminado")).alias("es_eliminado"),
        )
        .select(
            col("id_pais"),
            col("id_periodo"),
            col("thvd.cod_compania"),
            col("thvd.cod_sucursal"),
            col("thvd.cod_almacen_emisor"),
            col("thvd.cod_documento_transaccion"),
            col("thvd.nro_comprobante"),
            col("thvd.cod_cliente"),
            col("thvd.cod_articulo"),
            col("thvd.cod_procedimiento"),
            col("id_salesforce"),
            col("fecha_pedido"),
            col("cantidad_cajas"),
            col("cantidad_botellas"),
            col("cantidad_cajas_asignada"),
            col("cantidad_botellas_asignada"),
            col("fecha_creacion"),
            col("fecha_modificacion"),
            col("es_eliminado"),
        )
    )

    logger.info("Starting creation of df_t_historico_pedido_ades_detalle_left_anti")
    df_t_historico_pedido_ades_detalle_left_anti = (
        df_t_historico_pedido_ades_detalle_group.alias("a")
        .join(df_t_historico_pedido_detalle_group.alias("b"), (
                (col("a.cod_compania") == col("b.cod_compania"))
                & (col("a.cod_sucursal") == col("b.cod_sucursal"))
                & (col("a.cod_almacen_emisor") == col("b.cod_almacen_emisor"))
                & (col("a.cod_documento_transaccion") == col("b.cod_documento_transaccion"))
                & (col("a.nro_comprobante") == col("b.nro_comprobante"))
                & (col("a.cod_articulo") == col("b.cod_articulo"))
                & (col("a.cod_procedimiento") == col("b.cod_procedimiento"))
            ), "left_anti"
        )
    )

    logger.info("Starting creation of df_t_historico_pedido_detalle_union")
    df_t_historico_pedido_detalle_union = df_t_historico_pedido_detalle_group.unionByName(df_t_historico_pedido_ades_detalle_left_anti)
    
    logger.info("Starting creation of df_t_historico_pedido_detalle_articulo")
    df_t_historico_pedido_detalle_articulo = (
        df_t_historico_pedido_detalle_union.alias("dpd")
        .join(
            df_m_articulo.alias("ma"),
            (col("dpd.cod_compania") == col("ma.cod_compania"))
            & (col("dpd.cod_articulo") == col("ma.cod_articulo")),
            "inner",
        )
        .join(
            df_m_procedimiento.alias("mp"),
            (col("dpd.cod_compania") == col("mp.cod_compania"))
            & (col("dpd.cod_documento_transaccion")== col("mp.cod_documento_transaccion"))
            & (col("dpd.cod_procedimiento") == col("mp.cod_procedimiento")),
            "inner",
        )
        .where(upper(col("mp.id_tipo_operacion")).isin(["VEN", "PRO", "EXP", "OBS"]))
        .groupby(
            col("dpd.cod_compania"),
            col("dpd.cod_sucursal"),
            col("dpd.cod_almacen_emisor"),
            col("dpd.cod_documento_transaccion"),
            col("dpd.nro_comprobante"),
            col("dpd.cod_cliente"),
            col("dpd.cod_articulo"),
        )
        .agg(
            max(col("dpd.id_pais")).alias("id_pais"),
            max(col("dpd.id_periodo")).alias("id_periodo"),
            max(col("dpd.fecha_pedido")).alias("fecha_pedido"),
            max(col("ma.cant_unidad_paquete")).alias("cant_contenido"),
            max(col("ma.cant_unidad_volumen")).alias("cantidad_total"),
            sum(
                (when(upper(col("mp.id_tipo_operacion")) != "PRO", 1).otherwise(0))
                * (
                    when(
                        upper(col("dpd.cod_documento_transaccion")) == "NCC", -1
                    ).otherwise(1)
                )
                * (
                    col("dpd.cantidad_cajas")
                    + (col("dpd.cantidad_botellas") / col("ma.cant_unidad_paquete"))
                )
                * (col("ma.cant_paquete_caja"))
            ).alias("cant_cajafisica_ped"),
            sum(
                (when(upper(col("mp.id_tipo_operacion")) != "PRO", 1).otherwise(0))
                * (
                    when(
                        upper(col("dpd.cod_documento_transaccion")) == "NCC", -1
                    ).otherwise(1)
                )
                * (
                    col("dpd.cantidad_cajas") * col("ma.cant_unidad_paquete")
                    + col("dpd.cantidad_botellas")
                )
                * (col("ma.cant_unidad_volumen"))
            ).alias("cant_cajavolumen_ped"),
            sum(
                (when(upper(col("mp.id_tipo_operacion")) == "PRO", 1).otherwise(0))
                * (
                    when(
                        upper(col("dpd.cod_documento_transaccion")) == "NCC", -1
                    ).otherwise(1)
                )
                * (
                    col("dpd.cantidad_cajas")
                    + (col("dpd.cantidad_botellas") / col("ma.cant_unidad_paquete"))
                )
                * (col("ma.cant_paquete_caja"))
            ).alias("cant_cajafisica_ped_pro"),
            sum(
                (when(upper(col("mp.id_tipo_operacion")) == "PRO", 1).otherwise(0))
                * (
                    when(
                        upper(col("dpd.cod_documento_transaccion")) == "NCC", -1
                    ).otherwise(1)
                )
                * (
                    col("dpd.cantidad_cajas") * col("ma.cant_unidad_paquete")
                    + col("dpd.cantidad_botellas")
                )
                * (col("ma.cant_unidad_volumen"))
            ).alias("cant_cajavolumen_ped_pro"),
            sum(
                (when(upper(col("mp.id_tipo_operacion")) != "PRO", 1).otherwise(0))
                * (
                    when(
                        upper(col("dpd.cod_documento_transaccion")) == "NCC", -1
                    ).otherwise(1)
                )
                * (
                    col("dpd.cantidad_cajas_asignada")
                    + (
                        col("dpd.cantidad_botellas_asignada")
                        / col("ma.cant_unidad_paquete")
                    )
                )
                * (col("ma.cant_paquete_caja"))
            ).alias("cant_cajafisica_asignado_ped"),
            sum(
                (when(upper(col("mp.id_tipo_operacion")) != "PRO", 1).otherwise(0))
                * (
                    when(
                        upper(col("dpd.cod_documento_transaccion")) == "NCC", -1
                    ).otherwise(1)
                )
                * (
                    col("dpd.cantidad_cajas_asignada") * col("ma.cant_unidad_paquete")
                    + col("dpd.cantidad_botellas_asignada")
                )
                * (col("ma.cant_unidad_volumen"))
            ).alias("cant_cajavolumen_asignado_ped"),
            sum(
                (when(upper(col("mp.id_tipo_operacion")) == "PRO", 1).otherwise(0))
                * (
                    when(
                        upper(col("dpd.cod_documento_transaccion")) == "NCC", -1
                    ).otherwise(1)
                )
                * (
                    col("dpd.cantidad_cajas_asignada")
                    + (
                        col("dpd.cantidad_botellas_asignada")
                        / col("ma.cant_unidad_paquete")
                    )
                )
                * (col("ma.cant_paquete_caja"))
            ).alias("cant_cajafisica_asignado_ped_pro"),
            sum(
                (when(upper(col("mp.id_tipo_operacion")) == "PRO", 1).otherwise(0))
                * (
                    when(
                        upper(col("dpd.cod_documento_transaccion")) == "NCC", -1
                    ).otherwise(1)
                )
                * (
                    col("dpd.cantidad_cajas_asignada") * col("ma.cant_unidad_paquete")
                    + col("dpd.cantidad_botellas_asignada")
                )
                * (col("ma.cant_unidad_volumen"))
            ).alias("cant_cajavolumen_asignado_ped_pro"),
            max(col("dpd.fecha_creacion")).alias("fecha_creacion"),
            max(col("dpd.fecha_modificacion")).alias("fecha_modificacion"),
            max(col("dpd.es_eliminado")).alias("es_eliminado"),
        )
        .select(
            col("id_pais"),
            col("id_periodo"),
            col("dpd.cod_compania"),
            col("dpd.cod_sucursal"),
            col("dpd.cod_almacen_emisor"),
            col("dpd.cod_documento_transaccion"),
            col("dpd.nro_comprobante"),
            col("dpd.cod_cliente"),
            col("dpd.cod_articulo"),
            col("fecha_pedido"),
            col("cant_contenido"),
            col("cantidad_total"),
            col("cant_cajafisica_ped"),
            col("cant_cajavolumen_ped"),
            col("cant_cajafisica_ped_pro"),
            col("cant_cajavolumen_ped_pro"),
            col("cant_cajafisica_asignado_ped"),
            col("cant_cajavolumen_asignado_ped"),
            col("cant_cajafisica_asignado_ped_pro"),
            col("cant_cajavolumen_asignado_ped_pro"),
            col("fecha_creacion"),
            col("fecha_modificacion"),
            col("es_eliminado"),
        )
    )
    
    logger.info("Starting creation of df_t_pedido_detalle")
    df_t_pedido_detalle = (
        df_t_historico_pedido_detalle_articulo.alias("dcja") 
        .select(
            col("dcja.id_pais").cast("string").alias("id_pais"),
            date_format(col("dcja.fecha_pedido"), "yyyyMM").alias("id_periodo"),
            concat(trim(col("dcja.cod_compania")), lit("|"), trim(col("dcja.cod_sucursal")), lit("|"), trim(col("dcja.cod_documento_transaccion")), lit("|"), trim(col("dcja.nro_comprobante")), lit("|"), trim(col("dcja.cod_cliente"))).alias("id_pedido"),
            concat(trim(col("dcja.cod_compania")), lit("|"), trim(col("dcja.cod_articulo"))).alias("id_articulo"), 
            col("dcja.cant_cajafisica_ped").cast("decimal(38,12)").alias("cant_cajafisica_ped"),
            col("dcja.cant_cajavolumen_ped").cast("decimal(38,12)").alias("cant_cajavolumen_ped"),
            col("dcja.cant_cajafisica_ped_pro").cast("decimal(38,12)").alias("cant_cajafisica_ped_pro"),
            col("dcja.cant_cajavolumen_ped_pro").cast("decimal(38,12)").alias("cant_cajavolumen_ped_pro"),
            col("dcja.cant_cajafisica_asignado_ped").cast("decimal(38,12)").alias("cant_cajafisica_asignado_ped"),
            col("dcja.cant_cajavolumen_asignado_ped").cast("decimal(38,12)").alias("cant_cajavolumen_asignado_ped"),
            col("dcja.cant_cajafisica_asignado_ped_pro").cast("decimal(38,12)").alias("cant_cajafisica_asignado_ped_pro"),
            col("dcja.cant_cajavolumen_asignado_ped_pro").cast("decimal(38,12)").alias("cant_cajavolumen_asignado_ped_pro"),
            to_date(col("dcja.fecha_creacion"), "yyyy-MM-dd HH:mm:ss").alias(
                "fecha_creacion"
            ),
            to_date(col("dcja.fecha_modificacion"), "yyyy-MM-dd HH:mm:ss").alias(
                "fecha_modificacion"
            ),
            col("dcja.es_eliminado").cast("int").alias("es_eliminado")
        )
    ) 

    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(df_t_pedido_detalle, data_paths.DOMAIN, target_table_name, partition_columns_array)
except Exception as e:
    logger.error(e) 
    raise