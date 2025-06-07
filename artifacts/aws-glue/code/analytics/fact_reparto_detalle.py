from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col

spark_controller = SPARK_CONTROLLER()

try:
    PERIODOS= spark_controller.get_periods()
    logger.info(f"Periods: {PERIODOS}")

    t_pedido_detalle_cumplimiento = spark_controller.read_table(data_paths.DOMAIN, "t_pedido_detalle_cumplimiento")
    t_pedido = spark_controller.read_table(data_paths.DOMAIN, "t_pedido")
    t_reparto = spark_controller.read_table(data_paths.DOMAIN, "t_reparto")
    
    target_table_name = "fact_reparto_detalle"    
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:

    logger.info("Starting creation of tmp_fact_reparto_detalle")
    tmp_fact_reparto_detalle = (
        t_pedido_detalle_cumplimiento.alias("tpdc")
        .filter(col("tpdc.id_periodo").isin(PERIODOS))
        .join(t_pedido.alias("tp"), col("tp.id_pedido") == col("tpdc.id_pedido"), "left")
        .join(t_reparto.alias("tr"), col("tr.id_reparto") == col("tpdc.id_reparto"), "left")
        .select(
            col("tpdc.id_pais"),
            col("tpdc.id_periodo"),
            col("tpdc.id_reparto"),
            col("tpdc.id_pedido"),
            col("tpdc.id_producto"),
            col("tpdc.id_sucursal"),
            col("tr.id_transportista"),
            col("tr.id_chofer"),
            col("tr.id_medio_transporte"),
            col("tp.id_cliente"),
            # col("tp.id_modelo_atencion"),
            col("tp.id_origen_pedido"),
            col("tp.id_tipo_pedido"),
            col("tp.id_fuerza_venta"),
            col("tp.id_vendedor"),
            # col("tp.id_supervisor"),
            # col("tp.id_jefe_venta"),
            col("tp.id_lista_precio"),
            col("tp.id_forma_pago"),
            col("tp.desc_region"),
            col("tp.desc_subregion"),
            col("tp.desc_division"),
            col("tp.cod_zona"),
            col("tp.cod_ruta"),
            col("tp.cod_modulo"), 
            col("tp.nro_pedido_ref"), 
            col("tp.fecha_pedido"),
            col("tp.fecha_entrega"),
            col("tr.fecha_orden_carga"),
            col("tr.fecha_reparto").alias("fecha_movimiento_inventario"),
            col("tpdc.fecha_liquidacion"),
            col("tr.fecha_reparto").alias("fecha_almacen"),
            col("tp.nro_pedido"), 
            col("tr.estado_guia"),
            col("tpdc.cant_cajafisica_ped"),
            col("tpdc.cant_cajavolumen_ped"), 
            col("tpdc.cant_cajafisica_ped_pro"),
            col("tpdc.cant_cajavolumen_ped_pro"), 
            col("tpdc.cant_cajafisica_asignado_ped"),
            col("tpdc.cant_cajavolumen_asignado_ped"), 
            col("tpdc.cant_cajafisica_asignado_ped_pro"),
            col("tpdc.cant_cajavolumen_asignado_ped_pro"), 
            col("tpdc.cant_cajafisica_desp"),
            col("tpdc.cant_cajavolumen_desp"), 
            col("tpdc.cant_cajafisica_desp_pro"),
            col("tpdc.cant_cajavolumen_desp_pro"), 
            col("tpdc.cant_caja_fisica_ven").alias("cant_cajafisica_ven"), 
            col("tpdc.cant_caja_volumen_ven").alias("cant_cajavolumen_ven"),
            col("tpdc.cant_caja_fisica_pro").alias("cant_cajafisica_pro"),
            col("tpdc.cant_caja_volumen_pro").alias("cant_cajavolumen_pro"),
            col("tpdc.fecha_creacion"),
            col("tpdc.fecha_modificacion")
        )
    )

    df_fact_reparto_detalle = (
        tmp_fact_reparto_detalle
            .select(
                col("id_pais").cast("string").alias("id_pais"),
                col("id_periodo").cast("string").alias("id_periodo"),
                col("id_reparto").cast("string").alias("id_reparto"),
                col("id_pedido").cast("string").alias("id_pedido"),
                col("id_producto").cast("string").alias("id_producto"),
                col("id_sucursal").cast("string").alias("id_sucursal"),
                col("id_transportista").cast("string").alias("id_transportista"),
                col("id_chofer").cast("string").alias("id_chofer"),
                col("id_medio_transporte").cast("string").alias("id_medio_transporte"),
                col("id_cliente").cast("string").alias("id_cliente"),
                # col("id_modelo_atencion").cast("string").alias("id_modelo_atencion"),
                col("id_origen_pedido").cast("string").alias("id_origen_pedido"),
                col("id_tipo_pedido").cast("string").alias("id_tipo_pedido"),
                col("id_fuerza_venta").cast("string").alias("id_fuerza_venta"),
                col("id_vendedor").cast("string").alias("id_vendedor"),
                # col("id_supervisor").cast("string").alias("id_supervisor"),
                # col("id_jefe_venta").cast("string").alias("id_jefe_venta"),
                col("id_lista_precio").cast("string").alias("id_lista_precio"),
                col("id_forma_pago").cast("string").alias("id_forma_pago"),
                col("desc_region").cast("string").alias("desc_region"),
                col("desc_subregion").cast("string").alias("desc_subregion"),
                col("desc_division").cast("string").alias("desc_division"),
                col("cod_zona").cast("string").alias("cod_zona"),
                col("cod_ruta").cast("string").alias("cod_ruta"),
                col("cod_modulo").cast("string").alias("cod_modulo"),
                col("nro_pedido_ref").cast("string").alias("nro_pedido_ref"),
                col("fecha_pedido").cast("date").alias("fecha_pedido"),
                col("fecha_entrega").cast("date").alias("fecha_entrega"),
                col("fecha_orden_carga").cast("date").alias("fecha_orden_carga"),
                col("fecha_movimiento_inventario").cast("date").alias("fecha_movimiento_inventario"),
                col("fecha_liquidacion").cast("date").alias("fecha_liquidacion"),
                col("fecha_almacen").cast("date").alias("fecha_almacen"),
                col("nro_pedido").cast("string").alias("nro_pedido"),
                col("estado_guia").cast("string").alias("estado_guia"),
                col("cant_cajafisica_ped").cast("numeric(38,12)").alias("cant_cajafisica_ped"),
                col("cant_cajavolumen_ped").cast("numeric(38,12)").alias("cant_cajavolumen_ped"),
                col("cant_cajafisica_ped_pro").cast("numeric(38,12)").alias("cant_cajafisica_ped_pro"),
                col("cant_cajavolumen_ped_pro").cast("numeric(38,12)").alias("cant_cajavolumen_ped_pro"),
                col("cant_cajafisica_asignado_ped").cast("numeric(38,12)").alias("cant_cajafisica_asignado_ped"),
                col("cant_cajavolumen_asignado_ped").cast("numeric(38,12)").alias("cant_cajavolumen_asignado_ped"),
                col("cant_cajafisica_asignado_ped_pro").cast("numeric(38,12)").alias("cant_cajafisica_asignado_ped_pro"),
                col("cant_cajavolumen_asignado_ped_pro").cast("numeric(38,12)").alias("cant_cajavolumen_asignado_ped_pro"),
                col("cant_cajafisica_desp").cast("numeric(38,12)").alias("cant_cajafisica_desp"),
                col("cant_cajavolumen_desp").cast("numeric(38,12)").alias("cant_cajavolumen_desp"),
                col("cant_cajafisica_desp_pro").cast("numeric(38,12)").alias("cant_cajafisica_desp_pro"),
                col("cant_cajavolumen_desp_pro").cast("numeric(38,12)").alias("cant_cajavolumen_desp_pro"),
                col("cant_cajafisica_ven").cast("numeric(38,12)").alias("cant_cajafisica_ven"),
                col("cant_cajavolumen_ven").cast("numeric(38,12)").alias("cant_cajavolumen_ven"),
                col("cant_cajafisica_pro").cast("numeric(38,12)").alias("cant_cajafisica_pro"),
                col("cant_cajavolumen_pro").cast("numeric(38,12)").alias("cant_cajavolumen_pro"),
                col("fecha_creacion").cast("timestamp").alias("fecha_creacion"),
                col("fecha_modificacion").cast("timestamp").alias("fecha_modificacion"),
            )
        )

    partition_columns_array = ["id_pais", "id_periodo"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.write_table(df_fact_reparto_detalle, data_paths.ANALYTICS, target_table_name, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")     
except Exception as e:
    logger.error(f"Error processing df_fact_reparto_detalle: {e}")
    raise ValueError(f"Error processing df_fact_reparto_detalle: {e}") 