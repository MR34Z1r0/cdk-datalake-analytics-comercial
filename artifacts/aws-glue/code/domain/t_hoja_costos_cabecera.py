from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, date_format, lit, substring, floor,round

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")
    periodos= spark_controller.get_periods()
    logger.info(periodos)

    m_compania = spark_controller.read_table(data_paths.BIG_BAGIC, "m_compania", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.BIG_BAGIC, "m_pais", cod_pais=cod_pais)
    m_articulo = spark_controller.read_table(data_paths.BIG_BAGIC, "m_articulo", cod_pais=cod_pais)
    m_moneda = spark_controller.read_table(data_paths.BIG_BAGIC, "m_moneda", cod_pais=cod_pais)
    m_tipo_cambio = spark_controller.read_table(data_paths.BIG_BAGIC, "m_tipo_cambio", cod_pais=cod_pais)
    m_lista_precio_costo_estandard = spark_controller.read_table(data_paths.BIG_BAGIC, "m_lista_precio_costo_estandard", cod_pais=cod_pais)
    m_lista_precio_costo_estandar_detalle = spark_controller.read_table(data_paths.BIG_BAGIC, "m_lista_precio_costo_estandar_detalle", cod_pais=cod_pais)
    t_hoja_costos_cabecera = spark_controller.read_table(data_paths.BIG_BAGIC, "t_hoja_costos_cabecera", cod_pais=cod_pais)

    m_categoria_compra_dom = spark_controller.read_table(data_paths.DOMINIO, "m_categoria_compra").where(col("id_pais").isin(cod_pais))
    t_orden_produccion_cabecera = spark_controller.read_table(data_paths.BIG_BAGIC, "t_orden_produccion_cabecera", cod_pais=cod_pais)
    m_turno = spark_controller.read_table(data_paths.BIG_BAGIC, "m_turno", cod_pais=cod_pais)
    m_equipo_insumo = spark_controller.read_table(data_paths.BIG_BAGIC, "m_equipo_insumo", cod_pais=cod_pais)


    target_table_name = "t_hoja_costos_cabecera"
except Exception as e:
    logger.error(e)
    raise

try:
    m_pais = m_pais.filter(col("id_pais").isin(cod_pais))
    m_tipo_cambio = m_tipo_cambio.filter((col("fecha").isNotNull()) & (col("tc_compra") > 0) & (col("tc_venta") > 0))
    m_categoria_compra_dom = m_categoria_compra_dom.filter(col("id_pais").isin(cod_pais))
    t_orden_produccion_cabecera = t_orden_produccion_cabecera.filter((col("estado") != "A") & (date_format(col("fecha_inicio"), "yyyyMM").isin(periodos)))
    t_hoja_costos_cabecera = t_hoja_costos_cabecera.filter(col("processperiod").isin(periodos))

    m_lista_precio_costo_estandar_detalle = (
        m_lista_precio_costo_estandar_detalle.alias("mlpced")
        .join(m_articulo.alias("ma"), (col("mlpced.cod_compania") == col("ma.cod_compania")) & (col("mlpced.cod_articulo") == col("ma.cod_articulo")), "inner")
        .join(m_lista_precio_costo_estandard.alias("mlpce"), 
            (col("mlpced.cod_compania") == col("mlpce.cod_compania")) 
            & (col("mlpced.cod_sucursal") == col("mlpce.cod_sucursal")) 
            & (col("mlpced.anio") == col("mlpce.anio")) 
            & (col("mlpced.cod_lista_precio") == col("mlpce.cod_lista_precio")), 
            "inner"
        )
        .select(col("mlpced.*"))
    )
    
    tmp_dominio_t_hoja_costos_cabecera = (
        t_hoja_costos_cabecera.alias("thcc")
        .join(t_orden_produccion_cabecera.alias("topc"), (col("thcc.cod_compania") == col("topc.cod_compania")) & (col("thcc.cod_sucursal") == col("topc.cod_sucursal")) & (col("thcc.nro_operacion") == col("topc.nro_operacion")), "inner")
        .join(m_turno.alias("mt"), (col("topc.cod_compania") == col("mt.cod_compania")) & (col("topc.cod_sucursal") == col("mt.cod_sucursal")) & (col("topc.cod_turno") == col("mt.cod_turno")), "inner")
        .join(m_equipo_insumo.alias("mei"), (col("topc.cod_compania") == col("mei.cod_compania")) & (col("topc.cod_sucursal") == col("mei.cod_sucursal")) & (col("topc.cod_linea_equipo") == col("mei.cod_equipo_insumo")) & (col("topc.cod_familia_equipo") == col("mei.cod_familia_equipo")) , "inner")
        .join(m_compania.alias("mc"), col("thcc.cod_compania") == col("mc.cod_compania"), "inner")
        .join(m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .join(m_categoria_compra_dom.alias("mcc"), col("thcc.id_articulo") == col("mcc.id_articulo"), "left")
        .join(m_articulo.alias("ma"), col("thcc.id_articulo") == col("ma.id_articulo"), "inner")
        .join(m_moneda.alias("mm"), (col("thcc.cod_compania") == col("mm.cod_compania")) & (col("mm.flg_procedencia") == 'L'), "left")
        .join(m_tipo_cambio.alias("mtc"), (col("mm.cod_compania") == col("mtc.cod_compania")) & (col("mm.cod_moneda") == col("mtc.cod_moneda")) & (col("thcc.fecha_liquidacion") == col("mtc.fecha")), "inner")
        .join(m_lista_precio_costo_estandar_detalle.alias("mlpced"), (col("thcc.id_sucursal") == col("mlpced.id_sucursal")) & (col("thcc.id_articulo") == col("mlpced.id_articulo")) & (substring(col("thcc.fecha_liquidacion"), 1, 4) == col("mlpced.anio")) & (col("mlpced.cod_lista_precio") == '1'), "left")
        .select(
            col("mp.id_pais"),
            col("thcc.processperiod").alias("id_periodo"),
            col("thcc.id_compania"),
            col("thcc.id_sucursal"),
            col("thcc.id_articulo"),
            col("thcc.id_centro_costo"),
            col("thcc.nro_operacion"),
            col("thcc.fecha_liquidacion"),
            col("ma.unidad_manejo").alias("cod_unidad_manejo"),
            col("mcc.cod_unidad_global").alias("cod_unidad_medida"),
            col("thcc.cantidad_girada").alias("cant_girada"),
            (round(col("thcc.qfabcpmfin"),4)).alias("precio_cpm_mn"),
            (round(col("thcc.qfabcpmfin"),4) / (floor(col("mtc.tc_venta") * 10000) / 10000)).alias("precio_cpm_me"),
            (floor(col("mlpced.precio") * 10000) / 10000).alias("precio_std_mn"),
            (floor(col("mlpced.precio") * 10000) / 10000 / (floor(col("mtc.tc_venta") * 10000) / 10000)).alias("precio_std_me"),
            col("thcc.usuario_creacion"),
            col("thcc.fecha_creacion"),
            col("thcc.usuario_modificacion"),
            col("thcc.fecha_modificacion"),
            lit("1").alias("es_eliminado"),
        )
    )

    tmp = tmp_dominio_t_hoja_costos_cabecera.select(
            col("id_pais").cast("string").alias("id_pais"),
            col("id_periodo").cast("string").alias("id_periodo"),
            col("id_compania").cast("string").alias("id_compania"),
            col("id_sucursal").cast("string").alias("id_sucursal"),
            col("id_articulo").cast("string").alias("id_articulo"),
            col("id_centro_costo").cast("string").alias("id_centro_costo"),
            col("nro_operacion").cast("string").alias("nro_operacion"),
            col("fecha_liquidacion").cast("date").alias("fecha_liquidacion"),
            col("cod_unidad_manejo").cast("string").alias("cod_unidad_manejo"),
            col("cod_unidad_medida").cast("string").alias("cod_unidad_medida"),
            col("cant_girada").cast("numeric(38, 12)").alias("cant_girada"),
            col("precio_cpm_mn").cast("numeric(38, 12)").alias("precio_cpm_mn"),
            col("precio_cpm_me").cast("numeric(38, 12)").alias("precio_cpm_me"),
            col("precio_std_mn").cast("numeric(38, 12)").alias("precio_std_mn"),
            col("precio_std_me").cast("numeric(38, 12)").alias("precio_std_me"),
            col("usuario_creacion").cast("string").alias("usuario_creacion"),
            col("fecha_creacion").cast("timestamp").alias("fecha_creacion"),
            col("usuario_modificacion").cast("string").alias("usuario_modificacion"),
            col("fecha_modificacion").cast("timestamp").alias("fecha_modificacion"),
            col("es_eliminado").cast("int").alias("es_eliminado"),
    )

    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(tmp, data_paths.DOMINIO, target_table_name, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise