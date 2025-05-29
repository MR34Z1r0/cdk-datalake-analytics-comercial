from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import coalesce, col, concat_ws, date_format, lit, sum

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")
    periodos= spark_controller.get_periods()
    logger.info(periodos)

    m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", cod_pais=cod_pais, have_principal=True)
    m_articulo = spark_controller.read_table(data_paths.APDAYC, "m_articulo", cod_pais=cod_pais)
    t_orden_produccion_cabecera = spark_controller.read_table(data_paths.APDAYC, "t_orden_produccion_cabecera", cod_pais=cod_pais)
    t_tiempo_produccion_vs_parada_detalle = spark_controller.read_table(data_paths.APDAYC, "t_tiempo_produccion_vs_parada_detalle", cod_pais=cod_pais)
    t_tiempo_produccion_vs_parada_cabecera = spark_controller.read_table(data_paths.APDAYC, "t_tiempo_produccion_vs_parada_cabecera", cod_pais=cod_pais)
    m_equipo_insumo = spark_controller.read_table(data_paths.APDAYC, "m_equipo_insumo", cod_pais=cod_pais)
    m_turno = spark_controller.read_table(data_paths.APDAYC, "m_turno", cod_pais=cod_pais)

    target_table_name = "t_orden_produccion"

except Exception as e:
    logger.error(e)
    raise

try:
    m_pais = m_pais.filter(col("id_pais").isin(cod_pais))
    t_orden_produccion_cabecera = t_orden_produccion_cabecera.filter((col("estado") != "A") & (date_format(col("fecha_inicio"), "yyyyMM").isin(periodos)))


    tmp_orden_produccion = (
        t_orden_produccion_cabecera.alias("topc")
        .join(m_equipo_insumo.alias("mei"), (col("topc.cod_compania") == col("mei.cod_compania")) & (col("topc.cod_sucursal") == col("mei.cod_sucursal")) & (col("topc.cod_linea_equipo") == col("mei.cod_equipo_insumo")) & (col("topc.cod_familia_equipo") == col("mei.cod_familia_equipo")) , "inner")
        .join(m_turno.alias("mt"), (col("topc.cod_compania") == col("mt.cod_compania")) & (col("topc.cod_sucursal") == col("mt.cod_sucursal")) & (col("topc.cod_turno") == col("mt.cod_turno")), "inner")
        .join(m_compania.alias("mc"), col("topc.cod_compania") == col("mc.cod_compania"), "inner")
        .join(m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .select(
            col("mp.id_pais"),
            date_format(col("topc.fecha_inicio"), "yyyyMM").alias("id_periodo"),
            col("topc.id_orden_produccion"),
            col("topc.id_compania"),
            col("topc.id_sucursal"),
            col("topc.id_turno"),
            col("topc.id_articulo"),
            col("topc.id_equipo_familia").alias("id_familia_equipo"),
            col("topc.id_almacen").alias("id_almacen_origen"),
            col("topc.nro_operacion"),
            col("topc.id_avail").alias("cod_avail"),
            col("topc.cod_linea_equipo"),
            col("topc.nro_correlativo").alias("cod_correlativo"),
            col("topc.nro_lote"),
            col("topc.linea_fabricacion").alias("desc_linea_fabricacion"),
            col("topc.botellas_por_minuto").alias("cant_unidades_por_minuto"),
            col("topc.cant_teorica").alias("cant_unidades_teorica"),
            col("topc.cant_girada").alias("cant_unidades_girada"),
            col("topc.cant_girada_cajas").alias("cant_cajafisica_girada"),
            col("topc.cant_fabricada").alias("cant_unidades_fabricada"),
            col("topc.cant_cajas_fabricadas").alias("cant_cajafisica_fabricada"),
            col("topc.cant_plan").alias("cant_cajafisica_plan"),
            col("topc.fecha_giro"),
            col("topc.fecha_inicio"),
            col("topc.fecha_fin"),
            col("topc.usuario_creacion"),
            col("topc.fecha_creacion"),
            col("topc.usuario_modificacion"),
            col("topc.fecha_modificacion"),
            lit("1").alias("es_eliminado"),
        )
    )

    tmp_tiempo_produccion_parada = (
        tmp_orden_produccion.alias("t")
        .join(t_tiempo_produccion_vs_parada_detalle.alias("tppd"), col("t.id_orden_produccion") == concat_ws("|", col("tppd.id_sucursal"), col("tppd.nro_operacion")), "inner")
        .join(t_tiempo_produccion_vs_parada_cabecera.alias("tppc"), 
                (col("tppd.id_sucursal") == col("tppc.id_sucursal")) 
                & (col("tppd.nro_plan") == col("tppc.nro_planilla")) 
                & (col("tppd.cod_turno") == col("tppc.cod_turno"))
                , "inner")
        .groupby(col("t.id_orden_produccion"))
        .agg(
            sum(coalesce(col("tppd.cant_paradas_por_linea"),lit(0))).alias("cant_paradas_por_linea"),
            sum(coalesce(col("tppd.cant_paradas_ajenas_linea"),lit(0))).alias( "cant_paradas_ajena_linea"),
            sum(coalesce(col("tppd.cant_tiempo_ideal"),lit(0))).alias("cant_tiempo_ideal"),
            sum(coalesce(col("tppd.cant_velocidad"),lit(0))).alias("cant_velocidad"),
            sum(coalesce(col("tppd.cant_jornada_laboral"),lit(0))).alias("cant_jornada_laboral"),
            (sum(coalesce(col("tppc.total_tiempo_bruto"),lit(0))) / 60).alias("total_tiempo_bruto"),
            (sum(coalesce(col("tppc.total_tiempo_ideal"),lit(0))) / 60).alias("total_tiempo_ideal")               
        )
        .select(
            col("t.id_orden_produccion"),
            col("cant_paradas_por_linea"),
            col("cant_paradas_ajena_linea"),
            col("cant_tiempo_ideal"),
            col("cant_velocidad"),
            col("cant_jornada_laboral"),
            col("total_tiempo_bruto"),
            col("total_tiempo_ideal")                
        )
    )

    tmp_orden_produccion_dominio = (
        tmp_orden_produccion.alias("t")
        .join(tmp_tiempo_produccion_parada.alias("ptp"), col("t.id_orden_produccion") == col("ptp.id_orden_produccion"), "left")
        .join(m_articulo.alias("art"), col("t.id_articulo") == col("art.id_articulo"), "left")
        .select(
            col("t.id_pais"),
            col("t.id_periodo"),
            col("t.id_orden_produccion"),
            col("t.id_compania"),
            col("t.id_sucursal"),
            col("t.id_turno"),
            col("t.id_articulo"),
            col("t.id_familia_equipo"),
            col("t.id_almacen_origen"),
            col("t.nro_operacion"),
            col("t.cod_avail"),
            col("t.cod_linea_equipo"),
            col("t.cod_correlativo"),
            col("t.nro_lote"),
            col("t.desc_linea_fabricacion"),
            col("t.cant_unidades_por_minuto"),
            col("t.cant_unidades_teorica"),
            col("t.cant_unidades_girada"),
            col("t.cant_cajafisica_girada"),
            col("t.cant_unidades_fabricada"),
            col("t.cant_cajafisica_fabricada"),
            col("t.cant_cajafisica_plan"),
            (col("t.cant_unidades_fabricada") * col("art.cant_unidad_volumen")).cast("numeric(38,12)").alias("cant_volumen_fabricado"),
            (col("t.cant_cajafisica_girada") * col("art.cant_unidad_volumen") * col("art.cant_unidad_paquete")).cast("numeric(38,12)").alias("cant_cajavolumen_girada"),
            (coalesce(col("ptp.cant_velocidad"),lit(0)) * col("art.cant_unidad_volumen")).cast("numeric(38,12)").alias("cant_velocidad_volumen"),
            coalesce(col("ptp.cant_jornada_laboral"),lit(0)).alias("cant_jornada_laboral"),
            coalesce(col("ptp.cant_paradas_por_linea"),lit(0)).alias("cant_paradas_por_linea"),
            coalesce(col("ptp.cant_paradas_ajena_linea"),lit(0)).alias("cant_paradas_ajena_linea"),
            coalesce(col("ptp.cant_tiempo_ideal"),lit(0)).alias("cant_tiempo_ideal"),
            coalesce(col("ptp.cant_velocidad"),lit(0)).alias("cant_velocidad"),
            (coalesce(col("ptp.cant_velocidad"),lit(0)) * 60).alias("cant_velocidad_hora"),
            coalesce(col("ptp.total_tiempo_bruto"),lit(0)).alias("total_tiempo_bruto"),
            coalesce(col("ptp.total_tiempo_ideal"),lit(0)).alias("total_tiempo_ideal"),
            col("t.fecha_inicio"),
            col("t.fecha_giro"),
            col("t.fecha_fin"),
            col("t.usuario_creacion"),
            col("t.fecha_creacion"),
            col("t.usuario_modificacion"),
            col("t.fecha_modificacion"),
            col("t.es_eliminado")
        )
    )

    tmp = tmp_orden_produccion_dominio.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_periodo").cast("string").alias("id_periodo"),
        col("id_orden_produccion").cast("string").alias("id_orden_produccion"),
        col("id_compania").cast("string").alias("id_compania"),
        col("id_sucursal").cast("string").alias("id_sucursal"),
        col("id_turno").cast("string").alias("id_turno"),
        col("id_articulo").cast("string").alias("id_articulo"),
        col("id_familia_equipo").cast("string").alias("id_familia_equipo"),
        col("id_almacen_origen").cast("string").alias("id_almacen_origen"),
        col("nro_operacion").cast("string").alias("nro_operacion"),
        col("cod_avail").cast("string").alias("cod_avail"),
        col("cod_linea_equipo").cast("string").alias("cod_linea_equipo"),
        col("cod_correlativo").cast("string").alias("cod_correlativo"),
        col("nro_lote").cast("string").alias("nro_lote"),
        col("desc_linea_fabricacion").cast("string").alias("desc_linea_fabricacion"),
        col("cant_unidades_por_minuto").cast("numeric(38,12)").alias("cant_unidades_por_minuto"),
        col("cant_unidades_teorica").cast("numeric(38,12)").alias("cant_unidades_teorica"),
        col("cant_unidades_girada").cast("numeric(38,12)").alias("cant_unidades_girada"),
        col("cant_cajafisica_girada").cast("numeric(38,12)").alias("cant_cajafisica_girada"),
        col("cant_unidades_fabricada").cast("numeric(38,12)").alias("cant_unidades_fabricada"),
        col("cant_cajafisica_fabricada").cast("numeric(38,12)").alias("cant_cajafisica_fabricada"),
        col("cant_cajafisica_plan").cast("numeric(38,12)").alias("cant_cajafisica_plan"),
        col("cant_volumen_fabricado").cast("numeric(38,12)").alias("cant_volumen_fabricado"),
        col("cant_cajavolumen_girada").cast("numeric(38,12)").alias("cant_cajavolumen_girada"),
        col("cant_velocidad_volumen").cast("numeric(38,12)").alias("cant_velocidad_volumen"),
        col("cant_jornada_laboral").cast("numeric(38,12)").alias("cant_jornada_laboral"),
        col("cant_paradas_por_linea").cast("numeric(38,12)").alias("cant_paradas_por_linea"),
        col("cant_paradas_ajena_linea").cast("numeric(38,12)").alias("cant_paradas_ajena_linea"),
        col("cant_tiempo_ideal").cast("numeric(38,12)").alias("cant_tiempo_ideal"),
        col("cant_velocidad").cast("numeric(38,12)").alias("cant_velocidad"),
        col("cant_velocidad_hora").cast("numeric(38,12)").alias("cant_velocidad_hora"),
        col("total_tiempo_bruto").cast("numeric(38,12)").alias("total_tiempo_bruto"),
        col("total_tiempo_ideal").cast("numeric(38,12)").alias("total_tiempo_ideal"),
        col("fecha_giro").cast("date").alias("fecha_giro"),
        col("fecha_inicio").cast("date").alias("fecha_inicio"),
        col("fecha_fin").cast("date").alias("fecha_fin"),
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
