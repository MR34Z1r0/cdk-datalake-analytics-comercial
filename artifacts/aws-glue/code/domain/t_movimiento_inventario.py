from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import coalesce, col, date_format, lit, when

spark_controller = SPARK_CONTROLLER()
target_table_name = "t_movimiento_inventario"

try:
    cod_pais = COD_PAIS.split(",")
    periodos= spark_controller.get_periods()
    logger.info(periodos)

    m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", cod_pais=cod_pais, have_principal=True)
    m_documento_almacen = spark_controller.read_table(data_paths.APDAYC, "m_documento_almacen", cod_pais=cod_pais)
    m_documento_transaccion = spark_controller.read_table(data_paths.APDAYC, "m_documento_transaccion", cod_pais=cod_pais)
    t_movimiento_inventario = spark_controller.read_table(data_paths.APDAYC, "t_movimiento_inventario", cod_pais=cod_pais)
    t_movimiento_inventario_transito = spark_controller.read_table(data_paths.APDAYC, "t_movimiento_inventario_transito", cod_pais=cod_pais)
    
except Exception as e:
    logger.error(e)
    raise
try:
    m_pais = m_pais.filter(col("id_pais").isin(cod_pais))
    t_movimiento_inventario = t_movimiento_inventario.filter(date_format(col("fecha_almacen"), "yyyyMM").isin(periodos))
    m_documento_almacen = (
        m_documento_almacen.alias("mda")
        .join(m_documento_transaccion.alias("mdt"), (col("mda.cod_compania") == col("mdt.cod_compania")) & (col("mda.cod_transaccion") == col("mdt.cod_documento_transaccion")), "inner")
        .select(col("mda.*"))
    )
    t_movimiento_inventario = (
        t_movimiento_inventario.alias("tmi")
        .join(m_documento_almacen.alias("mda"), (col("tmi.cod_compania") == col("mda.cod_compania")) & (col("tmi.cod_procedimiento") == col("mda.cod_transaccion")), "inner")
        .select(col("tmi.*"))
    )

    tmp_dominio_t_movimiento_inventario = (
        t_movimiento_inventario.alias("tmi")
        .join(m_compania.alias("mc"), col("tmi.cod_compania") == col("mc.cod_compania"), "inner")
        .join(m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .join(t_movimiento_inventario_transito.alias("tmit"), (col("tmi.id_documento_almacen") == col("tmit.id_documento_almacen")), "left")
        .select(
            col("mp.id_pais").alias("id_pais"),
            date_format(col("tmi.fecha_almacen"), "yyyyMM").alias("id_periodo"),
            col("id_movimiento_almacen"),
            col("id_movimiento_ingreso"),
            col("tmi.cod_compania").alias("id_compania_origen"),
            col("tmi.id_sucursal").alias("id_sucursal_origen"),
            col("tmi.id_almacen").alias("id_almacen_origen"),
            col("tmit.id_compania_destino"),
            col("tmit.id_sucursal_destino"),
            col("tmit.id_almacen_destino"),
            col("tmi.id_compania_referencia"),
            col("tmi.id_sucursal_referencia"),
            col("tmi.id_almacen_referencia"),
            col("tmi.id_transportista"),
            col("tmi.id_vehiculo").alias("id_medio_transporte"),
            col("tmi.id_vendedor"),
            col("tmi.id_persona"),
            col("tmi.id_procedimiento").alias("id_tipo_procedimiento"),
            col("tmi.cod_procedimiento"),
            col("tmi.fecha_emision"),
            col("tmi.fecha_liquidacion"),
            col("tmi.fecha_almacen"),
            col("tmi.nro_documento_almacen"),
            col("tmi.nro_documento_movimiento"),
            coalesce(col("tmi.cod_estado_comprobante"), lit('000')).alias("cod_estado_comprobante"),
            col("tmi.nro_serie_alm").alias("nro_serie_almacen"),
            col("tmi.nropricoal").alias("nro_comprobante_pre"),
            coalesce(col("tmi.cod_tipo_documento_liquidacion"), lit('000')).alias("cod_documento_liquidacion"),
            col("tmi.nro_documento_liquidacion"),
            col("tmi.cod_documento_transaccion"),
            coalesce(col("tmi.cod_documento_transaccion1"), lit("")).alias("cod_documento_transaccion_ref1"),
            col("tmi.nro_documento_almacen1").alias("nro_documento_almacen_ref1"),
            col("tmi.cod_tipo_documento_referencia2").alias("cod_documento_transaccion_ref2"),
            col("tmi.nro_documento_almacen_referencia2").alias("nro_documento_almacen_ref2"),
            col("tmit.estado").alias("desc_estado_transito"),
            when(col("tmit.cod_compania").isNull(), lit(0))
            .otherwise(lit(1))
            .alias("tiene_transito"),
            col("tmi.usuario_creacion"),
            col("tmi.fecha_creacion"),
            col("tmi.usuario_modificacion"),
            col("tmi.fecha_modificacion"),
            lit(1).alias("es_eliminado"),
        )
    )

    tmp = tmp_dominio_t_movimiento_inventario.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_periodo").cast("string").alias("id_periodo"),
        col("id_movimiento_almacen").cast("string").alias("id_movimiento_almacen"),
        col("id_movimiento_ingreso").cast("string").alias("id_movimiento_ingreso"),
        col("id_compania_origen").cast("string").alias("id_compania_origen"),
        col("id_sucursal_origen").cast("string").alias("id_sucursal_origen"),
        col("id_almacen_origen").cast("string").alias("id_almacen_origen"),
        col("id_compania_destino").cast("string").alias("id_compania_destino"),
        col("id_sucursal_destino").cast("string").alias("id_sucursal_destino"),
        col("id_almacen_destino").cast("string").alias("id_almacen_destino"),
        col("id_compania_referencia").cast("string").alias("id_compania_referencia"),
        col("id_sucursal_referencia").cast("string").alias("id_sucursal_referencia"),
        col("id_almacen_referencia").cast("string").alias("id_almacen_referencia"),
        col("id_transportista").cast("string").alias("id_transportista"),
        col("id_medio_transporte").cast("string").alias("id_medio_transporte"),
        col("id_vendedor").cast("string").alias("id_vendedor"),
        col("id_persona").cast("string").alias("id_persona"),
        col("id_tipo_procedimiento").cast("string").alias("id_tipo_procedimiento"),
        col("cod_procedimiento").cast("string").alias("cod_procedimiento"),
        col("fecha_emision").cast("date").alias("fecha_emision"),
        col("fecha_liquidacion").cast("date").alias("fecha_liquidacion"),
        col("fecha_almacen").cast("date").alias("fecha_almacen"),
        col("nro_documento_almacen").cast("string").alias("nro_documento_almacen"),
        col("nro_documento_movimiento").cast("string").alias("nro_documento_movimiento"),
        col("cod_estado_comprobante").cast("string").alias("cod_estado_comprobante"),
        col("nro_serie_almacen").cast("string").alias("nro_serie_almacen"),
        col("nro_comprobante_pre").cast("string").alias("nro_comprobante_pre"),
        col("cod_documento_liquidacion").cast("string").alias("cod_documento_liquidacion"),
        col("nro_documento_liquidacion").cast("string").alias("nro_documento_liquidacion"),
        col("cod_documento_transaccion").cast("string").alias("cod_documento_transaccion"),
        col("cod_documento_transaccion_ref1").cast("string").alias("cod_documento_transaccion_ref1"),
        col("nro_documento_almacen_ref1").cast("string").alias("nro_documento_almacen_ref1"),
        col("cod_documento_transaccion_ref2").cast("string").alias("cod_documento_transaccion_ref2"),
        col("nro_documento_almacen_ref2").cast("string").alias("nro_documento_almacen_ref2"),
        col("desc_estado_transito").cast("string").alias("desc_estado_transito"),
        col("tiene_transito").cast("int").alias("tiene_transito"),
        col("usuario_creacion").cast("string").alias("usuario_creacion"),
        col("fecha_creacion").cast("timestamp").alias("fecha_creacion"),
        col("usuario_modificacion").cast("string").alias("usuario_modificacion"),
        col("fecha_modificacion").cast("timestamp").alias("fecha_modificacion"),
        col("es_eliminado").cast("int").alias("es_eliminado"),

    )

    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(tmp, data_paths.DOMAIN , target_table_name, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise