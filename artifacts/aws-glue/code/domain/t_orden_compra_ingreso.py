from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import coalesce, col, concat_ws, date_format, lit, when

spark_controller = SPARK_CONTROLLER()
target_table_name = "t_orden_compra_ingreso"

try:
    cod_pais = COD_PAIS.split(",")
    periodos= spark_controller.get_periods()
    logger.info(periodos)

    m_compania = spark_controller.read_table(data_paths.BIG_MAGIC, "m_compania", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.BIG_MAGIC, "m_pais", cod_pais=cod_pais, have_principal=True)
    m_documento_almacen = spark_controller.read_table(data_paths.BIG_MAGIC, "m_documento_almacen", cod_pais=cod_pais)
    m_documento_transaccion = spark_controller.read_table(data_paths.BIG_MAGIC, "m_documento_transaccion", cod_pais=cod_pais)
    t_orden_compra_ingreso = spark_controller.read_table(data_paths.BIG_MAGIC, "t_orden_compra_ingreso", cod_pais=cod_pais)
    t_orden_compra_cabecera = spark_controller.read_table(data_paths.BIG_MAGIC, "t_orden_compra_cabecera", cod_pais=cod_pais)
    t_orden_compra_detalle = spark_controller.read_table(data_paths.BIG_MAGIC, "t_orden_compra_detalle", cod_pais=cod_pais)
    t_movimiento_inventario = spark_controller.read_table(data_paths.BIG_MAGIC, "t_movimiento_inventario", cod_pais=cod_pais)
    t_movimiento_inventario_detalle = spark_controller.read_table(data_paths.BIG_MAGIC, "t_movimiento_inventario_detalle", cod_pais=cod_pais)
    
    m_categoria_compra_dom = spark_controller.read_table(data_paths.DOMINIO, "m_categoria_compra", cod_pais=cod_pais)

except Exception as e:
    logger.error(e)
    raise

try:
    m_pais = m_pais.filter(col("id_pais").isin(cod_pais))
    m_documento_almacen = (
        m_documento_almacen.alias("mda")
        .filter(col("mda.cod_tipo_documento") == 'OCO')
        .join(m_documento_transaccion.alias("mdt"), (col("mda.cod_compania") == col("mdt.cod_compania")) & (col("mda.cod_transaccion") == col("mdt.cod_documento_transaccion")), "inner")
        .select(col("mda.*"))
    )
    m_categoria_compra_dom = m_categoria_compra_dom.filter(col("id_pais").isin(cod_pais))
    t_orden_compra_ingreso = t_orden_compra_ingreso.filter(date_format(col("fecha_ingreso"), "yyyyMM").isin(periodos))

    t_orden_compra_ingreso = (
        t_orden_compra_ingreso.alias("toci")
        .join(t_orden_compra_cabecera.alias("tocc"), 
            (col("toci.cod_compania_ingreso") == col("tocc.cod_compania")) 
            & (col("toci.cod_sucursal_compra") == col("tocc.cod_sucursal")) 
            & (col("toci.cod_transaccion_compra") == col("tocc.cod_transaccion")) 
            & (col("toci.nro_orden_compra_compra") == col("tocc.nro_orden_compra"))
            ,"inner"
        )
        .join(t_orden_compra_detalle.alias("tocd"),
            (col("toci.cod_compania_compra") == col("tocd.cod_compania")) 
            & (col("toci.cod_sucursal_compra") == col("tocd.cod_sucursal"))
            & (col("toci.cod_transaccion_compra") == col("tocd.cod_transaccion"))
            & (col("toci.nro_orden_compra_compra") == col("tocd.nro_orden_compra"))
            & (col("toci.tip_detalle_orden_compra") == col("tocd.desc_tipo_detalle"))
            & (col("toci.cod_articulo") == col("tocd.cod_articulo"))
            & (col("toci.secuencia_compra") == col("tocd.nro_secuencia"))
            ,"inner"
        )
        .join(m_documento_almacen.alias("mda"), 
            (col("toci.cod_compania_compra") == col("mda.cod_compania")) 
            & (col("toci.cod_transaccion_ingreso") == col("mda.cod_transaccion"))
            ,"left"
        )
        .join(t_movimiento_inventario.alias("tmi"), 
            (col("toci.cod_compania_ingreso") == col("tmi.cod_compania"))
            & (col("toci.cod_sucursal_ingreso") == col("tmi.cod_sucursal"))
            & (col("toci.nro_nota_ingreso") == col("tmi.nro_documento_movimiento"))
            ,"left"
        )
        .join(t_movimiento_inventario_detalle.alias("tmid"), 
            (col("toci.cod_compania_ingreso") == col("tmid.cod_compania"))
            & (col("toci.cod_sucursal_ingreso") == col("tmid.cod_sucursal"))
            & (col("toci.nro_nota_ingreso") == col("tmid.nro_documento_movimiento"))
            & (col("toci.secuencia_ingreso") == col("tmid.nro_linea_comprobante"))
            & (col("toci.cod_articulo") == col("tmid.cod_articulo"))
            ,"inner"
        )
        .filter(col("tmi.cod_compania").isNotNull())
        .select(col("toci.*"))
    )

    tmp_dominio_t_orden_compra_ingreso = (
        t_orden_compra_ingreso.alias("toci")
        .join(m_compania.alias("mc"), col("toci.cod_compania_compra") == col("mc.cod_compania"), "left")
        .join(m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .join(m_categoria_compra_dom.alias("mcc"), col("mcc.id_articulo") == col("toci.id_articulo"), "left")
        .select(
            col("mp.id_pais").alias("id_pais"),
            date_format(col("toci.fecha_ingreso"), "yyyyMM").alias("id_periodo"),
            col("toci.id_orden_compra"),
            col("toci.id_orden_ingreso"),
            col("toci.id_movimiento_ingreso"),
            col("toci.cod_compania_compra").alias("id_compania_compra"),
            col("toci.id_sucursal_compra"),
            col("toci.cod_compania_ingreso").alias("id_compania_ingreso"),
            col("toci.id_sucursal_ingreso"),
            col("toci.id_articulo"),
            col("toci.fecha_ingreso"),
            col("toci.fecha_entrega"),
            col("toci.nro_orden_compra_compra").alias("nro_orden_compra"),
            col("toci.cod_transaccion_compra").alias("cod_tipo_documento_compra"),
            col("toci.cod_transaccion_ingreso").alias("cod_tipo_documento_ingreso"),
            col("toci.cod_trans_exp").alias("cod_tipo_documento_exp"),
            col("toci.secuencia_compra").alias("nro_secuencia_compra"),
            col("toci.secuencia_ingreso").alias("nro_secuencia_ingreso"),
            col("toci.nro_nota_ingreso"),
            when(col("toci.flg_factura") == 'T', 1).otherwise(0).alias("tiene_factura"),
            col("toci.nro_expediente"),
            col("toci.cod_doc_refer").alias("cod_doc_referencia"),
            col("toci.nro_doc_refer").alias("nro_doc_referencia"),
            col("toci.estado").alias("desc_estado"),
            col("toci.cod_unidad").alias("cod_unidad_ingreso"),
            col("toci.cantidad").alias("cant_ingresada"),
            col("toci.cantidad_facturada").alias("cant_facturada"),
            col("toci.usuario_creacion"),
            col("toci.fecha_creacion"),
            col("toci.usuario_modificacion"),
            col("toci.fecha_modificacion"),
            lit("1").alias("es_eliminado"),
        )
    )

    tmp = tmp_dominio_t_orden_compra_ingreso.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_periodo").cast("string").alias("id_periodo"),
        col("id_orden_compra").cast("string").alias("id_orden_compra"),
        col("id_orden_ingreso").cast("string").alias("id_orden_ingreso"),
        col("id_movimiento_ingreso").cast("string").alias("id_movimiento_ingreso"),
        col("id_compania_compra").cast("string").alias("id_compania_compra"),
        col("id_sucursal_compra").cast("string").alias("id_sucursal_compra"),
        col("id_compania_ingreso").cast("string").alias("id_compania_ingreso"),
        col("id_sucursal_ingreso").cast("string").alias("id_sucursal_ingreso"),
        col("id_articulo").cast("string").alias("id_articulo"),
        col("fecha_ingreso").cast("date").alias("fecha_ingreso"),
        col("fecha_entrega").cast("date").alias("fecha_entrega"),
        col("nro_orden_compra").cast("string").alias("nro_orden_compra"),
        col("cod_tipo_documento_compra").cast("string").alias("cod_tipo_documento_compra"),
        col("cod_tipo_documento_ingreso").cast("string").alias("cod_tipo_documento_ingreso"),
        col("cod_tipo_documento_exp").cast("string").alias("cod_tipo_documento_exp"),
        col("nro_secuencia_compra").cast("string").alias("nro_secuencia_compra"),
        col("nro_secuencia_ingreso").cast("string").alias("nro_secuencia_ingreso"),
        col("nro_nota_ingreso").cast("string").alias("nro_nota_ingreso"),
        col("tiene_factura").cast("int").alias("tiene_factura"),
        col("nro_expediente").cast("string").alias("nro_expediente"),
        col("cod_doc_referencia").cast("string").alias("cod_doc_referencia"),
        col("nro_doc_referencia").cast("string").alias("nro_doc_referencia"),
        col("desc_estado").cast("string").alias("desc_estado"),
        col("cod_unidad_ingreso").cast("string").alias("cod_unidad_ingreso"),
        col("cant_ingresada").cast("numeric(38, 12)").alias("cant_ingresada"),
        col("cant_facturada").cast("numeric(38, 12)").alias("cant_facturada"),
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