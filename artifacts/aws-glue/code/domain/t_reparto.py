from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import col, concat_ws, date_format, desc, row_number
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")
    periodos= spark_controller.get_periods()
    logger.info(periodos)

    m_pais = spark_controller.read_table(data_paths.BIG_BAGIC, "m_pais", cod_pais=cod_pais, have_principal=True)
    m_compania = spark_controller.read_table(data_paths.BIG_BAGIC, "m_compania", cod_pais=cod_pais)
    t_movimiento_inventario = spark_controller.read_table(data_paths.BIG_BAGIC, "t_movimiento_inventario", cod_pais=cod_pais)

    target_table_name = "t_reparto"

except Exception as e:
    logger.error(e)
    raise

try:
    m_pais = m_pais.filter(col("id_pais").isin(cod_pais))
    df_m_compania = (
        m_compania.alias("mc")
        .join(m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .select(col("mc.cod_compania"), col("mp.id_pais"))
    )
    t_movimiento_inventario_filtered = t_movimiento_inventario.filter(
        (date_format(col("fecha_almacen"), "yyyyMM").isin(periodos))
        & (col("cod_documento_transaccion").isin("GRA", "NIN"))
    )

    t_movimiento_inventario_filtered = t_movimiento_inventario_filtered.withColumn(
        "id_reparto",
        concat_ws("|", col("cod_compania"), col("cod_sucursal"), col("cod_almacen_emisor_origen"), col("cod_documento_transaccion"), col("nro_documento_almacen"))
    )  

    window_spec = Window.partitionBy("id_reparto").orderBy(desc("nro_documento_movimiento"))

    t_movimiento_inventario_filtered = t_movimiento_inventario_filtered.withColumn("orden", row_number().over(window_spec))

    t_movimiento_inventario_filtered = t_movimiento_inventario_filtered.filter(col("orden") == 1)

    tmp_final = (
        t_movimiento_inventario_filtered.alias("tmi")
        .join(df_m_compania.alias("mc"), "cod_compania", "inner")
        .select(
            col("mc.id_pais").alias("id_pais"),
            date_format(col("tmi.fecha_almacen"), "yyyyMM").alias("id_periodo"),
            col("tmi.cod_documento_transaccion"),
            col("tmi.id_reparto"),
            concat_ws("|", col("tmi.cod_compania"), col("tmi.cod_transportista")).alias("id_transportista"),
            concat_ws("|", col("tmi.cod_compania"), col("tmi.cod_vehiculo")).alias("id_medio_transporte"),
            concat_ws("|", col("tmi.cod_compania"), col("tmi.cod_chofer")).alias("id_chofer"),
            col("tmi.fecha_emision").alias("fecha_orden_carga"),
            col("tmi.fecha_almacen").alias("fecha_reparto"),
            col("tmi.fecha_creacion"),
            col("tmi.fecha_modificacion"),
            col("tmi.cod_estado_comprobante").alias("estado_guia"),
        )
    )

    tmp = tmp_final.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_periodo").cast("string").alias("id_periodo"),
        col("id_reparto").cast("string").alias("id_reparto"),
        col("id_transportista").cast("string").alias("id_transportista"),
        col("id_medio_transporte").cast("string").alias("id_medio_transporte"),
        col("id_chofer").cast("string").alias("id_chofer"),
        col("fecha_orden_carga").cast("date").alias("fecha_orden_carga"),
        col("fecha_reparto").cast("date").alias("fecha_reparto"),
        col("estado_guia").cast("string").alias("estado_guia"),
        col("fecha_creacion").cast("timestamp").alias("fecha_creacion"),
        col("fecha_modificacion").cast("timestamp").alias("fecha_modificacion"),

    )

    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(tmp, data_paths.DOMINIO, target_table_name, partition_columns_array)


except Exception as e:
    logger.error(e)
    raise
