import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat_ws, lit

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")

    # Load Stage
    m_pais = spark_controller.read_table(data_paths.BIG_BAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)
    m_compania = spark_controller.read_table(data_paths.BIG_BAGIC, "m_compania", cod_pais=cod_pais)
    m_almacen = spark_controller.read_table(data_paths.BIG_BAGIC, "m_almacen", cod_pais=cod_pais)
    m_canal_visibilidad = spark_controller.read_table(data_paths.BIG_BAGIC, "m_canal", cod_pais=cod_pais)
    m_cliente = spark_controller.read_table(data_paths.BIG_BAGIC, "m_cliente", cod_pais=cod_pais)
    m_departamento = spark_controller.read_table(data_paths.BIG_BAGIC, "m_ng1", cod_pais=cod_pais)
    m_persona = spark_controller.read_table(data_paths.BIG_BAGIC, "m_persona", cod_pais=cod_pais)
    m_persona_direccion = spark_controller.read_table(data_paths.BIG_BAGIC, "m_persona_direccion", cod_pais=cod_pais)
    m_provincia = spark_controller.read_table(data_paths.BIG_BAGIC, "m_ng2", cod_pais=cod_pais)

    target_table_name = "m_localidad"

except Exception as e:
    logger.error(str(e))
    raise

try:
    m_pais = m_pais.filter(col("id_pais").isin(cod_pais))

    tmp_m_localidad_destino = (
      m_persona_direccion.alias("mpd")
      .join(m_compania.alias("mc"), col("mpd.cod_compania") == col("mc.cod_compania"), "inner")
      .join(m_pais.alias("mp"), col("mpd.cod_pais") == col("mp.cod_pais"), "inner")
      .join(m_cliente.alias("mcli"), (col("mpd.cod_persona") == col("mcli.cod_cliente")) & (col("mpd.cod_compania") == col("mcli.cod_compania")), "inner")
      .join(m_canal_visibilidad.alias("mcv"), (col("mcli.cod_compania") == col("mcv.cod_compania")) & (col("mcli.cod_canal") == col("mcv.cod_canal")), "left")
      .join(m_persona.alias("mpers"), (col("mpd.cod_persona") == col("mpers.cod_persona")) & (col("mpd.cod_compania") == col("mpers.cod_compania")), "left")
      .join(m_departamento.alias("md"), (col("mpd.estado_direccion") == col("md.cod_ng1")) & (col("mpd.cod_pais") == col("md.id_pais")), "left")
      .join(m_provincia.alias("mprov"), (col("mpd.cod_provincia") == col("mprov.cod_ng2")) & (col("mpd.estado_direccion") == col("mprov.cod_ng1")) & (col("mpd.cod_pais") == col("mprov.id_pais")), "left")
      .select(
        col("mp.id_pais"),
        col("mc.cod_compania"),
        col("mpd.cod_persona"),
        col("mpd.cod_persona_direccion"),
        col("mpers.nomb_persona").alias("desc_localidad"),
        col("mcli.cod_canal").alias("cod_canal_visibilidad"),
        col("mcv.desc_canal"),
        col("mpd.estado_direccion").alias("cod_ng1"),
        col("md.desc_ng1"),
        col("mpd.cod_provincia").alias("cod_ng2"),
        col("mprov.desc_ng2"),
        col("mpd.direccion_proveedor1").alias("direccion"),
        col("mpd.estado"),
        col("mpd.fecha_creacion"),
        col("mpd.fecha_modificacion"),
      )
    )

    tmp_m_localidad_origen = (
      m_almacen.alias("ma")
      .join(m_compania.alias("mc"), col("ma.cod_compania") == col("mc.cod_compania"), "inner")
      .join(m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
      .join(m_departamento.alias("md"), (col("mc.cod_ng1") == col("md.cod_ng1")) & (col("mc.cod_pais") == col("md.id_pais")), "left")
      .join(m_provincia.alias("mprov"), (col("mc.cod_ng2") == col("mprov.cod_ng2")) & (col("mc.cod_ng1") == col("mprov.cod_ng1")) & (col("mc.cod_pais") == col("mprov.id_pais")), "left")
      .select(
        col("mp.id_pais"),
        col("ma.cod_compania"),
        col("ma.cod_sucursal"),
        col("ma.cod_almacen").alias("cod_almacen"),
        col("ma.desc_almacen").alias("desc_localidad"),
        col("mc.cod_ng1"),
        col("md.desc_ng1").alias("desc_ng1"),
        col("mc.cod_ng2"),
        col("mprov.desc_ng2"),
        col("mc.direccion"),
        col("ma.es_activo").alias("estado"),
        col("ma.fecha_creacion"),
        col("ma.fecha_modificacion"),
      )
    )

    tmp1 = tmp_m_localidad_destino.select(
        col("id_pais"),
        col("cod_compania").alias("id_compania"),
        concat_ws("|", col("cod_compania"), col("cod_persona"), col("cod_persona_direccion")).alias("id_localidad"),
        concat_ws("|", col("cod_persona"), col("cod_persona_direccion")).alias("cod_localidad"),
        col("cod_persona"),
        col("cod_persona_direccion"),
        lit(None).alias("cod_almacen"),
        col("desc_localidad"),
        col("cod_canal_visibilidad"),
        col("desc_canal"),
        col("cod_ng1"),
        col("desc_ng1"),
        col("cod_ng2"),
        col("desc_ng2"),
        col("direccion"),
        col("estado"),
        col("fecha_creacion"),
        col("fecha_modificacion"),
    )

    tmp2 = tmp_m_localidad_origen.select(
      col("id_pais"),
      col("cod_compania").alias("id_compania"),
      concat_ws("|", col("cod_compania"), col("cod_sucursal"), col("cod_almacen")).alias("id_localidad"),
      col("cod_almacen").alias("cod_localidad"),
      lit(None).alias("cod_persona"),
      lit(None).alias("cod_persona_direccion"),
      col("cod_almacen"),
      col("desc_localidad"),
      lit(None).alias("cod_canal_visibilidad"),
      lit(None).alias("desc_canal"),
      col("cod_ng1"),
      col("desc_ng1"),
      col("cod_ng2"),
      col("desc_ng2"),
      col("direccion"),
      col("estado"),
      col("fecha_creacion"),
      col("fecha_modificacion"),
    )

    tmp_union = tmp1.unionByName(tmp2)

    tmp = tmp_union.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_compania").cast("string").alias("id_compania"),
        col("id_localidad").cast("string").alias("id_localidad"),
        col("cod_localidad").cast("string").alias("cod_localidad"),
        col("cod_persona").cast("string").alias("cod_persona"),
        col("cod_persona_direccion").cast("string").alias("cod_persona_direccion"),
        col("cod_almacen").cast("string").alias("cod_almacen"),
        col("desc_localidad").cast("string").alias("desc_localidad"),
        col("cod_canal_visibilidad").cast("string").alias("cod_canal_visibilidad"),
        col("desc_canal").cast("string").alias("desc_canal"),
        col("cod_ng1").cast("string").alias("cod_ng1"),
        col("desc_ng1").cast("string").alias("desc_ng1"),
        col("cod_ng2").cast("string").alias("cod_ng2"),
        col("desc_ng2").cast("string").alias("desc_ng2"),
        col("direccion").cast("string").alias("direccion"),
        col("estado").cast("string").alias("estado"),
        col("fecha_creacion").cast("date").alias("fecha_creacion"),
        col("fecha_modificacion").cast("date").alias("fecha_modificacion"),
    )
    

    id_columns = ["id_localidad", "id_pais"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(tmp, data_paths.DOMINIO, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(str(e))
    raise
