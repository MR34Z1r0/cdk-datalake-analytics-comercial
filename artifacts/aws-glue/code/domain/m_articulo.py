import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, concat, lit, coalesce, when
from pyspark.sql.types import StringType, IntegerType, DecimalType, TimestampType

spark_controller = SPARK_CONTROLLER()
target_table_name = "m_articulo"    
try:
    m_articulo =  spark_controller.read_table(data_paths.APDAYC, "m_articulo")
    m_pais =  spark_controller.read_table(data_paths.APDAYC, "m_pais", have_principal = True)
    m_compania =  spark_controller.read_table(data_paths.APDAYC, "m_compania")
    m_linea =  spark_controller.read_table(data_paths.APDAYC, "m_linea")
    m_familia =  spark_controller.read_table(data_paths.APDAYC, "m_familia")
    m_subfamilia =  spark_controller.read_table(data_paths.APDAYC, "m_subfamilia")
    m_marca =  spark_controller.read_table(data_paths.APDAYC, "m_marca")
    m_presentacion =  spark_controller.read_table(data_paths.APDAYC, "m_presentacion")
    m_formato =  spark_controller.read_table(data_paths.APDAYC, "m_formato")
    m_sabor =  spark_controller.read_table(data_paths.APDAYC, "m_sabor")
    m_categoria =  spark_controller.read_table(data_paths.APDAYC, "m_categoria")
    m_tipo_envase =  spark_controller.read_table(data_paths.APDAYC, "m_tipo_envase")
    #m_aroma =  spark_controller.read_table(data_paths.APDAYC, "m_aroma")
    #m_gasificado =  spark_controller.read_table(data_paths.APDAYC, "m_gasificado")
    #m_unidad_negocio =  spark_controller.read_table(data_paths.APDAYC, "m_unidad_negocio")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("Starting creation of df_dom_m_articulo")
    df_dom_m_articulo = m_articulo.alias("ma") \
        .join(m_compania.alias("mc"), col("ma.cod_compania") == col("mc.cod_compania"), "inner") \
        .join(m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "left") \
        .join(
            m_linea.alias("ml"),
            (col("ma.cod_compania") == col("ml.cod_compania")) & 
            (col("ma.cod_linea") == col("ml.cod_linea")),
            "left"
        ) \
        .join(m_familia.alias("mf"),
            (col("ma.cod_compania") == col("mf.cod_compania")) &
            (col("ma.cod_linea") == col("mf.cod_linea")) &
            (col("ma.cod_familia") == col("mf.cod_familia")),
            "left"
        ) \
        .join(
            m_subfamilia.alias("ms"),
            (col("ma.cod_compania") == col("ms.cod_compania")) &
            (col("ma.cod_linea") == col("ms.cod_linea")) &
            (col("ma.cod_familia") == col("ms.cod_familia")) &
            (col("ma.cod_subfamilia") == col("ms.cod_subfamilia")),
            "left",
        ) \
        .join(
            m_marca.alias("mm"),
            (col("ma.cod_compania") == col("mm.cod_compania")) &
            (col("ma.cod_marca") == col("mm.cod_marca")),
            "left",
        ) \
        .join(
            m_presentacion.alias("mpr"),
            (col("ma.cod_compania") == col("mpr.cod_compania"))
            & (col("ma.cod_presentacion") == col("mpr.cod_presentacion")),
            "left",
        ) \
        .join(
            m_formato.alias("mfo"),
            (col("ma.cod_compania") == col("mfo.cod_compania"))
            & (col("ma.cod_formato") == col("mfo.cod_formato")),
            "left",
        ) \
        .join(
            m_sabor.alias("msa"),
            (col("ma.cod_compania") == col("msa.cod_compania"))
            & (col("ma.cod_sabor") == col("msa.cod_sabor")),
            "left",
        ) \
        .join(
            m_categoria.alias("mca"),
            (col("ma.cod_compania") == col("mca.cod_compania"))
            & (col("ma.cod_categoria") == col("mca.cod_categoria")),
            "left",
        ) \
        .join(
            m_tipo_envase.alias("mte"),
            (col("ma.cod_compania") == col("mte.cod_compania"))
            & (col("ma.cod_tipo_envase") == col("mte.cod_tipo_envase")),
            "left",
        ) \
        .select(
            concat(
                col("ma.cod_compania"),
                lit("|"),
                col("ma.cod_articulo"),
            ).cast(StringType()).alias("id_articulo"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            lit(None).cast(StringType()).alias("id_articulo_ref"),
            col("ma.cod_articulo").cast(StringType()).alias("cod_articulo"),
            when(
                coalesce(col("ma.cod_articulo_corp"), lit(0)) == 0,
                col("ma.cod_articulo"),
            ).otherwise(col("ma.cod_articulo_corp")).cast(StringType()).alias("cod_articulo_corp"),
            concat(
                col("ma.cod_compania"),
                lit("|"),
                when(
                    coalesce(col("ma.cod_articulo_corp"), lit(0)) == 0,
                    col("ma.cod_articulo"),
                ).otherwise(col("ma.cod_articulo_corp"))
            ).cast(StringType()).alias("id_articulo_corp"),
            lit(None).cast(StringType()).alias("cod_articulo_ref"),
            lit(None).cast(StringType()).alias("cod_articulo_ref2"),
            lit(None).cast(StringType()).alias("cod_articulo_ref3"),
            col("ma.desc_articulo_corp").cast(StringType()).alias("desc_articulo_corp"),
            col("ma.desc_articulo").cast(StringType()).alias("desc_articulo"),
            coalesce(col("mca.cod_categoria"), lit("000")).cast(StringType()).alias("cod_categoria"),
            coalesce(col("mca.desc_categoria"), lit("CATEGORIA DEFAULT")).cast(StringType()).alias("desc_categoria"),
            coalesce(col("mm.cod_marca"), lit("000")).cast(StringType()).alias("cod_marca"),
            coalesce(col("mm.desc_marca"), lit("MARCA DEFAULT")).cast(StringType()).alias("desc_marca"),
            coalesce(col("mfo.cod_formato"), lit("000")).cast(StringType()).alias("cod_formato"),
            coalesce(col("mfo.desc_formato"), lit("FORMATO DEFAULT")).cast(StringType()).alias("desc_formato"),
            coalesce(col("msa.cod_sabor"), lit("000")).cast(StringType()).alias("cod_sabor"),
            coalesce(col("msa.desc_sabor"), lit("SABOR DEFAULT")).cast(StringType()).alias("desc_sabor"),
            coalesce(col("mpr.cod_presentacion"), lit("000")).cast(StringType()).alias("cod_presentacion"),
            coalesce(col("mpr.desc_presentacion"), lit("PRESENTACION DEFAULT")).cast(StringType()).alias("desc_presentacion"),
            coalesce(col("mte.cod_tipo_envase"), lit("000")).cast(StringType()).alias("cod_tipo_envase"),
            coalesce(col("mte.desc_tipo_envase"), lit("TIPO ENVASE DEFAULT")).cast(StringType()).alias("desc_tipo_envase"),
            lit("000").cast(StringType()).alias("cod_aroma"),
            lit("AROMA DEFAULT").cast(StringType()).alias("desc_aroma"),
            lit("000").cast(StringType()).alias("cod_gasificado"),
            lit("GASIFICADO DEFAULT").cast(StringType()).alias("desc_gasificado"),
            coalesce(col("ml.cod_linea"), lit("00")).cast(StringType()).alias("cod_linea"),
            coalesce(col("ml.desc_linea"), lit("LINEA DEFAULT")).cast(StringType()).alias("desc_linea"),
            coalesce(col("ml.flg_linea"), lit("N")).cast(StringType()).alias("flg_linea"),
            coalesce(col("ma.es_explosion"), lit("N")).cast(StringType()).alias("flg_explosion"),
            coalesce(col("mf.cod_familia"), lit("000")).cast(StringType()).alias("cod_familia"),
            coalesce(col("mf.desc_familia"), lit("FAMILIA DEFAULT")).cast(StringType()).alias("desc_familia"),
            coalesce(col("ms.cod_subfamilia"), lit("00")).cast(StringType()).alias("cod_subfamilia"),
            coalesce(col("ms.desc_subfamilia"), lit("SUBFAMILIA DEFAULT")).cast(StringType()).alias("desc_subfamilia"),
            lit(None).cast(StringType()).alias("cod_unidad_negocio"),
            lit(None).cast(StringType()).alias("desc_unidad_negocio"),
            when(
                (coalesce(col("ml.cod_linea"), lit("00")) == "03")
                & (coalesce(col("mf.cod_familia"), lit("000")) == "003"),
                1,
            ).otherwise(0).cast(IntegerType()).alias("flg_jarabe"),
            when(
                (coalesce(col("ml.cod_linea"), lit("00")) == "04")
                & (coalesce(col("mf.cod_familia"), lit("000")) == "008"),
                1,
            ).otherwise(0).cast(IntegerType()).alias("flg_co2"),
            when(
                (coalesce(col("ml.cod_linea"), lit("00")) == "04")
                & (coalesce(col("mf.cod_familia"), lit("000")) == "001"),
                1,
            ).otherwise(0).cast(IntegerType()).alias("flg_azucar"),
            when(
                (coalesce(col("ml.cod_linea"), lit("00")) == "03") &
                (coalesce(col("mf.cod_familia"), lit("000")) == "002"),
                1,
            ).otherwise(0).cast(IntegerType()).alias("flg_jarabe_conver"),
            col("ma.unidad_compra").cast(StringType()).alias("cod_unidad_compra"),
            col("ma.unidad_manejo").cast(StringType()).alias("cod_unidad_manejo"),
            col("ma.unidad_volumen").cast(StringType()).alias("cod_unidad_volumen"),
            col("ma.cant_unidad_peso").cast(StringType()).alias("cant_unidad_peso"),
            col("ma.cant_unidad_volumen").cast(DecimalType(38,12)).alias("cant_unidad_volumen"),
            col("ma.cant_unidad_paquete").cast(DecimalType(38,12)).alias("cant_unidad_paquete"),
            col("ma.cant_paquete_caja").cast(DecimalType(38,12)).alias("cant_paquete_caja"),
            col("ma.cant_cajas_por_palet").cast(DecimalType(38,12)).alias("cant_cajas_por_palet"),
            col("ma.es_activo").cast(StringType()).alias("es_activo"),
            col("ma.flgskuplan").cast(StringType()).alias("flgskuplan"),
            col("ma.fecha_creacion").cast(TimestampType()).alias("fecha_creacion"),
            col("ma.fecha_modificacion").cast(TimestampType()).alias("fecha_modificacion"),
        )
    
    id_columns = ["id_articulo"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dom_m_articulo, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} completado exitosamente")
except Exception as e:
    logger.error(f"Error processing df_dom_m_articulo: {e}")
    raise ValueError(f"Error processing df_dom_m_articulo: {e}") 