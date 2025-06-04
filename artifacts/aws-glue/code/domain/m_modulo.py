import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, concat, concat_ws, lit, coalesce, when, split , trim , current_date
from pyspark.sql.types import StringType, DateType

spark_controller = SPARK_CONTROLLER()
target_table_name = "m_modulo"  
try:
    df_m_modulo = spark_controller.read_table(data_paths.APDAYC, "m_modulo")
    df_m_ruta = spark_controller.read_table(data_paths.APDAYC, "m_ruta") 
    df_m_sucursal = spark_controller.read_table(data_paths.APDAYC, "m_sucursal")
    df_m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais",have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    df_tmp_modulo = (
        df_m_modulo.alias("mm")
        .join(
            df_m_sucursal.alias("ms"),
            (col("ms.cod_compania") == col("mm.cod_compania")) 
            & (col("ms.cod_sucursal") == col("mm.cod_sucursal")),
            "inner",
        )
        .join(
            df_m_compania.alias("mc"),
            col("ms.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(
            df_m_ruta.alias("mrd"),
            (col("mrd.cod_compania") == col("mm.cod_compania")) 
            & (col("mrd.cod_sucursal") == col("mm.cod_sucursal"))
            & (col("mrd.cod_fuerza_venta") == col("mm.cod_fuerza_venta"))
            & (col("mrd.cod_ruta") == col("mm.cod_ruta")),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .select(
            concat_ws("|",
                trim(col("mm.cod_compania")), 
                trim(col("mm.cod_sucursal")), 
                col("mm.cod_fuerza_venta"), 
                trim(col("mm.cod_modulo")),
            ).alias("id_modulo"),
            col("mp.id_pais"),
            concat_ws("|",
                trim(col("mm.cod_compania")), 
                trim(col("mm.cod_sucursal")),
            ).alias("id_sucursal"),
            concat_ws("|",
                trim(col("mm.cod_compania")), 
                trim(col("mm.cod_sucursal")), 
                col("mm.cod_fuerza_venta").cast("string"), 
                col("mm.cod_ruta").cast("string"),
            ).alias("id_estructura_comercial"),
            col("mm.cod_modulo").cast("string"),
            col("mm.desc_modulo").alias("desc_modulo"),
            concat_ws("|",
                trim(col("mm.cod_compania")),
                trim(col("mrd.cod_modelo_atencion")),
            ).alias("id_modelo_atencion"),
            lit(None).alias("periodo_visita"),
            lit(None).alias("desc_fuerza_venta"),
            col("ms.es_activo").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion")
        )
    )

    df_dom_m_modulo = (
        df_tmp_modulo.alias("a")
        .select(
            col("a.id_modulo").cast(StringType()).alias("id_modulo"),
            col("a.id_pais").cast(StringType()).alias("id_pais"),
            col("a.id_sucursal").cast(StringType()).alias("id_sucursal"),
            col("a.id_estructura_comercial").cast(StringType()).alias("id_estructura_comercial"),
            coalesce(col("a.id_modelo_atencion"), lit(None)).cast(StringType()).alias("id_modelo_atencion"),
            col("a.cod_modulo").cast(StringType()).alias("cod_modulo"),
            col("a.desc_modulo").cast(StringType()).alias("desc_modulo"),
            col("a.desc_fuerza_venta").cast(StringType()).alias("desc_fuerza_venta"),
            col("a.periodo_visita").cast(StringType()).alias("periodo_visita"),
            col("a.estado").cast(StringType()).alias("estado"),
            col("a.fecha_creacion").cast(DateType()).alias("fecha_creacion"),
            col("a.fecha_modificacion").cast(DateType()).alias("fecha_modificacion")
        )
    )

    id_columns = ["id_modulo"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dom_m_modulo, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert of {target_table_name} finished")
except Exception as e:
    logger.error(f"Error processing df_dom_m_modulo: {e}")
    raise ValueError(f"Error processing df_dom_m_modulo: {e}")