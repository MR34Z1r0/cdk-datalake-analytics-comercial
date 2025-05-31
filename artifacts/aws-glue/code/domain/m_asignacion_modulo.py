import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, regexp_replace, trim, coalesce, when, substring, concat, current_date
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType

spark_controller = SPARK_CONTROLLER()

try:
    m_asignacion_modulo = spark_controller.read_table(data_paths.APDAYC, "m_asignacion_modulo")
    m_sucursal = spark_controller.read_table(data_paths.APDAYC, "m_sucursal")
    m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", have_principal=True)
    m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania")
    m_cliente = spark_controller.read_table(data_paths.APDAYC, "m_cliente")
    target_table_name = "m_asignacion_modulo"    
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")    
try:
    tmp_asignacion_modulo = m_asignacion_modulo.alias("mm") \
        .join(
            m_cliente.alias("mc"),
            (col("mm.cod_compania") == col("mc.cod_compania")) &
            (col("mm.cod_cliente") == col("mc.cod_cliente")),
            "left"
        ) \
        .join(
            m_sucursal.alias("suc"),
            (col("suc.cod_compania") == col("mm.cod_compania")) &
            (col("suc.cod_sucursal") == col("mm.cod_sucursal")),
            "inner"
        ) \
        .join(
            m_compania.alias("comp"),
            col("suc.cod_compania") == col("comp.cod_compania"),
            "inner"
        ) \
        .join(
            m_pais.alias("mp"),
            col("comp.cod_pais") == col("mp.cod_pais"),
            "inner"
        ) \
        .select(
            concat(
                trim(col("mm.cod_compania")),
                lit("|"),
                trim(col("mm.cod_sucursal")),
                lit("|"),
                trim(col("mm.cod_fuerza_venta")),
                lit("|"),
                trim(col("mm.cod_modulo")),
                lit("|"),
                trim(col("mm.cod_cliente"))
            ).alias("id_asignacion_modulo"),
            col("mp.id_pais").alias("id_pais"),  
            concat(
                trim(col("suc.cod_compania")),
                lit("|"),
                trim(col("suc.cod_sucursal"))
            ).alias("id_sucursal"),
            concat(
                trim(col("suc.cod_compania")),
                lit("|"),
                trim(col("mm.cod_cliente"))
            ).alias("id_cliente"),
            concat(
                trim(col("mm.cod_compania")),
                lit("|"),
                trim(col("mm.cod_sucursal")),
                lit("|"),
                trim(col("mm.cod_fuerza_venta")),
                lit("|"),
                trim(col("mm.cod_modulo"))
            ).alias("id_modulo"),
            trim(col("mm.cod_modulo")).alias("cod_modulo"),
            lit(None).cast(TimestampType()).alias("fecha_inicio"),
            lit(None).cast(TimestampType()).alias("fecha_fin"),
            lit(None).cast(StringType()).alias("frecuencia_visita"),
            lit(None).cast(StringType()).alias("periodo_visita"),
            when(
                (col("mc.cod_sucursal").isNull()) | (col("mm.cod_sucursal") == col("mc.cod_sucursal")),
                lit(1)
            ).otherwise(lit(0)).alias("es_activo"),
            lit(0).alias("es_eliminado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion")
        )

    df_dom_m_asignacion_modulo = tmp_asignacion_modulo.alias("mam") \
        .select(
            col("mam.id_asignacion_modulo").cast(StringType()).alias("id_asignacion_modulo"),
            col("mam.id_pais").cast(StringType()).alias("id_pais"),
            col("mam.id_sucursal").cast(StringType()).alias("id_sucursal"),
            col("mam.id_cliente").cast(StringType()).alias("id_cliente"),
            col("mam.id_modulo").cast(StringType()).alias("id_modulo"),
            col("mam.fecha_inicio").cast(TimestampType()).alias("fecha_inicio"),
            col("mam.fecha_fin").cast(TimestampType()).alias("fecha_fin"),
            col("mam.frecuencia_visita").cast(StringType()).alias("frecuencia_visita"),
            col("mam.periodo_visita").cast(StringType()).alias("periodo_visita"),
            col("mam.es_activo").cast(IntegerType()).alias("es_activo"),
            col("mam.es_eliminado").cast(IntegerType()).alias("es_eliminado"),
            col("mam.fecha_creacion").cast(TimestampType()).alias("fecha_creacion"), 
            col("mam.fecha_modificacion").cast(TimestampType()).alias("fecha_modificacion")
        )

    id_columns = ["id_asignacion_modulo"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dom_m_asignacion_modulo, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} completado exitosamente")
except Exception as e:
    logger.error(f"Error processing df_dom_m_asignacion_modulo: {e}")
    raise ValueError(f"Error processing df_dom_m_asignacion_modulo: {e}") 