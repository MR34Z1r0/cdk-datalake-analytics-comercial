import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, concat, concat_ws, lit, coalesce, when, trim, row_number,current_date,upper
from pyspark.sql.types import StringType, DateType, TimestampType

spark_controller = SPARK_CONTROLLER() 
target_table_name = "m_estructura_comercial"
try: 
    df_m_ruta_distribucion = spark_controller.read_table(data_paths.BIGMAGIC, "m_ruta")
    df_m_zona_distribucion = spark_controller.read_table(data_paths.BIGMAGIC, "m_zona")
    df_m_centro_distribucion = spark_controller.read_table(data_paths.BIGMAGIC, "m_division")
    df_m_subregion = spark_controller.read_table(data_paths.BIGMAGIC, "m_subregion")
    df_m_region = spark_controller.read_table(data_paths.BIGMAGIC, "m_region")
    df_m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    df_m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal = True)
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")  
try:
    df_estructura_comercial_ruta = (
        df_m_ruta_distribucion.alias("mrd")
        .join(
            df_m_compania.alias("mc"),
            col("mrd.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")   
        .select(
            concat_ws("|",
                trim(col("mrd.cod_compania")),
                trim(col("cod_sucursal")),
                trim(col("cod_fuerza_venta").cast("string")),
                trim(col("cod_ruta").cast("string")),
            ).alias("id_estructura_comercial"),
            col("mp.id_pais").alias("id_pais"),
            concat_ws("|",
                trim(col("mrd.cod_compania")), 
                trim(col("cod_sucursal"))
            ).alias("id_sucursal"),
            concat_ws("|",
                trim(col("mrd.cod_compania")),
                trim(col("cod_sucursal")),
                trim(col("cod_zona").cast("string")),
            ).alias("id_estructura_comercial_padre"),
            concat_ws("|",
                trim(col("mrd.cod_compania")),
                trim(col("cod_vendedor").cast("string")),
            ).alias("id_responsable_comercial"),
            col("cod_ruta").cast("string").alias("cod_estructura_comercial"),
            col("desc_ruta").alias("nomb_estructura_comercial"),
            lit("Ruta").alias("cod_tipo_estructura_comercial"),
            col("mrd.es_activo").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    df_estructura_comercial_zona = (
        df_m_zona_distribucion.alias("mrd")
        .join(
            df_m_compania.alias("mc"),
            col("mrd.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .select(
            concat_ws("|",
                trim(col("mrd.cod_compania")),
                trim(col("cod_sucursal")),
                trim(col("cod_zona").cast("string")),
            ).alias("id_estructura_comercial"),
            col("mp.id_pais").alias("id_pais"),
            concat_ws("|",
                trim(col("mrd.cod_compania")), 
                trim(col("cod_sucursal"))
            ).alias("id_sucursal"),
            concat_ws("|",
                trim(col("mrd.cod_compania")),
                trim(col("cod_sucursal")),
                col("mrd.cod_region"),
                col("mrd.cod_subregion"),
                trim(col("cod_centro_distribucion").cast("string")),
            ).alias("id_estructura_comercial_padre"),
            concat_ws("|",
                trim(col("mrd.cod_compania")),
                col("cod_supervisor").cast("string"),
            ).alias("id_responsable_comercial"),
            col("cod_zona").cast("string").alias("cod_estructura_comercial"),
            col("mrd.desc_zona").alias("nomb_estructura_comercial"),
            lit("Zona").alias("cod_tipo_estructura_comercial"),
            col("mrd.es_activo").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    df_m_zona_distribucion_distinct = df_m_zona_distribucion.select(
        col("cod_compania"),
        col("cod_sucursal"),
        col("cod_centro_distribucion"),
        col("cod_subregion"),
        col("cod_region"),
    ).distinct()

    df_estructura_comercial_division = (
        df_m_centro_distribucion.alias("mrd")
        .join(
            df_m_zona_distribucion_distinct.alias("mzd"),
            (col("mrd.cod_compania") == col("mzd.cod_compania")) & (col("mrd.cod_division") == col("mzd.cod_centro_distribucion")),
            "inner",
        )
        .join(
            df_m_compania.alias("mc"),
            col("mrd.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .select(
            concat_ws("|",
                trim(col("mrd.cod_compania")), 
                trim(col("mzd.cod_sucursal")), 
                col("mzd.cod_region"), 
                col("mzd.cod_subregion"), 
                col("mrd.cod_division").cast("string"),
            ).alias("id_estructura_comercial"),
            col("mp.id_pais").alias("id_pais"),
            concat_ws("|",
                trim(col("mrd.cod_compania")),
                trim(col("cod_sucursal"))
            ).alias("id_sucursal"),
            concat_ws("|",
                col("mp.id_pais"), 
                trim(col("mzd.cod_region").cast("string")), 
                trim(col("mzd.cod_subregion").cast("string")),
            ).alias("id_estructura_comercial_padre"),
            concat_ws("|",
                trim(col("mrd.cod_compania")), 
                trim(col("cod_jefe_venta").cast("string")),
            ).alias("id_responsable_comercial"),
            trim(col("mrd.cod_division").cast("string")).alias("cod_estructura_comercial"),
            col("mrd.desc_division").alias("nomb_estructura_comercial"),
            lit("División").alias("cod_tipo_estructura_comercial"),
            col("mrd.es_activo").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    df_estructura_comercial_subregion = (
        df_m_subregion.alias("msr")
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("msr.cod_pais"), "inner")
        .select(
            concat_ws("|",
                col("mp.id_pais"),
                trim(col("msr.cod_region").cast("string")),
                trim(col("msr.cod_subregion").cast("string")),
            ).alias("id_estructura_comercial"),
            col("mp.id_pais").alias("id_pais"),
            lit(None).alias("id_sucursal"),
            concat_ws("|",
                col("mp.id_pais"),
                trim(col("msr.cod_region").cast("string"))
            ).alias("id_estructura_comercial_padre"),
            lit(None).alias("id_responsable_comercial"),
            trim(col("cod_subregion").cast("string")).alias("cod_estructura_comercial"),
            col("msr.desc_subregion").alias("nomb_estructura_comercial"),
            lit("Subregión").alias("cod_tipo_estructura_comercial"),
            col("msr.es_activo").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    df_estructura_comercial_region = (
        df_m_region.alias("mrd")
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mrd.cod_pais"), "inner")
        .select(
            concat_ws("|",
                col("mp.id_pais"),
                trim(col("mrd.cod_region")).cast("string")
            ).alias("id_estructura_comercial"),
            col("mp.id_pais").alias("id_pais"),
            lit(None).alias("id_sucursal"),
            lit(None).alias("id_estructura_comercial_padre"),
            lit(None).alias("id_responsable_comercial"),
            trim(col("cod_region").cast("string")).alias("cod_estructura_comercial"),
            col("mrd.desc_region").alias("nomb_estructura_comercial"),
            lit("Región").alias("cod_tipo_estructura_comercial"),
            col("mrd.es_activo").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    df_dom_m_estructura_comercial = (
        df_estructura_comercial_ruta.union(df_estructura_comercial_zona.union(df_estructura_comercial_division.union(df_estructura_comercial_subregion.union(df_estructura_comercial_region))))
        .distinct()
        .select(
            col("id_estructura_comercial").cast(StringType()),
            col("id_pais").cast(StringType()),
            col("id_sucursal").cast(StringType()),
            col("id_estructura_comercial_padre").cast(StringType()),
            col("id_responsable_comercial").cast(StringType()),
            col("cod_estructura_comercial").cast(StringType()),
            col("nomb_estructura_comercial").cast(StringType()),
            col("cod_tipo_estructura_comercial").cast(StringType()),
            col("estado").cast(StringType()),
            col("fecha_creacion").cast(TimestampType()),
            col("fecha_modificacion").cast(TimestampType())
        )
    )

    id_columns = ["id_estructura_comercial"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dom_m_estructura_comercial, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
except Exception as e:
    logger.error(f"Error processing df_dom_m_estructura_comercial: {e}")
    raise ValueError(f"Error processing df_dom_m_estructura_comercial: {e}") 