import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, concat_ws, concat, lit, coalesce, when, trim, row_number,current_date
from pyspark.sql.types import StringType, DateType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "m_eje_territorial"
try: 
    df_m_distrito = spark_controller.read_table(data_paths.BIGMAGIC, "m_ng3")
    df_m_provincia = spark_controller.read_table(data_paths.BIGMAGIC, "m_ng2")
    df_m_departamento = spark_controller.read_table(data_paths.BIGMAGIC, "m_ng1")
    df_m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    df_m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal = True)
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")  
try: 
    # CREATE 
    df_ng4 = (
        df_m_distrito.alias("di")
        .join(df_m_pais.alias("p"), col("di.id_pais") == col("p.cod_pais"), "inner")
        .where(col("di.cod_zona_postal").isNotNull())
        .select(
            concat_ws("|",
                trim(col("p.id_pais")),
                trim(col("di.cod_zona_postal")),
            ).alias("id_eje_territorial"),
            concat_ws("|",
                trim(col("p.id_pais")),
                trim(col("di.cod_zona_postal")),
                lit("NG3"),
            ).alias("id_eje_territorial_padre"),
            col("p.id_pais").alias("id_pais"),
            trim(coalesce(col("di.cod_zona_postal"), lit("0"))).alias("cod_eje_territorial"),
            concat_ws("|",
                trim(col("p.id_pais")),
                trim(coalesce(col("di.cod_zona_postal"), lit("0"))),
            ).alias("cod_eje_territorial_ref"),
            col("di.desc_ng3").alias("nomb_eje_territorial"),
            lit("NG4").alias("cod_tipo_eje_territorial"),
            lit("A").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
            row_number()
            .over(
                Window.partitionBy(
                    col("di.id_pais"), coalesce(col("di.cod_zona_postal"), lit("0"))
                ).orderBy(col("di.cod_ng3").desc())
            )
            .alias("orden"),
        )
    )

    df_ng4 = df_ng4.where(col("orden") == 1).select(
        col("id_eje_territorial"),
        col("id_eje_territorial_padre"),
        col("id_pais"),
        col("cod_eje_territorial"),
        col("cod_eje_territorial_ref"),
        col("nomb_eje_territorial"),
        col("cod_tipo_eje_territorial"),
        col("estado"),
        col("fecha_creacion"),
        col("fecha_modificacion"),
    )

    df_ng3 = (
        df_m_distrito.alias("di")
        .join(df_m_pais.alias("p"), col("di.id_pais") == col("p.cod_pais"), "inner")
        .where(col("di.cod_zona_postal").isNotNull())
        .select(
            concat_ws("|",
                trim(col("p.id_pais")),
                trim(col("di.cod_zona_postal")),
                lit("NG3"),
            ).alias("id_eje_territorial"),
            concat_ws("|",
                trim(col("p.id_pais")),
                trim(col("di.cod_ng1")),
                trim(col("di.cod_ng2")),
            ).alias("id_eje_territorial_padre"),
            col("p.id_pais").alias("id_pais"),
            trim(coalesce(col("di.cod_zona_postal"), lit("0"))).alias("cod_eje_territorial"),
            concat_ws("|",
                trim(col("p.id_pais")),
                trim(coalesce(col("di.cod_zona_postal"), lit("0"))),
            ).alias("cod_eje_territorial_ref"),
            col("di.desc_ng3").alias("nomb_eje_territorial"),
            lit("NG3").alias("cod_tipo_eje_territorial"),
            lit("A").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
            row_number()
            .over(
                Window.partitionBy(
                    col("di.id_pais"), coalesce(col("di.cod_zona_postal"), lit("0"))
                ).orderBy(col("di.cod_ng3").desc())
            )
            .alias("orden"),
        )
    )

    df_ng3 = df_ng3.where(col("orden") == 1).select(
        col("id_eje_territorial"),
        col("id_eje_territorial_padre"),
        col("id_pais"),
        col("cod_eje_territorial"),
        col("cod_eje_territorial_ref"),
        col("nomb_eje_territorial"),
        col("cod_tipo_eje_territorial"),
        col("estado"),
        col("fecha_creacion"),
        col("fecha_modificacion"),
    )

    df_ng2 = (
        df_m_provincia.alias("pr")
        .join(df_m_pais.alias("p"), col("pr.id_pais") == col("p.cod_pais"), "inner")
        .where(col("pr.cod_ng2").isNotNull())
        .select(
            concat_ws("|",
                trim(col("p.id_pais")),
                trim((col("pr.cod_ng1"))),
                trim((col("pr.cod_ng2")))
                ).alias("id_eje_territorial"),
            concat_ws("|",
                      trim(col("p.id_pais")),
                      trim(col("pr.cod_ng1"))
                      ).alias("id_eje_territorial_padre"),
            col("p.id_pais").alias("id_pais"),
            trim(coalesce(col("pr.cod_ng2"), lit("0"))).alias("cod_eje_territorial"),
            lit(None).alias("cod_eje_territorial_ref"),
            col("pr.desc_ng2").alias("nomb_eje_territorial"),
            lit("NG2").alias("cod_tipo_eje_territorial"),
            lit("A").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    df_ng1 = (
        df_m_departamento.alias("de")
        .join(df_m_pais.alias("p"), col("de.id_pais") == col("p.cod_pais"), "inner")
        .where(col("de.cod_ng1").isNotNull())
        .select(
            concat_ws("|",
                trim(col("p.id_pais")),
                trim(coalesce(col("de.cod_ng1"), lit("0"))),
            ).alias("id_eje_territorial"),
            lit(None).alias("id_eje_territorial_padre"),
            col("p.id_pais").alias("id_pais"),
            trim(coalesce(col("de.cod_ng1"), lit("0"))).alias("cod_eje_territorial"),
            lit(None).alias("cod_eje_territorial_ref"),
            col("de.desc_ng1").alias("nomb_eje_territorial"),
            lit("NG1").alias("cod_tipo_eje_territorial"),
            lit("A").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    df_dom_m_eje_territorial = df_ng4.union(df_ng3.union(df_ng2.union(df_ng1))).distinct()

    df_dom_m_eje_territorial = (df_dom_m_eje_territorial
        .select(
            col("id_eje_territorial").cast(StringType()),
            col("id_eje_territorial_padre").cast(StringType()),
            col("id_pais").cast(StringType()),
            col("cod_eje_territorial").cast(StringType()),
            col("cod_eje_territorial_ref").cast(StringType()),
            col("nomb_eje_territorial").cast(StringType()),
            col("cod_tipo_eje_territorial").cast(StringType()),
            col("estado").cast(StringType()),
            col("fecha_creacion").cast(DateType()),
            col("fecha_modificacion").cast(DateType())
        )
    )
    
    id_columns = ["id_eje_territorial"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dom_m_eje_territorial, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
except Exception as e:
    logger.error(f"Error processing df_dom_m_eje_territorial: {e}")
    raise ValueError(f"Error processing df_dom_m_eje_territorial: {e}") 