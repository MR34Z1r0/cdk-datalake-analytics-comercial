import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat, lit, coalesce, when, trim, row_number,current_date,upper
from pyspark.sql.types import StringType, DateType

spark_controller = SPARK_CONTROLLER() 
try: 
    cod_pais = COD_PAIS.split(",")

    df_m_ruta_distribucion = spark_controller.read_table(data_paths.BIG_MAGIC, "m_ruta", cod_pais=cod_pais)
    df_m_zona_distribucion = spark_controller.read_table(data_paths.BIG_MAGIC, "m_zona", cod_pais=cod_pais)
    df_m_centro_distribucion = spark_controller.read_table(data_paths.BIG_MAGIC, "m_division", cod_pais=cod_pais)
    df_m_subregion = spark_controller.read_table(data_paths.BIG_MAGIC, "m_subregion", cod_pais=cod_pais)
    df_m_region = spark_controller.read_table(data_paths.BIG_MAGIC, "m_region", cod_pais=cod_pais)
    df_m_compania = spark_controller.read_table(data_paths.BIG_MAGIC, "m_compania", cod_pais=cod_pais)
    df_m_pais = spark_controller.read_table(data_paths.BIG_MAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)

    df_estructura_comercial__c = spark_controller.read_table(data_paths.SALESFORCE, "m_estructura_comercial", cod_pais=cod_pais)

    df_conf_origen_dom = spark_controller.read_table(data_paths.DOMINIO, "conf_origen") 
    target_table_name = "m_estructura_comercial"

except Exception as e:
    logger.error(e)
    raise 

try:
    tmp1 = (
        df_estructura_comercial__c.alias("sec")
        .join(
            df_conf_origen_dom.alias("co"),
            (col("co.id_pais") == col("sec.pais__c"))
            & (col("co.nombre_tabla") == "m_estructura_comercial")
            & (col("co.nombre_origen") == "salesforce"),
            "inner",
        )
        .where(col("sec.pais__c").isin(cod_pais))
        .select(
            col("sec.id").alias("id_estructura_comercial"),
            col("sec.pais__c").alias("id_pais"),
            lit(None).alias("id_sucursal"),
            when(upper(col("nivel__c")) == "REGIÓN", None)
            .when(upper(col("nivel__c")) == "SUBREGIÓN", col("sec.region__c"))
            .when(upper(col("nivel__c")) == "DIVISIÓN", col("sec.subregion__c"))
            .when(upper(col("nivel__c")) == "ZONA", col("sec.division__c"))
            .when(upper(col("nivel__c")) == "RUTA", col("sec.zona__c"))
            .alias("id_estructura_comercial_padre"),
            col("sec.responsable__c").alias("id_responsable_comercial"),
            col("sec.codigo__c").alias("cod_estructura_comercial"),
            col("sec.descripcion__c").alias("nomb_estructura_comercial"),
            col("sec.nivel__c").alias("cod_tipo_estructura_comercial"),
            col("sec.activo__c").alias("estado"),
            col("createddate").cast("date").alias("fecha_creacion"),
            col("lastmodifieddate").cast("date").alias("fecha_modificacion"),
        )
    )

    tmp2 = (
        df_m_ruta_distribucion.alias("mrd")
        .join(
            df_m_compania.alias("mc"),
            col("mrd.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .join(
            df_conf_origen_dom.alias("co"),
            (col("co.id_pais") == col("mp.id_pais"))
            & (col("co.nombre_tabla") == "m_estructura_comercial")
            & (col("co.nombre_origen") == "bigmagic"),
            "inner",
        )
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            concat(
                trim(col("mrd.cod_compania")),
                lit("|"),
                trim(col("cod_sucursal")),
                lit("|"),
                trim(col("cod_fuerza_venta").cast("string")),
                lit("|"),
                trim(col("cod_ruta").cast("string")),
            ).alias("id_estructura_comercial"),
            col("mp.id_pais").alias("id_pais"),
            concat(
                trim(col("mrd.cod_compania")), lit("|"), trim(col("cod_sucursal"))
            ).alias("id_sucursal"),
            concat(
                trim(col("mrd.cod_compania")),
                lit("|"),
                trim(col("cod_sucursal")),
                lit("|"),
                trim(col("cod_zona").cast("string")),
            ).alias("id_estructura_comercial_padre"),
            concat(
                trim(col("mrd.cod_compania")),
                lit("|"),
                trim(col("cod_vendedor").cast("string")),
            ).alias("id_responsable_comercial"),
            col("cod_ruta").cast("string").alias("cod_estructura_comercial"),
            col("desc_ruta").alias("nomb_estructura_comercial"),
            lit("Ruta").alias("cod_tipo_estructura_comercial"),
            col("mrd.es_activo"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    tmp3 = (
        df_m_zona_distribucion.alias("mrd")
        .join(
            df_m_compania.alias("mc"),
            col("mrd.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .join(
            df_conf_origen_dom.alias("co"),
            (col("co.id_pais") == col("mp.id_pais"))
            & (col("co.nombre_tabla") == "m_estructura_comercial")
            & (col("co.nombre_origen") == "bigmagic"),
            "inner",
        )
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            concat(
                trim(col("mrd.cod_compania")),
                lit("|"),
                trim(col("cod_sucursal")),
                lit("|"),
                trim(col("cod_zona").cast("string")),
            ).alias("id_estructura_comercial"),
            col("mp.id_pais").alias("id_pais"),
            concat(
                trim(col("mrd.cod_compania")), lit("|"), trim(col("cod_sucursal"))
            ).alias("id_sucursal"),
            concat(
                trim(col("mrd.cod_compania")),
                lit("|"),
                trim(col("cod_sucursal")),
                lit("|"),
                col("mrd.cod_region"),
                lit("|"),
                col("mrd.cod_subregion"),
                lit("|"),
                trim(col("cod_centro_distribucion").cast("string")),
            ).alias("id_estructura_comercial_padre"),
            concat(
                trim(col("mrd.cod_compania")),
                lit("|"),
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

    sub_tmp4 = df_m_zona_distribucion.select(
        col("cod_centro_distribucion"),
        col("cod_subregion"),
        col("cod_region"),
        col("cod_sucursal"),
    )

    tmp4 = (
        df_m_centro_distribucion.alias("mrd")
        .join(
            sub_tmp4.alias("mzd"),
            col("mrd.cod_division") == col("mzd.cod_centro_distribucion"),
            "inner",
        )
        .join(
            df_m_compania.alias("mc"),
            col("mrd.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .join(
            df_conf_origen_dom.alias("co"),
            (col("co.id_pais") == col("mp.id_pais"))
            & (col("co.nombre_tabla") == "m_estructura_comercial")
            & (col("co.nombre_origen") == "bigmagic"),
            "inner",
        )
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            concat(
                trim(col("mrd.cod_compania")),
                lit("|"),
                trim(col("cod_sucursal")),
                lit("|"),
                col("mzd.cod_region"),
                lit("|"),
                col("mzd.cod_subregion"),
                lit("|"),
                col("mrd.cod_division").cast("string"),
            ).alias("id_estructura_comercial"),
            col("mp.id_pais").alias("id_pais"),
            concat(
                trim(col("mrd.cod_compania")), lit("|"), trim(col("cod_sucursal"))
            ).alias("id_sucursal"),
            concat(
                col("mp.id_pais"),
                lit("|"),
                trim(col("mzd.cod_region").cast("string")),
                lit("|"),
                trim(col("mzd.cod_subregion").cast("string")),
            ).alias("id_estructura_comercial_padre"),
            concat(
                trim(col("mrd.cod_compania")),
                lit("|"),
                trim(col("cod_jefe_venta").cast("string")),
            ).alias("id_responsable_comercial"),
            trim(col("mrd.cod_division").cast("string")).alias(
                "cod_estructura_comercial"
            ),
            col("mrd.desc_division").alias("nomb_estructura_comercial"),
            lit("División").alias("cod_tipo_estructura_comercial"),
            col("mrd.es_activo").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    tmp5 = (
        df_m_subregion.alias("msr")
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("msr.cod_pais"), "inner")
        .join(
            df_conf_origen_dom.alias("co"),
            (col("co.id_pais") == col("mp.id_pais"))
            & (col("co.nombre_tabla") == "m_estructura_comercial")
            & (col("co.nombre_origen") == "bigmagic"),
            "inner",
        )
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            concat(
                col("mp.id_pais"),
                lit("|"),
                trim(col("msr.cod_region").cast("string")),
                lit("|"),
                trim(col("msr.cod_subregion").cast("string")),
            ).alias("id_estructura_comercial"),
            col("mp.id_pais").alias("id_pais"),
            lit(None).alias("id_sucursal"),
            concat(
                col("mp.id_pais"), lit("|"), trim(col("msr.cod_region").cast("string"))
            ).alias("id_estructura_comercial_padre"),
            lit(None).alias("id_responsable_comercial"),
            trim(col("cod_subregion").cast("string")).alias("cod_estructura_comercial"),
            col("msr.desc_subregion").alias("nomb_estructura_comercial"),
            lit("Subregión").alias("cod_tipo_estructura_comercial"),
            col("msr.es_activo"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    tmp6 = (
        df_m_region.alias("mrd")
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mrd.cod_pais"), "inner")
        .join(
            df_conf_origen_dom.alias("co"),
            (col("co.id_pais") == col("mp.id_pais"))
            & (col("co.nombre_tabla") == "m_estructura_comercial")
            & (col("co.nombre_origen") == "bigmagic"),
            "inner",
        )
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            concat(
                col("mp.id_pais"), lit("|"), trim(col("mrd.cod_region")).cast("string")
            ).alias("id_estructura_comercial"),
            col("mp.id_pais").alias("id_pais"),
            lit(None).alias("id_sucursal"),
            lit(None).alias("id_estructura_comercial_padre"),
            lit(None).alias("id_responsable_comercial"),
            trim(col("cod_region").cast("string")).alias("cod_estructura_comercial"),
            col("mrd.desc_region").alias("nomb_estructura_comercial"),
            lit("Región").alias("cod_tipo_estructura_comercial"),
            col("mrd.es_activo"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    tmp_m_estructura_comercial = (
        tmp1.union(
            tmp2.union(tmp3.union(tmp4.union(tmp5.union(tmp6))))
        )
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
            col("fecha_creacion").cast(DateType()),
            col("fecha_modificacion").cast(DateType())
        )
    )

    id_columns = ["id_estructura_comercial"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(tmp_m_estructura_comercial, data_paths.DOMINIO, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise