import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat, lit, coalesce, when, trim, row_number,current_date
from pyspark.sql.types import StringType, DateType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER() 
try: 
    cod_pais = COD_PAIS.split(",")
    df_m_distrito = spark_controller.read_table(data_paths.BIG_BAGIC, "m_ng3", cod_pais=cod_pais)
    df_m_provincia = spark_controller.read_table(data_paths.BIG_BAGIC, "m_ng2", cod_pais=cod_pais)
    df_m_departamento = spark_controller.read_table(data_paths.BIG_BAGIC, "m_ng1", cod_pais=cod_pais)
    df_m_compania = spark_controller.read_table(data_paths.BIG_BAGIC, "m_compania", cod_pais=cod_pais)
    df_m_pais = spark_controller.read_table(data_paths.BIG_BAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)

    df_ejeterritorial__c = spark_controller.read_table(data_paths.SALESFORCE, "m_eje_territorial", cod_pais=cod_pais)
    df_conf_origen_dom = spark_controller.read_table(data_paths.DOMINIO, "conf_origen")
    
    target_table_name = "m_eje_territorial"
except Exception as e:
    logger.error(e)
    raise 

try:
    tmp1 = (
        df_ejeterritorial__c.alias("ec")
        .where(col("tipo_de_eje__c") == "NG4")
        .select(
            col("ec.id").alias("id_eje_territorial"),
            col("ec.eje_superior__c").alias("id_eje_territorial_padre"),
            col("ec.pais__c").alias("id_pais"),
            col("ec.codigo__c").alias("cod_eje_territorial"),
            col("ec.codigo_unico__c").alias("cod_eje_territorial_ref"),
            col("ec.name").alias("nomb_eje_territorial"),
            col("ec.tipo_de_eje__c").alias("cod_tipo_eje_territorial"),
            lit("A").alias("estado"),
            col("createddate").cast("date").alias("fecha_creacion"),
            col("lastmodifieddate").cast("date").alias("fecha_modificacion"),
        )
    )

    tmp2 = (
        df_ejeterritorial__c.alias("ec")
        .where(col("tipo_de_eje__c") == "NG3")
        .select(
            col("ec.id").alias("id_eje_territorial"),
            col("ec.eje_superior__c").alias("id_eje_territorial_padre"),
            col("ec.pais__c").alias("id_pais"),
            col("ec.codigo__c").alias("cod_eje_territorial"),
            col("ec.codigo_unico__c").alias("cod_eje_territorial_ref"),
            col("ec.name").alias("nomb_eje_territorial"),
            col("ec.tipo_de_eje__c").alias("cod_tipo_eje_territorial"),
            lit("A").alias("estado"),
            col("createddate").cast("date").alias("fecha_creacion"),
            col("lastmodifieddate").cast("date").alias("fecha_modificacion"),
        )
    )

    tmp3 = (
        df_ejeterritorial__c.alias("ec")
        .where(col("tipo_de_eje__c") == "NG2")
        .select(
            col("ec.id").alias("id_eje_territorial"),
            col("ec.eje_superior__c").alias("id_eje_territorial_padre"),
            col("ec.pais__c").alias("id_pais"),
            col("ec.codigo__c").alias("cod_eje_territorial"),
            col("ec.codigo_unico__c").alias("cod_eje_territorial_ref"),
            col("ec.name").alias("nomb_eje_territorial"),
            col("ec.tipo_de_eje__c").alias("cod_tipo_eje_territorial"),
            lit("A").alias("estado"),
            col("createddate").cast("date").alias("fecha_creacion"),
            col("lastmodifieddate").cast("date").alias("fecha_modificacion"),
        )
    )

    tmp4 = (
        df_ejeterritorial__c.alias("ec")
        .where(col("tipo_de_eje__c") == "NG1")
        .select(
            col("ec.id").alias("id_eje_territorial"),
            col("ec.eje_superior__c").alias("id_eje_territorial_padre"),
            col("ec.pais__c").alias("id_pais"),
            col("ec.codigo__c").alias("cod_eje_territorial"),
            col("ec.codigo_unico__c").alias("cod_eje_territorial_ref"),
            col("ec.name").alias("nomb_eje_territorial"),
            col("ec.tipo_de_eje__c").alias("cod_tipo_eje_territorial"),
            lit("A").alias("estado"),
            col("createddate").cast("date").alias("fecha_creacion"),
            col("lastmodifieddate").cast("date").alias("fecha_modificacion"),
        )
    )

    tmp_dominio_m_eje_territorial_salesforce_1 = tmp1.union(
        tmp2.union(tmp3.union(tmp4))
    ).distinct()

    # CREATE

    sub5 = (
        df_m_distrito.alias("di")
        .join(df_m_pais.alias("p"), col("di.id_pais") == col("p.cod_pais"), "inner")
        .where(
            (col("di.cod_zona_postal").isNotNull()) & (col("p.id_pais").isin(cod_pais))
        )
        .select(
            concat(
                trim(col("p.id_pais")),
                lit("|"),
                trim(col("di.cod_zona_postal")),
            ).alias("id_eje_territorial"),
            concat(
                trim(col("p.id_pais")),
                lit("|"),
                trim(col("di.cod_zona_postal")),
                lit("|"),
                lit("NG3"),
            ).alias("id_eje_territorial_padre"),
            col("p.id_pais").alias("id_pais"),
            trim(coalesce(col("di.cod_zona_postal"), lit("0"))).alias(
                "cod_eje_territorial"
            ),
            concat(
                trim(col("p.id_pais")),
                lit("|"),
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

    tmp5 = sub5.where(col("orden") == 1).select(
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

    sub6 = (
        df_m_distrito.alias("di")
        .join(df_m_pais.alias("p"), col("di.id_pais") == col("p.cod_pais"), "inner")
        .where(
            (col("di.cod_zona_postal").isNotNull()) & (col("p.id_pais").isin(cod_pais))
        )
        .select(
            concat(
                trim(col("p.id_pais")),
                lit("|"),
                trim(col("di.cod_zona_postal")),
                lit("|"),
                lit("NG3"),
            ).alias("id_eje_territorial"),
            concat(
                trim(col("p.id_pais")),
                lit("|"),
                trim(col("di.cod_ng1")),
                lit("|"),
                trim(col("di.cod_ng2")),
            ).alias("id_eje_territorial_padre"),
            col("p.id_pais").alias("id_pais"),
            trim(coalesce(col("di.cod_zona_postal"), lit("0"))).alias(
                "cod_eje_territorial"
            ),
            concat(
                trim(col("p.id_pais")),
                lit("|"),
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

    tmp6 = sub6.where(col("orden") == 1).select(
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

    tmp7 = (
        df_m_provincia.alias("pr")
        .join(df_m_pais.alias("p"), col("pr.id_pais") == col("p.cod_pais"), "inner")
        .where((col("pr.cod_ng2").isNotNull()) & (col("p.id_pais").isin(cod_pais)))
        .select(
            concat(
                trim(col("p.id_pais")),
                lit("|"),
                trim((col("pr.cod_ng1"))),
                lit("|"),
                trim((col("pr.cod_ng2"))),
            ).alias("id_eje_territorial"),
            concat(trim(col("p.id_pais")), lit("|"), trim(col("pr.cod_ng1"))).alias(
                "id_eje_territorial_padre"
            ),
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

    tmp8 = (
        df_m_departamento.alias("de")
        .join(df_m_pais.alias("p"), col("de.id_pais") == col("p.cod_pais"), "inner")
        .where((col("de.cod_ng1").isNotNull()) & (col("p.id_pais").isin(cod_pais)))
        .select(
            concat(
                trim(col("p.id_pais")),
                lit("|"),
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

    tmp_dominio_m_eje_territorial_bmagic_2 = tmp5.union(
        tmp6.union(tmp7.union(tmp8))
    ).distinct()

    # CREATE

    tmp9 = (
        tmp_dominio_m_eje_territorial_salesforce_1.alias("ets")
        .join(
            df_conf_origen_dom.alias("co"),
            (col("co.id_pais") == col("ets.id_pais"))
            & (col("co.nombre_tabla") == "m_eje_territorial")
            & (col("co.nombre_origen") == "salesforce"),
            "inner",
        )
        .where(col("ets.id_pais").isin(cod_pais))
        .select(
            col("ets.id_eje_territorial"),
            col("ets.id_eje_territorial_padre"),
            col("ets.id_pais"),
            col("ets.cod_eje_territorial"),
            col("ets.cod_eje_territorial_ref"),
            col("ets.nomb_eje_territorial"),
            col("ets.cod_tipo_eje_territorial"),
            col("ets.estado"),
            col("ets.fecha_creacion"),
            col("ets.fecha_modificacion"),
        )
    )

    tmp10 = (
        tmp_dominio_m_eje_territorial_bmagic_2.alias("etbm")
        .join(
            df_conf_origen_dom.alias("co"),
            (col("co.id_pais") == col("etbm.id_pais"))
            & (col("co.nombre_tabla") == "m_eje_territorial")
            & (col("co.nombre_origen") == "bigmagic"),
            "inner",
        )
        .where(col("etbm.id_pais").isin(cod_pais))
        .select(
            col("etbm.id_eje_territorial"),
            col("etbm.id_eje_territorial_padre"),
            col("etbm.id_pais"),
            col("etbm.cod_eje_territorial"),
            col("etbm.cod_eje_territorial_ref"),
            col("etbm.nomb_eje_territorial"),
            col("etbm.cod_tipo_eje_territorial"),
            col("etbm.estado"),
            col("etbm.fecha_creacion"),
            col("etbm.fecha_modificacion"),
        )
    )

    tmp_dominio_m_eje_territorial = (
        tmp9.union(tmp10)
        .distinct()
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
    spark_controller.upsert(tmp_dominio_m_eje_territorial, data_paths.DOMINIO, target_table_name, id_columns, partition_columns_array)


except Exception as e:
    logger.error(e)
    raise