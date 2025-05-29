import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat, lit, coalesce, when, split , trim , current_date
from pyspark.sql.types import StringType, DateType

spark_controller = SPARK_CONTROLLER()

target_table_name = "m_modulo"  
try:
    cod_pais = COD_PAIS.split(",")
 
    df_m_modulo = spark_controller.read_table(data_paths.APDAYC, "m_modulo", cod_pais=cod_pais)
    df_m_ruta = spark_controller.read_table(data_paths.APDAYC, "m_ruta", cod_pais=cod_pais) 
    df_m_sucursal = spark_controller.read_table(data_paths.APDAYC, "m_sucursal", cod_pais=cod_pais)
    df_m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", cod_pais=cod_pais,have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania", cod_pais=cod_pais)
 
    df_modulo__c = spark_controller.read_table(data_paths.SALESFORCE, "m_modulo", cod_pais=cod_pais)
    df_fuerza_de_venta__c = spark_controller.read_table(data_paths.SALESFORCE, "m_fuerza_venta", cod_pais=cod_pais)
 
    df_conf_origen = spark_controller.read_table(data_paths.DOMAIN, "conf_origen")

except Exception as e:
    logger.error(str(e))
    raise
try:
    table_1 = (
        df_modulo__c.alias("mc")
        .join(
            df_fuerza_de_venta__c.alias("fv"),
            col("mc.fuerza_de_venta__c")==col("fv.id"),
            "left",
        )
        .join(
            df_conf_origen.alias("co"),
            (col("co.id_pais") == col("mc.pais__c"))
            & (col("co.nombre_tabla") == "m_modulo")
            & (col("co.nombre_origen") == "salesforce"),
            "inner",
        )
        .where(col("mc.pais__c").isin(cod_pais))
        .select(
            col("mc.id").alias("id_modulo"),
            col("mc.pais__c").alias("id_pais"),
            col("mc.sucursal__c").alias("id_sucursal"),
            col("mc.ruta__c").alias("id_estructura_comercial"),
            col("mc.codigo_unico__c").alias("cod_modulo"),
            col("mc.name").cast("string").alias("desc_modulo"),
            concat(
                split(col("mc.compania_sucursal__c"), "\\|").getItem(0),
                lit("|"),
                when(col("mc.modelo_de_atencion__c") == "PRE VENTA", "001")
                .when(col("mc.modelo_de_atencion__c") == "AUTO VENTA", "002")
                .when(col("mc.modelo_de_atencion__c") == "ECOMMERCE", "003")
                .when(col("mc.modelo_de_atencion__c") == "ESPECIALIZADO", "004")
                .when(col("mc.modelo_de_atencion__c") == "TELEVENTA", "005")
                .otherwise("000"),
            ).alias("id_modelo_atencion"),
            col("mc.periodo_de_visita__c").alias("periodo_visita"),
            col("fv.name").alias("desc_fuerza_venta"),
            lit("A").alias("estado"),
            col("mc.createddate").cast("date").alias("fecha_creacion"),
            col("mc.lastmodifieddate").cast("date").alias("fecha_modificacion"),
            col("co.nombre_origen").alias("origen"), #salesforce
        )
    )

    table_2 = (
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
        .join(
            df_conf_origen.alias("co"),
            (col("co.id_pais") == col("mp.id_pais"))
            & (col("co.nombre_tabla") == "m_modulo")
            & (col("co.nombre_origen") == "bigmagic"),
            "inner",
        )
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            concat(
                trim(col("mm.cod_compania")),
                lit("|"),
                trim(col("mm.cod_sucursal")),
                lit("|"),
                col("mm.cod_fuerza_venta"),
                lit("|"),
                trim(col("mm.cod_modulo")),
            ).alias("id_modulo"),
            col("mp.id_pais"),
            concat(
                trim(col("mm.cod_compania")),
                lit("|"),
                trim(col("mm.cod_sucursal")),
            ).alias("id_sucursal"),
            concat(
                trim(col("mm.cod_compania")),
                lit("|"),
                trim(col("mm.cod_sucursal")),
                lit("|"),
                col("mm.cod_fuerza_venta").cast("string"),
                lit("|"),
                col("mm.cod_ruta").cast("string"),
            ).alias("id_estructura_comercial"),
            col("mm.cod_modulo").cast("string"),
            col("mm.desc_modulo").alias("desc_modulo"),
            concat(
                trim(col("mm.cod_compania")),
                lit("|"),
                trim(col("mrd.cod_modelo_atencion")),
            ).alias("id_modelo_atencion"),
            lit(None).alias("periodo_visita"),
            lit(None).alias("desc_fuerza_venta"),
            col("ms.es_activo").alias("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
            col("co.nombre_origen").alias("origen"), #"bigmagic"
        )
    )

    tmp_dominio_modulo = table_1.union(table_2).distinct()
except Exception as e:
    logger.error(str(e))
    raise
    #add_log_to_dynamodb("Creando tablas", e)
    #send_error_message(PROCESS_NAME, "Creando tablas", f"{str(e)[:10000]}")
    #exit(1)


try:
    # 1) Creación de la tabla 'parche' con la info que necesitamos (desc_fuerza_venta) desde Salesforce
    tmp_dominio_modulo_patch = (
        df_modulo__c.alias("mc")
        .join(
            df_fuerza_de_venta__c.alias("fv"),
            col("mc.fuerza_de_venta__c") == col("fv.id"),
            "left",
        )
        .where(col("mc.pais__c").isin(cod_pais))
        .select(
            col("mc.pais__c").alias("id_pais"),
            trim(col("mc.compania_sucursal__c")).alias("id_sucursal"),
            trim(col("mc.name")).alias("cod_modulo"),
            col("fv.name").alias("desc_fuerza_venta")
        )
    )

    # 2) "Update" en tmp_dominio_modulo: rellenar desc_fuerza_venta nulo de BigMagic
    #    cuando coincidan id_sucursal, cod_modulo, id_pais, y "origen"=bigmagic (opcional).
    tmp_dominio_modulo = (
        tmp_dominio_modulo.alias("a")
        .join(
            tmp_dominio_modulo_patch.alias("b"),
            (col("a.id_sucursal") == col("b.id_sucursal"))
            & (col("a.cod_modulo") == col("b.cod_modulo"))
            & (col("a.id_pais") == col("b.id_pais")),
            "left"
        )
        .select(
            col("a.id_modulo").cast(StringType()).alias("id_modulo"),
            col("a.id_pais").cast(StringType()).alias("id_pais"),
            col("a.id_sucursal").cast(StringType()).alias("id_sucursal"),
            col("a.id_estructura_comercial").cast(StringType()).alias("id_estructura_comercial"),
            coalesce(col("a.id_modelo_atencion"), lit("")).cast(StringType()).alias("id_modelo_atencion"),
            col("a.cod_modulo").cast(StringType()).alias("cod_modulo"),
            col("a.desc_modulo").cast(StringType()).alias("desc_modulo"),
            col("a.periodo_visita").cast(StringType()).alias("periodo_visita"),
            col("a.estado").cast(StringType()).alias("estado"),
            col("a.fecha_creacion").cast(DateType()).alias("fecha_creacion"),
            col("a.fecha_modificacion").cast(DateType()).alias("fecha_modificacion"),
            # Si "b.desc_fuerza_venta" no es nulo y "origen" es "bigmagic", lo toma, sino usa "a.desc_fuerza_venta"
            when(
                (col("a.origen") == "bigmagic") & col("b.desc_fuerza_venta").isNotNull(),
                col("b.desc_fuerza_venta")
            ).otherwise(col("a.desc_fuerza_venta")).cast(StringType()).alias("desc_fuerza_venta")
        )
    )

    id_columns = ["id_modulo"]
    partition_columns_array = ["id_pais"]

    spark_controller.upsert(tmp_dominio_modulo, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise