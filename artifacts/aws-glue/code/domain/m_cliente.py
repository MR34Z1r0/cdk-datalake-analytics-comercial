import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat, lit, coalesce, when, split , trim , current_date, max,row_number,lower,to_date
from pyspark.sql.types import StringType, DateType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER() 
target_table_name = "m_cliente"   
try:
    cod_pais = COD_PAIS.split(",") 
    df_m_cliente_bm = spark_controller.read_table(data_paths.APDAYC, "m_cliente", cod_pais=cod_pais)
    df_m_estructura_cliente = spark_controller.read_table(data_paths.APDAYC, "m_asignacion_modulo", cod_pais=cod_pais)
    df_m_tipo_cliente = spark_controller.read_table(data_paths.APDAYC, "m_tipo_cliente", cod_pais=cod_pais)
    df_m_cuenta_clave = spark_controller.read_table(data_paths.APDAYC, "m_cuenta_clave", cod_pais=cod_pais)
    df_m_canal = spark_controller.read_table(data_paths.APDAYC, "m_canal", cod_pais=cod_pais)
    df_m_giro = spark_controller.read_table(data_paths.APDAYC, "m_giro", cod_pais=cod_pais)
    df_m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania", cod_pais=cod_pais)
    df_m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", cod_pais=cod_pais,have_principal = True)
 
    df_account_history = spark_controller.read_table(data_paths.SALESFORCE, "m_cliente_historico", cod_pais=cod_pais)
    df_account = spark_controller.read_table(data_paths.SALESFORCE, "m_cliente", cod_pais=cod_pais)
 
    df_conf_origen = spark_controller.read_table(data_paths.DOMINIO, "conf_origen")

except Exception as e:
    logger.info(str(e))
    raise 
try:
    tmp_dominio_m_cliente_fecha_baja = (
        df_account_history.alias("ah")
        .where((col("field") == "Status__c") & (col("newvalue") == "Baja"))
        .groupby(col("accountid"))
        .agg(max(col("createddate").cast("date")).alias("fecha_baja"))
        .select(col("accountid").alias("id_cliente"), col("fecha_baja"))
    )
    
    tmp_dominio_m_cliente_coordenadas_2 = (
        df_m_cliente_bm.alias("mcl")
        .join(
            df_m_estructura_cliente.alias("mecl"),
            (col("mcl.cod_compania") == col("mecl.cod_compania"))
            & (col("mcl.cod_cliente") == col("mecl.cod_cliente"))
            & (
                col("mcl.cod_sucursal")
                == when(
                    (col("mcl.cod_sucursal") == "00"), col("mcl.cod_sucursal")
                ).otherwise(col("mecl.cod_sucursal"))
            ),
            "inner",
        )
        .select(
            col("mcl.cod_compania"),
            col("mcl.cod_cliente"),
            col("mecl.coord_x"),
            col("mecl.coord_y"),
            row_number()
            .over(
                Window.partitionBy(
                    "mcl.cod_compania", "mcl.cod_sucursal", "mcl.cod_cliente"
                ).orderBy(col("mecl.cod_fuerza_venta").asc())
            )
            .alias("orden"),
        )
    )

    tmp_dominio_cliente_salesforce = (
        df_account.alias("a")
        .where(col("pais__c").isin(cod_pais))
        .select(
            col("id"),
            col("codigo_unico__c"),
            col("latitud__c"),
            col("longitud__c"),
            col("eje_potencial__c"),
            col("subsegmento__c"), 
            col("subgiro__c"),
            col("ng_4_id__c"),
            col("codigo_aws__c"), 
            col("codigo_unico_aws__c"),
            row_number()
            .over(
                Window.partitionBy("codigo_unico__c").orderBy(col("createddate").desc())
            )
            .alias("orden"),
        )
    ) 
    tmp_1 = (
        df_m_cliente_bm.alias("mc")
        .join(
            tmp_dominio_cliente_salesforce.alias("a"),
            (
                (
                    concat(
                        col("mc.cod_compania"),
                        lit("|"),
                        col("mc.cod_cliente").cast("string"),
                    ).cast("string")
                )
                == (col("a.codigo_unico__c").cast("string"))
            )
            & (col("a.orden") == 1),
            "left",
        )
        .join(
            tmp_dominio_m_cliente_fecha_baja.alias("fb"),
            col("fb.id_cliente") == col("a.id"),
            "left",
        )
        .join(
            df_m_tipo_cliente.alias("tc"),
            (col("mc.cod_compania") == col("tc.cod_compania"))
            & (col("mc.cod_cliente") == col("tc.cod_cliente"))
            & (lower(col("tc.tipo_cliente")).isin(["a", "v", "t"])),
            "left",
        )
        .join(
            df_m_cuenta_clave.alias("cc"),
            (col("mc.cod_compania") == col("cc.cod_compania"))
            & (col("mc.cod_cuenta_clave") == col("cc.cod_cuenta_clave")),
            "left",
        )
        .join(
            df_m_canal.alias("c"),
            (col("c.cod_compania") == col("mc.cod_compania"))
            & (col("c.cod_canal") == col("mc.cod_canal")),
            "left",
        )
        .join(
            df_m_giro.alias("g"),
            (col("g.cod_compania") == col("mc.cod_compania"))
            & (col("g.cod_giro") == col("mc.cod_giro")),
            "left",
        )
        .join(
            tmp_dominio_m_cliente_coordenadas_2.alias("mecl"),
            (col("mc.cod_compania") == col("mecl.cod_compania"))
            & (col("mc.cod_cliente") == col("mecl.cod_cliente"))
            & (col("mecl.orden") == 1),
            "left",
        )
        .join(
            df_m_compania.alias("mco"),
            (col("mco.cod_compania") == col("mc.cod_compania")),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mco.cod_pais") == col("mp.cod_pais"), "inner")
        .join(
            df_conf_origen.alias("co"),
            (col("co.id_pais") == col("mp.id_pais"))
            & (col("co.nombre_tabla") == "m_clasificacion_cliente")
            & (col("co.nombre_origen") == "salesforce"),
            "inner",
        )
        .join(
            df_conf_origen.alias("co2"),
            (col("co2.id_pais") == col("mp.id_pais"))
            & (col("co2.nombre_tabla") == "m_eje_territorial")
            & (col("co2.nombre_origen") == "salesforce"),
            "inner",
        )
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                trim(col("mc.cod_cliente")),
            ).cast(StringType()).alias("id_cliente"),
            col("a.id").cast(StringType()).alias("id_cliente_ref"),
            lit(None).cast(StringType()).alias("id_cliente_ref2"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                trim(col("mc.cod_sucursal")),
            ).cast(StringType()).alias("id_sucursal"),
            col("a.ng_4_id__c").cast(StringType()).alias("id_eje_territorial"),
            col("a.subgiro__c").cast(StringType()).alias("id_clasificacion_cliente"),
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                trim(col("mc.cod_lista_precio")),
            ).cast(StringType()).alias("id_lista_precio"), ## NEW
            col("mc.cod_cliente").cast(StringType()).alias("cod_cliente"),
            col("mc.nomb_cliente").cast(StringType()).alias("nomb_cliente"),
            col("cc.cod_cuenta_clave").cast(StringType()).alias("cod_cuenta_clave"),
            col("cc.descripcion").cast(StringType()).alias("nomb_cuenta_clave"),
            col("a.eje_potencial__c").cast(StringType()).alias("cod_segmento"),
            col("a.subsegmento__c").cast(StringType()).alias("desc_subsegmento"), ## NEW
            col("a.id").cast(StringType()).alias("cod_cliente_ref"), ## NEW
            lit(None).cast(StringType()).alias("cod_cliente_ref2"), ## NEW
            col("a.codigo_aws__c").cast(StringType()).alias("cod_cliente_ref3"), ## NEW
            col("a.codigo_unico_aws__c").cast(StringType()).alias("cod_cliente_ref4"), ## NEW
            col("c.desc_canal").cast(StringType()).alias("desc_canal_local"),
            col("g.desc_giro").cast(StringType()).alias("desc_giro_local"),
            col("mc.direccion").cast(StringType()).alias("direccion"),
            col("mc.tipo_documento_identidad").cast(StringType()).alias("tipo_documento"),
            col("mc.nro_documento_identidad").cast(StringType()).alias("nro_documento"),
            coalesce(col("tc.tipo_cliente"), lit("N")).cast(StringType()).alias("cod_tipo_cliente"),
            coalesce(col("mc.cod_cliente_principal"), lit(0)).cast(StringType()).alias(
                "cod_cliente_principal"
            ),
            lit(None).cast(StringType()).alias("cod_cliente_transferencia"),
            coalesce(
                col("a.latitud__c").cast(StringType()), col("mecl.coord_x").cast(StringType())
            ).alias("coord_x"),
            coalesce(
                col("a.longitud__c").cast(StringType()), col("mecl.coord_y").cast(StringType())
            ).alias("coord_y"),
            col("fb.fecha_baja").cast(DateType()).alias("fecha_baja"),
            col("mc.es_activo").cast(StringType()).alias("estado"),
            col("mc.fecha_creacion").cast(DateType()).alias("fecha_creacion"),
            col("mc.fecha_modificacion").cast(DateType()).alias("fecha_modificacion"),
        )
    )

    tmp_2 = (
        df_m_cliente_bm.alias("mc")
        .join(
            tmp_dominio_cliente_salesforce.alias("a"),
            (
                (
                    concat(
                        col("mc.cod_compania"),
                        lit("|"),
                        col("mc.cod_cliente").cast("string"),
                    ).cast("string")
                )
                == (col("a.codigo_unico__c").cast("string"))
            )
            & (col("a.orden") == 1),
            "left",
        )
        .join(
            tmp_dominio_m_cliente_fecha_baja.alias("fb"),
            col("fb.id_cliente") == col("a.id"),
            "left",
        )
        .join(
            df_m_tipo_cliente.alias("tc"),
            (col("mc.cod_compania") == col("tc.cod_compania"))
            & (col("mc.cod_cliente") == col("tc.cod_cliente"))
            & (lower(col("tc.tipo_cliente")).isin(["a", "v", "t"])),
            "left",
        )
        .join(
            df_m_cuenta_clave.alias("cc"),
            (col("mc.cod_compania") == col("cc.cod_compania"))
            & (col("mc.cod_cuenta_clave") == col("cc.cod_cuenta_clave")),
            "left",
        )
        .join(
            df_m_canal.alias("c"),
            (col("c.cod_compania") == col("mc.cod_compania"))
            & (col("c.cod_canal") == col("mc.cod_canal")),
            "left",
        )
        .join(
            df_m_giro.alias("g"),
            (col("g.cod_compania") == col("mc.cod_compania"))
            & (col("g.cod_giro") == col("mc.cod_giro")),
            "left",
        )
        .join(
            tmp_dominio_m_cliente_coordenadas_2.alias("mecl"),
            (col("mc.cod_compania") == col("mecl.cod_compania"))
            & (col("mc.cod_cliente") == col("mecl.cod_cliente"))
            & (col("mecl.orden") == 1),
            "left",
        )
        .join(
            df_m_compania.alias("mco"),
            (col("mco.cod_compania") == col("mc.cod_compania")),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mco.cod_pais") == col("mp.cod_pais"), "inner")
        .join(
            df_conf_origen.alias("co"),
            (col("co.id_pais") == col("mp.id_pais"))
            & (col("co.nombre_tabla") == "m_clasificacion_cliente")
            & (col("co.nombre_origen") == "bigmagic"),
            "inner",
        )
        .join(
            df_conf_origen.alias("co2"),
            (col("co2.id_pais") == col("mp.id_pais"))
            & (col("co2.nombre_tabla") == "m_eje_territorial")
            & (col("co2.nombre_origen") == "bigmagic"),
            "inner",
        )
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                trim(col("mc.cod_cliente")),
            ).cast(StringType()).alias("id_cliente"),
            col("a.id").cast(StringType()).alias("id_cliente_ref"),
            lit(None).cast(StringType()).alias("id_cliente_ref2"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                trim(col("mc.cod_sucursal")),
            ).cast(StringType()).alias("id_sucursal"),
            when(
                (col("mc.cod_zona_postal").isNull())
                | (col("mc.cod_zona_postal") == ""),
                lit(None),
            )
            .otherwise(
                concat(
                    trim(col("mp.id_pais")),
                    lit("|"),
                    trim(coalesce(col("mc.cod_zona_postal"), lit("0"))),
                )
            )
            .cast(StringType()).alias("id_eje_territorial"),
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                lit("SG"),
                lit("|"),
                trim(col("mc.cod_subgiro")),
            ).cast(StringType()).alias("id_clasificacion_cliente"),
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                trim(col("mc.cod_lista_precio")),
            ).cast(StringType()).alias("id_lista_precio"), ## NEW
            col("mc.cod_cliente").cast(StringType()).alias("cod_cliente"),
            col("mc.nomb_cliente").cast(StringType()).alias("nomb_cliente"),
            col("cc.cod_cuenta_clave").cast(StringType()).alias("cod_cuenta_clave"),
            col("cc.descripcion").cast(StringType()).alias("nomb_cuenta_clave"),
            col("a.eje_potencial__c").cast(StringType()).alias("cod_segmento"),
            col("a.subsegmento__c").cast(StringType()).alias("desc_subsegmento"), #NEW
            col("a.id").cast(StringType()).alias("cod_cliente_ref"), #NEW
            lit(None).cast(StringType()).alias("cod_cliente_ref2"), #NEW
            col("a.codigo_aws__c").cast(StringType()).alias("cod_cliente_ref3"), # NEW
            col("a.codigo_unico_aws__c").cast(StringType()).alias("cod_cliente_ref4"), # NEW
            col("c.desc_canal").cast(StringType()).alias("desc_canal_local"),
            col("g.desc_giro").cast(StringType()).alias("desc_giro_local"),
            col("mc.direccion").cast(StringType()).alias("direccion"),
            col("mc.tipo_documento_identidad").cast(StringType()).alias("tipo_documento"),
            col("mc.nro_documento_identidad").cast(StringType()).alias("nro_documento"),
            coalesce(col("tc.tipo_cliente"), lit("N")).cast(StringType()).alias("cod_tipo_cliente"),
            coalesce(col("mc.cod_cliente_principal"), lit(0)).cast(StringType()).alias(
                "cod_cliente_principal"
            ),
            lit(None).cast(StringType()).alias("cod_cliente_transferencia"),
            coalesce(
                col("a.latitud__c").cast(StringType()), col("mecl.coord_x").cast(StringType())
            ).alias("coord_x"),
            coalesce(
                col("a.longitud__c").cast(StringType()), col("mecl.coord_y").cast(StringType())
            ).alias("coord_y"),
            col("fb.fecha_baja").cast(DateType()).alias("fecha_baja"),
            col("mc.es_activo").cast(StringType()).alias("estado"),
            col("mc.fecha_creacion").cast(DateType()).alias("fecha_creacion"),
            col("mc.fecha_modificacion").cast(DateType()).alias("fecha_modificacion"),
        )
    )

    tmp_3 = (
        df_m_cliente_bm.alias("mc")
        .join(
            tmp_dominio_cliente_salesforce.alias("a"),
            (
                (
                    concat(
                        col("mc.cod_compania"),
                        lit("|"),
                        col("mc.cod_cliente").cast("string"),
                    ).cast("string")
                )
                == (col("a.codigo_unico__c").cast("string"))
            )
            & (col("a.orden") == 1),
            "left",
        )
        .join(
            tmp_dominio_m_cliente_fecha_baja.alias("fb"),
            col("fb.id_cliente") == col("a.id"),
            "left",
        )
        .join(
            df_m_tipo_cliente.alias("tc"),
            (col("mc.cod_compania") == col("tc.cod_compania"))
            & (col("mc.cod_cliente") == col("tc.cod_cliente"))
            & (lower(col("tc.tipo_cliente")).isin(["a", "v", "t"])),
            "left",
        )
        .join(
            df_m_cuenta_clave.alias("cc"),
            (col("mc.cod_compania") == col("cc.cod_compania"))
            & (col("mc.cod_cuenta_clave") == col("cc.cod_cuenta_clave")),
            "left",
        )
        .join(
            df_m_canal.alias("c"),
            (col("c.cod_compania") == col("mc.cod_compania"))
            & (col("c.cod_canal") == col("mc.cod_canal")),
            "left",
        )
        .join(
            df_m_giro.alias("g"),
            (col("g.cod_compania") == col("mc.cod_compania"))
            & (col("g.cod_giro") == col("mc.cod_giro")),
            "left",
        )
        .join(
            tmp_dominio_m_cliente_coordenadas_2.alias("mecl"),
            (col("mc.cod_compania") == col("mecl.cod_compania"))
            & (col("mc.cod_cliente") == col("mecl.cod_cliente"))
            & (col("mecl.orden") == 1),
            "left",
        )
        .join(
            df_m_compania.alias("mco"),
            (col("mco.cod_compania") == col("mc.cod_compania")),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mco.cod_pais") == col("mp.cod_pais"), "inner")
        .join(
            df_conf_origen.alias("co"),
            (col("co.id_pais") == col("mp.id_pais"))
            & (col("co.nombre_tabla") == "m_clasificacion_cliente")
            & (col("co.nombre_origen") == "salesforce"),
            "inner",
        )
        .join(
            df_conf_origen.alias("co2"),
            (col("co2.id_pais") == col("mp.id_pais"))
            & (col("co2.nombre_tabla") == "m_eje_territorial")
            & (col("co2.nombre_origen") == "bigmagic"),
            "inner",
        )
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                trim(col("mc.cod_cliente")),
            ).cast(StringType()).alias("id_cliente"),
            col("a.id").cast(StringType()).alias("id_cliente_ref"),
            lit(None).cast(StringType()).alias("id_cliente_ref2"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                trim(col("mc.cod_sucursal")),
            ).cast(StringType()).alias("id_sucursal"),
            when(
                (col("mc.cod_zona_postal").isNull())
                | (col("mc.cod_zona_postal") == ""),
                lit(None),
            )
            .otherwise(
                concat(
                    trim(col("mp.id_pais")),
                    lit("|"),
                    trim(coalesce(col("mc.cod_zona_postal"), lit("0"))),
                )
            )
            .cast(StringType()).alias("id_eje_territorial"),
            col("a.subgiro__c").cast(StringType()).alias("id_clasificacion_cliente"),
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                trim(col("mc.cod_lista_precio")),
            ).cast(StringType()).alias("id_lista_precio"), ## NEW
            col("mc.cod_cliente").cast(StringType()).alias("cod_cliente"),
            col("mc.nomb_cliente").cast(StringType()).alias("nomb_cliente"),
            col("cc.cod_cuenta_clave").cast(StringType()).alias("cod_cuenta_clave"),
            col("cc.descripcion").cast(StringType()).alias("nomb_cuenta_clave"),
            col("a.eje_potencial__c").cast(StringType()).alias("cod_segmento"),
            col("a.subsegmento__c").cast(StringType()).alias("desc_subsegmento"), # NEW
            col("a.id").cast(StringType()).alias("cod_cliente_ref"), #NEW
            lit(None).cast(StringType()).alias("cod_cliente_ref2"), #NEW
            col("a.codigo_aws__c").cast(StringType()).alias("cod_cliente_ref3"), # NEW
            col("a.codigo_unico_aws__c").cast(StringType()).alias("cod_cliente_ref4"), # NEW
            col("c.desc_canal").cast(StringType()).alias("desc_canal_local"),
            col("g.desc_giro").cast(StringType()).alias("desc_giro_local"),
            col("mc.direccion").cast(StringType()).alias("direccion"),
            col("mc.tipo_documento_identidad").cast(StringType()).alias("tipo_documento"),
            col("mc.nro_documento_identidad").cast(StringType()).alias("nro_documento"),
            coalesce(col("tc.tipo_cliente"), lit("N")).cast(StringType()).alias("cod_tipo_cliente"),
            coalesce(col("mc.cod_cliente_principal"), lit(0)).alias(
                "cod_cliente_principal"
            ),
            lit(None).cast(StringType()).alias("cod_cliente_transferencia"),
            coalesce(
                col("a.latitud__c").cast(StringType()), col("mecl.coord_x").cast(StringType())
            ).alias("coord_x"),
            coalesce(
                col("a.longitud__c").cast(StringType()), col("mecl.coord_y").cast(StringType())
            ).alias("coord_y"),
            col("fb.fecha_baja").cast(DateType()).alias("fecha_baja"),
            col("mc.es_activo").cast(StringType()).alias("estado"),
            col("mc.fecha_creacion").cast(DateType()).alias("fecha_creacion"),
            col("mc.fecha_modificacion").cast(DateType()).alias("fecha_modificacion"),
        )
    )

    tmp_4 = (
        df_m_cliente_bm.alias("mc")
        .join(
            tmp_dominio_cliente_salesforce.alias("a"),
            (
                (
                    concat(
                        col("mc.cod_compania"),
                        lit("|"),
                        col("mc.cod_cliente").cast("string"),
                    ).cast("string")
                )
                == (col("a.codigo_unico__c").cast("string"))
            )
            & (col("a.orden") == 1),
            "left",
        )
        .join(
            tmp_dominio_m_cliente_fecha_baja.alias("fb"),
            col("fb.id_cliente") == col("a.id"),
            "left",
        )
        .join(
            df_m_tipo_cliente.alias("tc"),
            (col("mc.cod_compania") == col("tc.cod_compania"))
            & (col("mc.cod_cliente") == col("tc.cod_cliente"))
            & (lower(col("tc.tipo_cliente")).isin(["a", "v", "t"])),
            "left",
        )
        .join(
            df_m_cuenta_clave.alias("cc"),
            (col("mc.cod_compania") == col("cc.cod_compania"))
            & (col("mc.cod_cuenta_clave") == col("cc.cod_cuenta_clave")),
            "left",
        )
        .join(
            df_m_canal.alias("c"),
            (col("c.cod_compania") == col("mc.cod_compania"))
            & (col("c.cod_canal") == col("mc.cod_canal")),
            "left",
        )
        .join(
            df_m_giro.alias("g"),
            (col("g.cod_compania") == col("mc.cod_compania"))
            & (col("g.cod_giro") == col("mc.cod_giro")),
            "left",
        )
        .join(
            tmp_dominio_m_cliente_coordenadas_2.alias("mecl"),
            (col("mc.cod_compania") == col("mecl.cod_compania"))
            & (col("mc.cod_cliente") == col("mecl.cod_cliente"))
            & (col("mecl.orden") == 1),
            "left",
        )
        .join(
            df_m_compania.alias("mco"),
            (col("mco.cod_compania") == col("mc.cod_compania")),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mco.cod_pais") == col("mp.cod_pais"), "inner")
        .join(
            df_conf_origen.alias("co"),
            (col("co.id_pais") == col("mp.id_pais"))
            & (col("co.nombre_tabla") == "m_clasificacion_cliente")
            & (col("co.nombre_origen") == "bigmagic"),
            "inner",
        )
        .join(
            df_conf_origen.alias("co2"),
            (col("co2.id_pais") == col("mp.id_pais"))
            & (col("co2.nombre_tabla") == "m_eje_territorial")
            & (col("co2.nombre_origen") == "salesforce"),
            "inner",
        )
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                trim(col("mc.cod_cliente")),
            ).cast(StringType()).alias("id_cliente"),
            col("a.id").cast(StringType()).alias("id_cliente_ref"),
            lit(None).cast(StringType()).alias("id_cliente_ref2"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                trim(col("mc.cod_sucursal")),
            ).cast(StringType()).alias("id_sucursal"),
            col("a.ng_4_id__c").cast(StringType()).alias("id_eje_territorial"),
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                lit("SG"),
                lit("|"),
                trim(col("mc.cod_subgiro")),
            ).cast(StringType()).alias("id_clasificacion_cliente"),
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                trim(col("mc.cod_lista_precio")),
            ).cast(StringType()).alias("id_lista_precio"), ## NEW
            col("mc.cod_cliente").cast(StringType()).alias("cod_cliente"),
            col("mc.nomb_cliente").cast(StringType()).alias("nomb_cliente"),
            col("cc.cod_cuenta_clave").cast(StringType()).alias("cod_cuenta_clave"),
            col("cc.descripcion").cast(StringType()).alias("nomb_cuenta_clave"),
            col("a.eje_potencial__c").cast(StringType()).alias("cod_segmento"),
            col("a.subsegmento__c").cast(StringType()).alias("desc_subsegmento"), # NEW
            col("a.id").cast(StringType()).alias("cod_cliente_ref"), #NEW
            lit(None).cast(StringType()).alias("cod_cliente_ref2"), #NEW
            col("a.codigo_aws__c").cast(StringType()).alias("cod_cliente_ref3"), # NEW
            col("a.codigo_unico_aws__c").cast(StringType()).alias("cod_cliente_ref4"), # NEW
            col("c.desc_canal").cast(StringType()).alias("desc_canal_local"),
            col("g.desc_giro").cast(StringType()).alias("desc_giro_local"),
            col("mc.direccion").cast(StringType()).alias("direccion"),
            col("mc.tipo_documento_identidad").cast(StringType()).alias("tipo_documento"),
            col("mc.nro_documento_identidad").cast(StringType()).alias("nro_documento"),
            coalesce(col("tc.tipo_cliente"), lit("N")).cast(StringType()).alias("cod_tipo_cliente"),
            coalesce(col("mc.cod_cliente_principal"), lit(0)).cast(StringType()).alias(
                "cod_cliente_principal"
            ),
            lit(None).cast(StringType()).alias("cod_cliente_transferencia"),
            coalesce(
                col("a.latitud__c").cast(StringType()), col("mecl.coord_x").cast(StringType())
            ).alias("coord_x"),
            coalesce(
                col("a.longitud__c").cast(StringType()), col("mecl.coord_y").cast(StringType())
            ).alias("coord_y"),
            col("fb.fecha_baja").cast(DateType()).alias("fecha_baja"),
            col("mc.es_activo").cast(StringType()).alias("estado"),
            col("mc.fecha_creacion").cast(DateType()).alias("fecha_creacion"),
            col("mc.fecha_modificacion").cast(DateType()).alias("fecha_modificacion"),
        )
    )
    tmp_dominio_m_cliente = tmp_1.union(tmp_2.union(tmp_3.union(tmp_4))).distinct()

    id_columns = ["id_cliente"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(tmp_dominio_m_cliente, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.info(e)
    raise