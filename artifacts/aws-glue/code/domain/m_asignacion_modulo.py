import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, lit, regexp_replace, trim, coalesce, when, substring, concat, current_date
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",") 

    m_asignacion_modulo = spark_controller.read_table(data_paths.APDAYC, "m_asignacion_modulo", cod_pais=cod_pais)
    m_sucursal = spark_controller.read_table(data_paths.APDAYC, "m_sucursal", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", cod_pais=cod_pais,have_principal = True)
    m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania", cod_pais=cod_pais)
    m_cliente = spark_controller.read_table(data_paths.APDAYC, "m_cliente", cod_pais=cod_pais)

    asignacion_modulo__c = spark_controller.read_table(data_paths.SALESFORCE, "m_asignacion_modulo")
    modulo__c = spark_controller.read_table(data_paths.SALESFORCE, "m_modulo")
    account = spark_controller.read_table(data_paths.SALESFORCE, "m_cliente")

    conf_origen = spark_controller.read_table(data_paths.DOMAIN , "conf_origen")
    target_table_name = "m_asignacion_modulo"

    tmp_dominio_config = (
        conf_origen
        .where(col("id_pais").isin(cod_pais))
        .select("id_pais", "nombre_tabla", "nombre_origen", "nombre_origen_erp")
    )

except Exception as e:
    logger.error(e)
    raise

try:
    sf_bm = asignacion_modulo__c.alias("vadmc") \
        .join(account.alias("a"), col("vadmc.cliente__c") == col("a.id"), "inner") \
        .join(modulo__c.alias("m"), col("m.id") == col("vadmc.modulo__c"), "left") \
        .join(tmp_dominio_config.alias("co"),
            (
                (col("co.id_pais") == col("vadmc.pais__c")) &
                (col("co.nombre_tabla") == lit("m_asignacion_modulo")) &
                (col("co.nombre_origen") == lit("salesforce")) &
                (col("co.nombre_origen_erp") == lit("bigmagic"))
            ),
            "inner",
        ) \
        .where(col("vadmc.pais__c").isin(cod_pais)) \
        .withColumn(
            "frecuencia_visita",
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(col("vadmc.dias_de_visita__c"), "1", "Lunes"),
                                    "2", "Martes"
                                ),
                                "3", "Miercoles"
                            ),
                            "4", "Jueves"
                        ),
                        "5", "Viernes"
                    ),
                    "6", "Sabado"
                ),
                "7", "Domingo"
            )
        ) \
        .withColumn(
            "periodo_visita",
            when(col("vadmc.periodo_de_visita__c") == lit("F1"), "Semanal")
            .when(col("vadmc.periodo_de_visita__c") == lit("F2"), "Cada 2 semanas")
            .when(col("vadmc.periodo_de_visita__c") == lit("F3"), "Cada 3 semanas")
            .when(col("vadmc.periodo_de_visita__c") == lit("F4"), "Cada 4 semanas")
            .otherwise(None)
        ) \
        .select(
            col("vadmc.id").alias("id_asignacion_modulo"),
            col("vadmc.pais__c").alias("id_pais"),
            col("m.compania_sucursal__c").alias("id_sucursal"),
            col("a.codigo_unico__c").alias("id_cliente"),
            col("vadmc.modulo__c").alias("id_modulo"),
            trim(col("m.name")).alias("cod_modulo"),
            col("vadmc.fecha_de_inicio__c").alias("fecha_inicio"),
            col("vadmc.fecha_fin__c").alias("fecha_fin"),
            col("frecuencia_visita"),
            col("periodo_visita"),
            when(substring(col("vadmc.activa__c"), 0, 1) == "t", lit(1)).otherwise(lit(0)).alias("es_activo"),
            when(substring(col("vadmc.isdeleted"), 0, 1) == "f", lit(0)).otherwise(lit(1)).alias("es_eliminado"),
            col("vadmc.createddate").cast("date").alias("fecha_creacion"),
            col("vadmc.lastmodifieddate").cast("date").alias("fecha_modificacion"),
        )
    
except Exception as e:
    logger.error(str(e))
    raise
try:
    if False:
        sf_oracle = (
            df_asignacion_modulo__c.alias("vadmc")
            .join(
                df_account.alias("a"),
                col("vadmc.cliente__c") == col("a.id"),
                "inner",
            )
            .join(
                df_custaccountsiteuse.alias("cs"),
                (
                    (col("cs.partysitenumber") == col("a.codigo_cliente__c")) &
                    (col("cs.siteusecode") == lit("SHIP_TO"))
                ),
                "inner",
            )
            .join(
                df_modulo__c.alias("m"),
                col("m.id") == col("vadmc.modulo__c"),
                "left",
            )
            .join(
                tmp_dominio_config.alias("co"),
                (
                    (col("co.id_pais") == col("vadmc.pais__c")) &
                    (col("co.nombre_tabla") == lit("m_asignacion_modulo")) &
                    (col("co.nombre_origen") == lit("salesforce")) &
                    (col("co.nombre_origen_erp") == lit("oracle"))
                ),
                "inner",
            )
            .where(col("vadmc.pais__c").isin(cod_pais))
            .withColumn(
                "frecuencia_visita",
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(col("vadmc.dias_de_visita__c"), lit("1"), lit("Lunes")),
                                        "2", "Martes"
                                    ),
                                    "3", "Miercoles"
                                ),
                                "4", "Jueves"
                            ),
                            "5", "Viernes"
                        ),
                        "6", "Sabado"
                    ),
                    "7", "Domingo"
                )
            )
            .withColumn(
                "periodo_visita",
                when(col("vadmc.periodo_de_visita__c") == lit("F1"), "Semanal")
                .when(col("vadmc.periodo_de_visita__c") == lit("F2"), "Cada 2 semanas")
                .when(col("vadmc.periodo_de_visita__c") == lit("F3"), "Cada 3 semanas")
                .when(col("vadmc.periodo_de_visita__c") == lit("F4"), "Cada 4 semanas")
                .otherwise(None)
            )
            .select(
                col("vadmc.id").alias("id_asignacion_modulo"),
                col("vadmc.pais__c").alias("id_pais"),
                col("m.compania_sucursal__c").alias("id_sucursal"),
                col("cs.custacctsiteid").alias("id_cliente"),  # Ojo: en Oracle se define distinto
                col("vadmc.modulo__c").alias("id_modulo"),
                trim(col("m.name")).alias("cod_modulo"),
                col("vadmc.fecha_de_inicio__c").alias("fecha_inicio"),
                col("vadmc.fecha_fin__c").alias("fecha_fin"),
                col("frecuencia_visita"),
                col("periodo_visita"),
                when(substring(col("vadmc.activa__c"), 0, 1) == "t", lit(1)).otherwise(lit(0)).alias("es_activo"),
                when(substring(col("vadmc.isdeleted"), 0, 1) == "f", lit(0)).otherwise(lit(1)).alias("es_eliminado"),
                col("vadmc.createddate").cast("date").alias("fecha_creacion"),
                col("vadmc.lastmodifieddate").cast("date").alias("fecha_modificacion"),
            )
        )
    else:
        pass
except Exception as e:
    logger.error(e)
    raise
try:
    table_bm = m_asignacion_modulo.alias("mm") \
        .join(
            m_cliente.alias("mc"),
            (col("mm.cod_compania") == col("mc.cod_compania")) &
            (col("mm.cod_cliente") == col("mc.cod_cliente"))
            ,"left"
        ) \
        .join(
            m_sucursal.alias("suc"),
            (col("suc.cod_compania") == col("mm.cod_compania")) &
            (col("suc.cod_sucursal") == col("mm.cod_sucursal"))
            ,"inner"
        ) \
        .join(
            m_compania.alias("comp"),
            col("suc.cod_compania") == col("comp.cod_compania"),
            "inner",
        ) \
        .join(
            m_pais.alias("mp"),
            col("comp.cod_pais") == col("mp.cod_pais"),
            "inner",
        ) \
        .join(
            tmp_dominio_config.alias("co"),
            (
                (col("co.id_pais") == col("mp.id_pais")) &
                (col("co.nombre_tabla") == lit("m_asignacion_modulo")) &
                (col("co.nombre_origen") == lit("bigmagic"))
            ),
            "inner",
        ) \
        .where(col("mp.id_pais").isin(cod_pais)) \
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
                trim(col("mm.cod_cliente")),
            ).alias("id_asignacion_modulo"),
            col("mp.id_pais").alias("id_pais"),  
            concat(
                trim(col("suc.cod_compania")),
                lit("|"),
                trim(col("suc.cod_sucursal")),
            ).alias("id_sucursal"),
            concat(
                trim(col("suc.cod_compania")),
                lit("|"),
                trim(col("mm.cod_cliente")),
            ).alias("id_cliente"),
            concat(
                trim(col("mm.cod_compania")),
                lit("|"),
                trim(col("mm.cod_sucursal")),
                lit("|"),
                trim(col("mm.cod_fuerza_venta")),
                lit("|"),
                trim(col("mm.cod_modulo")),
            ).alias("id_modulo"),
            trim(col("mm.cod_modulo")).alias("cod_modulo"),
            lit(None).alias("fecha_inicio"),
            lit(None).alias("fecha_fin"),
            lit(None).alias("frecuencia_visita"),
            lit(None).alias("periodo_visita"),
            when(
                (col("mc.cod_sucursal").isNull()) | (col("mm.cod_sucursal") == col("mc.cod_sucursal")),
                lit(1)
            ).otherwise(lit(0)).alias("es_activo"),
            lit(0).alias("es_eliminado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    
    tmp_dominio_m_asignacion_modulo = sf_bm \
        .union(table_bm) 

    tmp_dominio_m_asignacion_modulo_patch = asignacion_modulo__c.alias("vadmc") \
        .join(modulo__c.alias("mc"), col("vadmc.modulo__c") == col("mc.id"), "left") \
        .join(account.alias("a"), col("vadmc.cliente__c") == col("a.id"), "left") \
        .where(col("vadmc.pais__c").isin(cod_pais)) \
        .withColumn(
            "frecuencia_visita",
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(col("vadmc.dias_de_visita__c"), "1", "Lunes"),
                                    "2", "Martes"
                                ),
                                "3", "Miercoles"
                            ),
                            "4", "Jueves"
                        ),
                        "5", "Viernes"
                    ),
                    "6", "Sabado"
                ),
                "7", "Domingo"
            )
        ) \
        .withColumn(
            "periodo_visita",
            when(col("vadmc.periodo_de_visita__c") == "F1", "Semanal")
            .when(col("vadmc.periodo_de_visita__c") == "F2", "Cada 2 semanas")
            .when(col("vadmc.periodo_de_visita__c") == "F3", "Cada 3 semanas")
            .when(col("vadmc.periodo_de_visita__c") == "F4", "Cada 4 semanas")
            .otherwise(None)
        ) \
        .withColumn(
            "es_activo",
            when(substring(col("vadmc.activa__c"), 0, 1) == "t", lit(1)).otherwise(lit(0))
        ) \
        .withColumn(
            "es_eliminado",
            when(substring(col("vadmc.isdeleted"), 0, 1) == "f", lit(0)).otherwise(lit(1))
        ) \
        .select(
            col("mc.pais__c").alias("id_pais"),
            trim(col("mc.compania_sucursal__c")).alias("id_sucursal"),
            trim(col("a.codigo_unico__c")).alias("id_cliente"),
            trim(col("mc.name")).alias("cod_modulo"),
            col("frecuencia_visita"),
            col("periodo_visita"),
            col("vadmc.fecha_de_inicio__c").alias("fecha_inicio"),
            col("vadmc.fecha_fin__c").alias("fecha_fin"),
            col("es_activo"),
            col("es_eliminado"),
        )
    
except Exception as e:
    logger.error(str(e))
    raise

######################################


################################################################
# "UPDATE" tmp_dominio_m_asignacion_modulo usando tmp_dominio_m_asignacion_modulo_patch
#
#   SQL: 
#   update tmp_dominio_m_asignacion_modulo
#   set freq_visita=b.freq_visita, ...
#   from tmp_dominio_m_asignacion_modulo a
#   inner join tmp_dominio_m_asignacion_modulo_patch b 
#       on a.id_sucursal=b.id_sucursal and ...
#   inner join dominio_prod.conf_origen co ...
#
# En PySpark: join + coalesce
try:
    # alias principal: tdm (tmp_dominio_m_asignacion_modulo)
    # alias patch: p
    # Hacemos left-join: 
    # Ya no re-usa tmp_dominio_config porque esa unión se hizo antes, en la creación del sub-DataFrame de BigMagic (table_bm).
    updated_tmp = tmp_dominio_m_asignacion_modulo.alias("tdm") \
        .join( 
            tmp_dominio_m_asignacion_modulo_patch.alias("p"),
            (col("tdm.id_sucursal") == col("p.id_sucursal")) &
            (col("tdm.cod_modulo") == col("p.cod_modulo")) &
            (col("tdm.id_cliente") == col("p.id_cliente")) &
            (col("tdm.id_pais") == col("p.id_pais")) 
            ,"left"
        ) \
        .where(
            (col("p.es_activo") == 1)
            & (col("p.es_eliminado") == 0)
        )\
        .select(
            col("tdm.id_asignacion_modulo"),
            col("tdm.id_pais"),
            col("tdm.id_sucursal"),
            col("tdm.id_cliente"),
            col("tdm.id_modulo"),
            #col("tdm.cod_modulo"),
            coalesce(col("p.fecha_inicio"), col("tdm.fecha_inicio")).alias("fecha_inicio"),
            coalesce(col("p.fecha_fin"), col("tdm.fecha_fin")).alias("fecha_fin"),
            coalesce(col("p.frecuencia_visita"), col("tdm.frecuencia_visita")).alias("frecuencia_visita"),
            coalesce(col("p.periodo_visita"), col("tdm.periodo_visita")).alias("periodo_visita"),
            col("tdm.es_activo"),
            col("tdm.es_eliminado"),
            col("tdm.fecha_creacion"),
            col("tdm.fecha_modificacion"),
        )
    

    # Sobrescribimos tmp_dominio_m_asignacion_modulo con su versión “actualizada”
    tmp_dominio_m_asignacion_modulo = updated_tmp

except Exception as e:
    logger.error(str(e))
    raise


################################################################
# INSERT final a dominio_prod.m_asignacion_modulo
#
#   insert into dominio_prod.m_asignacion_modulo (...)
#   select ...
#   from tmp_dominio_m_asignacion_modulo a
#   where not exists(select 1 from dominio_prod.m_asignacion_modulo b 
#                    where a.id_asignacion_modulo=b.id_asignacion_modulo and a.id_pais=b.id_pais);

try:
    # join_cond = [
    #     tmp_dominio_m_asignacion_modulo["id_asignacion_modulo"] == m_asignacion_modulo_dom["id_asignacion_modulo"],
    #     tmp_dominio_m_asignacion_modulo["id_pais"] == m_asignacion_modulo_dom["id_pais"],
    # ]

    # df_nuevos = tmp_dominio_m_asignacion_modulo.join(m_asignacion_modulo_dom, join_cond, "left_anti")

    df_nuevos = tmp_dominio_m_asignacion_modulo

    # #Imprimimos los esquemas para debuggear
    # tmp_dominio_m_asignacion_modulo.printSchema()
    # df_nuevos.printSchema()

    # Concatenamos los que ya existen + los nuevos
    df_concatenado = df_nuevos

except Exception as e:
    raise

################################################################


################################################################
# UPDATE final a dominio_prod.m_asignacion_modulo
#
#   update dominio_prod.m_asignacion_modulo
#   set ...
#   from tmp_dominio_m_asignacion_modulo ...
#
try:
    df_final = df_concatenado.alias("mam") \
        .join(
            tmp_dominio_m_asignacion_modulo.alias("tmam"),
            on=[
                col("mam.id_asignacion_modulo") == col("tmam.id_asignacion_modulo"),
                col("mam.id_pais") == col("tmam.id_pais"),
            ],
            how="left"
        ) \
        .select(
            coalesce(col("tmam.id_asignacion_modulo"), col("mam.id_asignacion_modulo")).cast(StringType()).alias("id_asignacion_modulo"),
            coalesce(col("tmam.id_pais"), col("mam.id_pais")).cast(StringType()).alias("id_pais"),
            coalesce(col("tmam.id_sucursal"), col("mam.id_sucursal")).cast(StringType()).alias("id_sucursal"),
            coalesce(col("tmam.id_cliente"), col("mam.id_cliente")).cast(StringType()).alias("id_cliente"),
            coalesce(col("tmam.id_modulo"), col("mam.id_modulo")).cast(StringType()).alias("id_modulo"),
            coalesce(col("tmam.fecha_inicio"), col("mam.fecha_inicio")).cast(TimestampType()).alias("fecha_inicio"),
            coalesce(col("tmam.fecha_fin"), col("mam.fecha_fin")).cast(TimestampType()).alias("fecha_fin"),
            coalesce(col("tmam.frecuencia_visita"), col("mam.frecuencia_visita")).cast(StringType()).alias("frecuencia_visita"),
            coalesce(col("tmam.periodo_visita"), col("mam.periodo_visita")).cast(StringType()).alias("periodo_visita"),
            coalesce(col("tmam.es_activo"), col("mam.es_activo")).cast(IntegerType()).alias("es_activo"),
            coalesce(col("tmam.es_eliminado"), col("mam.es_eliminado")).cast(IntegerType()).alias("es_eliminado"),
            col("mam.fecha_creacion").cast(DateType()).alias("fecha_creacion"), 
            coalesce(col("tmam.fecha_modificacion"), col("mam.fecha_modificacion")).cast(DateType()).alias("fecha_modificacion"),
        )
    
    #patch drop duplicates
    df_final = df_final.dropDuplicates(["id_asignacion_modulo", "id_pais"]) #patch reactivates as to 28-04-2025

    id_columns = ["id_asignacion_modulo"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_final, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise