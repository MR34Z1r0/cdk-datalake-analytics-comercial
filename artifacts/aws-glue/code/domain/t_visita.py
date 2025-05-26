import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat, concat_ws, lit, coalesce, when, date_format, round, trim, to_date, substring, lower, to_timestamp, row_number, max, sum
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "t_visita"
try:
    PERIODOS= spark_controller.get_periods()
    cod_pais = COD_PAIS.split(",")
    logger.info(f"Databases: {cod_pais}")

    df_m_pais = spark_controller.read_table(data_paths.BIG_BAGIC, "m_pais", cod_pais=cod_pais, have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.BIG_BAGIC, "m_compania", cod_pais=cod_pais)
    df_m_parametro = spark_controller.read_table(data_paths.BIG_BAGIC, "m_parametro", cod_pais=cod_pais)
    df_t_historico_visita = spark_controller.read_table(data_paths.BIG_BAGIC, "t_visita", cod_pais=cod_pais)
    
    df_avance_del_dia__c = spark_controller.read_table(data_paths.SALESFORCE, "t_avance_dia", cod_pais=cod_pais)
    df_avance_autoventa__c = spark_controller.read_table(data_paths.SALESFORCE, "t_avance_autoventa", cod_pais=cod_pais)
    df_visita__c = spark_controller.read_table(data_paths.SALESFORCE, "t_visita", cod_pais=cod_pais) 
    df_account = spark_controller.read_table(data_paths.SALESFORCE, "m_cliente", cod_pais=cod_pais) 
    df_modulo__c = spark_controller.read_table(data_paths.SALESFORCE, "m_modulo", cod_pais=cod_pais) 

    df_conf_origen = spark_controller.read_table(data_paths.DOMINIO, "conf_origen").cache()

    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(e)
    raise
try:
    logger.info("starting filter of pais and periodo") 
    df_t_historico_visita = df_t_historico_visita.filter(date_format(col("fecha_visita"), "yyyyMM").isin(PERIODOS))
    df_visita__c = df_visita__c.filter(date_format(to_date(substring(col("fecha_de_vencimiento__c"),1,10), "yyyy-MM-dd"), "yyyyMM").isin(PERIODOS))
    df_avance_del_dia__c = df_avance_del_dia__c.filter(date_format(to_date(substring(col("fecha__c"),1,10), "yyyy-MM-dd"), "yyyyMM").isin(PERIODOS))
    df_avance_autoventa__c = df_avance_autoventa__c.filter(date_format(to_date(substring(col("fecha__c"),1,10), "yyyy-MM-dd"), "yyyyMM").isin(PERIODOS))

    logger.info("starting creation of df_m_compania")
    df_m_compania = (
        df_m_compania.alias("mc")
            .join(
                df_m_pais.alias("mp"),
                col("mp.cod_pais") == col("mc.cod_pais"),
            )
        .select(col("mp.id_pais"), col("mc.cod_pais"), col("mc.cod_compania"))
    )

    df_avance_del_dia__c_filter = (
        df_avance_del_dia__c.alias("ad")
        .select(
            col("id").alias("id_avance_dia"), 
            concat_ws("|", col("compania__c"), col("sucursal__c")).alias("id_sucursal")
        )
    )

    df_avance_autoventa__c_filter = (
        df_avance_autoventa__c.alias("ad")
        .select(
            col("id").alias("id_avance_dia"), 
            concat_ws("|", col("compania__c"), col("sucursal__c")).alias("id_sucursal")
        )
    )

    df_avance_del_dia__c_union = (
        df_avance_del_dia__c_filter.union(df_avance_autoventa__c_filter)
    )

    logger.info("starting creation of df_visita__c_select")
    df_visita__c_select = (
        df_visita__c.alias("vvc")
        .join(
            df_conf_origen.alias("co"),
            (col("co.id_pais") == trim(col("vvc.pais__c")))
            & (col("co.nombre_tabla") == "t_visita")
            & (col("co.nombre_origen") == "salesforce"),
            "inner",
        )
        .join(
            df_account.alias("va"),
            col("va.id") == col("vvc.cliente__c"),
            "left",
        )
        .join(
            df_modulo__c.alias("vm"),
            col("vm.id") == col("vvc.modulo__c"),
            "left",
        )
        .join(
            df_avance_del_dia__c_union.alias("ad"),
            col("ad.id_avance_dia") == col("vvc.avance_del_dia__c")
        )
        .select(
            trim(col("vvc.pais__c")).alias("id_pais"),
            date_format(to_date(substring(col("vvc.fecha_de_vencimiento__c"),1,10), "yyyy-MM-dd"), "yyyyMM").alias("id_periodo"),
            col("vvc.id").alias("id_visita"), 
            col("ad.id_sucursal").alias("id_sucursal"),
            concat_ws("|", col("vvc.compania__c"), col("va.codigo_cliente__c")).alias("id_cliente"),
            col("vm.fuerza_de_venta__c").alias("id_fuerza_venta"), 
            col("vvc.name").alias("cod_visita"),
            to_date(substring(col("vvc.fecha_de_vencimiento__c"), 1, 10), "yyyy-MM-dd").alias("fecha_visita"),
            when(lower(substring(col("vvc.activa__c"), 0, 1)) == "t", 1).otherwise(0).alias("es_activo"),
            to_timestamp(col("vvc.createddate"), "yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'").alias("fecha_creacion"),
            to_timestamp(col("vvc.lastmodifieddate"), "yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'").alias("fecha_modificacion"),
            when(lower(substring(col("vvc.isdeleted"), 0, 1)) == "f", 0).otherwise(1).alias("es_eliminado")
        )
    )


    logger.info("starting creation of df_t_historico_visita_select")
    df_t_historico_visita_select = (
        df_t_historico_visita.alias("tvi")
        .join(
            df_m_compania.alias("mc"),
            col("tvi.cod_compania") == col("mc.cod_compania"),
            "inner",
        ) 
        .join(
            df_conf_origen.alias("co"),
            (col("co.id_pais") == trim(col("mc.id_pais")))
            & (col("co.nombre_tabla") == "t_visita")
            & (col("co.nombre_origen") == "bigmagic"),
            "inner",
        )
        .select(
            trim(col("mc.id_pais")).alias("id_pais"),
            date_format(col("tvi.fecha_visita"), "yyyyMM").alias("id_periodo"),
            concat_ws("|", date_format(col("tvi.fecha_visita"), "yyyyMMdd"), col("tvi.cod_compania"), col("tvi.cod_cliente"), col("tvi.cod_sucursal"), col("tvi.cod_fuerza_venta")).alias("id_visita"),
            concat_ws("|", col("tvi.cod_compania"), col("tvi.cod_sucursal")).alias("id_sucursal"),
            concat_ws("|", col("tvi.cod_compania"), col("tvi.cod_cliente")).alias("id_cliente"),
            concat_ws("|", col("tvi.cod_compania"), col("tvi.cod_sucursal"), col("tvi.cod_fuerza_venta")).alias("id_fuerza_venta"),
            lit(None).alias("cod_visita"),
            col("tvi.fecha_visita").alias("fecha_visita"),
            lit(1).alias("es_activo"),
            col("tvi.fecha_visita").alias("fecha_creacion"),
            col("tvi.fecha_modificacion").alias("fecha_modificacion"),
            lit(0).alias("es_eliminado"),
        )
    )

    logger.info("starting creation of df_t_historico_visita_union")
    df_t_historico_visita_union = df_visita__c_select.union(df_t_historico_visita_select)

    logger.info("starting creation of df_t_visita")
    df_t_visita = df_t_historico_visita_union.select(
        col("id_pais").cast("string"),
        col("id_periodo").cast("string"),
        col("id_visita").cast("string"),
        col("id_sucursal").cast("string"),
        col("id_cliente").cast("string"),
        col("id_fuerza_venta").cast("string"),
        col("cod_visita").cast("string"),
        col("fecha_visita").cast("date"),
        col("es_activo").cast("int"),
        col("fecha_creacion").cast("timestamp"),
        col("fecha_modificacion").cast("timestamp"), 
        col("es_eliminado").cast("int"),
    )
 
    partition_columns_array = ["id_pais", "id_periodo"]
    logger.info(f"starting write of {target_table_name}")
    spark_controller.write_table(df_t_visita, data_paths.DOMINIO, target_table_name, partition_columns_array)
except Exception as e:
    logger.error(e) 
    raise 