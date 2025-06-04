import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, concat, concat_ws, lit, coalesce, when, date_format, round, trim, to_date, substring, lower, to_timestamp, row_number, max, sum
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "t_visita"
try:
    PERIODOS= spark_controller.get_periods()
    logger.info(f"Periods to filter: {PERIODOS}")
    df_m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
    df_m_parametro = spark_controller.read_table(data_paths.BIGMAGIC, "m_parametro")

    df_t_historico_visita = spark_controller.read_table(data_paths.BIGMAGIC, "t_visita") 
    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("starting filter of pais and periodo") 
    df_t_historico_visita = df_t_historico_visita.filter(date_format(col("fecha_visita"), "yyyyMM").isin(PERIODOS))

    logger.info("starting creation of df_m_compania")
    df_m_compania = (
        df_m_compania.alias("mc")
            .join(
                df_m_pais.alias("mp"),
                col("mp.cod_pais") == col("mc.cod_pais"),
            )
        .select(col("mp.id_pais"), col("mc.cod_pais"), col("mc.cod_compania"))
    ) 
    logger.info("starting creation of df_t_historico_visita_select")
    df_t_historico_visita_select = (
        df_t_historico_visita.alias("tvi")
        .join(
            df_m_compania.alias("mc"),
            col("tvi.cod_compania") == col("mc.cod_compania"),
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

    logger.info("starting creation of df_t_visita")
    df_dom_t_visita = df_t_historico_visita_select.select(
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
    
    logger.info(f"starting write of {target_table_name}")
    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(df_dom_t_visita, data_paths.DOMAIN, target_table_name, partition_columns_array)
    logger.info(f"Write de {target_table_name} success completed")
except Exception as e:
    logger.error(f"Error processing df_dom_t_visita: {e}")
    raise ValueError(f"Error processing df_dom_t_visita: {e}")