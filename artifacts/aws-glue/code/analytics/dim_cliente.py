import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_cliente" 
try:
    cod_pais = COD_PAIS.split(",")
    df_m_cliente = spark_controller.read_table(data_paths.DOMINIO, "m_cliente", cod_pais=cod_pais) 
    df_m_eje_territorial = spark_controller.read_table(data_paths.DOMINIO, "m_eje_territorial", cod_pais=cod_pais) 
    df_m_asignacion_modulo = spark_controller.read_table(data_paths.DOMINIO, "m_asignacion_modulo", cod_pais=cod_pais) 
    df_m_modulo = spark_controller.read_table(data_paths.DOMINIO, "m_modulo", cod_pais=cod_pais) 
    df_m_modelo_atencion = spark_controller.read_table(data_paths.DOMINIO, "m_modelo_atencion", cod_pais=cod_pais)

    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(e)
    raise
try:
    logger.info("Starting creation of df_m_asignacion_modulo_filter")
    df_m_asignacion_modulo_filter = (
        df_m_asignacion_modulo.alias("mam")
        .filter((col("es_activo") == 1) & (col("es_eliminado") == 0))
        .join(
            df_m_modulo.alias("mm"),
            col("mm.id_modulo") == col("mam.id_modulo"),
            "left",
        )
        .join(
            df_m_modelo_atencion.alias("mma"),
            col("mma.id_modelo_atencion") == col("mm.id_modelo_atencion"),
            "left",
        )
        .select(
            col("mam.id_cliente"),
            col("mm.id_modulo"),
            col("mam.frecuencia_visita"),
            col("mam.periodo_visita"),
            when(col("mma.desc_modelo_atencion") == "Pre Venta", 1)
            .when(col("mma.desc_modelo_atencion") == "Especializado", 2)
            .when(col("mma.desc_modelo_atencion") == "Auto Venta", 3)
            .when(col("mma.desc_modelo_atencion") == "Televenta", 4)
            .otherwise(5).alias("orden_modelo_atencion"),
            col("mm.fecha_creacion"),
        ).
        select(
            row_number()
            .over(
                Window.partitionBy("mam.id_cliente").orderBy(col("orden_modelo_atencion").asc(), col("mm.fecha_creacion").desc()
                )
            )
            .alias("orden"),
            col("mam.id_cliente"),
            col("mm.id_modulo"),
            col("mam.frecuencia_visita"),
            col("mam.periodo_visita")
        )
    )
    
    logger.info("Starting creation of df_m_cliente_select")
    df_m_cliente_select = (
        df_m_cliente.alias("mc")
        .join(
            df_m_asignacion_modulo_filter.alias("dc"),
            (col("mc.id_cliente") == col("dc.id_cliente")) & (col("dc.orden") == 1),
            "left",
        )
        .join(
            df_m_eje_territorial.alias("met"),
            (col("mc.id_eje_territorial") == col("met.id_eje_territorial")),
            "left",
        )
        .select(
            col("mc.id_cliente"),
            col("mc.id_pais"),
            col("mc.id_sucursal"),
            col("dc.id_modulo").alias("id_estructura_comercial"),
            col("mc.id_clasificacion_cliente"),
            col("mc.id_eje_territorial"),
            col("mc.id_lista_precio"),
            col("mc.cod_cliente"),
            col("mc.nomb_cliente"),
            col("mc.cod_segmento"),
            col("mc.desc_subsegmento"),
            col("mc.cod_cliente_ref"),
            col("mc.cod_cliente_ref2"),
            col("mc.cod_cliente_ref3"),
            col("mc.cod_cliente_ref4"), 
            col("mc.cod_tipo_cliente"),
            col("mc.cod_cuenta_clave"),
            col("mc.nomb_cuenta_clave"),
            col("mc.desc_canal_local"),
            col("mc.desc_giro_local"),
            col("mc.direccion"),
            col("mc.nro_documento"),
            col("mc.cod_cliente_principal"),
            col("mc.cod_cliente_transferencia"),
            col("met.cod_eje_territorial"),
            col("mc.coord_x").alias("coordx"),
            col("mc.coord_y").alias("coordy"),
            col("mc.fecha_creacion"),
            col("mc.fecha_baja"),
            col("mc.estado")
        )
    )

    df_dim_cliente = (
        df_m_cliente_select
        .select(
            col("id_cliente").cast("string"),
            col("id_pais").cast("string"),
            col("id_sucursal").cast("string"),
            col("id_estructura_comercial").cast("string"),
            col("id_clasificacion_cliente").cast("string"),
            col("id_eje_territorial").cast("string"),
            col("id_lista_precio").cast("string"),
            col("cod_cliente").cast("string"),
            col("nomb_cliente").cast("string"),
            col("cod_segmento").cast("string"),
            col("desc_subsegmento").cast("string"),
            col("cod_cliente_ref").cast("string"),
            col("cod_cliente_ref2").cast("string"),
            col("cod_cliente_ref3").cast("string"),
            col("cod_cliente_ref4").cast("string"),
            col("cod_tipo_cliente").cast("string"),
            col("cod_cuenta_clave").cast("string"),
            col("nomb_cuenta_clave").cast("string"),
            col("desc_canal_local").cast("string"),
            col("desc_giro_local").cast("string"),
            col("direccion").cast("string"),
            col("nro_documento").cast("string"),
            col("cod_cliente_principal").cast("string"),
            col("cod_cliente_transferencia").cast("string"),
            col("cod_eje_territorial").cast("string"),
            col("coordx").cast("string"),
            col("coordy").cast("string"),
            col("fecha_creacion").cast("timestamp"),
            col("fecha_baja").cast("timestamp"),
            col("estado").cast("string")
        )
    )

    id_columns = ["id_cliente"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(df_dim_cliente, data_paths.COMERCIAL, target_table_name, id_columns, partition_columns_array)
except Exception as e:
    logger.error(e)
    raise