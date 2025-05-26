import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_estructura_comercial" 
try:
    cod_pais = COD_PAIS.split(",")
    df_m_modulo = spark_controller.read_table(data_paths.DOMINIO, "m_modulo", cod_pais=cod_pais)
    df_m_pais = spark_controller.read_table(data_paths.DOMINIO, "m_pais", cod_pais=cod_pais)
    df_m_estructura_comercial = spark_controller.read_table(data_paths.DOMINIO, "m_estructura_comercial", cod_pais=cod_pais)
    df_m_modelo_atencion = spark_controller.read_table(data_paths.DOMINIO, "m_modelo_atencion", cod_pais=cod_pais)
    df_m_responsable_comercial = spark_controller.read_table(data_paths.DOMINIO, "m_responsable_comercial", cod_pais=cod_pais) 
    
    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(e)
    raise
try:
    logger.info("Starting creation of df_m_modulo_select")
    df_m_modulo_select = (
        df_m_modulo.alias("mm")
        .join(
            df_m_pais.alias("mp"),
            (col("mm.id_pais") == col("mp.id_pais")),
            "left",
        )
        .join(
            df_m_estructura_comercial.alias("mec_ruta"),
            (col("mm.id_estructura_comercial") == col("mec_ruta.id_estructura_comercial")),
            "left",
        )
        .join(
            df_m_responsable_comercial.alias("mrc_ruta"),
            (col("mec_ruta.id_responsable_comercial") == col("mrc_ruta.id_responsable_comercial")),
            "left",
        )
        .join(
            df_m_estructura_comercial.alias("mec_zona"),
            (col("mec_ruta.id_estructura_comercial_padre") == col("mec_zona.id_estructura_comercial")),
            "left",
        )
        .join(
            df_m_responsable_comercial.alias("mrc_zona"),
            (col("mec_zona.id_responsable_comercial") == col("mrc_zona.id_responsable_comercial")),
            "left",
        )
        .join(
            df_m_estructura_comercial.alias("mec_division"),
            (col("mec_zona.id_estructura_comercial_padre") == col("mec_division.id_estructura_comercial")),
            "left",
        )
        .join(
            df_m_responsable_comercial.alias("mrc_division"),
            (col("mec_division.id_responsable_comercial") == col("mrc_division.id_responsable_comercial")),
            "left",
        )
        .join(
            df_m_estructura_comercial.alias("mec_subregion"),
            (col("mec_division.id_estructura_comercial_padre") == col("mec_subregion.id_estructura_comercial")),
            "left",
        )
        .join(
            df_m_estructura_comercial.alias("mec_region"),
            (col("mec_subregion.id_estructura_comercial_padre") == col("mec_region.id_estructura_comercial")),
            "left",
        )
        .join(
            df_m_modelo_atencion.alias("mma"),
            (col("mm.id_modelo_atencion") == col("mma.id_modelo_atencion")),
            "left",
        )
        .select(
            col('mm.id_modulo').alias('id_estructura_comercial'),
            col('mm.id_pais'),
            lit(None).alias('cod_fuerza_venta'),
            col('mma.cod_modelo_atencion'),
            col('mp.cod_pais'),
            col('mec_region.cod_estructura_comercial').alias('cod_region'),
            col('mec_subregion.cod_estructura_comercial').alias('cod_subregion'),
            col('mec_division.cod_estructura_comercial').alias('cod_division'),
            col('mec_zona.cod_estructura_comercial').alias('cod_zona'),
            col('mec_ruta.cod_estructura_comercial').alias('cod_ruta'),
            col('mm.cod_modulo'),
            col('mrc_ruta.cod_responsable_comercial').alias('cod_vendedor'),
            col('mrc_ruta.nomb_responsable_comercial').alias('nomb_vendedor'),
            col('mrc_zona.cod_responsable_comercial').alias('cod_supervisor'),
            col('mrc_zona.nomb_responsable_comercial').alias('nomb_supervisor'),
            col('mrc_division.cod_responsable_comercial').alias('cod_jefe_venta'),
            col('mrc_division.nomb_responsable_comercial').alias('nomb_jefe_venta'),
            lit(None).alias('desc_fuerza_venta'),
            col('mma.desc_modelo_atencion').alias('desc_modelo_atencion'),
            col('mec_region.nomb_estructura_comercial').alias('desc_region'),
            col('mec_subregion.nomb_estructura_comercial').alias('desc_subregion'),
            col('mec_division.nomb_estructura_comercial').alias('desc_division'),
            col('mec_zona.nomb_estructura_comercial').alias('desc_zona'),
            col('mec_ruta.nomb_estructura_comercial').alias('desc_ruta'),
            col('mm.desc_modulo').alias('desc_modulo'),
        )
    )
    logger.info("Starting creation of df_dim_estructura_comercial")
    df_dim_estructura_comercial = (
        df_m_modulo_select
        .select(
            col("id_estructura_comercial").cast("string"),
            col("id_pais").cast("string"),
            col("cod_fuerza_venta").cast("string"),
            col("cod_modelo_atencion").cast("string"),
            col("cod_pais").cast("string"),
            col("cod_region").cast("string"),
            col("cod_subregion").cast("string"),
            col("cod_division").cast("string"),
            col("cod_zona").cast("string"),
            col("cod_ruta").cast("string"),
            col("cod_modulo").cast("string"),
            col("cod_vendedor").cast("string"),
            col("nomb_vendedor").cast("string"),
            col("cod_supervisor").cast("string"),
            col("nomb_supervisor").cast("string"),
            col("cod_jefe_venta").cast("string"),
            col("nomb_jefe_venta").cast("string"),
            col("desc_fuerza_venta").cast("string"),
            col("desc_modelo_atencion").cast("string"),
            col("desc_region").cast("string"),
            col("desc_subregion").cast("string"),
            col("desc_division").cast("string"),
            col("desc_zona").cast("string"),
            col("desc_ruta").cast("string"),
            col("desc_modulo").cast("string"),            
        )   
    )

    id_columns = ["id_estructura_comercial"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(df_dim_estructura_comercial, data_paths.COMERCIAL, target_table_name, id_columns, partition_columns_array)
except Exception as e:
    logger.error(e)
    raise