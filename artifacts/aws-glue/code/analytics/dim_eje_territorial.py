import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, split
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_eje_territorial" 
try:
    df_m_eje_territorial = spark_controller.read_table(data_paths.DOMAIN, "m_eje_territorial") 
    df_m_pais = spark_controller.read_table(data_paths.DOMAIN, "m_pais")
    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("Starting creation of df_dim_producto")
    df_dim_eje_territorial_ng4 = (
        df_m_eje_territorial.alias("di")
        .where(col("di.cod_tipo_eje_territorial") == 'NG4')
        .select(
            col('di.id_eje_territorial'),
            col('di.id_eje_territorial_padre'),
            col('di.id_pais'),
            col('di.cod_eje_territorial'),
            col('di.cod_eje_territorial_ref'),
            col('di.nomb_eje_territorial'),
            col('di.cod_tipo_eje_territorial')
        )
    ) 

    df_dim_eje_territorial_ng3 = (
        df_m_eje_territorial.alias("di")
        .where(col("di.cod_tipo_eje_territorial") == 'NG3')
        .select(
            col('di.id_eje_territorial'),
            col('di.id_eje_territorial_padre'),
            col('di.id_pais'),
            col('di.cod_eje_territorial'),
            col('di.cod_eje_territorial_ref'),
            col('di.nomb_eje_territorial'),
            col('di.cod_tipo_eje_territorial')
        )
    )
    
    df_dim_eje_territorial_ng2 = (
        df_m_eje_territorial.alias("pr")
        .where(col("pr.cod_tipo_eje_territorial") == 'NG2')
        .select(
            col('pr.id_eje_territorial'),
            col('pr.id_eje_territorial_padre'),
            col('pr.id_pais'),
            col('pr.cod_eje_territorial'),
            col('pr.nomb_eje_territorial'),
            col('pr.cod_tipo_eje_territorial')
        )
    )
    
    df_dim_eje_territorial_ng1 = (
        df_m_eje_territorial.alias("de")
        .where(col("de.cod_tipo_eje_territorial") == 'NG1')
        .select(
            col('de.id_eje_territorial'),
            col('de.id_eje_territorial_padre'),
            col('de.id_pais'),
            col('de.cod_eje_territorial'),
            col('de.nomb_eje_territorial'),
            col('de.cod_tipo_eje_territorial')
        )
    )
    
    df_dim_eje_territorial = (
        df_dim_eje_territorial_ng4.alias("ng4")
        .join(
            df_dim_eje_territorial_ng3.alias("ng3"),
            (col("ng3.id_eje_territorial") == col("ng4.id_eje_territorial_padre")),
            "inner",
        )
        .join(
            df_dim_eje_territorial_ng2.alias("ng2"),
            (col("ng2.id_eje_territorial") == col("ng3.id_eje_territorial_padre")),
            "inner",
        )
        .join(
            df_dim_eje_territorial_ng1.alias("ng1"),
            (col("ng1.id_eje_territorial") == col("ng2.id_eje_territorial_padre")),
            "inner",
        )
        .join(
            df_m_pais.alias("mp"),
            (col("ng4.id_pais") == col("mp.id_pais")),
            "inner",
        )
        .select(
            col('ng4.id_eje_territorial').cast("string").alias('id_eje_territorial'),
            col('mp.id_pais').cast("string"),
            col('mp.cod_pais').cast("string"),
            col('mp.desc_pais').cast("string"),
            col('ng1.cod_eje_territorial').cast("string").alias('cod_ng1'),
            col('ng1.nomb_eje_territorial').cast("string").alias('desc_ng1'),
            col('ng2.cod_eje_territorial').cast("string").alias('cod_ng2'),
            col('ng2.nomb_eje_territorial').cast("string").alias('desc_ng2'),
            col('ng3.cod_eje_territorial').cast("string").alias('cod_ng3'),
            col('ng3.nomb_eje_territorial').cast("string").alias('desc_ng3'),
            col('ng4.cod_eje_territorial').cast("string").alias('cod_ng4'),
            col('ng4.nomb_eje_territorial').cast("string").alias('desc_ng4'),
            split(col("ng4.cod_eje_territorial_ref"), '\\|').getItem(1).cast("string").alias("zona_postal"),
        )
    )

    id_columns = ["id_eje_territorial"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_eje_territorial, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} success completed")
except Exception as e:
    logger.error(f"Error processing df_dim_eje_territorial: {e}")
    raise ValueError(f"Error processing df_dim_eje_territorial: {e}") 