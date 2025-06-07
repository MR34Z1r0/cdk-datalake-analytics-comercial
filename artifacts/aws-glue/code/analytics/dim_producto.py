import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_producto"
try:
    df_m_articulo = spark_controller.read_table(data_paths.DOMAIN, "m_articulo")
    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("Starting creation of df_dim_producto")
    df_dim_producto = (
        df_m_articulo
        .select(
            col('id_articulo').cast("string").alias('id_producto'),
            col('id_pais').cast("string"),
            col('cod_articulo').cast("string").alias('cod_producto'),
            col('desc_articulo').cast("string").alias('desc_producto'),
            col('desc_articulo_corp').cast("string"),
            col('cod_categoria').cast("string"),
            col('desc_categoria').cast("string"),
            col('cod_marca').cast("string"),
            col('desc_marca').cast("string"),
            col('cod_presentacion').cast("string"),
            col('desc_presentacion').cast("string"),
            col('cod_formato').cast("string"),
            col('desc_formato').cast("string"),
            col('cod_sabor').cast("string"),
            col('desc_sabor').cast("string"),
            col('cod_tipo_envase').cast("string"),
            col('desc_tipo_envase').cast("string"),
            col('cod_unidad_negocio').cast("string"),
            col('desc_unidad_negocio').cast("string"),
            col('cod_unidad_manejo').cast("string").alias('cod_unidad_paquete'),
            col('cod_unidad_volumen').cast("numeric(38,12)").alias('cod_unidad_volumen'),
            col('cant_unidad_paquete').cast("numeric(38,12)").alias('cant_unidad_paquete'),
            col('cant_unidad_volumen').cast("numeric(38,12)").alias('cant_unidad_volumen'),
            col('es_activo').cast("int").alias('es_activo')
        )
    )

    id_columns = ["id_producto"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dim_producto, data_paths.ANALYTICS, target_table_name, id_columns, partition_columns_array) 
    logger.info(f"Upsert de {target_table_name} success completed")     
except Exception as e:
    logger.error(f"Error processing df_dim_producto: {e}")
    raise ValueError(f"Error processing df_dim_producto: {e}") 