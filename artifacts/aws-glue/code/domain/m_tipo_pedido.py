import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, concat, lit, coalesce, when, trim, row_number,current_date,upper
from pyspark.sql.types import StringType,TimestampType

spark_controller = SPARK_CONTROLLER() 
target_table_name = "m_tipo_pedido"
try:
    df_m_documento_transaccion = spark_controller.read_table(data_paths.BIGMAGIC, "m_documento_transaccion")
    df_m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania" )
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("Starting creation of df_m_tipo_pedido")
    df_dom_m_tipo_pedido = (
        df_m_documento_transaccion.alias("a")
        .join(
            df_m_compania.alias("e"),
            col("a.cod_compania") == col("e.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("e.cod_pais") == col("mp.cod_pais"), "inner")
        .select(
            concat(
                trim(col("a.cod_compania")),
                lit("|"),
                trim(col("a.cod_documento_transaccion")),
            ).cast(StringType()).alias("id_tipo_pedido"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            trim(col("a.cod_documento_transaccion")).cast(StringType()).alias("cod_tipo_pedido"),
            coalesce(col("a.desc_documento_transaccion"), lit("ninguno")).cast(StringType()).alias("nomb_tipo_pedido"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    id_columns = ["id_tipo_pedido"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dom_m_tipo_pedido, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert of {target_table_name} finished")
except Exception as e:
    logger.error(f"Error processing df_m_tipo_pedido: {e}")
    raise ValueError(f"Error processing df_m_tipo_pedido: {e}")