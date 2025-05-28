import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat, lit, coalesce, when, trim, row_number,current_date,upper
from pyspark.sql.types import StringType,TimestampType

spark_controller = SPARK_CONTROLLER() 
try:
    cod_pais = COD_PAIS.split(",") 
    df_m_documento_transaccion = spark_controller.read_table(
        data_paths.BIG_MAGIC, "m_documento_transaccion", cod_pais=cod_pais
    )
    df_m_pais = spark_controller.read_table(data_paths.BIG_MAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.BIG_MAGIC, "m_compania", cod_pais=cod_pais)
 
    target_table_name = "m_tipo_pedido"
except Exception as e:
    logger.error(e)
    raise 
try:
    tmp_m_tipo_pedido = (
        df_m_documento_transaccion.alias("a")
        .join(
            df_m_compania.alias("e"),
            col("a.cod_compania") == col("e.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("e.cod_pais") == col("mp.cod_pais"), "inner")
        .where(col("mp.id_pais").isin(cod_pais))
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
    spark_controller.upsert(tmp_m_tipo_pedido, data_paths.DOMINIO, target_table_name, id_columns, partition_columns_array)
 
except Exception as e:
    logger.error(str(e))
    raise