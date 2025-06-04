import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import StringType, DateType

spark_controller = SPARK_CONTROLLER() 
try: 
    cod_pais = COD_PAIS.split(",")

    df_m_condicion_pago = spark_controller.read_table(data_paths.BIGMAGIC, "m_forma_pago", cod_pais=cod_pais)
    df_m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania", cod_pais=cod_pais)
    df_m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)
 
    target_table_name = "m_forma_pago"

except Exception as e:
    logger.error(e)
    raise 
try:
    tmp_m_forma_pago = (
        df_m_condicion_pago.alias("mfp")
        .join(
            df_m_compania.alias("mc"),
            col("mfp.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            concat_ws("|", col("mfp.cod_compania"), col("mfp.cod_forma_pago")).cast(StringType()).alias("id_forma_pago"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            col("mfp.cod_forma_pago").cast(StringType()).alias("cod_forma_pago"),
            col("mfp.desc_forma_pago").cast(StringType()).alias("nomb_forma_pago"),
            col("mfp.fecha_creacion").cast(DateType()).alias("fecha_creacion"),
            col("mfp.fecha_modificacion").cast(DateType()).alias("fecha_modificacion"),
        )
    )

    id_columns = ["id_forma_pago"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(tmp_m_forma_pago, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(str(e))
    raise