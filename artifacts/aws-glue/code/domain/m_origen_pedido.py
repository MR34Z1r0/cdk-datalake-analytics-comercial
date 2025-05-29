import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat, lit, coalesce, when, trim, row_number,current_date,upper,split
from pyspark.sql.types import StringType, DateType

spark_controller = SPARK_CONTROLLER() 
try: 
    cod_pais = COD_PAIS.split(",") 

    df_m_origen_pedido = spark_controller.read_table(data_paths.BIG_MAGIC, "m_origen_pedido", cod_pais=cod_pais)
    df_m_pais = spark_controller.read_table(data_paths.BIG_MAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.BIG_MAGIC, "m_compania", cod_pais=cod_pais) 
    target_table_name = "m_origen_pedido"

except Exception as e:
    logger.error(str(e))
    raise 
try:
    tmp_m_origen_pedido = (
        df_m_origen_pedido.alias("mtdp")
        .join(
            df_m_compania.alias("mc"),
            col("mtdp.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            concat(
                trim(col("mtdp.cod_compania")),
                lit("|"),
                trim(col("mtdp.cod_origen_pedido"))
            ).cast(StringType()).alias("id_origen_pedido"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            trim(col("mtdp.cod_origen_pedido")).cast(StringType()).alias("cod_origen_pedido"),
            col("mtdp.desc_origen_pedido").cast(StringType()).alias("nomb_origen_pedido"),
            current_date().cast(DateType()).alias("fecha_creacion"),
            current_date().cast(DateType()).alias("fecha_modificacion")
        )
    )

    id_columns = ["id_origen_pedido"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(tmp_m_origen_pedido, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(str(e))
    raise