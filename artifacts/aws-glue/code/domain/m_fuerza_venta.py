import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat, lit, coalesce, when, trim, row_number,current_date,upper
from pyspark.sql.types import StringType

spark_controller = SPARK_CONTROLLER()
try:
    cod_pais = COD_PAIS.split(",")
    df_m_fuerza_venta = spark_controller.read_table(data_paths.BIG_MAGIC, "m_fuerza_venta", cod_pais=cod_pais)
    df_m_pais = spark_controller.read_table(data_paths.BIG_MAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.BIG_MAGIC, "m_compania", cod_pais=cod_pais)

    df_conf_origen = spark_controller.read_table(data_paths.DOMINIO, "conf_origen")

    target_table_name = "m_fuerza_venta"

except Exception as e:
    logger.error(e)
    raise
try:
    tmp1 = (
        df_m_fuerza_venta.alias("mfv")
        .join(
            df_m_compania.alias("mc"),
            col("mfv.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .join(df_conf_origen.alias("co"), (col("co.id_pais") == col("mp.id_pais"))
              & (col("co.nombre_tabla") == "m_fuerza_venta")
              & (col("co.nombre_origen") == "bigmagic"),
              "inner",
        )
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            concat(
                trim(col("mfv.cod_compania")),
                lit("|"),
                trim(col("mfv.cod_sucursal")),
                lit("|"),
                trim(col("mfv.cod_fuerza_venta")),
            ).cast(StringType()).alias("id_fuerza_venta"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            trim(col("mfv.cod_fuerza_venta")).cast(StringType()).alias("cod_fuerza_venta"),
            col("mfv.desc_fuerza_venta").cast(StringType()).alias("desc_fuerza_venta")
        )
    )
    tmp_m_fuerza_venta = tmp1

    id_columns = ["id_fuerza_venta"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(tmp_m_fuerza_venta, data_paths.DOMINIO, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(str(e))
    raise