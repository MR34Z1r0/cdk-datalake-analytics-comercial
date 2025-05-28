import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import StringType, DateType

spark_controller = SPARK_CONTROLLER() 
try: 
    cod_pais = COD_PAIS.split(",")
 
    df_m_sucursal = spark_controller.read_table(data_paths.BIG_MAGIC, "m_sucursal", cod_pais=cod_pais)
    df_m_pais = spark_controller.read_table(data_paths.BIG_MAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.BIG_MAGIC, "m_compania", cod_pais=cod_pais)
 
    df_sucursal__c = spark_controller.read_table(data_paths.SALESFORCE, "m_sucursal", cod_pais=cod_pais)
 
    target_table_name = "m_sucursal"

except Exception as e:
    logger.error(e)
    raise 
try:
    tmp_dominio_m_sucursal = (
        df_m_sucursal.alias("ms")
        .join(
            df_sucursal__c.alias("ss"),
            (col("ss.codigo__c") == col("ms.cod_sucursal"))
            & (col("ss.codigo_de_compania__c") == col("ms.cod_compania")),
            "left",
        )
        .join(
            df_m_compania.alias("mc"),
            col("ms.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            concat_ws("|", col("ms.cod_compania"), col("ms.cod_sucursal")).cast(StringType()).alias("id_sucursal"),
            col("ss.id").cast(StringType()).alias("id_sucursal_ref"),
            col("ms.cod_compania").cast(StringType()).alias("id_compania"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            col("ms.cod_sucursal").cast(StringType()).alias("cod_sucursal"),
            col("ms.desc_sucursal").cast(StringType()).alias("nomb_sucursal"),
            col("ss.tipo_de_sucursal__c").cast(StringType()).alias("cod_tipo_sucursal"),
            col("ms.es_activo").cast(StringType()).alias("estado"),
            col("ms.fecha_creacion").cast(DateType()).alias("fecha_creacion"),
            col("ms.fecha_modificacion").cast(DateType()).alias("fecha_modificacion"),
        )
    )

    id_columns = ["id_sucursal"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(tmp_dominio_m_sucursal, data_paths.DOMINIO, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(str(e))
    raise