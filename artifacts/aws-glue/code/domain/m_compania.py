import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, DateType

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")
    m_compania = spark_controller.read_table(data_paths.BIG_BAGIC, "m_compania", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.BIG_BAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)

    compania__c = spark_controller.read_table(data_paths.SALESFORCE, "m_compania", cod_pais=cod_pais)
    target_table_name = "m_compania"
except Exception as e:
    logger.error(e)
    raise 
try:
    tmp_dominio_m_compania = (
        m_compania.alias("mc")
        .join(
            compania__c.alias("sc"),
            col("mc.cod_compania") == col("sc.codigo__c"),
            "left",
        )
        .join(m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            col("mc.cod_compania").cast(StringType()).alias("id_compania"),
            col("sc.id").cast(StringType()).alias("id_compania_ref"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            col("cod_compania").cast(StringType()).alias("cod_compania"),
            col("desc_compania").cast(StringType()).alias("nomb_compania"),
            lit(None).cast(StringType()).alias("cod_tipo_compania"), 
            col("es_activo").cast(StringType()).alias("estado"),
            col("mc.fecha_creacion").cast(DateType()).alias("fecha_creacion"),
            col("mc.fecha_modificacion").cast(DateType()).alias("fecha_modificacion"),
        )
    )

    id_columns = ["id_compania"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(tmp_dominio_m_compania, data_paths.DOMINIO, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(str(e))
    raise