from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat,lit
from pyspark.sql.types import StringType

spark_controller = SPARK_CONTROLLER()

try:

    cod_pais = COD_PAIS.split(",")

    m_compania = spark_controller.read_table(data_paths.BIG_MAGIC, "m_compania", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.BIG_MAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)
    m_unidad_medida_equivalente = spark_controller.read_table(data_paths.BIG_MAGIC, "m_unidad_medida_equivalente", cod_pais=cod_pais)

    target_table_name = "m_unidad_medida_equivalente"
    
except Exception as e:
    logger.error(e)
    raise 
try: 
    m_pais = m_pais.filter(col("id_pais").isin(cod_pais))

    tmp = (
        m_unidad_medida_equivalente.alias("mume")
        .join(m_compania.alias("mc"), col("mc.cod_compania") == col("mume.cod_compania"), "inner")
        .join(m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .select(
            concat(
                col("mume.cod_compania").cast("string"),
                lit("|"),
                col("mume.cod_unidad_medida_desde").cast("string"),
                lit("|"),
                col("mume.cod_unidad_medida_hasta").cast("string"),
            ).cast(StringType()).alias("id_unidad_medida_equivalente"),
            col("mp.id_pais").cast("string").alias("id_pais"),
            col("mume.id_compania").cast("string").alias("id_compania"),
            col("mume.cod_unidad_medida_desde").cast("string").alias("cod_unidad_medida_desde"),
            col("mume.cod_unidad_medida_hasta").cast("string").alias("cod_unidad_medida_hasta"),
            col("mume.cod_operador").cast("string").alias("cod_operador"),
            col("mume.cant_operacion").cast("numeric(38, 12)").alias("cant_operacion"),
        )
    )

    id_columns = ["id_unidad_medida_equivalente"]
    partition_columns_array = ["id_pais"]    
    spark_controller.upsert(tmp, data_paths.DOMINIO, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise