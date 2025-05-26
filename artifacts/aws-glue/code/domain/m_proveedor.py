from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql.types import StructType, StructField, StringType

spark_controller = SPARK_CONTROLLER()
target_table_name = "m_proveedor"

try:
    cod_pais = COD_PAIS.split(",")
    m_compania = spark_controller.read_table(data_paths.BIG_BAGIC, "m_compania", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.BIG_BAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)
    m_proveedor = spark_controller.read_table(data_paths.BIG_BAGIC, "m_proveedor", cod_pais=cod_pais)

except Exception as e:
    logger.error(e)
    raise

try:
    # create default m_proveedor
    companies = m_compania.alias("mc").join(m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
    companies = companies.select("cod_compania").distinct()
    schema = StructType([
        StructField("cod_proveedor", StringType(), True),
        StructField("nombre", StringType(), True),
        StructField("ruc", StringType(), True),
    ])
    default_m_proveedor = spark_controller.spark.createDataFrame(
        [("0", "PROVEEDOR DEFAULT", "")], 
        ["cod_proveedor", "nombre", "ruc"]
    )
    default_m_proveedor = default_m_proveedor.crossJoin(companies)
    m_proveedor = m_proveedor.select(
        col("cod_proveedor"),
        col("nombre"),
        col("ruc"),
        col("cod_compania")
    ).unionByName(default_m_proveedor)

    logger.info("Starting creation of tmp_dominio_m_proveedor")
    tmp_dominio_m_proveedor = (
        m_proveedor.alias("mpe")
        .join(m_compania.alias("mc"), col("mc.cod_compania") == col("mpe.cod_compania"), "inner")
        .join(m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            concat_ws("|", col("mpe.cod_compania"), col("mpe.cod_proveedor")).cast("string").alias("id_proveedor"),
            col("mp.id_pais").cast("string").alias("id_pais"),
            col("mpe.cod_compania").cast("string").alias("id_compania"),
            col("mpe.cod_proveedor").cast("string").alias("cod_proveedor"),
            col("mpe.nombre").cast("string").alias("nomb_proveedor"),
            col("mpe.ruc").cast("string").alias("ruc_proveedor"),
            lit("A").cast("string").alias("desc_estado")
        )
    )

    id_columns = ["id_proveedor"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(tmp_dominio_m_proveedor, data_paths.DOMINIO, target_table_name, id_columns, partition_columns_array) 

except Exception as e:
    logger.error(str(e))
    raise