from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, lit, concat_ws, coalesce
from pyspark.sql.types import StructType, StructField, StringType

spark_controller = SPARK_CONTROLLER()
target_table_name = "m_categoria_compra"

try:
    cod_pais = COD_PAIS.split(",")
    m_compania = spark_controller.read_table(data_paths.BIG_MAGIC, "m_compania", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.BIG_MAGIC, "m_pais", cod_pais=cod_pais, have_principal = True)
    m_articulo_categoria_compra = spark_controller.read_table(data_paths.BIG_MAGIC, "m_articulo_categoria_compra", cod_pais=cod_pais)
    m_categoria_compra = spark_controller.read_table(data_paths.BIG_MAGIC, "m_categoria_compra", cod_pais=cod_pais)
    m_sub_categoria_compra = spark_controller.read_table(data_paths.BIG_MAGIC, "m_sub_categoria_compra", cod_pais=cod_pais)
    m_sub_categoria2_compra = spark_controller.read_table(data_paths.BIG_MAGIC, "m_sub_categoria2_compra", cod_pais=cod_pais)
except Exception as e:
    logger.error(e)
    raise

try:
    m_pais = m_pais.filter(col("id_pais").isin(cod_pais))
    companies = m_compania.alias("mc").join(m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
    companies = companies.select("id_compania").distinct()
    companies.show(5)

    schema = StructType([
        StructField("cod_categoria", StringType(), True),
        StructField("descripcion", StringType(), True),
        StructField("unidad_medida", StringType(), True),
    ])
    # add default values
    default_m_categoria_compra = spark_controller.spark.createDataFrame(
        [("00", "CATEGORIA COMPRA DEFAULT", "00")], 
        ["cod_categoria", "descripcion", "unidad_medida"]
    )
    default_m_categoria_compra = default_m_categoria_compra.crossJoin(companies)
    default_m_categoria_compra.show(5)

    m_categoria_compra = m_categoria_compra.select(
        col("cod_categoria"),
        col("descripcion"),
        col("unidad_medida"),
        col("id_compania")
    )

    m_categoria_compra = m_categoria_compra.unionByName(default_m_categoria_compra)

    logger.info("Starting creation of tmp_dominio_m_categoria_compra")
    tmp_dominio_m_categoria_compra = (
        m_articulo_categoria_compra.alias("ircc")
        .join(m_compania.alias("mc"), col("mc.cod_compania") == col("ircc.cod_compania"), "inner")
        .join(m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .join(m_categoria_compra.alias("mcc"), 
              (col("mcc.id_compania") == col("ircc.cod_compania")) 
              & (col("mcc.cod_categoria") == coalesce(col("ircc.cod_categoria"), lit("00"))), 
              "inner"
        )
        .join(
            m_sub_categoria_compra.alias("mscc"),
            (col("mscc.id_compania") == col("ircc.cod_compania"))
            & (col("mscc.cod_categoria") == col("ircc.cod_categoria"))
            & (col("mscc.cod_sub_categoria") == col("ircc.cod_sub_categoria")),
            "left"
        )
        .join(
            m_sub_categoria2_compra.alias("msc2c"),
            (col("msc2c.id_compania") == col("ircc.cod_compania"))
            & (col("msc2c.cod_categoria") == col("ircc.cod_categoria"))
            & (col("msc2c.cod_sub_categoria") == col("ircc.cod_sub_categoria"))
            & (col("msc2c.cod_sub_categoria2") == col("ircc.cod_sub_categoria2")),
            "left"
        )
        .select(
            col("mp.id_pais").alias("id_pais"),
            col("ircc.cod_compania").alias("id_compania"),
            concat_ws(
                "|",
                coalesce(col("ircc.cod_compania"), lit("")),
                coalesce(col("ircc.cod_articulo"), lit("")),
                coalesce(col("ircc.cod_categoria"), lit("00")),
                coalesce(col("ircc.cod_sub_categoria"), lit("00")),
                coalesce(col("ircc.cod_sub_categoria2"), lit("00")),
            ).alias("id_categoria_compra"),
            concat_ws(
                "|",
                coalesce(col("ircc.cod_compania"), lit("")),
                coalesce(col("ircc.cod_categoria"), lit("00")),
            ).alias("id_categoria"),
            concat_ws(
                "|",
                coalesce(col("ircc.cod_compania"), lit("")),
                coalesce(col("ircc.cod_articulo"), lit("")),
            ).alias("id_articulo"),
            coalesce(col("mcc.cod_categoria"), lit("00")).alias("cod_categoria"),
            coalesce(col("mcc.descripcion"), lit("CATEGORIA COMPRA DEFAULT")).alias("desc_categoria"),
            coalesce(col("mscc.cod_sub_categoria"), lit("00")).alias("cod_subcategoria"),
            coalesce(col("mscc.descripcion"), lit("SUB CATEGORIA COMPRA DEFAULT")).alias("desc_subcategoria"),
            coalesce(col("msc2c.cod_sub_categoria2"), lit("00")).alias("cod_subcategoria2"),
            coalesce(col("msc2c.descripcion"), lit("SUB CATEGORIA2 COMPRA DEFAULT")).alias("desc_subcategoria2"),
            coalesce(col("mcc.unidad_medida"), lit("00")).alias("cod_unidad_global"),
        )
    )

    tmp = tmp_dominio_m_categoria_compra.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_compania").cast("string").alias("id_compania"),
        col("id_categoria_compra").cast("string").alias("id_categoria_compra"),
        col("id_categoria").cast("string").alias("id_categoria"),
        col("id_articulo").cast("string").alias("id_articulo"),
        col("cod_categoria").cast("string").alias("cod_categoria"),
        col("desc_categoria").cast("string").alias("desc_categoria"),
        col("cod_subcategoria").cast("string").alias("cod_subcategoria"),
        col("desc_subcategoria").cast("string").alias("desc_subcategoria"),
        col("cod_subcategoria2").cast("string").alias("cod_subcategoria2"),
        col("desc_subcategoria2").cast("string").alias("desc_subcategoria2"),
        col("cod_unidad_global").cast("string").alias("cod_unidad_global"),
    )

    id_columns = ["id_categoria_compra"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(tmp, data_paths.DOMINIO, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(str(e))
    raise
