from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import col, concat, length, lit, regexp_extract, regexp_replace, substring, to_date, trim, when

from pyspark.sql.types import StructType, StructField, StringType

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")
    periodos= spark_controller.get_periods()
    logger.info(periodos)

    schema_t_compra_rendimiento = StructType([
        StructField("fecha_rendimiento", StringType(), True),
        StructField("desc_pais", StringType(), True),
        StructField("cod_compania", StringType(), True),
        StructField("cod_sucursal", StringType(), True),
        StructField("cod_categoria", StringType(), True),
        StructField("cantidad_consumida", StringType(), True),
        StructField("cantidad_producida", StringType(), True),
        StructField("precio_std_me", StringType(), True),
    ])

    t_compras_rendimiento = spark_controller.read_table(data_paths.EXTERNAL, "cadena/global/compras_rendimiento", schema=schema_t_compra_rendimiento)
    m_pais = spark_controller.read_table(data_paths.BIG_MAGIC, "m_pais", cod_pais=cod_pais, have_principal=True)

    target_table_name = "t_compra_rendimiento"

except Exception as e:
    logger.error(e)
    raise

try:
    m_pais = m_pais.filter(col("id_pais").isin(cod_pais))

    tmp_dominio_t_compra_rendimiento = (
        t_compras_rendimiento.alias("cr")
        .join(m_pais.alias("mp"), col("mp.desc_pais") == col("cr.desc_pais"), "inner")
        .select(
            col("mp.id_pais"),
            substring(regexp_replace(col("cr.fecha_rendimiento"), "[^0-9]", ""), 1, 6).alias("id_periodo"),
            col("cr.cod_compania").alias("id_compania"),
            concat(
                trim(col("cr.cod_compania")),
                lit("|"),
                trim(col("cr.cod_sucursal")),
            ).alias("id_sucursal"),
            when(
                length(regexp_replace(col("cr.fecha_rendimiento"), "[^0-9]", "")) == 8,
                to_date(regexp_replace(col("cr.fecha_rendimiento"), "[^0-9]", ""), 'yyyyMMdd')
            ).otherwise(None).alias("fecha_rendimiento"),
            when(
                regexp_extract(col("cr.cantidad_consumida"), '^[0-9]+(\.[0-9]+)?$', 0) != '',
                col("cr.cantidad_consumida")
            ).otherwise(None).alias("cant_consumida"),
            when(
                regexp_extract(col("cr.cantidad_producida"), '^[0-9]+(\.[0-9]+)?$', 0) != '',
                col("cr.cantidad_producida")
            ).otherwise(None).alias("cant_producida"),
            when(
                regexp_extract(col("cr.precio_std_me"), '^[0-9]+(\.[0-9]+)?$', 0) != '',
                col("cr.precio_std_me")
            ).otherwise(None).alias("precio_std_me"),
            col("cr.cod_categoria").alias("cod_categoria"),
        )
    )

    tmp = tmp_dominio_t_compra_rendimiento.select(
        col("id_pais").cast("string").alias("id_pais") ,
        col("id_periodo").cast("string").alias("id_periodo") ,
        col("id_compania").cast("string").alias("id_compania") ,
        col("id_sucursal").cast("string").alias("id_sucursal") ,
        col("fecha_rendimiento").cast("date").alias("fecha_rendimiento"),
        col("cant_consumida").cast("numeric(38, 12)").alias("cant_consumida"),
        col("cant_producida").cast("numeric(38, 12)").alias("cant_producida"),
        col("precio_std_me").cast("numeric(38, 12)").alias("precio_std_me"),
        col("cod_categoria").cast("string").alias("cod_categoria"),
    )

    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(tmp, data_paths.DOMINIO, target_table_name, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise