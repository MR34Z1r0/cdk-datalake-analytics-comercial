import sys
import boto3
from pyspark.sql.functions import col, coalesce, lit, substring, lpad, cast
from decimal import Decimal
import datetime as dt
import pytz
import logging
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")

    df_centro_costo = spark_controller.read_table(data_paths.APDAYC, "m_centro_costo", cod_pais=cod_pais)
    df_centro_costo_corp = spark_controller.read_table(data_paths.APDAYC, "m_centro_costo_corporativo", cod_pais=cod_pais)
    df_area = spark_controller.read_table(data_paths.APDAYC, "m_area", cod_pais=cod_pais)
    df_gerencia = spark_controller.read_table(data_paths.APDAYC, "m_gerencia", cod_pais=cod_pais)
    m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", cod_pais=cod_pais,have_principal = True)
    df_ebitda_centro_costo = spark_controller.read_table(data_paths.EXTERNAL, "CONFIGURACION_EBITDA_CENTRO_COSTO")
    
    target_table_name = "m_centro_costo"

    
except Exception as e:
    logger.error(e)
    raise

try:
   
    tmp_m_centro_costo_1 = df_centro_costo.alias("a")\
        .join(df_centro_costo_corp.alias("b"), col("a.id_centro_costo_corp") == col("b.id_centro_costo_corporativo_query"), "left")\
        .join(df_area.alias("c"), col("a.id_area") == col("c.id_area"), "left")\
        .join(df_gerencia.alias("d"), col("a.id_gerencia") == col("d.id_gerencia"), "left")\
        .join(m_compania.alias("mc"), col("a.id_compania") == col("mc.cod_compania"), "left")\
        .join(m_pais.alias("mp"), col("mc.id_pais") == col("mp.cod_pais"), "left")\
        .filter(col("mp.id_pais").isin(cod_pais))\
        .select(
            col("mc.id_pais"),
            col("a.id_compania"),
            col("a.id_centro_costo"),
            col("a.id_centro_costo_corp"),
            col("a.id_area"),
            col("a.id_gerencia"),
            col("a.cod_ejercicio").alias("cod_ejercicio"),
            col("a.cod_centro_costo").alias("cod_centro_costo"),
            col("a.desc_centro_costo").alias("desc_centro_costo"),
            coalesce(col("b.cod_centro_costo_corporativo"), lit("000000000")).alias("cod_centro_costo_corp"),
            coalesce(col("b.desc_centro_costo_corporativo"), lit("CENTRO COSTO CORP")).alias("desc_centro_costo_corp"),
            coalesce(col("c.cod_area"), lit("000")).alias("cod_area"),
            coalesce(col("c.desc_area"), lit("AREA DEFAULT")).alias("desc_area"),
            coalesce(col("d.cod_gerencia"), lit("00")).alias("cod_gerencia"),
            coalesce(col("d.desc_gerencia"), lit("GERENCIA DEFAULT")).alias("desc_gerencia"),
            col("a.cod_tipo"),
            col("a.cod_tipo_almacen"),
            col("a.estado")
        )
    
    # CAMBIADO COD_PAIS POR id_pais
    

    tmp_m_centro_costo_add_centro_costo_bp = tmp_m_centro_costo_1.alias("a")\
        .join(df_ebitda_centro_costo.alias("b"), 
          (col("a.cod_ejercicio") <= substring(col("b.PERIODO_FIN").cast("string"), 1, 4)) &
          (col("a.cod_ejercicio") >= substring(col("b.PERIODO_INICIO").cast("string"), 1, 4)) &
          (col("a.cod_centro_costo") == col("b.cod_centro_costo").cast("string")) &
          (col("a.id_compania") == lpad(col("b.cod_compania").cast("string"), 4, '0')),
          "left"
        )\
        .select(
            col("a.id_pais"), # 
            col("a.id_compania"),
            col("a.id_centro_costo"),
            col("a.id_centro_costo_corp"),
            col("a.id_area"),
            col("a.id_gerencia"),
            col("a.cod_ejercicio"),
            col("a.cod_centro_costo"),
            col("a.desc_centro_costo"),
            coalesce(col("a.cod_centro_costo_corp"), lit("0000")).alias("cod_centro_costo_corp"),
            coalesce(col("a.desc_centro_costo_corp"), lit("[NO IDENTIFICADO]")).alias("desc_centro_costo_corp"),
            col("a.cod_area"),
            col("a.desc_area"),
            col("a.cod_gerencia"),
            col("a.desc_gerencia"),
            col("a.cod_tipo"),
            col("a.cod_tipo_almacen"),
            col("a.estado"),
            coalesce(col("b.cod_centro_costo_bp").cast("string"), lit("0")).alias("cod_centro_costo_bp"),
            coalesce(col("b.desc_centro_costo_bp"), lit("[NO IDENTIFICADO]")).alias("desc_centro_costo_bp"),
            coalesce(col("b.desc_centro_costo_bp_corp"), lit("[NO IDENTIFICADO]")).alias("desc_centro_costo_bp_corp")
        )

    id_columns = ["id_centro_costo"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(tmp_m_centro_costo_add_centro_costo_bp, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)


except Exception as e:
    logger.error(e)
    raise