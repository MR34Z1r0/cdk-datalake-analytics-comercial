import sys
import boto3
from pyspark.sql.functions import col, concat_ws, current_date, lpad, to_date, date_add, lit
from pyspark.sql.types import IntegerType
from decimal import Decimal
import datetime as dt
import pytz
import logging
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from delta.tables import DeltaTable

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")
    m_periodo = spark_controller.read_table(data_paths.BIG_BAGIC, "m_periodo", cod_pais=cod_pais)
    # m_fecha = spark_controller.read_table(data_paths.BIG_BAGIC, "m_fecha", cod_pais=cod_pais)
    m_compania = spark_controller.read_table(data_paths.BIG_BAGIC, "m_compania", cod_pais=cod_pais)
    #m_pais = spark_controller.read_table(data_paths.BIG_BAGIC, "m_pais", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.BIG_BAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)
    
    target_table_name = "m_periodo_contable"

except Exception as e:
    logger.error(e)
    raise

try:

    mpa_filtered = m_pais.filter(col("id_pais").isin(cod_pais))
    mco_joined = m_compania.join(mpa_filtered, "cod_pais", "left")
    
    # .join(m_fecha.alias("mf"), col("mp.cierremes") == col("mf.cod_fecha"), "left") \
    tmp_periodo_contable_1 = m_periodo.alias("mp") \
        .join(m_compania.alias("mco"), col("mp.cod_compania") == col("mco.cod_compania"), "left") \
        .join(m_pais.alias("mpa"), col("mco.cod_pais") == col("mpa.cod_pais"), "inner") \
        .select(
            col("mpa.id_pais").alias("cod_pais"),
            col("mp.cod_compania"),
            col("mp.cod_sucursal"),
            col("mp.cod_ejercicio").cast("string").substr(1, 4).alias("cod_ejercicio"),
            lpad(col("cod_periodo").cast("string"), 2, "0").alias("cod_periodo"),
            to_date(date_add(to_date(lit("1900-01-01")), col("mp.cierre_mes").cast(IntegerType()) - lit(693596)), "yyyy-MM-dd").alias("cierre_mes"),
            col("mp.estado").alias("estado")
        )
    
    # Crear tmp_periodo_contable_2
    tmp_periodo_contable_2 = tmp_periodo_contable_1.select(
            concat_ws("|", col("cod_compania"), col("cod_ejercicio"), col("cod_periodo")).alias("id_periodo_contable"),
            col("cod_pais").alias("id_pais"),
            col("cod_compania").alias("id_compania"),
            concat_ws("|", col("cod_compania"), col("cod_sucursal")).alias("id_sucursal"),
            col("cod_ejercicio"),
            col("cod_periodo"),
            col("cierre_mes"),
            col("estado"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion")
        )
        
    print(f"tmp_periodo_contable_2 : {tmp_periodo_contable_2.count()}")
    print(f"tmp_periodo_contable_2 distinct : {tmp_periodo_contable_2.dropDuplicates(['id_periodo_contable']).count()}")
    # tmp_periodo_contable_2 = tmp_periodo_contable_2
    

     # Insertar nuevos registros
    if spark_controller.table_exists(data_paths.DOMINIO, "m_periodo_contable"):
        print("existe")
        m_periodo_contable = spark_controller.read_table(data_paths.DOMINIO, "m_periodo_contable")
        new_records = tmp_periodo_contable_2.alias("tmp_pc2") \
            .join(m_periodo_contable.alias("mpc"), col("tmp_pc2.id_periodo_contable") == col("mpc.id_periodo_contable"), "left_anti") \
            .select(
                col("tmp_pc2.id_pais"),
                col("tmp_pc2.id_compania"),
                col("tmp_pc2.id_sucursal"),
                col("tmp_pc2.cod_ejercicio"),
                col("tmp_pc2.cod_periodo"),
                col("tmp_pc2.cierre_mes"),
                col("tmp_pc2.estado"),
                col("tmp_pc2.fecha_modificacion"),
                col("tmp_pc2.id_periodo_contable")
            )
    else:
        print("no existe")
        new_records = tmp_periodo_contable_2
    spark_controller.insert_into_table(new_records, data_paths.DOMINIO, target_table_name)
    m_periodo_contable = spark_controller.read_table(data_paths.DOMINIO, "m_periodo_contable")
    # Actualizar registros existentes
    tmp_periodo_contable_2.show()
    update_records = tmp_periodo_contable_2.alias("b") \
        .join(m_periodo_contable.alias("a"), col("a.id_periodo_contable") == col("b.id_periodo_contable"), "inner") \
        .select(
            col("a.id_pais"),
            col("a.id_compania"),
            col("a.id_sucursal"),
            col("a.cod_ejercicio"),
            col("a.cod_periodo"),
            col("a.cierre_mes"),
            col("a.estado"),
            col("a.fecha_modificacion"),
            col("a.id_periodo_contable")
        ).dropDuplicates(['id_periodo_contable'])
        
    print(5)

    update_expr = [
        "id_periodo_contable"
    ]
    print(6)
    spark_controller.update_table(data_paths.DOMINIO, target_table_name, update_records, update_expr)



except Exception as e:
    logger.error(e)
    raise