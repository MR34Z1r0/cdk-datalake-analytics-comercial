import sys
import boto3
from pyspark.sql.functions import col, when, lit, concat_ws, lpad, coalesce
from decimal import Decimal
import datetime as dt
import pytz
import logging
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")
    m_compania = spark_controller.read_table(data_paths.BIG_BAGIC, "m_compania", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.BIG_BAGIC, "m_pais", cod_pais=cod_pais)
    t_voucher_cabecera = spark_controller.read_table(data_paths.BIG_BAGIC, "t_voucher_cabecera", cod_pais=cod_pais)
    t_voucher_eliminados = spark_controller.read_table(data_paths.BIG_BAGIC, "t_voucher_eliminados", cod_pais=cod_pais)
    t_voucher_detalle = spark_controller.read_table(data_paths.BIG_BAGIC, "t_voucher_detalle", cod_pais=cod_pais)

    global_sccc_configuracion_cds = spark_controller.read_table(data_paths.EXTERNAL, "CONFIGURACION_CDS")
    global_sccc_configuracion_ebitda_centro_costo_plan_cuenta = spark_controller.read_table(data_paths.EXTERNAL, "CONFIGURACION_EBITDA_CENTRO_COSTO_PLAN_CUENTA")
   
    # df_ebitda_centro_costo = spark_controller.read_table(data_paths.EXTERNAL, "CONFIGURACION_EBITDA_CENTRO_COSTO")
    
    target_table_name = "t_movimiento_contable"

    
except Exception as e:
    logger.error(e)
    raise

try:
    m_pais = m_pais.filter(col("id_pais").isin(cod_pais))
   
    # Filtrar los registros que no están en t_voucher_eliminados
    tmp_t_voucher_resumen_1 = t_voucher_cabecera.alias("tvc")\
        .join(t_voucher_eliminados.alias("tve"),col("tvc.id_voucher_cabecera") == col("tve.id_voucher_eliminados"),"left") \
        .join(t_voucher_detalle.alias("tvd"), col("tvd.id_voucher_cabecera") == col("tvc.id_voucher_cabecera"),"left") \
        .join(m_compania.alias("mc"), col("tvc.id_compania") == col("mc.id_compania"), "inner") \
        .join(m_pais.alias("mp"), col("mc.id_pais") == col("mp.cod_pais"), "inner") \
        .select(
            col("mp.id_pais").alias("cod_pais"),
            col("tvc.id_compania").alias("cod_compania"),
            col("tvc.id_sucursal").alias("cod_sucursal"),
            col("tvc.cod_ejercicio"),
            lpad(col("tvc.cod_periodo").cast("string"), 2, '0').alias("cod_periodo"),
            
            col("tvd.cod_contable").alias("cod_cuenta_contable"),
            col("tvd.cod_centro_costo"),
            
            when(col("tvd.naturaleza") == "D", col("tvd.importe_sol")).otherwise(0).alias("debe_mn"),
            when(col("tvd.naturaleza") == "H", -1 * col("tvd.importe_sol")).otherwise(0).alias("haber_mn"),
            when(col("tvd.naturaleza") == "D", col("tvd.importe_dolar")).otherwise(0).alias("debe_me"),
            when(col("tvd.naturaleza") == "H", -1 * col("tvd.importe_dolar")).otherwise(0).alias("haber_me"),
            when(col("tve.id_voucher_eliminados").isNull(), 0).otherwise(1).alias("is_deleted")

        )
    

    tmp_t_voucher_resumen_2 = tmp_t_voucher_resumen_1 \
        .select(
            col("cod_pais").alias("id_pais"),
            col("cod_compania").alias("id_compania"),
            concat_ws("|", col("cod_compania"), col("cod_sucursal")).alias("id_sucursal"),
            concat_ws("", col("cod_ejercicio"), col("cod_periodo")).alias("id_periodo"),
            concat_ws("|", col("cod_compania"), col("cod_ejercicio"), col("cod_periodo")).alias("id_periodo_contable"),
            concat_ws("|", col("cod_compania"), col("cod_ejercicio"), col("cod_cuenta_contable")).alias("id_cuenta_contable"),
            concat_ws("|", col("cod_compania"), col("cod_ejercicio"), col("cod_centro_costo")).alias("id_centro_costo"),
            col("debe_mn"),
            col("debe_me"),
            col("haber_mn"),
            col("haber_me"),
            col("cod_pais"),
            col("cod_compania"),
            col("cod_sucursal"),
            col("cod_cuenta_contable"),
            col("cod_centro_costo")
        )
    

    tmp_t_voucher_resumen_2 = tmp_t_voucher_resumen_2.alias("tv2") \
    .join(
        global_sccc_configuracion_cds.alias("cfcds"),
        (col("tv2.id_periodo").between(
            col("cfcds.periodo_inicio").cast("string"),
            col("cfcds.periodo_fin").cast("string")
        )) &
        (col("tv2.cod_compania") == lpad(
            col("cfcds.cod_compania").cast("string"), 4, "0")) &
        (col("tv2.cod_sucursal") == lpad(
            col("cfcds.cod_sucursal").cast("string"), 2, "0")) &
        (col("tv2.cod_centro_costo") == col(
            "cfcds.cod_centro_costo")) &
        (col("tv2.cod_cuenta_contable") == col(
            "cfcds.cod_cuenta_contable")),
        "left"
    ) \
    .join(
        global_sccc_configuracion_ebitda_centro_costo_plan_cuenta.alias("cfpl"),
        (col("tv2.id_pais") == col(
            "cfpl.cod_pais")) &
        (col("tv2.cod_compania") == lpad(
            col("cfpl.cod_compania").cast("string"), 4, "0")) &
        (col("tv2.cod_cuenta_contable") == col(
            "cfpl.cod_cuenta_contable")) &
        (col("tv2.cod_centro_costo") == col(
            "cfpl.cod_centro_costo")) &
        (col("tv2.id_periodo") >= col(
            "cfpl.periodo_inicio").cast("string")) &
        (col("tv2.id_periodo") <= col(
            "cfpl.periodo_fin").cast("string")),
        "left"
    ) \
    .select(
        col("tv2.id_pais"),
        col("tv2.id_compania"),
        col("tv2.id_sucursal"),
        col("tv2.id_periodo"),
        col("tv2.id_periodo_contable"),
        col("tv2.id_cuenta_contable"),
        col("tv2.id_centro_costo"),
        coalesce(col("cfcds.cod_tipo_gasto_cds"), lit("00")).alias("cod_tipo_gasto_cds"),
        concat_ws("|", col("tv2.cod_pais"), 
                  coalesce(col("cfcds.cod_tipo_gasto_cds"), lit("00"))).alias("id_tipo_gasto_cds"),
        coalesce(col("cfpl.cod_clasificacion_pl"), lit("0")).alias("cod_clasificacion_pl"),
        concat_ws("|", col("tv2.cod_pais"), 
                  coalesce(col("cfpl.cod_clasificacion_pl"), lit("0"))).alias("id_clasificacion_pl"),
        col("tv2.debe_mn"),
        col("tv2.debe_me"),
        col("tv2.haber_mn"),
        col("tv2.haber_me")
    )

    
    spark_controller.write_table(tmp_t_voucher_resumen_2, data_paths.DOMINIO, target_table_name)


    # Para la lógica con configuracion_cds, seguir la referencia en dominio_old/t_voucher_resumen


except Exception as e:
    logger.error(e)
    raise