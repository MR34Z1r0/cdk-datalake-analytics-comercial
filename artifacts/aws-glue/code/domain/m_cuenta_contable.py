import sys
import boto3
from pyspark.sql.functions import col, when, coalesce, trim, length, lpad, substring, lit, concat
from decimal import Decimal
import datetime as dt
import pytz
import logging
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS

spark_controller = SPARK_CONTROLLER()

try:

    cod_pais = COD_PAIS.split(",")
    m_plan_cuentas = spark_controller.read_table(data_paths.BIG_MAGIC, "m_plan_cuentas", cod_pais=cod_pais)
    m_plan_cuentas.show()
    m_plan_cuentas_corporativo = spark_controller.read_table(data_paths.BIG_MAGIC, "m_plan_cuentas_corporativo", cod_pais=cod_pais)
    m_compania =spark_controller.read_table(data_paths.BIG_MAGIC, "m_compania", cod_pais=cod_pais)
    m_pais = spark_controller.read_table(data_paths.BIG_MAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)
    m_clasificacion_cuenta_contable_cogs = spark_controller.read_table(data_paths.BIG_MAGIC, "m_clasificacion_cuenta_contable_cogs", cod_pais=cod_pais)
    m_clasificacion_cogs = spark_controller.read_table(data_paths.BIG_MAGIC, "m_clasificacion_cogs", cod_pais=cod_pais)
    configuracion_ebitda_plan_cuenta = spark_controller.read_table(data_paths.EXTERNAL, "CONFIGURACION_EBITDA_PLAN_CUENTA")
    configuracion_ebitda_plan_cuenta_no_considerada = spark_controller.read_table(data_paths.EXTERNAL, "CONFIGURACION_EBITDA_PLAN_CUENTA_NO_CONSIDERADA")
    
    

    target_table_name = "m_cuenta_contable"
    
except Exception as e:
    logger.error(e)
    raise
try:
    tmp_dim_cuenta_contable = m_plan_cuentas.alias("mcc")\
        .join(m_plan_cuentas_corporativo.alias("mccorp"), col("mcc.id_cuenta_contable_corp") == col("mccorp.id_plan_cuentas_corporativo"), "left")\
        .join(m_compania.alias("mc"), col("mcc.id_compania") == col("mc.cod_compania"), "left") \
        .join(m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")\
        .select(
            col("mp.id_pais"),
            col("mcc.id_compania"),
            col("mcc.id_ejercicio"),
            col("mcc.id_cuenta_contable"),
            col("mcc.id_cuenta_contable_corp"),
            col("mcc.cod_ejercicio"),
            col("mcc.cod_cuenta_contable"),
            col("mcc.desc_cuenta_contable"),
            col("mcc.cod_tipo_cuenta"),
            col("mcc.cod_tipo_moneda"),
            col("mcc.cod_naturaleza"),
            col("mcc.cod_indicador_balance"),
            col("mcc.cod_indicador_resultado"),
            col("mcc.cod_tipo_cuenta_auxiliar"),

            when(col("mcc.flg_centro_costo") == lit('T'), lit(1)).otherwise(lit(0)).alias("tiene_centro_costo"),
            when(col("mcc.flg_cuenta_auxiliar") == lit('T'), lit(1)).otherwise(lit(0)).alias("tiene_cuenta_auxiliar"),
            when(col("mcc.flg_ifrs") == lit('T'), lit(1)).otherwise(lit(0)).alias("tiene_ifrs"),
            when(col("cod_cuenta_contable_corporativa") == '', '0000').otherwise(col("cod_cuenta_contable_corporativa")).alias("cod_cuenta_contable_corporativa"),
            when(trim(col("desc_cuenta_contable_corporativa")) == '', '[NO IDENTIFICADO]').otherwise(trim(col("desc_cuenta_contable_corporativa"))).alias("desc_cuenta_contable_corporativa"),
            coalesce(col("mcc.flg_tipres"), lit('N')).alias("flg_tipres"),
            length(trim(col("mcc.cod_cuenta_contable"))).alias("longitud")
        )
    tmp_dim_cuenta_contable_add_cuenta_contable_bp = tmp_dim_cuenta_contable.alias("mcc")\
        .join(configuracion_ebitda_plan_cuenta.alias("ebitdapc"), 
            (col("mcc.cod_cuenta_contable") == col("ebitdapc.cod_cuenta_contable").cast("string")) &
            (col("ebitdapc.cod_pais") == col("mcc.id_pais")) &
            (col("mcc.cod_ejercicio") <= substring(col("ebitdapc.periodo_fin").cast("string"), 1, 4)) &
            (col("mcc.cod_ejercicio") >= substring(col("ebitdapc.periodo_inicio").cast("string"), 1, 4)) &
            (lpad(col("ebitdapc.cod_compania").cast("string"), 4, '0') == col("mcc.id_compania")), "left") \
        .join(configuracion_ebitda_plan_cuenta_no_considerada.alias("ebitdapcnc"), 
            (col("mcc.cod_cuenta_contable") == col("ebitdapcnc.cod_cuenta_contable").cast("string")) &
            (col("ebitdapcnc.cod_pais") == col("mcc.id_pais")) &
            (col("mcc.cod_ejercicio") <= substring(col("ebitdapcnc.periodo_fin").cast("string"), 1, 4)) &
            (col("mcc.cod_ejercicio") >= substring(col("ebitdapcnc.periodo_inicio").cast("string"), 1, 4)) &
            (lpad(col("ebitdapcnc.cod_compania").cast("string"), 4, '0') == col("mcc.id_compania")), "left") \
        .select(
            col("mcc.id_pais"),
            col("mcc.id_compania"),
            col("mcc.id_ejercicio"),
            col("mcc.id_cuenta_contable"),
            col("mcc.id_cuenta_contable_corp"),
            col("mcc.cod_ejercicio"),
            col("mcc.cod_cuenta_contable"),
            col("mcc.desc_cuenta_contable"),
            col("mcc.cod_tipo_cuenta"),
            col("mcc.cod_tipo_moneda"),
            col("mcc.cod_naturaleza"),
            col("mcc.cod_indicador_balance"),
            col("mcc.cod_indicador_resultado"),
            col("mcc.tiene_centro_costo"),
            col("mcc.tiene_cuenta_auxiliar"),
            col("mcc.cod_tipo_cuenta_auxiliar"),
            col("mcc.tiene_ifrs"),
            col("mcc.flg_tipres"),
            col("mcc.cod_cuenta_contable_corporativa"),
            col("mcc.desc_cuenta_contable_corporativa"),
            col("mcc.longitud"),
            coalesce(col("ebitdapc.COD_CUENTA_CONTABLE_BP"), lit('00')).alias("cod_cuenta_contable_bp"),
            coalesce(col("ebitdapc.DESC_CUENTA_CONTABLE_BP"), lit('[NO IDENTIFICADO]')).alias("desc_cuenta_contable_bp"),
            coalesce(col("ebitdapc.DESC_CUENTA_CONTABLE_BP_CORP"), lit('[NO IDENTIFICADO]')).alias("desc_cuenta_contable_bp_corp"),
            when(col("ebitdapcnc.cod_cuenta_contable").isNull(), lit(1)).otherwise(lit(0)).alias("tiene_indicador_bp")
        )   
    aux_dim_cuenta_contable_add_cuenta_clas_1 = tmp_dim_cuenta_contable\
        .filter(col("longitud") == 2)\
        .select(
            col("id_compania"),
            col("cod_ejercicio"),
            col("cod_cuenta_contable"),
            when(coalesce(col("desc_cuenta_contable"), lit("")) == "", lit("[NO IDENTIFICADO]"))
            .otherwise(concat(trim(col("cod_cuenta_contable")), lit("-"), col("desc_cuenta_contable")))
            .alias("cuenta_clase")
        )
    
    aux_dim_cuenta_contable_add_cuenta_clas_2 = tmp_dim_cuenta_contable\
        .filter(col("longitud") == 1)\
        .select(
            col("id_compania"),
            col("cod_ejercicio"),
            col("cod_cuenta_contable"),
            when(coalesce(col("desc_cuenta_contable"), lit("")) == "", lit("[NO IDENTIFICADO]"))
            .otherwise(concat(trim(col("cod_cuenta_contable")), lit("-"), col("desc_cuenta_contable")))
            .alias("cuenta_clase")
        )
    
    
        
    tmp_dim_cuenta_contable_add_cuenta_clase = tmp_dim_cuenta_contable_add_cuenta_contable_bp.alias("mcc_1")\
        .join(aux_dim_cuenta_contable_add_cuenta_clas_1.alias("mccc"), 
            (col("mccc.id_compania") == col("mcc_1.id_compania")) &
            (col("mccc.cod_ejercicio") == col("mcc_1.cod_ejercicio")) &
            (col("mccc.cod_cuenta_contable") == substring(col("mcc_1.cod_cuenta_contable"), 1, 2)) &
            (col("mcc_1.longitud") > 2),
            "left")\
        .join(aux_dim_cuenta_contable_add_cuenta_clas_2.alias("mcccc"),
            (col("mcccc.id_compania") == col("mcc_1.id_compania")) &
            (col("mcccc.cod_ejercicio") == col("mcc_1.cod_ejercicio")) &
            (col("mcccc.cod_cuenta_contable") == substring(col("mcc_1.cod_cuenta_contable"), 1, 2)) &
            (col("mcc_1.longitud") == 1),
            "left")\
        .select(
            col("mcc_1.id_pais"),
            col("mcc_1.id_compania"),
            col("mcc_1.id_ejercicio"),
            col("mcc_1.id_cuenta_contable"),
            col("mcc_1.id_cuenta_contable_corp"),
            col("mcc_1.cod_ejercicio"),
            col("mcc_1.cod_cuenta_contable"),
            col("mcc_1.desc_cuenta_contable"),
            col("mcc_1.cod_tipo_cuenta"),
            col("mcc_1.cod_tipo_moneda"),
            col("mcc_1.cod_naturaleza"),
            col("mcc_1.cod_indicador_balance"),
            col("mcc_1.cod_indicador_resultado"),
            col("mcc_1.tiene_centro_costo"),
            col("mcc_1.tiene_cuenta_auxiliar"),
            col("mcc_1.cod_tipo_cuenta_auxiliar"),
            col("mcc_1.tiene_ifrs"),
            col("mcc_1.flg_tipres"),
            col("mcc_1.cod_cuenta_contable_corporativa"),
            col("mcc_1.desc_cuenta_contable_corporativa"),
            col("mcc_1.longitud"),
            col("mcc_1.cod_cuenta_contable_bp"),
            col("mcc_1.desc_cuenta_contable_bp"),
            col("mcc_1.desc_cuenta_contable_bp_corp"),
            col("mcc_1.tiene_indicador_bp"),
            when(coalesce(col("mccc.cuenta_clase"), lit("")) == lit(""), lit("[NO IDENTIFICADO]")).otherwise(col("mccc.cuenta_clase")).alias("cuenta_clase_02"),
            when(coalesce(col("mcccc.cuenta_clase"), lit("")) == lit(""), lit("[NO IDENTIFICADO]")).otherwise(col("mccc.cuenta_clase")).alias("cuenta_clase_01")
        )

    tmp_clasificacion_grupos_cogs = m_clasificacion_cuenta_contable_cogs.alias("cgcogs")\
        .join(m_clasificacion_cogs.alias("ccogs"), col("cgcogs.id_clasificacion_cogs") == col("ccogs.id_clasificacion_cogs"), "inner")\
        .filter(col("cgcogs.estado") == "A")\
        .select(
            col("cgcogs.id_COMPANIA"),
            col("cgcogs.id_clasificacion_cogs"),
            col("cgcogs.id_ejercicio"),
            col("cgcogs.id_cuenta_contable"),
            col("cgcogs.cod_clasificacion_cogs"),
            col("cgcogs.cod_ejercicio"),
            col("cgcogs.cod_cuenta_contable"),
            col("ccogs.desc_clasificacion_cogs_esp"),
            col("ccogs.desc_clasificacion_cogs_ing")
        )

    tmp_dim_cuenta_contable_add_grupo_cogs = tmp_dim_cuenta_contable_add_cuenta_clase.alias("mcc")\
        .join(tmp_clasificacion_grupos_cogs.alias("tcg"),
            (col("mcc.id_compania") == col("tcg.id_COMPANIA")) &
            (col("mcc.cod_ejercicio") == col("tcg.cod_ejercicio")) &
            (col("mcc.cod_cuenta_contable") == col("tcg.cod_cuenta_contable")),
        "left"
    ).select(
        col("mcc.id_pais"),
        col("mcc.id_compania"),
        col("mcc.id_ejercicio"),
        col("mcc.id_cuenta_contable"),
        col("mcc.id_cuenta_contable_corp"),
        col("mcc.cod_ejercicio"),
        col("mcc.cod_cuenta_contable"),
        col("mcc.desc_cuenta_contable"),
        col("mcc.cod_tipo_cuenta"),
        col("mcc.cod_tipo_moneda"),
        col("mcc.cod_naturaleza"),
        col("mcc.cod_indicador_balance"),
        col("mcc.cod_indicador_resultado"),
        col("mcc.tiene_centro_costo"),
        col("mcc.tiene_cuenta_auxiliar"),
        col("mcc.cod_tipo_cuenta_auxiliar"),
        col("mcc.tiene_ifrs"),
        col("mcc.flg_tipres"),
        col("mcc.cod_cuenta_contable_corporativa"),
        col("mcc.desc_cuenta_contable_corporativa"),
        col("mcc.longitud"),
        col("mcc.cod_cuenta_contable_bp"),
        col("mcc.desc_cuenta_contable_bp"),
        col("mcc.desc_cuenta_contable_bp_corp"),
        col("mcc.tiene_indicador_bp"),
        col("mcc.cuenta_clase_02"),
        col("mcc.cuenta_clase_01"),
        coalesce(col("tcg.cod_clasificacion_cogs"), lit("000")).alias("cod_clasificacion_cogs"),
        coalesce(col("tcg.desc_clasificacion_cogs_esp"), lit("No Identificado")).alias("desc_clasificacion_cogs_esp"),
        coalesce(col("tcg.desc_clasificacion_cogs_ing"), lit("Not Defined")).alias("desc_clasificacion_cogs_ing")
    )

    id_columns = ["id_cuenta_contable"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(tmp_dim_cuenta_contable_add_grupo_cogs, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)
except Exception as e:
    logger.error(e)
    raise