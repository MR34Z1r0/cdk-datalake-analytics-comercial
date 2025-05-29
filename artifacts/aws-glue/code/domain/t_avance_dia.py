# import sys
# import json
# import boto3
# from pyspark.sql.types import *
# from aje.get_schemas import *
# from delta.tables import DeltaTable
# from awsglue.context import GlueContext
# from awsglue.utils import getResolvedOptions
# from pyspark.sql.utils import AnalysisException
# from pyspark.sql import SparkSession, SQLContext
# from pyspark.sql.functions import (
#     col,
#     lit,
#     concat,
#     trim,
#     when,
#     to_date,
#     date_format,
#     to_timestamp,
#     upper,
#     substring
# )
# import logging
# import os
# import datetime as dt
# import pytz


# ######################################
# # JOB PARAMETERS

# args = getResolvedOptions(
#     sys.argv,
#     [
#         "S3_PATH_STG_BM",
#         "S3_PATH_STG_SF",
#         "S3_PATH_DOM",
#         "REGION_NAME",
#         "DYNAMODB_DATABASE_NAME",
#         "COD_PAIS",
#         "PERIODO_INI",
#         "PERIODO_FIN",
#         "PERIODOS",
#         "DYNAMODB_LOGS_TABLE",
#         "ERROR_TOPIC_ARN",
#         "PROJECT_NAME",
#         "FLOW_NAME",
#         "PROCESS_NAME",
#     ],
# )

# S3_PATH_STG_BM = args["S3_PATH_STG_BM"]
# S3_PATH_STG_SF = args["S3_PATH_STG_SF"]
# S3_PATH_DOM = args["S3_PATH_DOM"]
# REGION_NAME = args["REGION_NAME"]
# DYNAMODB_DATABASE_NAME = args["DYNAMODB_DATABASE_NAME"]
# COD_PAIS = args["COD_PAIS"]

# PERIODO_INI = args["PERIODO_INI"]
# PERIODO_FIN = args["PERIODO_FIN"]

# if (PERIODO_INI == "-") and (PERIODO_FIN == "-"):
#     PERIODOS = json.loads(args["PERIODOS"])
# else:
#     PERIODOS = [args["PERIODO_INI"], args["PERIODO_FIN"]]

# DYNAMODB_LOGS_TABLE = args["DYNAMODB_LOGS_TABLE"]
# ERROR_TOPIC_ARN = args["ERROR_TOPIC_ARN"]
# PROJECT_NAME = args["PROJECT_NAME"]
# FLOW_NAME = args["FLOW_NAME"]
# PROCESS_NAME = args["PROCESS_NAME"]

# sns_client = boto3.client("sns", region_name=REGION_NAME)

# logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s")
# logger = logging.getLogger(PROCESS_NAME)
# logger.setLevel(os.environ.get("LOGGING", logging.INFO))

# TZ_LIMA = pytz.timezone("America/Lima")
# NOW_LIMA = dt.datetime.now(pytz.utc).astimezone(TZ_LIMA)
# ######################################
# # JOB SETUP

# spark = (
#     SparkSession.builder.config(
#         "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
#     )
#     .config(
#         "spark.sql.catalog.spark_catalog",
#         "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#     )
#     .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
#     .getOrCreate()
# )

# sc = spark.sparkContext
# glue_context = GlueContext(sc)
# logger = glue_context.get_logger()
# sqlContext = SQLContext(sparkSession=spark, sparkContext=sc)
# dynamodb_client = boto3.client("dynamodb", region_name=REGION_NAME)


# ######################################
# # FUNCTIONS
# def add_log_to_dynamodb(task_name, error_message=""):
#     dynamodb_client = boto3.resource("dynamodb")
#     dynamo_table = dynamodb_client.Table(DYNAMODB_LOGS_TABLE)
#     task_status = "satisfactorio" if error_message == "" else "error"
#     process_type = "D" if len(PERIODOS) > 0 else "F"
#     date_system = NOW_LIMA.strftime("%Y%m%d_%H%M%S")

#     record = {
#         "PROCESS_ID": f"DLB_{PROCESS_NAME}_{date_system}",
#         "DATE_SYSTEM": date_system,
#         "PROJECT_NAME": args["PROJECT_NAME"],
#         "FLOW_NAME": args["FLOW_NAME"],
#         "TASK_NAME": task_name,
#         "TASK_STATUS": task_status,
#         "MESSAGE": str(error_message),
#         "PROCESS_TYPE": process_type,
#     }
#     dynamo_table.put_item(Item=record)


# def send_error_message(table_name, msg, error):
#     response = sns_client.publish(
#         TopicArn=ERROR_TOPIC_ARN,
#         Message=f"Failed table: {table_name}\nMessage: {msg}\nLog ERROR:\n{error}"
#     )


# def get_databases_from_dynamodb(COD_PAIS):
#     response = dynamodb_client.scan(
#         TableName=DYNAMODB_DATABASE_NAME,
#         AttributesToGet=["ENDPOINT_NAME"],
#     )
#     endpoint_names = [
#         item["ENDPOINT_NAME"]["S"]
#         for item in response["Items"]
#         if item["ENDPOINT_NAME"]["S"].startswith(COD_PAIS)
#     ]
#     return endpoint_names


# def create_df_union(table_name, databases, s3_path):
#     df = None
#     for bd in databases:
#         try:
#             if df is None:
#                 df = read_table_stg(f"{s3_path}/{bd}", table_name)
#                 if df.count() == 0:
#                     continue
#             else:
#                 df_bd = read_table_stg(f"{s3_path}/{bd}", table_name)
#                 if df_bd.count() == 0:
#                     continue
#                 df = df.union(df_bd)
#         except AnalysisException as e:
#             logger.error(str(e))
#             add_log_to_dynamodb("Leyendo tablas ingesta", e)
#             continue

#     if df is None:
#         add_log_to_dynamodb("Leyendo tablas ingesta", f"No se encontró en {databases}, la tabla {table_name}")
#         #send_error_message(PROCESS_NAME, "Leyendo tablas ingesta", f"No se encontró en {databases}, la tabla {table_name}")
#         exit(1)
#     else:
#         return df


# def create_df_union_w_period(table_name, databases, s3_path):
#     df = None
#     for bd in databases:
#         try:
#             if df is None:
#                 df = read_table_stg(f"{s3_path}/{bd}", table_name)
#                 if df.count() == 0:
#                     continue
#                 else:
#                     df = df.where(col("processperiod").isin(PERIODOS))
#             else:
#                 df_bd = read_table_stg(f"{s3_path}/{bd}", table_name)
#                 if df_bd.count() == 0:
#                     continue
#                 else:
#                     df_bd = df_bd.where(col("processperiod").isin(PERIODOS))
#                 df = df.union(df_bd)
#         except AnalysisException as e:
#             logger.error(str(e))
#             add_log_to_dynamodb("Leyendo tablas ingesta", e)
#             continue

#     if df is None:
#         add_log_to_dynamodb("Leyendo tablas ingesta", f"No se encontró en {databases}, la tabla {table_name}")
#         #send_error_message(PROCESS_NAME, "Leyendo tablas ingesta", f"No se encontró en {databases}, la tabla {table_name}")
#         exit(1)
#     else:
#         return df


# def read_table_stg(s3_path, table_name):
#     s3_path = f"{s3_path}/{table_name}/"
#     df = spark.read.option("basePath", s3_path).format("delta").load(s3_path)
#     return df


# def read_table_stg_salesforce(table_name):
#     try:
#         s3_path = f"{S3_PATH_STG_SF}/{table_name}/"
#         df = spark.read.format("delta").load(s3_path)
#         return df
#     except Exception as e:
#         logger.info(str(e))
#         add_log_to_dynamodb("Leyendo tablas salesforce", e)
#         #send_error_message(PROCESS_NAME, "Leyendo tablas salesforce", f"{str(e)[:10000]}")
#         exit(1)


# def create_df_schema(table_name):
#     try:
#         schemas = SchemaDominioComercial(logger)
#         schema = schemas.get_schema(table_name)

#         df = spark.createDataFrame([], schema)

#         return df
#     except Exception as e:
#         logger.error(str(e))
#         add_log_to_dynamodb("Creando schema", e)
#         #send_error_message(PROCESS_NAME, "Creando schema", f"{str(e)[:10000]}")
#         exit(1)


# def read_table(table_name, path):
#     s3_path = f"{path}/{table_name}"
#     try:
#         df = spark.read.format("delta").load(s3_path)
#     except AnalysisException:
#         # If Path does not exist:
#         df = create_df_schema(table_name)
#     return df


##################################################################################################################
import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat, lit, coalesce, when, trim ,date_format, round, substring, to_date, to_timestamp, upper, regexp_replace, expr, current_timestamp, current_date
from pyspark.sql.types import StringType, IntegerType, DecimalType, TimestampType, DateType, BooleanType

spark_controller = SPARK_CONTROLLER()

# READ

try:
    # databases = get_databases_from_dynamodb(COD_PAIS)

    # df_t_avance_dia_dom = read_table("t_avance_dia", S3_PATH_DOM).where(
    #     (col("id_pais").isin(cod_pais)) & (col("id_periodo").isin(PERIODOS))
    # )

    # df_vw_avance_del_dia__c = read_table_stg_salesforce("t_avance_dia")
    # df_vw_avance_autoventa__c = read_table_stg_salesforce("t_avance_autoventa")
    # df_vw_modulo__c = read_table_stg_salesforce("m_modulo")

    PERIODOS= spark_controller.get_periods()

    cod_pais = COD_PAIS.split(",")

    # # Load Dominio
    # df_t_avance_dia_dom = spark_controller.read_table(data_paths.DOMAIN, "t_avance_dia").where(
    #     (col("id_pais").isin(cod_pais)) & (col("id_periodo").isin(PERIODOS))
    # )

    # Load Salesforce
    df_vw_avance_del_dia__c = spark_controller.read_table(data_paths.SALESFORCE, "t_avance_dia", cod_pais=cod_pais)
    df_vw_avance_autoventa__c = spark_controller.read_table(data_paths.SALESFORCE, "t_avance_autoventa", cod_pais=cod_pais)
    df_vw_modulo__c = spark_controller.read_table(data_paths.SALESFORCE, "m_modulo", cod_pais=cod_pais)

    target_table_name ="t_avance_dia"

    # temporal deduplicacion:
    df_vw_modulo__c = df_vw_modulo__c.distinct()

except Exception as e:
    logger.error(str(e))
    #add_log_to_dynamodb("Leyendo tablas fuente", e)
    #send_error_message(PROCESS_NAME, "Leyendo tablas fuente", f"{str(e)[:10000]}")
    exit(1)

# CREATE
try:

    tmp1 = df_vw_avance_del_dia__c.select(
        col("avance_cajas_unitarias__c"),
        col("avance_cajas__c"),
        col("avance_del_dia__c"),
        col("avance_del_mes__c"),
        col("avance_importe__c"),
        col("bateria__c"),
        col("cajas_totales_cliente_nuevo__c"),
        col("cajas_totales_extra_ruta__c"),
        col("cajas_totales_prospecto__c"),
        col("cajas_totales_vendedor__c"),
        col("cajas_totales__c"),
        col("categorias__c"),
        col("clientes_compra_0__c"),
        col("clientes_compra__c"),
        col("clientes_visitados__c"),
        lit(None).alias("codigo_vendedor_fecha__c"),
        col("codigo_unico_vendedor__c"),
        col("codigo_vendedor__c"),
        col("compania__c"),
        col("createdbyid"),
        col("createddate"),
        col("cuota_de_cambio__c"),
        col("cuota_de_ruta__c"),
        col("currencyisocode"),
        col("device_id__c"),
        col("diferencia_clientes_compra_total_pedidos__c"),
        col("drop_size__c"),
        col("efectividad__c"),
        col("enviar_a_call_center__c"),
        col("fecha_fin_dia__c"),
        col("fecha_hora_primer_pedido__c"),
        col("fecha_hora_primer_visita__c"),
        col("fecha_hora_ultima_visita_c__c"),
        col("fecha_hora_ultima_sincronizacion__c"),
        col("fecha_hora_ultimo_pedido__c"),
        col("fecha_inicio_dia__c"),
        col("fecha_vendedor__c"),
        substring(col("fecha__c"),1,10).alias("fecha__c"),
        col("fin_de_dia__c"),
        col("fuerza_de_venta__c"),
        col("hora_fin_dia__c"),
        col("hora_inicio_dia__c"),
        col("id"),
        col("id_agente__c"),
        col("inicio_de_dia__c"),
        col("isdeleted"),
        col("kpi_cajas__c"),
        col("kpi_cobertura_simple__c"),
        col("kpi_importe__c"),
        col("lastactivitydate"),
        col("lastmodifiedbyid"),
        col("lastmodifieddate"),
        col("lastreferenceddate"),
        col("lastvieweddate"),
        col("latitud_fin__c"),
        col("latitud__c"),
        col("longitud_fin__c"),
        col("longitud__c"),
        col("marcas__c"),
        col("modelo_atencion__c"),
        col("modulo__c"),
        col("name"),
        col("necesidad_cu_diaria__c"),
        col("necesidad_diaria_importe__c"),
        col("necesidad_diaria__c"),
        lit(None).alias("no_requiere_liquidacion__c"),
        col("nombre_region__c"),
        col("nombre_subregion__c"),
        col("nombre_vendedor__c"),
        col("numero__c"),
        col("ownerid"),
        col("pais__c"),
        lit(None).alias("perfil__c"),
        col("portafolio__c"),
        col("region__c"),
        col("rutan__c"),
        col("ruta__c"),
        col("subregion__c"),
        col("sucursal__c"),
        col("systemmodstamp"),
        col("telefono_vendedor__c"),
        col("total_cajas_cliente_nuevo__c"),
        col("total_cajas_extra_ruta__c"),
        col("total_cajas_fisicas__c"),
        col("total_cajas_prospectos__c"),
        col("total_cajas_vendedor__c"),
        col("total_clientes_activos__c"),
        col("total_clientes_compra2__c"),
        col("total_clientes__c"),
        col("total_de_cajas__c"),
        col("total_de_caja_unidades__c"),
        col("total_importe_cliente_nuevo__c"),
        col("total_importe_extra_ruta__c"),
        col("total_importe_prospecto__c"),
        col("total_importe_vendedor__c"),
        col("total_importe__c"),
        lit(None).alias("total_no_ventas__c"),
        col("total_no_pedidos__c"),
        col("total_pedidos_cliente_nuevo__c"),
        col("total_pedidos_extra_ruta__c"),
        col("total_pedidos_prospecto__c"),
        col("total_pedidos__c"),
        col("total_skus_cliente_nuevo__c"),
        col("total_skus_extra_ruta__c"),
        col("total_skus_prospecto__c"),
        col("total_skus__c"),
        col("total_unidades_cliente_nuevo__c"),
        col("total_unidades_extra_ruta__c"),
        col("total_unidades_prospecto__c"),
        col("total_unidades__c"),
        lit(None).alias("total_ventas__c"),
        col("unidades_cambiadas__c"),
        lit(None).alias("unidad__c"),
        col("vendedor_volante__c"),
        col("vendedor__c"),
        col("version_app__c"),
        col("version_del_app__c"),
        col("zonan__c"),
        col("zona__c"),
        lit(None).alias("cajas_totales_cliente_nuevo_c__c"),
        col("cu_totales_vendedor__c"),
        col("efectividad_preventa_ecommerce__c"),
        col("efectividad_visita_ecommerce__c"),
        col("clientes_pedidos_ecommerce__c"),
        col("efectividad_contacto__c"),
    )

    tmp2 = df_vw_avance_autoventa__c.select(
            lit(None).alias("avance_cajas_unitarias__c"),
            col("avance_cajas__c"),
            col("avance_del_dia__c"),
            col("avance_del_mes__c"),
            col("avance_importe__c"),
            col("bateria__c"),
            col("cajas_totales_cliente_nuevo__c").cast("float"),
            col("cajas_totales_extra_ruta__c"),
            col("cajas_totales_prospecto__c"),
            col("cajas_totales_vendedor__c"),
            col("cajas_totales__c"),
            lit(None).alias("categorias__c"),
            col("clientes_compra_0__c"),
            col("clientes_compra__c"),
            col("clientes_visitados__c"),
            col("codigo_vendedor_fecha__c"),
            lit(None).alias("codigo_unico_vendedor__c"),
            col("codigo_vendedor__c"),
            col("compania__c"),
            col("createdbyid"),
            col("createddate"),
            col("cuota_de_cambio__c"),
            col("cuota_de_ruta__c"),
            col("currencyisocode"),
            col("device_id__c"),
            lit(None).alias("diferencia_clientes_compra_total_pedidos__c"),
            col("drop_size__c"),
            col("efectividad__c"),
            lit(None).alias("enviar_a_call_center__c"),
            lit(None).alias("fecha_fin_dia__c"),
            col("fecha_hora_primer_pedido__c"),
            lit(None).alias("fecha_hora_primer_visita__c"),
            lit(None).alias("fecha_hora_ultima_visita_c__c"),
            col("fecha_hora_ultima_sincronizacion__c"),
            col("fecha_hora_ultimo_pedido__c"),
            lit(None).alias("fecha_inicio_dia__c"),
            col("fecha_vendedor__c"),
            substring(col("fecha__c"), 1, 10).alias("fecha__c"),
            col("fin_de_dia__c"),
            col("fuerza_de_venta__c"),
            lit(None).alias("hora_fin_dia__c"),
            lit(None).alias("hora_inicio_dia__c"),
            col("id"),
            lit(None).alias("id_agente__c"),
            col("inicio_de_dia__c"),
            when(col("isdeleted").isin(["f", "false"]), "false").otherwise("true").alias("isdeleted"),
            lit(None).alias("kpi_cajas__c"),
            lit(None).alias("kpi_cobertura_simple__c"),
            lit(None).alias("kpi_importe__c"),
            col("lastactivitydate"),
            col("lastmodifiedbyid"),
            col("lastmodifieddate"),
            col("lastreferenceddate"),
            col("lastvieweddate"),
            col("latitud_final__c").alias("latitud_fin__c"),
            col("latitud__c"),
            col("longitud_final__c").alias("longitud_fin__c"),
            col("longitud__c"),
            lit(None).alias("marcas__c"),
            col("modelo_atencion__c"),
            col("modulo__c"),
            col("name"),
            col("necesidad_cu_diaria__c"),
            col("necesidad_diaria_importe__c"),
            col("necesidad_diaria__c"),
            col("no_requiere_liquidacion__c"),
            lit(None).alias("nombre_region__c"),
            lit(None).alias("nombre_subregion__c"),
            lit(None).alias("nombre_vendedor__c"),
            lit(None).alias("numero__c"),
            col("ownerid"),
            col("pais__c"),
            col("perfil__c"),
            col("portafolio__c"),
            col("region__c"),
            col("rutan__c"),
            col("ruta__c"),
            col("subregion__c"),
            col("sucursal__c"),
            col("systemmodstamp"),
            col("telefono_vendedor__c"),
            col("total_cajas_cliente_nuevo__c").cast("float"),
            col("total_cajas_extra_ruta__c").cast("float"),
            col("total_cajas_fisicas__c"),
            col("total_cajas_prospectos__c").cast("float"),
            col("total_cajas_vendedor__c"),
            col("total_clientes_activos__c"),
            col("total_clientes_compra2__c").cast("float"),
            col("total_clientes__c"),
            col("total_de_cajas__c"),
            lit(None).alias("total_de_caja_unidades__c"),
            col("total_importe_cliente_nuevo__c"),
            col("total_importe_extra_ruta__c"),
            col("total_importe_prospecto__c"),
            col("total_importe_vendedor__c"),
            col("total_importe__c"),
            col("total_no_ventas__c"),
            lit(None).alias("total_no_pedidos__c"),
            col("total_pedidos_cliente_nuevo__c"),
            col("total_pedidos_extra_ruta__c"),
            col("total_pedidos_prospecto__c"),
            lit(None).alias("total_pedidos__c"),
            col("total_skus_cliente_nuevo__c"),
            col("total_skus_extra_ruta__c"),
            col("total_skus_prospecto__c"),
            col("total_skus__c"),
            col("total_unidades_cliente_nuevo__c").cast("float"),
            col("total_unidades_extra_ruta__c").cast("float"),
            col("total_unidades_prospecto__c").cast("float"),
            col("total_unidades__c"),
            col("total_ventas__c"),
            col("unidades_cambiadas__c"),
            col("unidad__c"),
            col("vendedor_volante__c"),
            col("vendedor__c"),
            col("version_app__c"),
            col("version_del_app__c"),
            col("zonan__c"),
            col("zona__c"),
            col("cajas_totales_cliente_nuevo_c__c"),
            lit(None).alias("cu_totales_vendedor__c"),
            lit(None).alias("efectividad_preventa_ecommerce__c"),
            lit(None).alias("efectividad_visita_ecommerce__c"),
            lit(None).alias("clientes_pedidos_ecommerce__c"),
            lit(None).alias("efectividad_contacto__c"),
        )

    df_union = tmp1.union(tmp2).distinct()


    tmp_t_avance_dia = (
        df_union.alias("vvc")
        .join(
            df_vw_modulo__c.alias("mc"),
            (col("mc.id") == col("vvc.modulo__c")),
            "left",
        )
        .where(
            (
                date_format(to_date(col("vvc.fecha__c"), "yyyy-MM-dd"), "yyyyMM").isin(
                    PERIODOS
                )
            )
            & (col("vvc.pais__c").isin(cod_pais))
        )
        .select(
            col("vvc.pais__c").alias("id_pais"),
            date_format(to_date(col("vvc.fecha__c"), "yyyy-MM-dd"), "yyyyMM").alias(
                "id_periodo"
            ),
            col("vvc.id").alias("id_avance_dia"),
            concat(
                trim(col("vvc.compania__c")),
                lit("|"),
                trim(col("vvc.codigo_vendedor__c")),
            ).alias("id_vendedor"),
            concat(
                trim(col("vvc.compania__c")),
                lit("|"),
                trim(col("vvc.sucursal__c")),
            ).alias("id_sucursal"),
            concat(
                trim(col("vvc.compania__c")),
                lit("|"),
                when(upper(col("vvc.modelo_atencion__c")) == "PRE VENTA", "001")
                .when(upper(col("vvc.modelo_atencion__c")) == "AUTO VENTA", "002")
                .when(upper(col("vvc.modelo_atencion__c")) == "ECOMMERCE", "003")
                .when(upper(col("vvc.modelo_atencion__c")) == "ESPECIALIZADO", "004")
                .when(upper(col("vvc.modelo_atencion__c")) == "TELEVENTA", "005")
                .otherwise("000"),
            ).alias("id_modelo_atencion"),
            col("vvc.name").alias("cod_avance_dia"),
            col("vvc.unidad__c").alias("cod_unidad"),
            col("vvc.modulo__c").alias("cod_modulo"),
            col("mc.name").alias("desc_modulo"),
            col("vvc.ruta__c").alias("cod_ruta"),
            col("vvc.rutan__c").alias("desc_ruta"),
            col("vvc.zona__c").alias("cod_zona"),
            col("vvc.zonan__c").alias("desc_zona"),
            to_date(col("vvc.fecha__c"), "yyyy-MM-dd").alias("fecha_avance_dia"),
            to_timestamp(
                col("vvc.inicio_de_dia__c"), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ'
            ).alias("fecha_inicio"),
            to_timestamp(
                col("vvc.fin_de_dia__c"), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ'
            ).alias("fecha_fin"),
            col("vvc.latitud__c").alias("coordx_inicio"),
            col("vvc.latitud_fin__c").alias("coordx_fin"),
            col("vvc.longitud__c").alias("coordy_inicio"),
            col("vvc.longitud_fin__c").alias("coordy_fin"),
            lit(None).alias("distancia_recorrida"),
            col("vvc.cuota_de_cambio__c").alias("cant_cuota_cambio"),
            to_timestamp(col("vvc.createddate"), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ').alias(
                "fecha_creacion"
            ),
            to_timestamp(
                col("vvc.lastmodifieddate"), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ'
            ).alias("fecha_modificacion"),
            when(col("vvc.isdeleted") == "false", 0).otherwise(1).alias("es_eliminado"),
            col("vvc.avance_cajas_unitarias__c"),
            col("vvc.avance_cajas__c"),
            col("vvc.avance_del_dia__c"),
            col("vvc.avance_del_mes__c"),
            col("vvc.avance_importe__c"),
            col("vvc.bateria__c"),
            col("vvc.cajas_totales_cliente_nuevo__c"),
            col("vvc.cajas_totales_extra_ruta__c"),
            col("vvc.cajas_totales_prospecto__c"),
            col("vvc.cajas_totales_vendedor__c"),
            col("vvc.cajas_totales__c"),
            col("vvc.categorias__c"),
            col("vvc.clientes_compra_0__c"),
            col("vvc.clientes_compra__c"),
            col("vvc.clientes_visitados__c"),
            col("vvc.codigo_unico_vendedor__c"),
            col("vvc.codigo_vendedor__c"),
            col("vvc.compania__c"),
            col("vvc.createdbyid"),
            col("vvc.createddate"),
            col("vvc.cuota_de_cambio__c"),
            col("vvc.cuota_de_ruta__c"),
            col("vvc.currencyisocode"),
            col("vvc.device_id__c"),
            col("vvc.diferencia_clientes_compra_total_pedidos__c"),
            col("vvc.drop_size__c"),
            col("vvc.efectividad__c"),
            col("vvc.enviar_a_call_center__c"),
            col("vvc.fecha_fin_dia__c"),
            col("vvc.fecha_hora_primer_pedido__c"),
            col("vvc.fecha_hora_primer_visita__c"),
            col("vvc.fecha_hora_ultima_sincronizacion__c"),
            col("vvc.fecha_hora_ultima_visita_c__c"),
            col("vvc.fecha_hora_ultimo_pedido__c"),
            col("vvc.fecha_inicio_dia__c"),
            col("vvc.fecha_vendedor__c"),
            col("vvc.fecha__c"),
            col("vvc.fin_de_dia__c"),
            col("vvc.fuerza_de_venta__c"),
            col("vvc.hora_fin_dia__c"),
            col("vvc.hora_inicio_dia__c"),
            col("vvc.id"),
            col("vvc.id_agente__c"),
            col("vvc.inicio_de_dia__c"),
            col("vvc.isdeleted"),
            col("vvc.kpi_cajas__c"),
            col("vvc.kpi_cobertura_simple__c"),
            col("vvc.kpi_importe__c"),
            col("vvc.lastactivitydate"),
            col("vvc.lastmodifiedbyid"),
            col("vvc.lastmodifieddate"),
            col("vvc.lastreferenceddate"),
            col("vvc.lastvieweddate"),
            col("vvc.latitud_fin__c"),
            col("vvc.latitud__c"),
            col("vvc.longitud_fin__c"),
            col("vvc.longitud__c"),
            col("vvc.marcas__c"),
            col("vvc.modelo_atencion__c"),
            col("vvc.modulo__c"),
            col("vvc.name"),
            col("vvc.necesidad_cu_diaria__c"),
            col("vvc.necesidad_diaria_importe__c"),
            col("vvc.necesidad_diaria__c"),
            col("vvc.nombre_region__c"),
            col("vvc.nombre_subregion__c"),
            col("vvc.nombre_vendedor__c"),
            col("vvc.numero__c"),
            col("vvc.ownerid"),
            col("vvc.pais__c"),
            col("vvc.portafolio__c"),
            col("vvc.region__c"),
            col("vvc.rutan__c"),
            col("vvc.ruta__c"),
            col("vvc.subregion__c"),
            col("vvc.sucursal__c"),
            col("vvc.systemmodstamp"),
            col("vvc.telefono_vendedor__c"),
            col("vvc.total_cajas_cliente_nuevo__c"),
            col("vvc.total_cajas_extra_ruta__c"),
            col("vvc.total_cajas_fisicas__c"),
            col("vvc.total_cajas_prospectos__c"),
            col("vvc.total_cajas_vendedor__c"),
            col("vvc.total_clientes_activos__c"),
            col("vvc.total_clientes_compra2__c"),
            col("vvc.total_clientes__c"),
            col("vvc.total_de_cajas__c"),
            col("vvc.total_de_caja_unidades__c"),
            col("vvc.total_importe_cliente_nuevo__c"),
            col("vvc.total_importe_extra_ruta__c"),
            col("vvc.total_importe_prospecto__c"),
            col("vvc.total_importe_vendedor__c"),
            col("vvc.total_importe__c"),
            col("vvc.total_no_pedidos__c"),
            col("vvc.total_pedidos_cliente_nuevo__c"),
            col("vvc.total_pedidos_extra_ruta__c"),
            col("vvc.total_pedidos_prospecto__c"),
            col("vvc.total_pedidos__c"),
            col("vvc.total_skus_cliente_nuevo__c"),
            col("vvc.total_skus_extra_ruta__c"),
            col("vvc.total_skus_prospecto__c"),
            col("vvc.total_skus__c"),
            col("vvc.total_unidades_cliente_nuevo__c"),
            col("vvc.total_unidades_extra_ruta__c"),
            col("vvc.total_unidades_prospecto__c"),
            col("vvc.total_unidades__c"),
            col("vvc.unidades_cambiadas__c"),
            col("vvc.vendedor_volante__c"),
            col("vvc.vendedor__c"),
            col("vvc.version_app__c"),
            col("vvc.version_del_app__c"),
            col("vvc.zonan__c"),
            col("vvc.zona__c"),
            col("vvc.cu_totales_vendedor__c"),
            col("vvc.efectividad_preventa_ecommerce__c"),
            col("vvc.efectividad_visita_ecommerce__c"),
            col("vvc.clientes_pedidos_ecommerce__c"),
            col("vvc.efectividad_contacto__c"),
        )
    )
except Exception as e:
    logger.error(str(e))
    #add_log_to_dynamodb("Creando tablas", e)
    #send_error_message(PROCESS_NAME, "Creando tablas", f"{str(e)[:10000]}")
    exit(1)


# # DELETE
# try:
#     df_t_avance_dia_dom = df_t_avance_dia_dom.where(
#         (col("id_pais") != COD_PAIS) | (~col("id_periodo").isin(PERIODOS))
#     ).select("*")
# except Exception as e:
#     logger.error(str(e))
#     add_log_to_dynamodb("Eliminando datos", e)
#     #send_error_message(PROCESS_NAME, "Eliminando datos", f"{str(e)[:10000]}")
#     exit(1)

# INSERT
try:
    tmp_insert = tmp_t_avance_dia.select(
        col("id_pais").cast(StringType()),
        col("id_periodo").cast(StringType()),
        col("id_avance_dia").cast(StringType()),
        col("id_vendedor").cast(StringType()),
        col("id_sucursal").cast(StringType()),
        col("id_modelo_atencion").cast(StringType()),
        col("cod_avance_dia").cast(StringType()),
        col("cod_unidad").cast(StringType()),
        col("cod_modulo").cast(StringType()),
        col("desc_modulo").cast(StringType()),
        col("cod_ruta").cast(StringType()),
        col("desc_ruta").cast(StringType()),
        col("cod_zona").cast(StringType()),
        col("desc_zona").cast(StringType()),
        col("fecha_avance_dia").cast(DateType()),
        col("fecha_inicio").cast(TimestampType()),
        col("fecha_fin").cast(TimestampType()),
        col("coordx_inicio").cast(StringType()),
        col("coordx_fin").cast(StringType()),
        col("coordy_inicio").cast(StringType()),
        col("coordy_fin").cast(StringType()),
        col("distancia_recorrida").cast(DecimalType(38,12)),
        col("cant_cuota_cambio").cast(StringType()),
        col("fecha_creacion").cast(TimestampType()),
        col("fecha_modificacion").cast(TimestampType()),
        col("es_eliminado").cast(IntegerType()),
        col("avance_cajas_unitarias__c").cast(StringType()),
        col("avance_cajas__c").cast(DecimalType(38,12)),
        col("avance_del_dia__c").cast(DecimalType(38,12)),
        col("avance_del_mes__c").cast(StringType()),
        col("avance_importe__c").cast(DecimalType(38,12)),
        col("bateria__c").cast(StringType()),
        col("cajas_totales_cliente_nuevo__c").cast(DecimalType(38,12)),
        col("cajas_totales_extra_ruta__c").cast(DecimalType(38,12)),
        col("cajas_totales_prospecto__c").cast(DecimalType(38,12)),
        col("cajas_totales_vendedor__c").cast(DecimalType(38,12)),
        col("cajas_totales__c").cast(DecimalType(38,12)),
        col("categorias__c").cast(StringType()),
        col("clientes_compra_0__c").cast(DecimalType(38,12)),
        col("clientes_compra__c").cast(DecimalType(38,12)),
        col("clientes_visitados__c").cast(DecimalType(38,12)),
        col("codigo_unico_vendedor__c").cast(StringType()),
        col("codigo_vendedor__c").cast(StringType()),
        col("compania__c").cast(StringType()),
        col("createdbyid").cast(StringType()),
        to_timestamp(col("createddate"), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ').alias("createddate"),
        col("cuota_de_cambio__c").cast(DecimalType(38,12)),
        col("cuota_de_ruta__c").cast(StringType()),
        col("currencyisocode").cast(StringType()),
        col("device_id__c").cast(StringType()),
        # col("diferencia_clientes_compra_total_pedidos__c"),
        when(col("diferencia_clientes_compra_total_pedidos__c") == "false", False)
        .otherwise(True).cast(BooleanType()).alias("diferencia_clientes_compra_total_pedidos__c"),
        col("drop_size__c").cast(DecimalType(38,12)),
        col("efectividad__c").cast(DecimalType(38,12)),
        # col("enviar_a_call_center__c"),
        when(col("enviar_a_call_center__c") == "false", False)
        .otherwise(True).cast(BooleanType()).alias("enviar_a_call_center__c"),
        to_timestamp(
            col("fecha_hora_primer_pedido__c"), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ'
        ),
        to_timestamp(
            col("fecha_hora_primer_visita__c"), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ'
        ),
        to_timestamp(
            col("fecha_hora_ultima_sincronizacion__c"), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ'
        ),
        to_timestamp(
            col("fecha_hora_ultima_visita_c__c"), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ'
        ),
        to_timestamp(
            col("fecha_hora_ultimo_pedido__c"), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ'
        ),
        col("fecha_inicio_dia__c").cast("date"),
        col("fecha_vendedor__c"),
        to_timestamp(col("fecha__c"), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ'),
        col("fin_de_dia__c"),
        col("fuerza_de_venta__c"),
        col("hora_fin_dia__c"),
        col("hora_inicio_dia__c"),
        col("id"),
        col("id_agente__c"),
        col("inicio_de_dia__c"),
        # col("isdeleted"),
        when(col("isdeleted") == "false", False).otherwise(True),
        col("kpi_cajas__c"),
        col("kpi_cobertura_simple__c"),
        col("kpi_importe__c"),
        to_timestamp(col("lastactivitydate"), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ'),
        col("lastmodifiedbyid"),
        to_timestamp(col("lastmodifieddate"), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ'),
        to_timestamp(col("lastreferenceddate"), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ'),
        to_timestamp(col("lastvieweddate"), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ'),
        col("latitud_fin__c"),
        col("latitud__c"),
        col("longitud_fin__c"),
        col("longitud__c"),
        col("marcas__c"),
        col("modelo_atencion__c"),
        col("modulo__c"),
        col("name"),
        col("necesidad_cu_diaria__c").cast("double"),
        col("necesidad_diaria_importe__c").cast("double"),
        col("necesidad_diaria__c").cast("double"),
        col("nombre_region__c"),
        col("nombre_subregion__c"),
        col("nombre_vendedor__c"),
        col("numero__c"),
        col("ownerid"),
        col("pais__c"),
        col("portafolio__c"),
        col("region__c"),
        col("rutan__c"),
        col("ruta__c"),
        col("subregion__c"),
        col("sucursal__c"),
        col("systemmodstamp"),
        col("telefono_vendedor__c"),
        col("total_cajas_cliente_nuevo__c").cast("double"),
        col("total_cajas_extra_ruta__c").cast("double"),
        col("total_cajas_fisicas__c").cast("double"),
        col("total_cajas_prospectos__c").cast("double"),
        col("total_cajas_vendedor__c").cast("double"),
        col("total_clientes_activos__c").cast("double"),
        col("total_clientes_compra2__c").cast("double"),
        col("total_clientes__c").cast("double"),
        col("total_de_cajas__c").cast("double"),
        col("total_de_caja_unidades__c").cast("double"),
        col("total_importe_cliente_nuevo__c").cast("double"),
        col("total_importe_extra_ruta__c").cast("double"),
        col("total_importe_prospecto__c").cast("double"),
        col("total_importe_vendedor__c").cast("double"),
        col("total_importe__c").cast("double"),
        col("total_no_pedidos__c").cast("double"),
        col("total_pedidos_cliente_nuevo__c").cast("double"),
        col("total_pedidos_extra_ruta__c").cast("double"),
        col("total_pedidos_prospecto__c").cast("double"),
        col("total_pedidos__c").cast("double"),
        col("total_skus_cliente_nuevo__c").cast("double"),
        col("total_skus_extra_ruta__c").cast("double"),
        col("total_skus_prospecto__c").cast("double"),
        col("total_skus__c").cast("double"),
        col("total_unidades_cliente_nuevo__c").cast("double"),
        col("total_unidades_extra_ruta__c").cast("double"),
        col("total_unidades_prospecto__c").cast("double"),
        col("total_unidades__c").cast("double"),
        col("unidades_cambiadas__c").cast("double"),
        col("vendedor_volante__c"),
        col("vendedor__c"),
        col("version_app__c"),
        col("version_del_app__c"),
        col("zonan__c"),
        col("zona__c"),
        col("cu_totales_vendedor__c").cast("double"),
        col("efectividad_preventa_ecommerce__c").cast("double"),
        col("efectividad_visita_ecommerce__c").cast("double"),
        col("clientes_pedidos_ecommerce__c").cast("double"),
        col("efectividad_contacto__c").cast("double"),
    )

    partition_columns_array = ["id_pais","id_periodo"]
    spark_controller.write_table(tmp_insert, data_paths.DOMAIN, target_table_name, partition_columns_array)    

    #df_t_avance_dia_dom = df_t_avance_dia_dom.union(tmp_insert)

except Exception as e:
    logger.error(str(e))
    #add_log_to_dynamodb("Insertando datos", e)
    #send_error_message(PROCESS_NAME, "Insertando datos", f"{str(e)[:10000]}")
    exit(1)


# # SAVE
# try:
#     table_name = "t_avance_dia"
#     df = df_t_avance_dia_dom

#     s3_path_dom = f"{S3_PATH_DOM}/{table_name}"

#     partition_columns_array = ["id_pais", "id_periodo"]

#     df.write.partitionBy(*partition_columns_array).format("delta").option(
#         "overwriteSchema", "true"
#     ).mode("overwrite").option("mergeSchema", "true").option(
#         "partitionOverwriteMode", "dynamic"
#     ).save(
#         s3_path_dom
#     )

#     delta_table = DeltaTable.forPath(spark, s3_path_dom)
#     delta_table.generate("symlink_format_manifest")
# except Exception as e:
#     logger.error(str(e))
#     add_log_to_dynamodb("Guardando tablas", e)
#     #send_error_message(PROCESS_NAME, "Guardando tablas", f"{str(e)[:10000]}")
#     exit(1)
