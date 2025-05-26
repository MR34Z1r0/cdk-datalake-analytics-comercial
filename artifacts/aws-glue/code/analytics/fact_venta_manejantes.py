import sys
import json
import boto3
from pyspark.sql.types import *
from aje.get_schemas import *
from delta.tables import DeltaTable
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession, SQLContext
import logging
import os
import datetime as dt
import pytz
from pyspark.sql.functions import (
    col,
    upper,
    floor,
    date_format,
    sum,
    max,
    current_date,
    trim,
    lit,
    trunc,
    add_months,
    to_date,
    date_trunc,
    when,
    round
)

######################################
# JOB PARAMETERS

args = getResolvedOptions(
    sys.argv,
    [
        "S3_PATH_STG_BM",
        "S3_PATH_STG_SF",
        "S3_PATH_DOM",
        "S3_PATH_COM",
        "REGION_NAME",
        "DYNAMODB_DATABASE_NAME",
        "COD_PAIS",
        "PERIODO_INI",
        "PERIODO_FIN",
        "PERIODOS",
        "S3_PATH_EXTERNAL_DATA_RAW",
        'DYNAMODB_LOGS_TABLE',
        'ERROR_TOPIC_ARN',
        'PROJECT_NAME',
        'FLOW_NAME',
        'PROCESS_NAME'
    ],
)

S3_PATH_STG_BM = args["S3_PATH_STG_BM"]
S3_PATH_STG_SF = args["S3_PATH_STG_SF"]
S3_PATH_DOM = args["S3_PATH_DOM"]
S3_PATH_COM = args["S3_PATH_COM"]
REGION_NAME = args["REGION_NAME"]
DYNAMODB_DATABASE_NAME = args["DYNAMODB_DATABASE_NAME"]
COD_PAIS = args["COD_PAIS"]

PERIODO_INI = args["PERIODO_INI"]
PERIODO_FIN = args["PERIODO_FIN"]
S3_PATH_EXTERNAL_DATA_RAW = args["S3_PATH_EXTERNAL_DATA_RAW"]

if (PERIODO_INI == "-") and (PERIODO_FIN == "-"):
    PERIODOS = json.loads(args['PERIODOS'])
else:
    PERIODOS = [args["PERIODO_INI"],args["PERIODO_FIN"]]

DYNAMODB_LOGS_TABLE = args['DYNAMODB_LOGS_TABLE']
ERROR_TOPIC_ARN = args['ERROR_TOPIC_ARN']
PROJECT_NAME = args['PROJECT_NAME']
FLOW_NAME = args['FLOW_NAME']
PROCESS_NAME = args['PROCESS_NAME']

sns_client = boto3.client("sns", region_name=REGION_NAME)

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(PROCESS_NAME)
logger.setLevel(os.environ.get("LOGGING", logging.INFO))

TZ_LIMA = pytz.timezone('America/Lima')
NOW_LIMA = dt.datetime.now(pytz.utc).astimezone(TZ_LIMA)
######################################
# JOB SETUP

spark = (
    SparkSession.builder.config(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
    .getOrCreate()
)

sc = spark.sparkContext
glue_context = GlueContext(sc)
logger = glue_context.get_logger()
sqlContext = SQLContext(sparkSession=spark, sparkContext=sc)
dynamodb_client = boto3.client("dynamodb", region_name=REGION_NAME)

######################################
# FUNCTIONS
def add_log_to_dynamodb(task_name, error_message = ''):
    dynamodb_client = boto3.resource('dynamodb')
    dynamo_table = dynamodb_client.Table(DYNAMODB_LOGS_TABLE)
    task_status = 'satisfactorio' if error_message == '' else 'error'
    process_type = 'D' if len(PERIODOS)>0 else 'F'
    date_system = NOW_LIMA.strftime('%Y%m%d_%H%M%S')

    record = {
        'PROCESS_ID': f"DLB_{PROCESS_NAME}_{date_system}",
        'DATE_SYSTEM': date_system,
        'PROJECT_NAME': args['PROJECT_NAME'],
        'FLOW_NAME': args['FLOW_NAME'],
        'TASK_NAME': task_name,
        'TASK_STATUS': task_status,
        'MESSAGE': str(error_message),
        'PROCESS_TYPE': process_type
    }
    dynamo_table.put_item(Item=record)

def send_error_message(table_name, msg, error):
    response = sns_client.publish(
        TopicArn=ERROR_TOPIC_ARN,
        Message=f"Failed table: {table_name}\nMessage: {msg}\nLog ERROR:\n{error}"
    )

def read_table(table_name, path):
    try:
        s3_path = f"{path}/{table_name}"
        df = spark.read.format("delta").load(s3_path)
        return df
    except Exception as e:
        logger.error(str(e))
        add_log_to_dynamodb("Leyendo tablas", e)
        #send_error_message(PROCESS_NAME, "Leyendo tablas", f"{str(e)[:10000]}")
        exit(1)


def create_df_schema(table_name):
    try:
        schemas = SchemaModeloComercial(logger)
        schema = schemas.get_schema(table_name)

        df = spark.createDataFrame([], schema)

        return df
    except Exception as e:
        logger.error(str(e))
        add_log_to_dynamodb("Creando schema", e)
        #send_error_message(PROCESS_NAME, "Creando schema", f"{str(e)[:10000]}")
        exit(1)

def get_table(table_name, path):
    s3_path = f"{path}/{table_name}"
    try:
        df = spark.read.format("delta").load(s3_path)
    except AnalysisException:
        # If Path does not exist:
        df = create_df_schema(table_name)
    return df

def read_table_external_data_raw_csv(table_name):
    s3_path = f"{S3_PATH_EXTERNAL_DATA_RAW}/{table_name}.csv"
    df = spark.read.options(delimiter=";", header=True).csv(s3_path)
    return df

######################################
# READ

try:
    df_t_venta = read_table("t_venta", S3_PATH_DOM)
    df_m_tipo_venta = read_table("m_tipo_venta", S3_PATH_DOM)
    df_m_cliente = read_table("m_cliente", S3_PATH_DOM)
    df_m_compania = read_table("m_compania", S3_PATH_DOM)
    df_m_articulo = read_table("m_articulo", S3_PATH_DOM)
    df_t_venta_detalle = read_table("t_venta_detalle", S3_PATH_DOM)

    df_fact_venta_manejantes_com = get_table("fact_venta_manejantes", S3_PATH_COM).where(
        (col('id_pais').isin(cod_pais))
            & (col('id_periodo').isin(PERIODOS))
    )

    df_aje_comercial_corp_manejantes = read_table_external_data_raw_csv("marcas_pe")

except Exception as e:
    logger.error(str(e))
    add_log_to_dynamodb('Leyendo tablas fuente', e)
    #send_error_message(PROCESS_NAME, 'Leyendo tablas fuente', f"{str(e)[:10000]}")
    exit(1)

# CREATE
try:
    # Lista de los ultimos 12 meses cerrados
    tmp_manejantes_periodos_12_meses_cerrados = (
        df_t_venta.alias("tv")
        .join(
            df_m_tipo_venta.alias("mtv"),
            (col("tv.id_tipo_venta") == col("mtv.id_tipo_venta")),
            "inner",
        )
        .where(
            ((
                (~col("tv.id_pais").isin(["PE","EC","MX"]))
                & (col("tv.id_periodo") >= date_format(add_months(date_trunc('month',current_date()),-12),'yyyyMM'))
            )
            |
            (
                (col("tv.id_pais").isin(["PE","EC","MX"]))
                & (col("tv.id_periodo") >= date_format(add_months(date_trunc('month',current_date()),-6),'yyyyMM'))
            ))
            & (col("id_periodo") != date_format(current_date(),'yyyyMM'))
            & (col("tv.es_anulado") == 0)
            & (col("tv.es_eliminado") == 0)
            & (upper(col("mtv.cod_tipo_operacion")).isin(["VEN", "EXP"]))
            & (col("tv.id_pais").isin(cod_pais))
        )
        .select(
            col("tv.id_periodo")
        )
        .distinct()
    )

    # Lista de clientes que compraron en los años identificados
    tmp_manejantes_clientes_12_meses_cerrados = (
        # translate
        df_t_venta.alias("tv")
        .join(
            df_m_tipo_venta.alias("mtv"),
            (col("tv.id_tipo_venta") == col("mtv.id_tipo_venta")),
            "inner",
        )
        .join(
            tmp_manejantes_periodos_12_meses_cerrados.alias("p12mc"),
            (col("tv.id_periodo") == col("p12mc.id_periodo")),
            "inner",
        )
        .join(
            df_m_cliente.alias("mc"),
            (col("tv.id_cliente") == col("mc.id_cliente"))
            & (col("tv.id_pais") == col("mc.id_pais"))
            & (col("mc.cod_tipo_cliente").isin(["N", "T"])),
            "inner",
        )
        .join(
            df_m_compania.alias("mco"),
            (col("mc.id_pais") == col("mco.id_pais"))
            & (col("mco.cod_compania") == col("tv.id_compania"))
            & (
                (col("tv.id_pais") != "PE")
                | col("mco.nomb_compania").isin([
                    "AJEPER DEL ORIENTE S.A.",
                    "AJEPER S.A.",
                    "COMERCIALIZADORA ARAGORN S.A.C.",
                    "COMERCIALIZADORA BABEL S.A.C",
                    "COMERCIALIZADORA BETANIA S.A.C",
                    "COMERCIALIZADORA JERICO S.A.C.",
                    "COMERCIALIZADORA LOS TULIPANES SAC",
                    "COMERCIALIZADORA SALEM SAC",
                ])
            ),
            "inner",
        )
        .where(
            (
                (~col("tv.id_pais").isin(["PE", "EC", "MX"]) & (col("tv.id_periodo") >= date_format(add_months(date_trunc('month',current_date()),-12),'yyyyMM')))
                |
                (col("tv.id_pais").isin(["PE", "EC", "MX"]) & (col("tv.id_periodo") >= date_format(add_months(date_trunc('month',current_date()),-6),'yyyyMM')))
            )
            & (upper(col("mtv.cod_tipo_operacion")).isin(["VEN", "EXP"]))
            & (col("tv.es_anulado") == 0)
            & (col("tv.es_eliminado") == 0)
            & (col("tv.id_pais").isin(cod_pais))
        )
        .select(
            col("tv.id_cliente"),
            col("tv.id_pais"),
        )
        .distinct()
    )

    # Cruce de clientes con los ultimos 12 meses cerrados (CROSSJOIN)
    tmp_manejantes_clientes_12_meses_cerrados_cj = (
        tmp_manejantes_clientes_12_meses_cerrados.alias("tv")
        .crossJoin(
            tmp_manejantes_periodos_12_meses_cerrados.alias("p12mc")
        )
        .select(
            col("tv.id_cliente"),
            col("tv.id_pais"),
            col("p12mc.id_periodo"),
        )
        .distinct()
    )

    # Cruce de clientes con los ultimos 12 meses cerrados con lista de marcas (CROSSJOIN)

    tmp1 = (
        df_aje_comercial_corp_manejantes
        .where(col("cod_pais").isin(cod_pais))
        .select(
            col("*")
        )
    )

    tmp_manejantes_clientes_12_meses_cerrados_cj_prod = (
        tmp_manejantes_clientes_12_meses_cerrados_cj.alias("tv")
        .crossJoin(
            tmp1.alias("ma")
        )
        .select(
            col("tv.id_cliente"),
            col("tv.id_pais"),
            col("tv.id_periodo"),
            col("ma.cod_marca"),
        )
        .distinct()
    )


    # Todas las combinaciones de sabor y formato para cada marca

    tmp2 = (
        df_m_articulo
        .where(
            (col("id_pais").isin(cod_pais))
            & (
                (col("id_pais") != "PE")
                | col("cod_marca").isin([500, 530])
            )
        )
        .select(
            col("cod_marca"),
            col("cod_sabor"),
            col("cod_formato"),
            col("id_pais"),
        )
        .distinct()
    )

    tmp_manejantes_clientes_12_meses_cerrados_cj_prod_marca = (
        tmp_manejantes_clientes_12_meses_cerrados_cj_prod.alias("tv")
        .join(
            tmp2.alias("ma"),
            (col("tv.cod_marca") == trim(col("ma.cod_marca"))),
            "inner",
        )
        .select(
            col("tv.id_cliente"),
            col("tv.id_pais"),
            col("tv.id_periodo"),
            trim(col("ma.cod_marca")).alias("cod_marca"),
            trim(col("ma.cod_sabor")).alias("cod_sabor"),
            trim(col("ma.cod_formato")).alias("cod_formato"),
        )
    )


    # id_producto model for each combination of marca, sabor and formato

    tmp_manejantes_id_producto_model = (
        df_m_articulo.alias("ma")
        .where(
            (col("ma.id_pais").isin(cod_pais))
        )
        .groupby(
            col("ma.cod_marca"),
            col("ma.cod_sabor"),
            col("ma.cod_formato")
        )
        .agg(
            max(col("ma.id_articulo")).alias("id_producto_modelo")
        )
        .select(
            col("ma.cod_marca"),
            col("ma.cod_sabor"),
            col("ma.cod_formato"),
            col("id_producto_modelo")
        )
        .distinct()
    )

    # identificación de pares únicos de cliente-fecha_liquidación

    tmp_manejantes_pares_cliente_mes_venta = (
        df_t_venta.alias("tv")
        .join(
            df_t_venta_detalle.alias("tvd"),
            (col("tv.id_venta") == col("tvd.id_venta")),
            "inner",
        )
        .join(
            df_m_articulo.alias("ma"),
            (col("tvd.id_articulo") == col("ma.id_articulo")),
            "inner",
        )
        .join(
            df_m_tipo_venta.alias("mtv"),
            (col("tv.id_tipo_venta") == col("mtv.id_tipo_venta")),
            "inner",
        )
        .where(
            (
                (~col("tv.id_pais").isin(["PE", "EC", "MX"]) & (col("tv.id_periodo") >= date_format(add_months(date_trunc('month',current_date()),-12),'yyyyMM')))
                |
                (col("tv.id_pais").isin(["PE", "EC", "MX"]) & (col("tv.id_periodo") >= date_format(add_months(date_trunc('month',current_date()),-6),'yyyyMM')))
            )
            & (col("tv.es_anulado") == 0)
            & (col("tv.es_eliminado") == 0)
            & (col("tv.id_periodo") != date_format(current_date(),'yyyyMM'))
            & (upper(col("mtv.cod_tipo_operacion")).isin(["VEN", "EXP"]))
            & (col("tv.id_pais").isin(cod_pais))
        )
        .groupby(
            col("tv.id_cliente"),
            col("tv.id_periodo"),
            col("tv.id_pais"),
            col("ma.cod_marca"),
            col("ma.cod_sabor"),
            col("ma.cod_formato")
        )
        .agg(
            sum(col("tvd.imp_neto_vta_mn")).alias("imp_neto_vta_mn"),
            sum(col("tvd.imp_neto_vta_me")).alias("imp_neto_vta_me"),
            sum(col("tvd.imp_bruto_vta_mn")).alias("imp_bruto_vta_mn"),
            sum(col("tvd.imp_bruto_vta_me")).alias("imp_bruto_vta_me"),
            sum(col("tvd.cant_caja_fisica_ven")).alias("cant_cajafisica_vta"),
            sum(col("tvd.cant_caja_volumen_ven")/30).alias("cant_cajaunitaria_vta")
        )
        .select(
            col("tv.id_cliente"),
            trim(col("ma.cod_marca")).alias("cod_marca"),
            trim(col("ma.cod_sabor")).alias("cod_sabor"),
            trim(col("ma.cod_formato")).alias("cod_formato"),
            col("tv.id_periodo"),
            col("tv.id_pais"),
            col("imp_neto_vta_mn"),
            col("imp_neto_vta_me"),
            col("imp_bruto_vta_mn"),
            col("imp_bruto_vta_me"),
            col("cant_cajafisica_vta"),
            col("cant_cajaunitaria_vta")
        )
    )


    tmp_manejantes_pares_cliente_mes_venta_sin_previo = (
        tmp_manejantes_pares_cliente_mes_venta.alias("tmppcmv")
        .join(
            tmp_manejantes_id_producto_model.alias("ma"),
            (col("tmppcmv.cod_marca") == col("ma.cod_marca"))
            & (col("tmppcmv.cod_sabor") == col("ma.cod_sabor"))
            & (col("tmppcmv.cod_formato") == col("ma.cod_formato")),
            "inner",
        )
        .join(
            tmp_manejantes_clientes_12_meses_cerrados_cj_prod_marca.alias("cj"),
            (col("tmppcmv.id_cliente") == col("cj.id_cliente"))
            & (col("tmppcmv.id_periodo") == col("cj.id_periodo"))
            & (col("tmppcmv.id_pais") == col("cj.id_pais"))
            & (col("tmppcmv.cod_marca") == col("cj.cod_marca"))
            & (col("tmppcmv.cod_sabor") == col("cj.cod_sabor"))
            & (col("tmppcmv.cod_formato") == col("cj.cod_formato")),
            "right",
        )
        .select(
            col("cj.id_cliente"),
            col("cj.cod_marca"),
            col("cj.cod_sabor"),
            col("cj.cod_formato"),
            col("cj.id_periodo"),
            col("cj.id_pais"),
            col("tmppcmv.imp_neto_vta_mn"),
            col("tmppcmv.imp_neto_vta_me"),
            col("tmppcmv.imp_bruto_vta_mn"),
            col("tmppcmv.imp_bruto_vta_me"),
            col("tmppcmv.cant_cajafisica_vta"),
            col("tmppcmv.cant_cajaunitaria_vta")
        )
    )

    # proyección a cuatro meses hacia adelante de la liquidación registrada

    tmp_manejantes_proyeccion_ventas = (
        df_t_venta.alias("tv")
        .join(
            df_t_venta_detalle.alias("tvd"),
            (col("tv.id_venta") == col("tvd.id_venta")),
            "inner",
        )
        .join(
            df_m_articulo.alias("ma"),
            (col("tvd.id_articulo") == col("ma.id_articulo")),
            "inner",
        )
        .join(
            df_m_tipo_venta.alias("mtv"),
            (col("tv.id_tipo_venta") == col("mtv.id_tipo_venta")),
            "inner",
        )
        .where(
            (col("tv.id_periodo") >= date_format(add_months(date_trunc('month',current_date()),-12-13),'yyyyMM'))
            & (col("tv.es_anulado") == 0)
            & (col("tv.es_eliminado") == 0)
            & (col("tvd.cant_caja_fisica_pro") == 0)
            & (col("tvd.cant_caja_volumen_pro") == 0)
            & (col("tvd.cant_caja_fisica_ven") >= 0)
            & (col("tvd.cant_caja_volumen_ven") >= 0)
            & (col("tv.id_periodo") != date_format(current_date(),'yyyyMM'))
            & (upper(col("mtv.cod_tipo_operacion")).isin(["VEN", "EXP"]))
            & (col("tv.id_pais").isin(cod_pais))
        )
        .groupby(
            col("tv.id_cliente"),
            col("tv.id_periodo"),
            col("tv.id_pais"),
            col("ma.cod_marca"),
            col("ma.cod_sabor"),
            col("ma.cod_formato")
        )
        .agg(
            sum(col("tv.id_cliente")).alias("foo")  # arbitrary aggregation
        )
        .select(
            col("tv.id_cliente"),
            col("ma.cod_marca"),
            col("ma.cod_sabor"),
            col("ma.cod_formato"),
            col("tv.id_periodo"),
            col("tv.id_pais"),
            date_format(add_months(to_date(col("tv.id_periodo"),'yyyyMM'),1),'yyyyMM').alias("proyeccion_1_mes"),
            date_format(add_months(to_date(col("tv.id_periodo"),'yyyyMM'),2),'yyyyMM').alias("proyeccion_2_mes"),
            date_format(add_months(to_date(col("tv.id_periodo"),'yyyyMM'),3),'yyyyMM').alias("proyeccion_3_mes")
        )
        .distinct()
    )

    # molde de las columnas 24 meses hacia atras

    tmp_manejantes_proyeccion_24_meses = (
        tmp_manejantes_pares_cliente_mes_venta_sin_previo
        .select(
            col("id_cliente"),
            col("cod_marca"),
            col("cod_sabor"),
            col("cod_formato"),
            col("id_periodo"),
            col("id_pais"),
            date_format(add_months(to_date(col("id_periodo"),'yyyyMM'),0),'yyyyMM').alias("m0_fecha"),
            date_format(add_months(to_date(col("id_periodo"),'yyyyMM'),-1),'yyyyMM').alias("m1_fecha"),
            date_format(add_months(to_date(col("id_periodo"),'yyyyMM'),-2),'yyyyMM').alias("m2_fecha"),
            date_format(add_months(to_date(col("id_periodo"),'yyyyMM'),-3),'yyyyMM').alias("m3_fecha"),
            date_format(add_months(to_date(col("id_periodo"),'yyyyMM'),-4),'yyyyMM').alias("m4_fecha"),
            date_format(add_months(to_date(col("id_periodo"),'yyyyMM'),-5),'yyyyMM').alias("m5_fecha"),
            date_format(add_months(to_date(col("id_periodo"),'yyyyMM'),-6),'yyyyMM').alias("m6_fecha"),
            date_format(add_months(to_date(col("id_periodo"),'yyyyMM'),-7),'yyyyMM').alias("m7_fecha"),
            date_format(add_months(to_date(col("id_periodo"),'yyyyMM'),-8),'yyyyMM').alias("m8_fecha"),
            date_format(add_months(to_date(col("id_periodo"),'yyyyMM'),-9),'yyyyMM').alias("m9_fecha"),
            date_format(add_months(to_date(col("id_periodo"),'yyyyMM'),-10),'yyyyMM').alias("m10_fecha"),
            date_format(add_months(to_date(col("id_periodo"),'yyyyMM'),-11),'yyyyMM').alias("m11_fecha"),
            date_format(add_months(to_date(col("id_periodo"),'yyyyMM'),-12),'yyyyMM').alias("m12_fecha")
        )
    )


    # para cada uno de los 24 meses hacia atrás calcular el número de proyecciones

    tmp_m0 = (
        tmp_manejantes_proyeccion_ventas.alias("pv")
        .join(
            tmp_manejantes_proyeccion_24_meses.alias("p24m"),
            (col("pv.id_cliente") == col("p24m.id_cliente"))
            & (col("pv.cod_marca") == col("p24m.cod_marca"))
            & (col("pv.cod_sabor") == col("p24m.cod_sabor"))
            & (col("pv.cod_formato") == col("p24m.cod_formato"))
            & (col("pv.id_pais") == col("p24m.id_pais"))
            & (
                (col("pv.proyeccion_1_mes") == col("p24m.m0_fecha"))
                | (col("pv.proyeccion_2_mes") == col("p24m.m0_fecha"))
                | (col("pv.proyeccion_3_mes") == col("p24m.m0_fecha"))
                | (col("pv.id_periodo") == col("p24m.m0_fecha"))
            ),
            "inner",
        )
        .select(
            col("*")
        )
        .count()
    )

    tmp_m1 = (
        tmp_manejantes_proyeccion_ventas.alias("pv")
        .join(
            tmp_manejantes_proyeccion_24_meses.alias("p24m"),
            (col("pv.id_cliente") == col("p24m.id_cliente"))
            & (col("pv.cod_marca") == col("p24m.cod_marca"))
            & (col("pv.cod_sabor") == col("p24m.cod_sabor"))
            & (col("pv.cod_formato") == col("p24m.cod_formato"))
            & (col("pv.id_pais") == col("p24m.id_pais"))
            & (
                (col("pv.proyeccion_1_mes") == col("p24m.m1_fecha"))
                | (col("pv.proyeccion_2_mes") == col("p24m.m1_fecha"))
                | (col("pv.proyeccion_3_mes") == col("p24m.m1_fecha"))
                | (col("pv.id_periodo") == col("p24m.m1_fecha"))
            ),
            "inner",
        )
        .select(
            col("*")
        )
        .count()
    )

    tmp_m2 = (
        tmp_manejantes_proyeccion_ventas.alias("pv")
        .join(
            tmp_manejantes_proyeccion_24_meses.alias("p24m"),
            (col("pv.id_cliente") == col("p24m.id_cliente"))
            & (col("pv.cod_marca") == col("p24m.cod_marca"))
            & (col("pv.cod_sabor") == col("p24m.cod_sabor"))
            & (col("pv.cod_formato") == col("p24m.cod_formato"))
            & (col("pv.id_pais") == col("p24m.id_pais"))
            & (
                (col("pv.proyeccion_1_mes") == col("p24m.m2_fecha"))
                | (col("pv.proyeccion_2_mes") == col("p24m.m2_fecha"))
                | (col("pv.proyeccion_3_mes") == col("p24m.m2_fecha"))
                | (col("pv.id_periodo") == col("p24m.m2_fecha"))
            ),
            "inner",
        )
        .select(
            col("*")
        )
        .count()
    )

    tmp_m3 = (
        tmp_manejantes_proyeccion_ventas.alias("pv")
        .join(
            tmp_manejantes_proyeccion_24_meses.alias("p24m"),
            (col("pv.id_cliente") == col("p24m.id_cliente"))
            & (col("pv.cod_marca") == col("p24m.cod_marca"))
            & (col("pv.cod_sabor") == col("p24m.cod_sabor"))
            & (col("pv.cod_formato") == col("p24m.cod_formato"))
            & (col("pv.id_pais") == col("p24m.id_pais"))
            & (
                (col("pv.proyeccion_1_mes") == col("p24m.m3_fecha"))
                | (col("pv.proyeccion_2_mes") == col("p24m.m3_fecha"))
                | (col("pv.proyeccion_3_mes") == col("p24m.m3_fecha"))
                | (col("pv.id_periodo") == col("p24m.m3_fecha"))
            ),
            "inner",
        )
        .select(
            col("*")
        )
        .count()
    )

    tmp_m4 = (
        tmp_manejantes_proyeccion_ventas.alias("pv")
        .join(
            tmp_manejantes_proyeccion_24_meses.alias("p24m"),
            (col("pv.id_cliente") == col("p24m.id_cliente"))
            & (col("pv.cod_marca") == col("p24m.cod_marca"))
            & (col("pv.cod_sabor") == col("p24m.cod_sabor"))
            & (col("pv.cod_formato") == col("p24m.cod_formato"))
            & (col("pv.id_pais") == col("p24m.id_pais"))
            & (
                (col("pv.proyeccion_1_mes") == col("p24m.m4_fecha"))
                | (col("pv.proyeccion_2_mes") == col("p24m.m4_fecha"))
                | (col("pv.proyeccion_3_mes") == col("p24m.m4_fecha"))
                | (col("pv.id_periodo") == col("p24m.m4_fecha"))
            ),
            "inner",
        )
        .select(
            col("*")
        )
        .count()
    )

    tmp_m5 = (
        tmp_manejantes_proyeccion_ventas.alias("pv")
        .join(
            tmp_manejantes_proyeccion_24_meses.alias("p24m"),
            (col("pv.id_cliente") == col("p24m.id_cliente"))
            & (col("pv.cod_marca") == col("p24m.cod_marca"))
            & (col("pv.cod_sabor") == col("p24m.cod_sabor"))
            & (col("pv.cod_formato") == col("p24m.cod_formato"))
            & (col("pv.id_pais") == col("p24m.id_pais"))
            & (
                (col("pv.proyeccion_1_mes") == col("p24m.m5_fecha"))
                | (col("pv.proyeccion_2_mes") == col("p24m.m5_fecha"))
                | (col("pv.proyeccion_3_mes") == col("p24m.m5_fecha"))
                | (col("pv.id_periodo") == col("p24m.m5_fecha"))
            ),
            "inner",
        )
        .select(
            col("*")
        )
        .count()
    )

    tmp_m6 = (
        tmp_manejantes_proyeccion_ventas.alias("pv")
        .join(
            tmp_manejantes_proyeccion_24_meses.alias("p24m"),
            (col("pv.id_cliente") == col("p24m.id_cliente"))
            & (col("pv.cod_marca") == col("p24m.cod_marca"))
            & (col("pv.cod_sabor") == col("p24m.cod_sabor"))
            & (col("pv.cod_formato") == col("p24m.cod_formato"))
            & (col("pv.id_pais") == col("p24m.id_pais"))
            & (
                (col("pv.proyeccion_1_mes") == col("p24m.m6_fecha"))
                | (col("pv.proyeccion_2_mes") == col("p24m.m6_fecha"))
                | (col("pv.proyeccion_3_mes") == col("p24m.m6_fecha"))
                | (col("pv.id_periodo") == col("p24m.m6_fecha"))
            ),
            "inner",
        )
        .select(
            col("*")
        )
        .count()
    )

    tmp_m7 = (
        tmp_manejantes_proyeccion_ventas.alias("pv")
        .join(
            tmp_manejantes_proyeccion_24_meses.alias("p24m"),
            (col("pv.id_cliente") == col("p24m.id_cliente"))
            & (col("pv.cod_marca") == col("p24m.cod_marca"))
            & (col("pv.cod_sabor") == col("p24m.cod_sabor"))
            & (col("pv.cod_formato") == col("p24m.cod_formato"))
            & (col("pv.id_pais") == col("p24m.id_pais"))
            & (
                (col("pv.proyeccion_1_mes") == col("p24m.m7_fecha"))
                | (col("pv.proyeccion_2_mes") == col("p24m.m7_fecha"))
                | (col("pv.proyeccion_3_mes") == col("p24m.m7_fecha"))
                | (col("pv.id_periodo") == col("p24m.m7_fecha"))
            ),
            "inner",
        )
        .select(
            col("*")
        )
        .count()
    )

    tmp_m8 = (
        tmp_manejantes_proyeccion_ventas.alias("pv")
        .join(
            tmp_manejantes_proyeccion_24_meses.alias("p24m"),
            (col("pv.id_cliente") == col("p24m.id_cliente"))
            & (col("pv.cod_marca") == col("p24m.cod_marca"))
            & (col("pv.cod_sabor") == col("p24m.cod_sabor"))
            & (col("pv.cod_formato") == col("p24m.cod_formato"))
            & (col("pv.id_pais") == col("p24m.id_pais"))
            & (
                (col("pv.proyeccion_1_mes") == col("p24m.m8_fecha"))
                | (col("pv.proyeccion_2_mes") == col("p24m.m8_fecha"))
                | (col("pv.proyeccion_3_mes") == col("p24m.m8_fecha"))
                | (col("pv.id_periodo") == col("p24m.m8_fecha"))
            ),
            "inner",
        )
        .select(
            col("*")
        )
        .count()
    )

    tmp_m9 = (
        tmp_manejantes_proyeccion_ventas.alias("pv")
        .join(
            tmp_manejantes_proyeccion_24_meses.alias("p24m"),
            (col("pv.id_cliente") == col("p24m.id_cliente"))
            & (col("pv.cod_marca") == col("p24m.cod_marca"))
            & (col("pv.cod_sabor") == col("p24m.cod_sabor"))
            & (col("pv.cod_formato") == col("p24m.cod_formato"))
            & (col("pv.id_pais") == col("p24m.id_pais"))
            & (
                (col("pv.proyeccion_1_mes") == col("p24m.m9_fecha"))
                | (col("pv.proyeccion_2_mes") == col("p24m.m9_fecha"))
                | (col("pv.proyeccion_3_mes") == col("p24m.m9_fecha"))
                | (col("pv.id_periodo") == col("p24m.m9_fecha"))
            ),
            "inner",
        )
        .select(
            col("*")
        )
        .count()
    )

    tmp_m10 = (
        tmp_manejantes_proyeccion_ventas.alias("pv")
        .join(
            tmp_manejantes_proyeccion_24_meses.alias("p24m"),
            (col("pv.id_cliente") == col("p24m.id_cliente"))
            & (col("pv.cod_marca") == col("p24m.cod_marca"))
            & (col("pv.cod_sabor") == col("p24m.cod_sabor"))
            & (col("pv.cod_formato") == col("p24m.cod_formato"))
            & (col("pv.id_pais") == col("p24m.id_pais"))
            & (
                (col("pv.proyeccion_1_mes") == col("p24m.m10_fecha"))
                | (col("pv.proyeccion_2_mes") == col("p24m.m10_fecha"))
                | (col("pv.proyeccion_3_mes") == col("p24m.m10_fecha"))
                | (col("pv.id_periodo") == col("p24m.m10_fecha"))
            ),
            "inner",
        )
        .select(
            col("*")
        )
        .count()
    )

    tmp_m11 = (
        tmp_manejantes_proyeccion_ventas.alias("pv")
        .join(
            tmp_manejantes_proyeccion_24_meses.alias("p24m"),
            (col("pv.id_cliente") == col("p24m.id_cliente"))
            & (col("pv.cod_marca") == col("p24m.cod_marca"))
            & (col("pv.cod_sabor") == col("p24m.cod_sabor"))
            & (col("pv.cod_formato") == col("p24m.cod_formato"))
            & (col("pv.id_pais") == col("p24m.id_pais"))
            & (
                (col("pv.proyeccion_1_mes") == col("p24m.m11_fecha"))
                | (col("pv.proyeccion_2_mes") == col("p24m.m11_fecha"))
                | (col("pv.proyeccion_3_mes") == col("p24m.m11_fecha"))
                | (col("pv.id_periodo") == col("p24m.m11_fecha"))
            ),
            "inner",
        )
        .select(
            col("*")
        )
        .count()
    )

    tmp_m12 = (
        tmp_manejantes_proyeccion_ventas.alias("pv")
        .join(
            tmp_manejantes_proyeccion_24_meses.alias("p24m"),
            (col("pv.id_cliente") == col("p24m.id_cliente"))
            & (col("pv.cod_marca") == col("p24m.cod_marca"))
            & (col("pv.cod_sabor") == col("p24m.cod_sabor"))
            & (col("pv.cod_formato") == col("p24m.cod_formato"))
            & (col("pv.id_pais") == col("p24m.id_pais"))
            & (
                (col("pv.proyeccion_1_mes") == col("p24m.m12_fecha"))
                | (col("pv.proyeccion_2_mes") == col("p24m.m12_fecha"))
                | (col("pv.proyeccion_3_mes") == col("p24m.m12_fecha"))
                | (col("pv.id_periodo") == col("p24m.m12_fecha"))
            ),
            "inner",
        )
        .select(
            col("*")
        )
        .count()
    )

    tmp_manejantes_cant_4_meses_ventas_previos_proyeccion_24 = (
        tmp_manejantes_proyeccion_24_meses.alias("p24m")
        .select(
            col("p24m.id_cliente"),
            col("p24m.cod_marca"),
            col("p24m.cod_sabor"),
            col("p24m.cod_formato"),
            col("p24m.id_periodo"),
            col("p24m.id_pais"),
            lit(tmp_m0).alias("m0"),
            lit(tmp_m1).alias("m1"),
            lit(tmp_m2).alias("m2"),
            lit(tmp_m3).alias("m3"),
            lit(tmp_m4).alias("m4"),
            lit(tmp_m5).alias("m5"),
            lit(tmp_m6).alias("m6"),
            lit(tmp_m7).alias("m7"),
            lit(tmp_m8).alias("m8"),
            lit(tmp_m9).alias("m9"),
            lit(tmp_m10).alias("m10"),
            lit(tmp_m11).alias("m11"),
            lit(tmp_m12).alias("m12")
        )
    )


    # tmp final
    tmp_final = (
        tmp_manejantes_pares_cliente_mes_venta_sin_previo.alias("tv")
        .join(
            tmp_manejantes_cant_4_meses_ventas_previos_proyeccion_24.alias("cant24"),
            (col("tv.id_cliente") == col("cant24.id_cliente"))
            & (col("tv.cod_marca") == col("cant24.cod_marca"))
            & (col("tv.cod_sabor") == col("cant24.cod_sabor"))
            & (col("tv.cod_formato") == col("cant24.cod_formato"))
            & (col("tv.id_periodo") == col("cant24.id_periodo"))
            & (col("tv.id_pais") == col("cant24.id_pais")),
            "inner",
        )
        .join(
            tmp_manejantes_id_producto_model.alias("ma"),
            (col("tv.cod_marca") == col("ma.cod_marca"))
            & (col("tv.cod_sabor") == col("ma.cod_sabor"))
            & (col("tv.cod_formato") == col("ma.cod_formato")),
            "inner",
        )
        .select(
            col("tv.id_pais"),
            col("tv.id_periodo"),
            col("tv.id_cliente").alias("id_cliente"),
            col("ma.id_producto_modelo").alias("id_producto"),
            col("tv.imp_neto_vta_mn"),
            col("tv.imp_neto_vta_me"),
            col("tv.imp_bruto_vta_mn"),
            col("tv.imp_bruto_vta_me"),
            col("tv.cant_cajafisica_vta"),
            col("tv.cant_cajaunitaria_vta"),
            to_date(col("tv.id_periodo"), "yyyyMM").alias("fecha_liquidacion"),
            when(col("cant24.m0") == 4, "Manejante").when(col("cant24.m0") > 0, "Itinerante").otherwise("Sin compra").alias("m0"),
            when(col("cant24.m1") == 4, "Manejante").when(col("cant24.m1") > 0, "Itinerante").otherwise("Sin compra").alias("m1"),
            when(col("cant24.m2") == 4, "Manejante").when(col("cant24.m2") > 0, "Itinerante").otherwise("Sin compra").alias("m2"),
            when(col("cant24.m3") == 4, "Manejante").when(col("cant24.m3") > 0, "Itinerante").otherwise("Sin compra").alias("m3"),
            when(col("cant24.m4") == 4, "Manejante").when(col("cant24.m4") > 0, "Itinerante").otherwise("Sin compra").alias("m4"),
            when(col("cant24.m5") == 4, "Manejante").when(col("cant24.m5") > 0, "Itinerante").otherwise("Sin compra").alias("m5"),
            when(col("cant24.m6") == 4, "Manejante").when(col("cant24.m6") > 0, "Itinerante").otherwise("Sin compra").alias("m6"),
            when(col("cant24.m7") == 4, "Manejante").when(col("cant24.m7") > 0, "Itinerante").otherwise("Sin compra").alias("m7"),
            when(col("cant24.m8") == 4, "Manejante").when(col("cant24.m8") > 0, "Itinerante").otherwise("Sin compra").alias("m8"),
            when(col("cant24.m9") == 4, "Manejante").when(col("cant24.m9") > 0, "Itinerante").otherwise("Sin compra").alias("m9"),
            when(col("cant24.m10") == 4, "Manejante").when(col("cant24.m10") > 0, "Itinerante").otherwise("Sin compra").alias("m10"),
            when(col("cant24.m11") == 4, "Manejante").when(col("cant24.m11") > 0, "Itinerante").otherwise("Sin compra").alias("m11"),
            when(col("cant24.m12") == 4, "Manejante").when(col("cant24.m12") > 0, "Itinerante").otherwise("Sin compra").alias("m12"),
        )
    )
except Exception as e:
    logger.error(str(e))
    add_log_to_dynamodb('Creando tablas', e)
    #send_error_message(PROCESS_NAME, 'Creando tablas', f"{str(e)[:10000]}")
    exit(1)

# DELETE
try:
    df_fact_venta_manejantes_com = df_fact_venta_manejantes_com.where(
        (col("id_pais") != COD_PAIS) |
        (~col("id_periodo").isin(PERIODOS))
    ).select("*")
except Exception as e:
    logger.error(str(e))
    add_log_to_dynamodb('Eliminando datos', e)
    #send_error_message(PROCESS_NAME, 'Eliminando datos', f"{str(e)[:10000]}")
    exit(1)
# INSERT
try:
    df_fact_venta_manejantes_com = df_fact_venta_manejantes_com.union(tmp_final)
except Exception as e:
    logger.error(str(e))
    add_log_to_dynamodb('Insertando datos', e)
    #send_error_message(PROCESS_NAME, 'Insertando datos', f"{str(e)[:10000]}")
    exit(1)

#CASTEANDO
try:
    df_fact_venta_manejantes_com = df_fact_venta_manejantes_com.withColumn("id_pais", col("id_pais").cast("string")) \
        .withColumn("id_periodo", col("id_periodo").cast("string")) \
        .withColumn("id_cliente", col("id_cliente").cast("string")) \
        .withColumn("id_producto", col("id_producto").cast("string")) \
        .withColumn("imp_neto_vta_mn", col("imp_neto_vta_mn").cast("decimal(18,2)")) \
        .withColumn("imp_neto_vta_me", col("imp_neto_vta_me").cast("decimal(18,2)")) \
        .withColumn("imp_bruto_vta_mn", col("imp_bruto_vta_mn").cast("decimal(18,2)")) \
        .withColumn("imp_bruto_vta_me", col("imp_bruto_vta_me").cast("decimal(18,2)")) \
        .withColumn("cant_cajafisica_vta", col("cant_cajafisica_vta").cast("int")) \
        .withColumn("cant_cajaunitaria_vta", col("cant_cajaunitaria_vta").cast("int")) \
        .withColumn("fecha_liquidacion", col("fecha_liquidacion").cast("date")) \
        .withColumn("m0", col("m0").cast("string")) \
        .withColumn("m1", col("m1").cast("string")) \
        .withColumn("m2", col("m2").cast("string")) \
        .withColumn("m3", col("m3").cast("string")) \
        .withColumn("m4", col("m4").cast("string")) \
        .withColumn("m5", col("m5").cast("string")) \
        .withColumn("m6", col("m6").cast("string")) \
        .withColumn("m7", col("m7").cast("string")) \
        .withColumn("m8", col("m8").cast("string")) \
        .withColumn("m9", col("m9").cast("string")) \
        .withColumn("m10", col("m10").cast("string")) \
        .withColumn("m11", col("m11").cast("string")) \
        .withColumn("m12", col("m12").cast("string"))

except Exception as e:
    logger.error(str(e))
    add_log_to_dynamodb('Casteando datos', e)
    exit(1)

# SAVE
try:
    table_name = "fact_venta_manejantes"
    df = df_fact_venta_manejantes_com

    s3_path_dom = f"{S3_PATH_COM}/{table_name}"

    partition_columns_array = ["id_pais", "id_periodo"]

    df.write.partitionBy(*partition_columns_array).format("delta").option("overwriteSchema", "true").mode("overwrite").option(
        "mergeSchema", "true"
    ).option("partitionOverwriteMode", "dynamic").save(s3_path_dom)

    delta_table = DeltaTable.forPath(spark, s3_path_dom)
    delta_table.generate("symlink_format_manifest")
except Exception as e:
    logger.error(str(e))
    add_log_to_dynamodb('Guardando tablas', e)
    #send_error_message(PROCESS_NAME, 'Guardando tablas', f"{str(e)[:10000]}")
    exit(1)
