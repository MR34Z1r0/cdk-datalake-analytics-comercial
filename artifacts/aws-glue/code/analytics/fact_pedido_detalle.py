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
from pyspark.sql.functions import (
    col,
    lit,
    upper,
    to_date,
    date_format,
    sum,
    max
)
import logging
import os
import datetime as dt
import pytz
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


######################################
# READ
try:
    df_t_pedido_detalle = read_table("t_pedido_detalle", S3_PATH_DOM)
    df_t_pedido = read_table("t_pedido", S3_PATH_DOM)
    
    df_fact_pedido_detalle_com = get_table("fact_pedido_detalle", S3_PATH_COM).where(
        (col('id_pais').isin(cod_pais))
            & (col('id_periodo').isin(PERIODOS))
    )
except Exception as e:
    logger.error(str(e))
    add_log_to_dynamodb('Leyendo tablas fuente', e)
    #send_error_message(PROCESS_NAME, 'Leyendo tablas fuente', f"{str(e)[:10000]}")
    exit(1)
# CREATE
try:
    tmp_pre_fact_pedido_detalle = (
        df_t_pedido.alias("tp")
        .join(
            df_t_pedido_detalle.alias("tpd"),
            (col("tp.id_pedido") == col("tpd.id_pedido")),
            "inner",
        )
        .where(
            (col('tp.id_pais').isin(cod_pais))
            & (col('tp.id_periodo').isin(PERIODOS))
        )
        .select(
            col("tp.id_pais"),
            col("tp.id_periodo"),
            col("tp.id_pedido"),
            col("tpd.id_articulo"),
            col("tp.id_sucursal"),
            col("tp.id_visita"),
            col("tp.id_cliente"),
            col("tp.id_modelo_atencion"),
            col("tp.id_origen_pedido"),
            col("tp.id_tipo_pedido"),
            col("tp.id_fuerza_venta"),
            col("tp.id_vendedor"),
            col("tp.id_supervisor"),
            col("tp.id_jefe_venta"),
            col("tp.id_lista_precio"),
            col("tp.id_forma_pago"),
            col("tp.desc_region"),
            col("tp.desc_subregion"),
            col("tp.desc_division"),
            col("tp.desc_estado"),
            col("tp.cod_zona"),
            col("tp.cod_ruta"),
            col("tp.cod_modulo"),
            col("tp.cod_pedido_salesforce"),
            col("tp.cod_pedido_sugerido"),
            col("tp.es_pedido_sugerido"),
            col("tp.es_prospecto"),
            col("tp.es_extramodulo"),
            col("tpd.es_eliminado"),
            col("tp.fecha_pedido"),
            col("tp.fecha_entrega"),
            col("tp.fecha_visita"),
            col("tp.nro_pedido"),
            col("tp.cod_motivo_no_pedido"),
            col("tp.fecha_no_pedido"),
            col("tpd.cant_cajafisica_ped"),
            col("tpd.cant_cajavolumen_ped"),
            col("tpd.cant_cajafisica_ped_pro"),
            col("tpd.cant_cajavolumen_ped_pro"),
            col("tpd.cant_cajafisica_asignado_ped"),
            col("tpd.cant_cajavolumen_asignado_ped"),
            col("tpd.cant_cajafisica_asignado_ped_pro"),
            col("tpd.cant_cajavolumen_asignado_ped_pro"),
            col("tpd.imp_bruto_ped_mn"),
            col("tpd.imp_bruto_ped_me"),
            col("tpd.imp_dscto_ped_mn"),
            col("tpd.imp_dscto_ped_me"),
            col("tpd.imp_neto_ped_mn"),
            col("tpd.imp_neto_ped_me"),
            lit(None).alias('fuente') #NOTA -> ESTO DEBE CAMBIAR CUANDO ACTUALICE T_PEDIDO Y T_PEDIDO_DETALLE
            # EL CODIGO CORRECTO ES ASI tp.fuente as fuente
        )
    )
except Exception as e:
    logger.error(str(e))
    add_log_to_dynamodb('Creando tablas', e)
    #send_error_message(PROCESS_NAME, 'Creando tablas', f"{str(e)[:10000]}")
    exit(1)
# DELETE
try:
    df_fact_pedido_detalle_com = df_fact_pedido_detalle_com.where(
        (~col("id_pais") != COD_PAIS) |
        (~col("id_periodo").isin(PERIODOS))
    ).select("*")
except Exception as e:
    logger.error(str(e))
    add_log_to_dynamodb('Eliminando datos', e)
    #send_error_message(PROCESS_NAME, 'Eliminando datos', f"{str(e)[:10000]}")
    exit(1)
# INSERT
try:
    df_fact_pedido_detalle_com = df_fact_pedido_detalle_com.union(tmp_pre_fact_pedido_detalle)
except Exception as e:
    logger.error(str(e))
    add_log_to_dynamodb('Insertando datos', e)
    #send_error_message(PROCESS_NAME, 'Insertando datos', f"{str(e)[:10000]}")
    exit(1)
# SAVE
try:
    table_name = "fact_pedido_detalle"
    df = df_fact_pedido_detalle_com
    
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