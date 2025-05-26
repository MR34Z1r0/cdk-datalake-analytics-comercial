import sys
from aje.get_schemas import *
from delta.tables import DeltaTable
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, SQLContext

######################################
# JOB PARAMETERS

args = getResolvedOptions(
    sys.argv,
    [
        "S3_PATH_COM",
    ],
)

S3_PATH_COM = args["S3_PATH_COM"]

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

######################################
# CREATE TABLE

data = [
    ("PE","500"),
    ("PE","530"),
    ("PE","535"),
    ("BO","500"),
    ("BO","530"),
    ("BO","535"),
    ("EC","500"),
    ("EC","530"),
    ("EC","535"),
    ("MX","500"),
    ("MX","530"),
    ("MX","535"),
    ("HN","500"),
    ("HN","530"),
    ("HN","535"),
    ("PA","500"),
    ("PA","530"),
    ("PA","535"),
    ("CR","500"),
    ("CR","530"),
    ("CR","535"),
    ("NI","500"),
    ("NI","530"),
    ("NI","535"),
    ("GT","500"),
    ("GT","530"),
    ("GT","535"),
    ("SV","500"),
    ("SV","530"),
    ("SV","535")
]

df_conf_marcas = spark.createDataFrame(
    data, ["cod_pais", "cod_marca"]
)

# SAVE
table_name = "conf_marcas"
df = df_conf_marcas

s3_path_dom = f"{S3_PATH_COM}/{table_name}"
df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").option(
    "mergeSchema", "true"
).option("partitionOverwriteMode", "dynamic").save(s3_path_dom)
delta_table = DeltaTable.forPath(spark, s3_path_dom)
delta_table.generate("symlink_format_manifest")
