import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat, lit, coalesce, when
from pyspark.sql.types import StringType

spark_controller = SPARK_CONTROLLER()
target_table_name = "conf_origen" 
try:
    df_conf_origen =  spark_controller.read_table(data_paths.ARTIFACTS_CSV, "conf_origen.csv")

    spark_controller.write_table(df_conf_origen, data_paths.DOMINIO, target_table_name)
except Exception as e:
    logger.error(e)
    raise