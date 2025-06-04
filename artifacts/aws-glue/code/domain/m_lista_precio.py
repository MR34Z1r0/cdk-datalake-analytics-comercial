import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, concat, lit, coalesce, when, trim, row_number,current_date,upper
from pyspark.sql.types import StringType, DateType

spark_controller = SPARK_CONTROLLER()
target_table_name = "m_lista_precio"  
try: 
    df_m_lista_precio = spark_controller.read_table(data_paths.BIGMAGIC, "m_lista_precio")
    df_m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais",have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania")
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("Starting creation of df_dom_m_lista_precio")
    df_dom_m_lista_precio = (
        df_m_lista_precio.alias("mlp")
        .join(
            df_m_compania.alias("mc"),
            col("mlp.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(
            df_m_pais.alias("mp"), 
            (col("mc.cod_pais") == col("mp.cod_pais")) & (col("mc.id_pais") == col("mp.id_pais")), 
            "inner"
        )
        .select(
            concat(
                trim(col("mlp.cod_compania")),
                lit("|"),
                trim(col("mlp.cod_lista_precio")),
            ).cast(StringType()).alias("id_lista_precio"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            trim(col("mlp.cod_lista_precio")).cast(StringType()).alias("cod_lista_precio"),
            col("mlp.desc_lista_precio").cast(StringType()).alias("nomb_lista_precio"),
            current_date().cast(DateType()).alias("fecha_creacion"),
            current_date().cast(DateType()).alias("fecha_modificacion"),
        )
    ) 

    id_columns = ["id_lista_precio"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dom_m_lista_precio, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert of {target_table_name} finished")

except Exception as e:
    logger.error(f"Error processing df_dom_m_lista_precio: {e}")
    raise ValueError(f"Error processing df_dom_m_lista_precio: {e}")