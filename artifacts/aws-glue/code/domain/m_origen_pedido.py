import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, concat, lit, coalesce, when, trim, row_number,current_date,upper,split
from pyspark.sql.types import StringType, DateType

spark_controller = SPARK_CONTROLLER() 
target_table_name = "m_origen_pedido"
try: 
    
    df_m_origen_pedido = spark_controller.read_table(data_paths.BIGMAGIC, "m_origen_pedido")
    df_m_pais = spark_controller.read_table(data_paths.BIGMAGIC, "m_pais",have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.BIGMAGIC, "m_compania") 

except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")
try:
    logger.info("Starting creation of df_m_origen_pedido")

    df_dom_m_origen_pedido = (
        df_m_origen_pedido.alias("mtdp")
        .join(df_m_compania.alias("mc"),
            col("mtdp.cod_compania") == col("mc.cod_compania"),"inner")
        .join(df_m_pais.alias("mp"), 
              col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .select(
            col("mtdp.id_origen_pedido").cast(StringType()).alias("id_origen_pedido"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            trim(col("mtdp.cod_origen_pedido")).cast(StringType()).alias("cod_origen_pedido"),
            col("mtdp.desc_origen_pedido").cast(StringType()).alias("nomb_origen_pedido"),
            current_date().cast(DateType()).alias("fecha_creacion"),
            current_date().cast(DateType()).alias("fecha_modificacion")
        )
    )

    id_columns = ["id_origen_pedido"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dom_m_origen_pedido, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert of {target_table_name} finished")
except Exception as e:
    logger.error(f"Error processing df_dom_m_origen_pedido: {e}")
    raise ValueError(f"Error processing df_dom_m_origen_pedido: {e}")