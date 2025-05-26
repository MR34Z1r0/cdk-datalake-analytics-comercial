import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat, lit, coalesce, when, trim, row_number,current_date,upper
from pyspark.sql.types import StringType, DateType
spark_controller = SPARK_CONTROLLER() 
try: 
    cod_pais = COD_PAIS.split(",")
    
    df_m_lista_precio = spark_controller.read_table(data_paths.BIG_BAGIC, "m_lista_precio", cod_pais=cod_pais)
    df_m_pais = spark_controller.read_table(data_paths.BIG_BAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.BIG_BAGIC, "m_compania", cod_pais=cod_pais)
 
    df_conf_origen = spark_controller.read_table(data_paths.DOMINIO, "conf_origen")

    target_table_name = "m_lista_precio" 
except Exception as e:
    logger.error(e)
    raise
try:
    tmp1 = (
        df_m_lista_precio.alias("mlp")
        .join(
            df_m_compania.alias("mc"),
            col("mlp.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .join(
            df_conf_origen.alias("co"),
            (col("co.id_pais") == col("mp.id_pais"))
            & (col("co.nombre_tabla") == "m_lista_precio")
            & (col("co.nombre_origen") == "bigmagic"),
            "inner",
        )
        .where(col("mp.id_pais").isin(cod_pais))
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
    
    tmp_m_lista_precio = tmp1

    id_columns = ["id_lista_precio"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(tmp_m_lista_precio, data_paths.DOMINIO, target_table_name, id_columns, partition_columns_array)


except Exception as e:
    logger.error(str(e))
    raise