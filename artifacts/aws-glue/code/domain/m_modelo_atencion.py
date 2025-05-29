import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat, lit, coalesce, when, trim, row_number,current_date,upper,split
from pyspark.sql.types import StringType, DateType 
spark_controller = SPARK_CONTROLLER() 
try: 
    cod_pais = COD_PAIS.split(",")
 
    df_m_modelo_atencion = spark_controller.read_table(data_paths.BIG_MAGIC, "m_modelo_atencion", cod_pais=cod_pais)
    df_m_pais = spark_controller.read_table(data_paths.BIG_MAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.BIG_MAGIC, "m_compania", cod_pais=cod_pais)
 
    df_conf_origen = spark_controller.read_table(data_paths.DOMAIN, "conf_origen")

    df_modulo__c = spark_controller.read_table(data_paths.SALESFORCE, "m_modulo", cod_pais=cod_pais)
 
    target_table_name = "m_modelo_atencion"


except Exception as e:
    logger.error(e)
    raise 
try:
    table_1 = (
        df_modulo__c.alias("mc")
        .join(
            df_conf_origen.alias("co"),
            (col("co.id_pais") == col("mc.pais__c"))
            & (col("co.nombre_tabla") == "m_modelo_atencion")
            & (col("co.nombre_origen") == "salesforce"),
            "inner",
        )
        .where(col("mc.pais__c").isin(cod_pais))
        .select(
            concat(
                split(col("mc.compania_sucursal__c"), "\\|").getItem(0),
                lit("|"),
                when(upper(col("mc.modelo_de_atencion__c")) == "PRE VENTA", "001")
                .when(upper(col("mc.modelo_de_atencion__c")) == "AUTO VENTA", "002")
                .when(upper(col("mc.modelo_de_atencion__c")) == "ECOMMERCE", "003")
                .when(upper(col("mc.modelo_de_atencion__c")) == "ESPECIALIZADO", "004")
                .when(upper(col("mc.modelo_de_atencion__c")) == "TELEVENTA", "005")
                .otherwise("000"),
            ).alias("id_modelo_atencion"),
            col("mc.pais__c").alias("id_pais"),
            when(upper(col("mc.modelo_de_atencion__c")) == "PRE VENTA", "001")
            .when(upper(col("mc.modelo_de_atencion__c")) == "AUTO VENTA", "002")
            .when(upper(col("mc.modelo_de_atencion__c")) == "ECOMMERCE", "003")
            .when(upper(col("mc.modelo_de_atencion__c")) == "ESPECIALIZADO", "004")
            .when(upper(col("mc.modelo_de_atencion__c")) == "TELEVENTA", "005")
            .otherwise("000")
            .alias("cod_modelo_atencion"),
            col("mc.modelo_de_atencion__c").alias("desc_modelo_atencion"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
        .distinct()
    )

    table_2 = (
        df_m_modelo_atencion.alias("mma")
        .join(
            df_m_compania.alias("mc"),
            (col("mma.id_compania") == col("mc.cod_compania")),
            "inner",
        )
        .join(
            df_m_pais.alias("mp"),
            (col("mc.cod_pais") == col("mp.cod_pais")),
            "inner",
        )
        .join(
            df_conf_origen.alias("co"),
            (col("co.id_pais") == col("mp.id_pais"))
            & (col("co.nombre_tabla") == "m_modelo_atencion")
            & (col("co.nombre_origen") == "bigmagic"),
            "inner",
        )
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            concat(
                trim(col("mma.id_compania")),
                lit("|"),
                trim(col("mma.cod_modelo_atencion").cast("string")),
            ).alias("id_modelo_atencion"),
            col("mp.id_pais").alias("id_pais"),
            col("mma.cod_modelo_atencion"),
            col("mma.desc_modelo_atencion").alias("desc_modelo_atencion"),
            current_date().alias("fecha_creacion"),
            current_date().alias("fecha_modificacion"),
        )
    )

    #tmp_m_modelo_atencion = table_1.union(table_2).distinct()
    tmp_m_modelo_atencion = (
        table_1.union(table_2)
        .distinct()
        .select(
            col("id_modelo_atencion").cast(StringType()),
            col("id_pais").cast(StringType()),
            col("cod_modelo_atencion").cast(StringType()),
            col("desc_modelo_atencion").cast(StringType()),
            col("fecha_creacion").cast(DateType()),
            col("fecha_modificacion").cast(DateType()),
        )
    )
    
    id_columns = ["id_modelo_atencion"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(tmp_m_modelo_atencion, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise