import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat, lit, coalesce, when , current_date, trim
from pyspark.sql.types import StringType, IntegerType, DecimalType, TimestampType,DateType

spark_controller = SPARK_CONTROLLER() 
try: 
    cod_pais = COD_PAIS.split(",") 
    
    df_m_vendedor = spark_controller.read_table(data_paths.BIG_MAGIC, "m_vendedor", cod_pais=cod_pais)
    df_m_persona = spark_controller.read_table(data_paths.BIG_MAGIC, "m_persona", cod_pais=cod_pais)
    df_m_pais = spark_controller.read_table(data_paths.BIG_MAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.BIG_MAGIC, "m_compania", cod_pais=cod_pais)

    #df_vw_vendedor_c =""
    #df_vw_user=""
    
    df_conf_origen = spark_controller.read_table(data_paths.DOMINIO, "conf_origen")
    
    target_table_name = "m_responsable_comercial"
except Exception as e:
    logger.error(str(e))
    raise 
try:
    tmp_m_vendedor_bm = (
        df_m_vendedor.alias("mv")
        .join(
            df_m_persona.alias("mpe"),
            (col("mv.cod_vendedor") == col("mpe.cod_persona")) & (col("mv.cod_compania") == col("mpe.cod_compania")),
            "inner",
        )
        .join(
            df_m_compania.alias("mc"),
            col("mv.cod_compania") == col("mc.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mc.cod_pais") == col("mp.cod_pais"), "inner")
        .join(df_conf_origen.alias("co"), (col("co.id_pais") == col("mp.id_pais"))
              & (col("co.nombre_tabla") == "m_responsable_comercial")
              & (col("co.nombre_origen") == "bigmagic"),
              "inner",
        )
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            concat(
                trim(col("mv.cod_compania")), lit("|"), trim(col("mv.cod_vendedor"))
            ).cast(StringType()).alias("id_responsable_comercial"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            trim(col("mv.cod_vendedor")).cast(StringType()).alias("cod_responsable_comercial"),
            col("mpe.nomb_persona").cast(StringType()).alias("nomb_responsable_comercial"),
            col("mv.cod_tipo_vendedor").cast(StringType()).alias("cod_tipo_responsable_comercial"),
            lit(None).cast(StringType()).alias("estado"), 
            current_date().cast(DateType()).alias("fecha_creacion"),
            current_date().cast(DateType()).alias("fecha_modificacion")
        )
    )

    # 2) REGISTROS DE SALESFORCE
    # =============================================================================
    # tmp_m_responsable_comercial_salesforce = (
    #     df_vw_vendedor_c.alias("vc")
    #     .join(
    #         df_vw_user.alias("u"),
    #         col("vc.vendedor__c") == col("u.id"),
    #         "inner"
    #     )
    #     .join(
    #         df_conf_origen.alias("co"),
    #         (col("co.id_pais") == col("vc.pais__c"))
    #         & (col("co.nombre_tabla") == "m_responsable_comercial")
    #         & (col("co.nombre_origen") == "salesforce"),
    #         "inner"
    #     )
    #     .where(col("vc.pais__c").isin(cod_pais))
    #     .select(
    #         col("u.id").cast(StringType()).alias("id_responsable_comercial"),
    #         col("vc.pais__c").cast(StringType()).alias("id_pais"),
    #         col("u.codigo_unico__c").cast(StringType()).alias("cod_responsable_comercial"),
    #         col("u.name").cast(StringType()).alias("nomb_responsable_comercial"),
    #         lit("001").cast(StringType()).alias("cod_tipo_responsable_comercial"),
    #         lit("A").cast(StringType()).alias("estado"),
    #         current_date().cast(DateType()).alias("fecha_creacion"),
    #         current_date().cast(DateType()).alias("fecha_modificacion"),
    #     )
    #     .dropDuplicates([  # Emula DISTINCT. Ajusta las columnas clave si hace falta
    #         "id_responsable_comercial", 
    #         "cod_responsable_comercial", 
    #         "id_pais"
    #     ])
    # )

    # =============================================================================
    # 3) UNIÓN DE AMBAS FUENTES
    # =============================================================================
    # tmp_m_responsable_comercial = tmp_m_vendedor_bm.unionByName(
    #     tmp_m_responsable_comercial_salesforce, allowMissingColumns=True
    # )

    id_columns = ["id_responsable_comercial"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(tmp_m_vendedor_bm, data_paths.DOMINIO, target_table_name, id_columns, partition_columns_array) 
except Exception as e:
    logger.error(str(e))
    raise