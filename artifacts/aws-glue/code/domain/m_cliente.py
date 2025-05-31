import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths
from pyspark.sql.functions import col, concat, lit, coalesce, when, split , trim , current_date, max, row_number, lower, to_date
from pyspark.sql.types import StringType, DateType
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER() 
target_table_name = "m_cliente"   
try:
    df_m_cliente = spark_controller.read_table(data_paths.APDAYC, "m_cliente")
    df_m_estructura_cliente = spark_controller.read_table(data_paths.APDAYC, "m_asignacion_modulo")
    df_m_tipo_cliente = spark_controller.read_table(data_paths.APDAYC, "m_tipo_cliente")
    df_m_cuenta_clave = spark_controller.read_table(data_paths.APDAYC, "m_cuenta_clave")
    df_m_canal = spark_controller.read_table(data_paths.APDAYC, "m_canal")
    df_m_giro = spark_controller.read_table(data_paths.APDAYC, "m_giro")
    df_m_compania = spark_controller.read_table(data_paths.APDAYC, "m_compania")
    df_m_pais = spark_controller.read_table(data_paths.APDAYC, "m_pais", have_principal = True)
except Exception as e:
    logger.error(f"Error reading tables: {e}")
    raise ValueError(f"Error reading tables: {e}")  
try:    
    df_tmp_estructura_cliente = (
        df_m_cliente.alias("mcl")
        .join(
            df_m_estructura_cliente.alias("mecl"),
            (col("mcl.cod_compania") == col("mecl.cod_compania"))
            & (col("mcl.cod_cliente") == col("mecl.cod_cliente"))
            & (
                col("mcl.cod_sucursal")
                == when(
                    (col("mcl.cod_sucursal") == "00"), col("mcl.cod_sucursal")
                ).otherwise(col("mecl.cod_sucursal"))
            ),
            "inner",
        )
        .select(
            col("mcl.cod_compania"),
            col("mcl.cod_cliente"),
            col("mecl.coord_x"),
            col("mecl.coord_y"),
            row_number()
            .over(
                Window.partitionBy(
                    "mcl.cod_compania", "mcl.cod_sucursal", "mcl.cod_cliente"
                ).orderBy(col("mecl.cod_fuerza_venta").asc())
            )
            .alias("orden"),
        )
    )
  
    df_dom_m_cliente = (
        df_m_cliente.alias("mc")
        .join(
            df_m_tipo_cliente.alias("tc"),
            (col("mc.cod_compania") == col("tc.cod_compania"))
            & (col("mc.cod_cliente") == col("tc.cod_cliente"))
            & (lower(col("tc.tipo_cliente")).isin(["a", "v", "t"])),
            "left",
        )
        .join(
            df_m_cuenta_clave.alias("cc"),
            (col("mc.cod_compania") == col("cc.cod_compania"))
            & (col("mc.cod_cuenta_clave") == col("cc.cod_cuenta_clave")),
            "left",
        )
        .join(
            df_m_canal.alias("c"),
            (col("c.cod_compania") == col("mc.cod_compania"))
            & (col("c.cod_canal") == col("mc.cod_canal")),
            "left",
        )
        .join(
            df_m_giro.alias("g"),
            (col("g.cod_compania") == col("mc.cod_compania"))
            & (col("g.cod_giro") == col("mc.cod_giro")),
            "left",
        )
        .join(
            df_tmp_estructura_cliente.alias("mecl"),
            (col("mc.cod_compania") == col("mecl.cod_compania"))
            & (col("mc.cod_cliente") == col("mecl.cod_cliente"))
            & (col("mecl.orden") == 1),
            "left",
        )
        .join(
            df_m_compania.alias("mco"),
            (col("mco.cod_compania") == col("mc.cod_compania")),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mco.cod_pais") == col("mp.cod_pais"), "inner")
        .select(
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                trim(col("mc.cod_cliente")),
            ).cast(StringType()).alias("id_cliente"),
            lit(None).cast(StringType()).alias("id_cliente_ref"),
            lit(None).cast(StringType()).alias("id_cliente_ref2"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                trim(col("mc.cod_sucursal")),
            ).cast(StringType()).alias("id_sucursal"),
            when(
                (col("mc.cod_zona_postal").isNull())
                | (col("mc.cod_zona_postal") == ""),
                lit(None),
            )
            .otherwise(
                concat(
                    trim(col("mp.id_pais")),
                    lit("|"),
                    trim(coalesce(col("mc.cod_zona_postal"), lit("0"))),
                )
            )
            .cast(StringType()).alias("id_eje_territorial"),
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                lit("SG"),
                lit("|"),
                trim(col("mc.cod_subgiro")),
            ).cast(StringType()).alias("id_clasificacion_cliente"),
            concat(
                trim(col("mc.cod_compania")),
                lit("|"),
                trim(col("mc.cod_lista_precio")),
            ).cast(StringType()).alias("id_lista_precio"), 
            col("mc.cod_cliente").cast(StringType()).alias("cod_cliente"),
            col("mc.nomb_cliente").cast(StringType()).alias("nomb_cliente"),
            col("cc.cod_cuenta_clave").cast(StringType()).alias("cod_cuenta_clave"),
            col("cc.descripcion").cast(StringType()).alias("nomb_cuenta_clave"),
            lit(None).cast(StringType()).alias("cod_segmento"),
            lit(None).cast(StringType()).alias("desc_subsegmento"),
            lit(None).cast(StringType()).alias("cod_cliente_ref"),
            lit(None).cast(StringType()).alias("cod_cliente_ref2"),
            lit(None).cast(StringType()).alias("cod_cliente_ref3"),
            lit(None).cast(StringType()).alias("cod_cliente_ref4"),
            col("c.desc_canal").cast(StringType()).alias("desc_canal_local"),
            col("g.desc_giro").cast(StringType()).alias("desc_giro_local"),
            col("mc.direccion").cast(StringType()).alias("direccion"),
            col("mc.tipo_documento_identidad").cast(StringType()).alias("tipo_documento"),
            col("mc.nro_documento_identidad").cast(StringType()).alias("nro_documento"),
            coalesce(col("tc.tipo_cliente"), lit("N")).cast(StringType()).alias("cod_tipo_cliente"),
            coalesce(col("mc.cod_cliente_principal"), lit(0)).cast(StringType()).alias(
                "cod_cliente_principal"
            ),
            lit(None).cast(StringType()).alias("cod_cliente_transferencia"),
            col("mecl.coord_x").cast(StringType()).alias("coord_x"),
            col("mecl.coord_y").cast(StringType()).alias("coord_y"),
            lit(None).cast(DateType()).alias("fecha_baja"),
            col("mc.es_activo").cast(StringType()).alias("estado"),
            col("mc.fecha_creacion").cast(TimestampType()).alias("fecha_creacion"),
            col("mc.fecha_modificacion").cast(TimestampType()).alias("fecha_modificacion"),
        )
    )

    id_columns = ["id_cliente"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(df_dom_m_cliente, data_paths.DOMAIN, target_table_name, id_columns, partition_columns_array)
    logger.info(f"Upsert de {target_table_name} completado exitosamente")
except Exception as e:
    logger.error(f"Error processing df_dom_m_cliente: {e}")
    raise ValueError(f"Error processing df_dom_m_cliente: {e}") 