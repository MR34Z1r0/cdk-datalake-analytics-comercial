import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, lit, when, concat, trim, row_number, lower, coalesce, cast, countDistinct,datediff, current_date, to_date, add_months, sum, max
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "fact_cliente_venta"
try:
    PERIODOS= spark_controller.get_periods()
    cod_pais = COD_PAIS.split(",")
    logger.info(f"Databases: {cod_pais}")

    df_t_venta = spark_controller.read_table(data_paths.DOMINIO, "t_venta", cod_pais=cod_pais)
    df_t_venta_detalle = spark_controller.read_table(data_paths.DOMINIO, "t_venta_detalle", cod_pais=cod_pais)
    df_m_tipo_venta = spark_controller.read_table(data_paths.DOMINIO, "m_tipo_venta", cod_pais=cod_pais)
            
    df_dim_cliente = spark_controller.read_table(data_paths.COMERCIAL, "dim_cliente", cod_pais=cod_pais)
    df_dim_producto = spark_controller.read_table(data_paths.COMERCIAL, "dim_producto", cod_pais=cod_pais)

    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(e)
    raise
try:
    logger.info("starting filter of pais and periodo") 
    df_t_venta = df_t_venta.filter(col("id_periodo").isin(PERIODOS))
    df_t_venta_detalle = df_t_venta_detalle.filter(col("id_periodo").isin(PERIODOS))

    tmp_aux_cantidades_general = (
        df_t_venta.alias("tv")
        .join(
            df_t_venta_detalle.alias("tvd"),
            (col("tv.id_venta") == col("tvd.id_venta")) 
            & (col("tv.es_eliminado") == 0),
            "inner",
        )
        .join(
            df_dim_producto.alias("dp"),
            (col("tvd.id_producto") == col("dp.id_producto")),
            "left",
        )
        .groupby(
            col("tv.id_pais"),
            col("tv.id_periodo"),
            col("tv.id_compania"),
            col("tv.id_cliente"),
            col("tv.cod_modulo")
        )
        .agg(
            sum(col("tvd.cant_caja_fisica_ven")).alias("cant_caja_fisica_ven"),
            (sum(col("tvd.cant_caja_volumen_ven")) / 30).alias("cant_caja_unitaria_ven"),
            sum(col("tvd.cant_caja_fisica_pro")).alias("cant_caja_fisica_pro"),
            (sum(col("tvd.cant_caja_volumen_pro")) / 30).alias("cant_caja_unitaria_pro"),
            sum(col("tvd.imp_neto_vta_mn")).alias("imp_neto_mn"),
            sum(col("tvd.imp_neto_vta_me")).alias("imp_neto_me"),
            sum(col("tvd.imp_cobrar_vta_mn")).alias("imp_bruto_mn"),
            sum(col("tvd.imp_cobrar_vta_me")).alias("imp_bruto_me"),
            countDistinct(col("tvd.id_producto")).alias("cant_producto"),
            countDistinct(col("dp.desc_marca")).alias("cant_marca"),
            countDistinct(col("tv.id_venta")).alias("cant_venta"),
            max(col("tv.fecha_liquidacion")).alias("ult_fecha_compra_cliente"),
            datediff(current_date(), max("tv.fecha_liquidacion")).alias("ult_dia_compra_cliente")
        )
        .select(
            col("tv.id_pais"),
            col("tv.id_periodo"),
            col("tv.id_compania"),
            col("tv.id_cliente"),
            col("tv.cod_modulo"),
            col("cant_caja_fisica_ven"),
            col("cant_caja_unitaria_ven"),
            col("cant_caja_fisica_pro"),
            col("cant_caja_unitaria_pro"),
            col("imp_neto_mn"),
            col("imp_neto_me"),
            col("imp_bruto_mn"),
            col("imp_bruto_me"),
            col("cant_producto"),
            col("cant_marca"),
            col("cant_venta"),
            col("ult_fecha_compra_cliente"),
            col("ult_dia_compra_cliente")
        )
    )

    # CREATE
    tmp_fact_cliente_venta_metricas_general_3meses = (
        tmp_aux_cantidades_general.alias("current")
        .join(
            tmp_aux_cantidades_general.alias("past"),
            (col("current.id_pais") == col("past.id_pais"))
            & (to_date(col("past.id_periodo"), "yyyyMM").between(add_months(to_date(col("current.id_periodo"), "yyyyMM"), -3), to_date(col("current.id_periodo"), "yyyyMM")))
            & (col("past.id_cliente") == col("current.id_cliente")),
            "left",
        )
        .groupby(
            col("current.id_pais"),
            col("current.id_periodo"),
            col("current.id_compania"),
            col("current.id_cliente"),
            col("current.cod_modulo")
        )
        .agg(
            coalesce(sum(col("past.cant_caja_fisica_ven")), lit(0)).alias("cant_caja_fisica_ven_3meses"),
            coalesce(sum(col("past.cant_caja_unitaria_ven")), lit(0)).alias("cant_caja_unitaria_ven_3meses"),
            coalesce(sum(col("past.cant_caja_fisica_pro")), lit(0)).alias("cant_caja_fisica_pro_3meses"),
            coalesce(sum(col("past.cant_caja_unitaria_pro")), lit(0)).alias("cant_caja_unitaria_pro_3meses"),
            coalesce(sum(col("past.imp_neto_mn")), lit(0)).alias("imp_neto_mn_3meses"),
            coalesce(sum(col("past.imp_neto_me")), lit(0)).alias("imp_neto_me_3meses"),
            coalesce(sum(col("past.imp_bruto_mn")), lit(0)).alias("imp_bruto_mn_3meses"),
            coalesce(sum(col("past.imp_bruto_me")), lit(0)).alias("imp_bruto_me_3meses")
        )
        .select(
            col("current.id_pais"),
            col("current.id_periodo"),
            col("current.id_compania"),
            col("current.id_cliente"),
            col("current.cod_modulo"),
            col("cant_caja_fisica_ven_3meses"),
            col("cant_caja_unitaria_ven_3meses"),
            col("cant_caja_fisica_pro_3meses"),
            col("cant_caja_unitaria_pro_3meses"),
            col("imp_neto_mn_3meses"),
            col("imp_neto_me_3meses"),
            col("imp_bruto_mn_3meses"),
            col("imp_bruto_me_3meses")
        )
    )

    # CREATE
    tmp_fact_cliente_venta_metricas_general_12meses = (
        tmp_aux_cantidades_general.alias("current")
        .join(
            tmp_aux_cantidades_general.alias("past"),
            (col("current.id_pais") == col("past.id_pais"))
            & (to_date(col("past.id_periodo"), "yyyyMM").between(add_months(to_date(col("current.id_periodo"), "yyyyMM"), -12), to_date(col("current.id_periodo"), "yyyyMM")))
            & (col("past.id_cliente") == col("current.id_cliente")),
            "left",
        )
        .groupby(
            col("current.id_pais"),
            col("current.id_periodo"),
            col("current.id_compania"),
            col("current.id_cliente"),
            col("current.cod_modulo")
        )
        .agg(
            coalesce(sum(col("past.cant_caja_fisica_ven")), lit(0)).alias("cant_caja_fisica_ven_12meses"),
            coalesce(sum(col("past.cant_caja_unitaria_ven")), lit(0)).alias("cant_caja_unitaria_ven_12meses"),
            coalesce(sum(col("past.cant_caja_fisica_pro")), lit(0)).alias("cant_caja_fisica_pro_12meses"),
            coalesce(sum(col("past.cant_caja_unitaria_pro")), lit(0)).alias("cant_caja_unitaria_pro_12meses"),
            coalesce(sum(col("past.imp_neto_mn")), lit(0)).alias("imp_neto_mn_12meses"),
            coalesce(sum(col("past.imp_neto_me")), lit(0)).alias("imp_neto_me_12meses"),
            coalesce(sum(col("past.imp_bruto_mn")), lit(0)).alias("imp_bruto_mn_12meses"),
            coalesce(sum(col("past.imp_bruto_me")), lit(0)).alias("imp_bruto_me_12meses")
        )
        .select(
            col("current.id_pais"),
            col("current.id_periodo"),
            col("current.id_compania"),
            col("current.id_cliente"),
            col("current.cod_modulo"),
            col("cant_caja_fisica_ven_12meses"),
            col("cant_caja_unitaria_ven_12meses"),
            col("cant_caja_fisica_pro_12meses"),
            col("cant_caja_unitaria_pro_12meses"),
            col("imp_neto_mn_12meses"),
            col("imp_neto_me_12meses"),
            col("imp_bruto_mn_12meses"),
            col("imp_bruto_me_12meses")
        )
    )

    # CREATE
    tmp_aux_cantidades_general_nn = (
        df_t_venta.alias("tv")
        .join(
            df_t_venta_detalle.alias("tvd"),
            (col("tv.id_venta") == col("tvd.id_venta"))
            & (col("tv.id_pais") == col("tvd.id_pais"))
            & (col("tvd.es_eliminado") == 0),
            "inner",
        )
        .join(
            df_dim_producto.alias("dp"),
            (col("tv.id_pais") == col("dp.id_pais"))
            & (col("tvd.id_producto") == col("dp.id_producto")),
            "left",
        )
        .where(
            (col("tv.id_periodo").isin(PERIODOS)) &
                (col("tv.id_pais").isin(cod_pais)) &
                (col("tvd.id_periodo").isin(PERIODOS)) &
                (col("tvd.id_pais").isin(cod_pais)) &
                (col("dp.cod_unidad_negocio") == '003')
        )
        .groupby(col("tv.id_pais"), col("tv.id_periodo"), col("tv.id_compania"), col("tv.id_cliente"), col("tv.cod_modulo"))
        .agg(
            sum(col("tvd.cant_caja_volumen_ven")/30).alias("cant_caja_unitaria_ven"),
            sum(col("tvd.imp_neto_vta_mn")).alias("imp_neto_mn"),
            countDistinct(col("dp.desc_marca")).alias("cant_marca"),
            countDistinct(col("tv.id_venta")).alias("cant_venta"),
            max(col("tv.fecha_liquidacion")).alias("ult_fecha_compra_cliente")
        )
        .select(
            col("tv.id_pais"),
            col("tv.id_periodo"),
            col("tv.id_compania"),
            col("tv.id_cliente"),
            col("tv.cod_modulo"),
            col("cant_caja_unitaria_ven"),
            col("imp_neto_mn"),
            col("cant_marca"),
            col("cant_venta"),
            col("ult_fecha_compra_cliente"),
        )
    )

    # CREATE
    tmp_fact_cliente_venta_metricas_general_3meses_nn = (
        tmp_aux_cantidades_general_nn.alias("current")
        .join(
            tmp_aux_cantidades_general_nn.alias("past"),
            (col("current.id_pais") == col("past.id_pais"))
            & (to_date(col("past.id_periodo"), "yyyyMM").between(add_months(to_date(col("current.id_periodo"), "yyyyMM"), -3), to_date(col("current.id_periodo"), "yyyyMM")))
            & (col("past.id_cliente") == col("current.id_cliente"))
            & (col("past.id_compania") == col("current.id_compania"))
            & (col("past.cod_modulo") == col("current.cod_modulo")),
            "left",
        )
        .groupby(
            col("current.id_pais"),
            col("current.id_periodo"),
            col("current.id_compania"),
            col("current.id_cliente"),
            col("current.cod_modulo")
        )
        .agg(
            coalesce(sum(col("past.cant_caja_unitaria_ven")), lit(0)).alias("cant_caja_unitaria_ven_3meses"),
            coalesce(sum(col("past.imp_neto_mn")), lit(0)).alias("imp_neto_mn_3meses"),
        )
        .select(
            col("current.id_pais"),
            col("current.id_periodo"),
            col("current.id_compania"),
            col("current.id_cliente"),
            col("current.cod_modulo"),
            col("cant_caja_unitaria_ven_3meses"),
            col("imp_neto_mn_3meses")
        )
    )

    # CREATE
    tmp_fact_cliente_venta_metricas_general_12meses_nn = (
        tmp_aux_cantidades_general_nn.alias("current")
        .join(
            tmp_aux_cantidades_general_nn.alias("past"),
            (col("current.id_pais") == col("past.id_pais"))
            & (to_date(col("past.id_periodo"), "yyyyMM").between(add_months(to_date(col("current.id_periodo"), "yyyyMM"), -12), to_date(col("current.id_periodo"), "yyyyMM")))
            & (col("past.id_cliente") == col("current.id_cliente"))
            & (col("past.id_compania") == col("current.id_compania"))
            & (col("past.cod_modulo") == col("current.cod_modulo")),
            "left",
        )
        .groupby(
            col("current.id_pais"),
            col("current.id_periodo"),
            col("current.id_compania"),
            col("current.id_cliente"),
            col("current.cod_modulo")
        )
        .agg(
            coalesce(sum(col("past.cant_caja_unitaria_ven")), lit(0)).alias("cant_caja_unitaria_ven_12meses"),
            coalesce(sum(col("past.imp_neto_mn")), lit(0)).alias("imp_neto_mn_12meses"),
        )
        .select(
            col("current.id_pais"),
            col("current.id_periodo"),
            col("current.id_compania"),
            col("current.id_cliente"),
            col("current.cod_modulo"),
            col("cant_caja_unitaria_ven_12meses"),
            col("imp_neto_mn_12meses")
        )
    )

    df_fact_cliente_venta = (
        tmp_aux_cantidades_general.alias("fcv")
        .join(
            tmp_fact_cliente_venta_metricas_general_3meses.alias("fcv_3"),
            (col("fcv.id_pais") == col("fcv_3.id_pais"))
            & (col("fcv.id_periodo") == col("fcv_3.id_periodo"))
            & (col("fcv.id_cliente") == col("fcv_3.id_cliente"))
            & (col("fcv.cod_modulo") == col("fcv_3.cod_modulo")),
            "left",
        )
        .join(
            tmp_fact_cliente_venta_metricas_general_12meses.alias("fcv_12"),
            (col("fcv.id_pais") == col("fcv_12.id_pais"))
            & (col("fcv.id_periodo") == col("fcv_12.id_periodo"))
            & (col("fcv.id_cliente") == col("fcv_12.id_cliente"))
            & (col("fcv.cod_modulo") == col("fcv_12.cod_modulo")),
            "left",
        )
        .join(
            tmp_aux_cantidades_general_nn.alias("fcv_nn"),
            (col("fcv.id_pais") == col("fcv_nn.id_pais"))
            & (col("fcv.id_periodo") == col("fcv_nn.id_periodo"))
            & (col("fcv.id_cliente") == col("fcv_nn.id_cliente"))
            & (col("fcv.cod_modulo") == col("fcv_nn.cod_modulo")),
            "left",
        )
        .join(
            tmp_fact_cliente_venta_metricas_general_3meses_nn.alias("fcv_3_nn"),
            (col("fcv.id_pais") == col("fcv_3_nn.id_pais"))
            & (col("fcv.id_periodo") == col("fcv_3_nn.id_periodo"))
            & (col("fcv.id_cliente") == col("fcv_3_nn.id_cliente"))
            & (col("fcv.cod_modulo") == col("fcv_3_nn.cod_modulo")),
            "left",
        )
        .join(
            tmp_fact_cliente_venta_metricas_general_12meses_nn.alias("fcv_12_nn"),
            (col("fcv.id_pais") == col("fcv_12_nn.id_pais"))
            & (col("fcv.id_periodo") == col("fcv_12_nn.id_periodo"))
            & (col("fcv.id_cliente") == col("fcv_12_nn.id_cliente"))
            & (col("fcv.cod_modulo") == col("fcv_12_nn.cod_modulo")),
            "left",
        )
        .where(
            (col("fcv.id_pais").isin(cod_pais))
            & (col("fcv.id_periodo").isin(PERIODOS))
        )
        .select(
            col("fcv.id_pais").cast("string"),
            col("fcv.id_periodo").cast("string"),
            col("fcv.id_compania").cast("string"),
            col("fcv.id_cliente").cast("string"),
            col("fcv.cod_modulo").cast("string"),
            col("fcv_3.cant_caja_fisica_ven_3meses").cast("numeric(38,12)"),
            col("fcv_12.cant_caja_fisica_ven_12meses").cast("numeric(38,12)"),
            col("fcv_3.cant_caja_unitaria_ven_3meses").cast("numeric(38,12)"),
            col("fcv_12.cant_caja_unitaria_ven_12meses").cast("numeric(38,12)"),
            col("fcv_3.cant_caja_fisica_pro_3meses").cast("numeric(38,12)"),
            col("fcv_12.cant_caja_fisica_pro_12meses").cast("numeric(38,12)"),
            col("fcv_3.cant_caja_unitaria_pro_3meses").cast("numeric(38,12)"),
            col("fcv_12.cant_caja_unitaria_pro_12meses").cast("numeric(38,12)"),
            col("fcv_3.imp_neto_mn_3meses").cast("numeric(38,12)"),
            col("fcv_12.imp_neto_mn_12meses").cast("numeric(38,12)"),
            col("fcv_3.imp_neto_me_3meses").cast("numeric(38,12)"),
            col("fcv_12.imp_neto_me_12meses").cast("numeric(38,12)"),
            col("fcv_3.imp_bruto_mn_3meses").cast("numeric(38,12)"),
            col("fcv_12.imp_bruto_mn_12meses").cast("numeric(38,12)"),
            col("fcv_3.imp_bruto_me_3meses").cast("numeric(38,12)"),
            col("fcv_12.imp_bruto_me_12meses").cast("numeric(38,12)"),
            col("fcv.cant_producto").cast("int"), 
            col("fcv.cant_venta").cast("int"),
            col("fcv.cant_marca").cast("int"),
            col("fcv.ult_fecha_compra_cliente").cast("date"),
            col("fcv.ult_dia_compra_cliente").cast("int"),
            col("fcv_12_nn.cant_caja_unitaria_ven_12meses").cast("int").alias("cant_caja_unit_venta_12meses_nn"),
            col("fcv_3_nn.cant_caja_unitaria_ven_3meses").cast("int").alias("cant_caja_unit_venta_3meses_nn"),
            col("fcv_12_nn.imp_neto_mn_12meses").cast("numeric(38,12)").alias("imp_neto_mn_12meses_nn"),
            col("fcv_3_nn.imp_neto_mn_3meses").cast("numeric(38,12)").alias("imp_neto_mn_3meses_nn"),
            col("fcv_nn.ult_fecha_compra_cliente").cast("date").alias("ult_fecha_compra_12meses_nn"),
            col("fcv_nn.cant_venta").cast("int").alias("cant_venta_nn"),
            col("fcv_nn.cant_marca").cast("int").alias("cant_marca_nn")
        )
    )
    
    logger.info(f"starting write of {target_table_name}")
    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(df_fact_cliente_venta, data_paths.COMERCIAL, target_table_name, partition_columns_array)
except Exception as e:
    logger.error(e) 
    raise