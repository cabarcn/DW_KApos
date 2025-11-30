from utils import get_connection
import pandas as pd

def load_dim_tiempo(conn):
    cur = conn.cursor()
    print("🕒 Cargando dw.dim_tiempo...")

    cur.execute("DELETE FROM dw.dim_tiempo;")

    sql = """
    INSERT INTO dw.dim_tiempo (
        anio, trimestre, fecha_inicio_trimestre, fecha_fin_trimestre,
        nombre_trimestre, es_primer_trimestre, es_ultimo_trimestre
    )
    SELECT
        anio,
        trimestre,
        make_date(anio, (trimestre - 1) * 3 + 1, 1) AS fecha_inicio,
        (make_date(anio, (trimestre - 1) * 3 + 1, 1) + INTERVAL '3 month - 1 day')::date AS fecha_fin,
        CONCAT('T', trimestre, ' ', anio) AS nombre_trimestre,
        (trimestre = 1) AS es_primer_trimestre,
        (trimestre = 4) AS es_ultimo_trimestre
    FROM (
        SELECT DISTINCT
            EXTRACT(YEAR FROM fecha_pago)::int AS anio,
            ((EXTRACT(MONTH FROM fecha_pago)::int - 1) / 3 + 1) AS trimestre
        FROM stg.stg_payment
        WHERE fecha_pago IS NOT NULL
    ) t
    ORDER BY anio, trimestre;
    """
    cur.execute(sql)
    conn.commit()


def load_dimensions(conn):
    cur = conn.cursor()
    print("📚 Cargando dimensiones...")

    # Limpiar
    cur.execute("DELETE FROM dw.dim_organizacion;")
    cur.execute("DELETE FROM dw.dim_plan;")
    cur.execute("DELETE FROM dw.dim_metodo_pago;")
    cur.execute("DELETE FROM dw.dim_psp;")
    cur.execute("DELETE FROM dw.dim_socio;")
    cur.execute("DELETE FROM dw.dim_captador;")

    # Organizacion
    cur.execute("""
        INSERT INTO dw.dim_organizacion (
            organizacion_id_origen, nombre_org, rut, region, segmento, fecha_ingreso, activo
        )
        SELECT
            organizacion_id, nombre_org, rut, region, segmento, fecha_ingreso, TRUE
        FROM stg.stg_organizacion;
    """)

    # Plan
    cur.execute("""
        INSERT INTO dw.dim_plan (
            plan_id_origen, nombre_plan, monto_mensual, periodicidad, tipo_plan,
            fecha_inicio, fecha_fin, activo
        )
        SELECT
            plan_id, nombre_plan, monto_mensual, periodicidad, tipo_plan,
            NULL::date, NULL::date, TRUE
        FROM stg.stg_plan;
    """)

    # Método de pago
    cur.execute("""
        INSERT INTO dw.dim_metodo_pago (
            metodopago_id_origen, tipo_metodo, entidad_bancaria, categoria
        )
        SELECT
            metodopago_id, tipo_metodo, entidad_bancaria, categoria
        FROM stg.stg_metodo_pago;
    """)

    # PSP
    cur.execute("""
        INSERT INTO dw.dim_psp (
            psp_id_origen, nombre_psp, pais, tipo_conciliacion, activo
        )
        SELECT
            psp_id, nombre_psp, pais, tipo_conciliacion, TRUE
        FROM stg.stg_psp;
    """)

    # Socio (SCD2 simple, sin cambios históricos todavía)
    cur.execute("""
        INSERT INTO dw.dim_socio (
            socio_id_origen, rut_hash, genero, edad, comuna,
            fecha_ingreso, estado, fecha_inicio_vigencia, fecha_fin_vigencia, is_current
        )
        SELECT
            socio_id,
            md5(COALESCE(rut, '')),
            genero,
            edad,
            comuna,
            fecha_ingreso,
            estado,
            COALESCE(fecha_ingreso, CURRENT_DATE),
            NULL,
            TRUE
        FROM stg.stg_socio;
    """)

    # Captador
    cur.execute("""
        INSERT INTO dw.dim_captador (
            captador_id_origen, nombre_captador, canal,
            tipo_captacion, campania, fecha_inicio_campania
        )
        SELECT
            captador_id, nombre_captador, canal,
            tipo_captacion, campania, fecha_inicio_campania
        FROM stg.stg_captador;
    """)

    conn.commit()


def load_fact(conn):
    cur = conn.cursor()
    print("📊 Cargando dw.fact_ciclo_recaudacion...")

    cur.execute("DELETE FROM dw.fact_ciclo_recaudacion;")

    sql = """
    WITH pagos AS (
        SELECT
            s.subscription_id,
            s.socio_id,
            s.plan_id,
            s.organizacion_id,
            s.metodopago_id,
            s.psp_id,
            s.captador_id,
            p.fecha_pago,
            p.monto_intentado,
            p.monto_pagado,
            p.estado_pago,
            COALESCE(se.monto_abonado, 0) AS monto_abonado,
            se.dias_hasta_abono
        FROM stg.stg_subscription s
        LEFT JOIN stg.stg_payment p
            ON p.subscription_id = s.subscription_id
        LEFT JOIN stg.stg_settlement se
            ON se.payment_id = p.payment_id
    ),
    agg AS (
        SELECT
            subscription_id,
            EXTRACT(YEAR FROM fecha_pago)::int AS anio,
            ((EXTRACT(MONTH FROM fecha_pago)::int - 1) / 3 + 1) AS trimestre,
            MIN(fecha_pago) AS primera_fecha_pago,
            SUM(monto_intentado) AS monto_intentado,
            SUM(monto_pagado) AS monto_cobrado_ok,
            SUM(monto_abonado) AS monto_abonado,
            COUNT(*) AS intentos_cobro,
            CASE WHEN SUM(CASE WHEN estado_pago = 'OK' THEN 1 ELSE 0 END) > 0
                 THEN TRUE ELSE FALSE END AS exito_trimestre,
            AVG(dias_hasta_abono) AS dias_hasta_abono_promedio,
            CASE WHEN COUNT(*) > 0
                 THEN SUM(monto_pagado)::numeric / COUNT(*)
                 ELSE 0 END AS ticket_promedio
        FROM pagos
        WHERE fecha_pago IS NOT NULL
        GROUP BY subscription_id, anio, trimestre
    )
    INSERT INTO dw.fact_ciclo_recaudacion (
        organizacion_key, tiempo_key, plan_key, metodopago_key, psp_key,
        socio_key, captador_key,
        subscription_id_origen, anio, trimestre,
        monto_intentado, monto_cobrado_ok, monto_abonado,
        intentos_cobro, exito_trimestre, dias_hasta_abono_promedio,
        ticket_promedio, retencion_trimestral, churn_trimestral,
        antiguedad_suscripcion_trimestres
    )
    SELECT
        dorg.organizacion_key,
        dt.tiempo_key,
        dpl.plan_key,
        dmp.metodopago_key,
        dpsp.psp_key,
        ds.socio_key,
        dcap.captador_key,
        a.subscription_id,
        a.anio,
        a.trimestre,
        a.monto_intentado,
        a.monto_cobrado_ok,
        a.monto_abonado,
        a.intentos_cobro,
        a.exito_trimestre,
        a.dias_hasta_abono_promedio,
        a.ticket_promedio,
        1.0 AS retencion_trimestral,   -- placeholder: 100% retenido en trimestre
        0.0 AS churn_trimestral,       -- placeholder: 0% baja en trimestre
        1    AS antiguedad_suscripcion_trimestres
    FROM agg a
    JOIN stg.stg_subscription s
        ON s.subscription_id = a.subscription_id
    JOIN dw.dim_organizacion dorg
        ON dorg.organizacion_id_origen = s.organizacion_id
    JOIN dw.dim_plan dpl
        ON dpl.plan_id_origen = s.plan_id
    JOIN dw.dim_metodo_pago dmp
        ON dmp.metodopago_id_origen = s.metodopago_id
    JOIN dw.dim_psp dpsp
        ON dpsp.psp_id_origen = s.psp_id
    JOIN dw.dim_socio ds
        ON ds.socio_id_origen = s.socio_id
       AND ds.is_current = TRUE
    LEFT JOIN dw.dim_captador dcap
        ON dcap.captador_id_origen = s.captador_id
    JOIN dw.dim_tiempo dt
        ON dt.anio = a.anio
       AND dt.trimestre = a.trimestre;
    """
    cur.execute(sql)
    conn.commit()


def run_load():
    conn = get_connection()
    cur = conn.cursor()

    fecha_inicio = pd.Timestamp.now()
    try:
        load_dim_tiempo(conn)
        load_dimensions(conn)
        load_fact(conn)

        # Log OK
        cur.execute("""
            INSERT INTO meta.log_etl (proceso, estado, fecha_inicio, fecha_fin, mensaje)
            VALUES (%s, %s, %s, NOW(), %s)
        """, (
            "load",
            "OK",
            fecha_inicio,
            "Carga de dimensiones y fact realizada"
        ))
        conn.commit()

        print("✅ Carga a DW completada.")
    except Exception as e:
        conn.rollback()
        cur.execute("""
            INSERT INTO meta.log_etl (proceso, estado, fecha_inicio, fecha_fin, mensaje)
            VALUES (%s, %s, %s, NOW(), %s)
        """, (
            "load",
            "ERROR",
            fecha_inicio,
            str(e)
        ))
        conn.commit()
        raise e
    finally:
        conn.close()

