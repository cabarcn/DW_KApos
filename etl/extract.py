import os
import pandas as pd
from utils import get_connection, load_csv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

def load_to_staging(df, table_name, file_name):
    conn = get_connection()
    cur = conn.cursor()

    fecha_inicio = pd.Timestamp.now()
    registros_origen = len(df)

    # Convertir NaN a NULL
    df = df.where(pd.notnull(df), None)

    try:
        # Vaciar tabla
        cur.execute(f"DELETE FROM stg.{table_name};")

        # Preparar inserción
        columns = list(df.columns)
        cols_sql = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO stg.{table_name} ({cols_sql}) VALUES ({placeholders});"

        registros_insertados = 0

        for _, row in df.iterrows():
            values = []
            for v in row.tolist():
                if isinstance(v, float) and pd.isna(v):
                    values.append(None)
                else:
                    values.append(v)

            cur.execute(sql, values)
            registros_insertados += 1

        conn.commit()

        # Registrar OK en meta.log_etl
        cur.execute("""
            INSERT INTO meta.log_etl (proceso, estado, fecha_inicio, fecha_fin,
                                      registros_origen, registros_insertados, mensaje, archivo_fuente)
            VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s)
        """, (
            "extract",
            "OK",
            fecha_inicio,
            registros_insertados,
            registros_origen,
            registros_insertados,
            file_name
        ))

        conn.commit()

    except Exception as e:
        conn.rollback()
        # Registrar ERROR
        cur.execute("""
            INSERT INTO meta.log_etl (proceso, estado, fecha_inicio, fecha_fin,
                                      registros_origen, registros_insertados, mensaje, archivo_fuente)
            VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s)
        """, (
            "extract",
            "ERROR",
            fecha_inicio,
            0,
            registros_origen,
            0,
            str(e),
            file_name
        ))
        conn.commit()
        raise e

    finally:
        cur.close()
        conn.close()


def run_extract():
    files = {
        "stg_socio": "socios.csv",
        "stg_subscription": "subscriptions.csv",
        "stg_payment": "payments.csv",
        "stg_settlement": "settlements.csv",
        "stg_plan": "planes.csv",
        "stg_organizacion": "organizaciones.csv",
        "stg_metodo_pago": "metodos_pago.csv",
        "stg_psp": "psp.csv",
        "stg_captador": "captador.csv",
    }

    for table, filename in files.items():
        path = os.path.join(DATA_DIR, filename)
        print(f"📥 Cargando {filename} → {table}")
        df = load_csv(path)
        load_to_staging(df, table, filename)

    print("✅ Extract finalizado correctamente.")

if __name__ == "__main__":
    run_extract()
