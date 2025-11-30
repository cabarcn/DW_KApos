import os
from dotenv import load_dotenv
import psycopg2

# Cargar variables del archivo .env
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(BASE_DIR, ".env")
load_dotenv(env_path)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def test_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        print("✅ Conexión a PostgreSQL EXITOSA")
        conn.close()
    except Exception as e:
        print("❌ Error al conectar a PostgreSQL:")
        print(e)

if __name__ == "__main__":
    test_connection()
