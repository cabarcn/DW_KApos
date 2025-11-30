📊 Data Warehouse KApos
ETL en Python + PostgreSQL + Modelo Dimensional

Este proyecto implementa un Data Warehouse profesional basado en:

✔️ Modelo estrella (Star Schema)
✔️ ETL en Python usando pandas + psycopg2
✔️ Staging (stg), Data Warehouse (dw) y Auditoría (meta)
✔️ Fact table trimestral por suscripción
✔️ Dimensiones: socio, plan, PSP, método de pago, captador, organización y tiempo

📁 Estructura del proyecto
DW_KApos/
│
├── etl/
│   ├── extract.py
│   ├── load.py
│   ├── transform.py (si aplica)
│   └── utils.py
│
├── sql/
│   ├── create_schemas.sql
│   ├── create_dimensions.sql
│   ├── create_fact.sql
│   └── indexes.sql
│
├── meta/
│   └── log_etl.sql
│
├── README.md
└── .gitignore

🚀 Tecnologías utilizadas

- Python 3.10+
- PostgreSQL 15
- pgAdmin 4
- psycopg2
- Pandas

🧱 Esquema del Data Warehouse

![alt text](image.png)

📈 Métricas principales (KPIs)

- Recaudación trimestral
- Ticket promedio
- Tasa de éxito
- Retención y churn trimestral

🛠 Instalación

- Crear entorno virtual
- Instalar dependencias
- Configurar variables de conexión
- Ejecutar ETL
- Ver dashboards en Power BI