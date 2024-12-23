from airflow import DAG
from airflow.operators.python_operator import PythonOperator  # type: ignore
from datetime import datetime
import requests
import psycopg2.extras
import os

# Configuración de la base de datos
###db_config = {
###    "host": "postgres",  # Cambiar a 'postgres' si usas Docker Compose
###    "port": 5432,
###    "database": "energy_data",
###    "user": "airflow",
###    "password": "airflow",
###}

# Configuración de la base de datos desde variables de entorno
db_config = {
    "host": os.getenv("DB_HOST", "localhost"),  # Predeterminado: localhost
    "port": int(os.getenv("DB_PORT", 5432)),    # Predeterminado: 5432
    "database": os.getenv("DB_NAME", "energy_data"),
    "user": os.getenv("DB_USER", "airflow"),
    "password": os.getenv("DB_PASSWORD", "airflow"),
}

# Función para conectar a la base de datos
def connect_to_db():
    try:
        conn = psycopg2.connect(**db_config)
        return conn
    except psycopg2.Error as e:
        raise Exception(f"Error al conectar a la base de datos: {e}")


# **1. Extracción de Datos**
def extract_data_from_eia(**kwargs):
    api_key = os.environ.get("API_KEY", "default_api_key")
    print("###############API KEY DESDE .ENV#############")
    print(api_key)

    base_url = "https://api.eia.gov/v2/electricity/retail-sales/data/"
    params = kwargs["params"]
    
    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        data = response.json().get("response", {}).get("data", [])
        if not data:
            raise Exception("No se recibieron datos desde la API.")
        kwargs["ti"].xcom_push(key="raw_data", value=data)
        print(f"Datos extraídos: {data[:5]}")  # Muestra los primeros 5 registros
    else:
        raise Exception(
            f"Error al extraer datos: {response.status_code}, {response.text}"
        )


# **2. Transformación de Datos**
def transform_data(**kwargs):
    raw_data = kwargs["ti"].xcom_pull(key="raw_data", task_ids="extract_task")
    if not raw_data:
        raise Exception(
            "No se encontraron datos para transformar. Revisa extract_task."
        )
    ignored_count = 0
    states, sectors, periods, units, sales_data = {}, {}, {}, {}, []

    for record in raw_data:
        if all(record[k] is None for k in ["customers", "price", "revenue", "sales"]):
            ignored_count += 1
            continue

        states[record["stateid"]] = record["stateDescription"]
        sectors[record["sectorid"]] = record["sectorName"]
        year, month = map(int, record["period"].split("-"))
        periods[record["period"]] = (year, month)

        sales_data.append(
            {
                "state_id": record["stateid"],
                "sector_id": record["sectorid"],
                "period_id": record["period"],
                "customers": int(record["customers"]) if record["customers"] else 0,
                "price": float(record["price"]) if record["price"] else 0.0,
                "revenue": float(record["revenue"]) if record["revenue"] else 0.0,
                "sales": float(record["sales"]) if record["sales"] else 0.0,
                "customer_unit": "number of customers",
                "price_unit": "cents per kilowatt-hour",
                "revenue_unit": "million dollars",
                "sales_unit": "million kilowatt hours",
            }
        )

    kwargs["ti"].xcom_push(
        key="transformed_data",
        value={
            "states": states,
            "sectors": sectors,
            "periods": periods,
            "sales_data": sales_data,
        },
    )
    print(f"Registros ignorados: {ignored_count}")
    print(f"Datos transformados: {len(sales_data)} ventas procesadas.")


# **3. Carga de Datos**
def load_data(**kwargs):
    transformed_data = kwargs["ti"].xcom_pull(
        key="transformed_data", task_ids="transform_task"
    )
    
    if not transformed_data:
        raise Exception("No se encontraron datos transformados para cargar.")
    
    print(f"Datos a cargar: {len(transformed_data['sales_data'])} ventas.")
    
    conn = connect_to_db()
    if not conn:
        raise Exception("No se pudo conectar a la base de datos.")

    try:
        with conn.cursor() as cursor:
            
            # Insertar unidades si no existen
            unit_names = ["number of customers", "cents per kilowatt-hour", "million dollars", "million kilowatt hours"]
            for unit_name in unit_names:
                cursor.execute("""
                    INSERT INTO Units (unit_name)
                    VALUES (%s)
                    ON CONFLICT (unit_name) DO NOTHING
                """, (unit_name,))
            conn.commit()
            
            # Recuperar IDs de unidades
            cursor.execute("SELECT unit_id, unit_name FROM Units")
            unit_ids = {row[1]: row[0] for row in cursor.fetchall()}

            
            # Insertar estados
            for state_id, state_desc in transformed_data["states"].items():
                cursor.execute("""
                    INSERT INTO States (state_id, state_description)
                    VALUES (%s, %s) ON CONFLICT (state_id) DO NOTHING
                """, (state_id, state_desc))

            # Insertar sectores
            for sector_id, sector_name in transformed_data["sectors"].items():
                cursor.execute("""
                    INSERT INTO Sectors (sector_id, sector_name)
                    VALUES (%s, %s) ON CONFLICT (sector_id) DO NOTHING
                """, (sector_id, sector_name))

            # Insertar períodos
            for period_id, (year, month) in transformed_data["periods"].items():
                cursor.execute("""
                    INSERT INTO Periods (period_id, year, month)
                    VALUES (%s, %s, %s) ON CONFLICT (period_id) DO NOTHING
                """, (period_id, year, month))

            # Insertar datos de ventas
            for record in transformed_data["sales_data"]:
                cursor.execute("""
                    INSERT INTO SalesData (
                        state_id, sector_id, period_id, customers, price, revenue, sales,
                        customer_unit_id, price_unit_id, revenue_unit_id, sales_unit_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (state_id, sector_id, period_id) DO NOTHING
                """, (
                    record["state_id"], record["sector_id"], record["period_id"],
                    record["customers"], record["price"], record["revenue"], record["sales"],
                    unit_ids["number of customers"], unit_ids["cents per kilowatt-hour"],
                    unit_ids["million dollars"], unit_ids["million kilowatt hours"]
                ))
        conn.commit()
        print("Datos cargados correctamente en la base de datos.")
    except psycopg2.Error as e:
        conn.rollback()
        raise Exception(f"Error al cargar los datos: {e}")
    finally:
        conn.close()


# Definir el DAG en Airflow
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    "etl_dag",
    default_args=default_args,
    description="ETL Pipeline para datos de la API de Energía",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_data_from_eia,
        op_kwargs={
            "params": {
                "frequency": "monthly",
                "data[]": ["customers", "price", "revenue", "sales"],
                "facets[stateid][]": [
                    "AK",
                    "AL",
                    "AR",
                    "AZ",
                    "CA",
                    "CO",
                    "CT"
                ],
                "start": "2021-01",
                "end": "2021-06",
                "sort[0][column]": "price",
                "sort[0][direction]": "desc",
                "offset": 0,
                "length": 200,
                "api_key": os.environ.get("API_KEY", "default_api_key"),
            }
        },
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_data,
    )

    extract_task >> transform_task >> load_task
