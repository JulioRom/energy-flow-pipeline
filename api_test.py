import requests
import psycopg2.extras

# from datetime import datetime  # Importar para manejar fechas

db_config = {
    "host": "localhost",  # Cambiar a 'postgres' si usas Docker Compose
    "port": 5432,
    "database": "energy_data",
    "user": "airflow",
    "password": "airflow",
}


# Función para conectar a la base de datos
def connect_to_db():
    try:
        conn = psycopg2.connect(**db_config)
        return conn
    except psycopg2.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None


# **1. Extracción de Datos**
def extract_data_from_eia(base_url, params):
    """
    Extrae datos desde la API de la EIA.
    """
    response = requests.get(base_url, params=params)
    print(response)
    if response.status_code == 200:
        return response.json()["response"]["data"]
    else:
        raise Exception(
            f"Error al extraer datos: {response.status_code}, {response.text}"
        )


# **2. Transformación de Datos**


def transform_data(raw_data):

    ignored_count = 0
    states = {}
    sectors = {}
    periods = {}
    units = {
        "number of customers": None,
        "cents per kilowatt-hour": None,
        "million dollars": None,
        "million kilowatt hours": None,
    }
    sales_data = []

    for record in raw_data:

        # Filtrar registros inválidos: si todos los valores clave son null, se ignora el registro
        if (
            record["customers"] is None
            and record["price"] is None
            and record["revenue"] is None
            and record["sales"] is None
        ):
            ignored_count += 1
            continue  # Ignorar registros con todos los valores clave en null

        # Estados
        state_id = record["stateid"]
        state_description = record["stateDescription"]
        states[state_id] = state_description

        # Sectores
        sector_id = record["sectorid"]
        sector_name = record["sectorName"]
        sectors[sector_id] = sector_name

        # Períodos
        period_id = record["period"]
        year, month = map(int, period_id.split("-"))
        periods[period_id] = (year, month)

        # Métricas
        sales_data.append(
            {
                "state_id": state_id,
                "sector_id": sector_id,
                "period_id": period_id,
                "customers": (
                    int(record["customers"]) if record["customers"] is not None else 0
                ),
                "price": float(record["price"]) if record["price"] is not None else 0.0,
                "revenue": (
                    float(record["revenue"]) if record["revenue"] is not None else 0.0
                ),
                "sales": float(record["sales"]) if record["sales"] is not None else 0.0,
                "customer_unit": "number of customers",
                "price_unit": "cents per kilowatt-hour",
                "revenue_unit": "million dollars",
                "sales_unit": "million kilowatt hours",
            }
        )
    print(f"Registros ignorados: {ignored_count}")
    print(f"Registros transformados: {len(sales_data)}")

    return states, sectors, periods, units, sales_data


# **3. Carga de Datos**


def load_data(conn, states, sectors, periods, units, sales_data):
    loaded_count = 0
    if not conn:
        print("Conexión no válida. No se puede proceder con la carga de datos.")
        return

    try:
        # Crear un cursor para ejecutar las consultas
        with conn.cursor() as cursor:
            # Insertar estados
            for state_id, state_description in states.items():
                cursor.execute(
                    """
                    INSERT INTO States (state_id, state_description)
                    VALUES (%s, %s) ON CONFLICT (state_id) DO NOTHING
                """,
                    (state_id, state_description),
                )

            # Insertar sectores
            for sector_id, sector_name in sectors.items():
                cursor.execute(
                    """
                    INSERT INTO Sectors (sector_id, sector_name)
                    VALUES (%s, %s) ON CONFLICT (sector_id) DO NOTHING
                """,
                    (sector_id, sector_name),
                )

            # Insertar períodos
            for period_id, (year, month) in periods.items():
                cursor.execute(
                    """
                    INSERT INTO Periods (period_id, year, month)
                    VALUES (%s, %s, %s) ON CONFLICT (period_id) DO NOTHING
                """,
                    (period_id, year, month),
                )

            # Insertar unidades
            for unit_name in units.keys():
                cursor.execute(
                    """
                    INSERT INTO Units (unit_name)
                    VALUES (%s) ON CONFLICT (unit_name) DO NOTHING
                """,
                    (unit_name,),
                )
            conn.commit()

            # Recuperar IDs de unidades
            cursor.execute("SELECT unit_id, unit_name FROM Units")
            unit_ids = {row[1]: row[0] for row in cursor.fetchall()}

            # Insertar datos de ventas
            for record in sales_data:
                cursor.execute(
                    """
                    INSERT INTO SalesData (
                        state_id, sector_id, period_id, customers, price, revenue, sales, 
                        customer_unit_id, price_unit_id, revenue_unit_id, sales_unit_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (state_id, sector_id, period_id) DO NOTHING
                """,
                    (
                        record["state_id"],
                        record["sector_id"],
                        record["period_id"],
                        record["customers"],
                        record["price"],
                        record["revenue"],
                        record["sales"],
                        unit_ids[record["customer_unit"]],
                        unit_ids[record["price_unit"]],
                        unit_ids[record["revenue_unit"]],
                        unit_ids[record["sales_unit"]],
                    ),
                )
            conn.commit()
            print(f"Registros cargados en la base de datos: {loaded_count}")
            print("Datos cargados correctamente en la base de datos")
    except psycopg2.Error as e:
        # Revertir cambios si ocurre un error
        conn.rollback()
        print(f"Error al cargar los datos: {e}")
    except Exception as e:
        print(f"Error inesperado: {e}")
    finally:
        # Cerrar la conexión si no se utiliza el contexto 'with'
        if conn:
            conn.close()
            print("Conexión cerrada tras la carga de datos.")


def run_etl():
    # Configuración de la API y base de datos
    api_key = "atNFhDAOauxj8ccx5WB9WQaqGZj5vvm7SCeJirmL"  # Reemplaza con tu clave API
    base_url = "https://api.eia.gov/v2/electricity/retail-sales/data/"
    params = {
        "frequency": "monthly",  # Frecuencia de los datos (mensual)
        "data[]": [
            "customers",
            "price",
            "revenue",
            "sales",
        ],  # Campos específicos a incluir en los datos
        # "facets[sectorid][]": ["RES", "COM"],  # Filtros para los sectores residencial y comercial
        "facets[stateid][]": ["TX", "NY"],  # Datos de California ["CA", "TX", "NY"]
        "start": "2021-07",  # Fecha de inicio del rango de datos (formato YYYY-MM)
        "end": "2021-12",  # Fecha de fin del rango de datos (formato YYYY-MM)
        "sort[0][column]": "price",  # Ordenar por la columna "price"
        "sort[0][direction]": "desc",  # Dirección del orden, descendente
        "offset": 0,  # Índice del primer registro (paginación)
        "length": 50,  # Número máximo de registros a devolver
        "api_key": api_key,  # Clave API
    }
    print("Iniciando el pipeline ETL...")

    # Conectar a la base de datos
    conn = connect_to_db()
    if not conn:
        print("No se pudo conectar a la base de datos. Finalizando ETL.")
        return

    try:
        # Extraer datos
        print("Extracting data...")
        raw_data = extract_data_from_eia(base_url, params)
        print(f"Total registros extraídos: {len(raw_data)}")
        print("Data extracted successfully.")

        # Transformar datos
        print("Transforming data...")
        states, sectors, periods, units, sales_data = transform_data(raw_data)
        print(f"Total estados transformados: {len(states)}")
        print(f"Total sectores transformados: {len(sectors)}")
        print(f"Total períodos transformados: {len(periods)}")
        print(f"Total ventas transformadas: {len(sales_data)}")
        print("Data transformed successfully.")

        # Cargar datos
        print("Loading data...")
        load_data(conn, states, sectors, periods, units, sales_data)
        print("Data loaded successfully.")

    except Exception as e:
        print(f"Error durante el proceso ETL: {e}")
    finally:
        # Cerrar conexión
        if conn:
            conn.close()
            print("Conexión a la base de datos cerrada.")


# Ejecutar el pipeline
if __name__ == "__main__":
    run_etl()
