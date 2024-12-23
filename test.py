import json
import requests
import psycopg2.extras
from datetime import datetime  # Importar para manejar fechas

# Configuración de la API y base de datos
api_key = "atNFhDAOauxj8ccx5WB9WQaqGZj5vvm7SCeJirmL"  # Reemplaza con tu clave API
base_url = "https://api.eia.gov/v2/electricity/retail-sales/data/"
db_config = {
    "host": "localhost",  # Cambiar a 'postgres' si usas Docker Compose
    "port": 5432,
    "database": "energy_data",
    "user": "airflow",
    "password": "airflow",
}
# Parámetros de extracción
params = {
    "frequency": "monthly",  # Frecuencia de los datos (mensual)
    "data[]": [
        "customers",
        "price",
        "revenue",
        "sales",
    ],  # Campos específicos a incluir en los datos
    # "facets[sectorid][]": ["RES", "COM"],  # Filtros para los sectores residencial y comercial
    "facets[stateid][]": ["CA"],  # Datos de California
    "start": "2021-01",  # Fecha de inicio del rango de datos (formato YYYY-MM)
    "end": "2021-06",  # Fecha de fin del rango de datos (formato YYYY-MM)
    "sort[0][column]": "price",  # Ordenar por la columna "price"
    "sort[0][direction]": "desc",  # Dirección del orden, descendente
    "offset": 0,  # Índice del primer registro (paginación)
    "length": 100,  # Número máximo de registros a devolver
    "api_key": api_key,  # Clave API
}


# **1. Extracción de Datos**
def extract_data_from_eia(base_url, params):
    """
    Extrae datos desde la API de la EIA.
    """
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(
            f"Error al extraer datos: {response.status_code}, {response.text}"
        )

# Extracción
raw_data = extract_data_from_eia(base_url, params)

print(print(json.dumps(raw_data, indent=4)))
