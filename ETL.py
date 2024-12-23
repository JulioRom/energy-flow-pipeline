##version: 2.0.0
import aiohttp
import asyncio
import psycopg2.extras
import json
import os

# Configuración de la API y la base de datos
api_key = "32f15c8e72c425b8e12485b8aad8b2ea"
db_config = {
    "host": "localhost",  # Cambiar a 'postgres' si usas Docker Compose
    "port": 5432,
    "database": "weather_data",
    "user": "airflow",
    "password": "airflow",
}
cities_file_path = os.path.join("./", "filtered_data.json")

def format_city_name(city):
   
    city = city.strip()  # Elimina espacios adicionales
    if ',' in city:
        parts = city.split(', ')
        return f"{parts[1]} {parts[0]}".strip()  # Reorganiza y asegura limpieza adicional
    return city  # Retorna el nombre limpio

async def fetch_weather_data(api_key, city):
    """
    Solicita datos de clima de la API OpenWeatherMap para una ciudad especificada.
    """
    formatted_city = format_city_name(city)
    url = f"https://api.openweathermap.org/data/2.5/weather?q={formatted_city}&appid={api_key}"
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(f"Error: API request for {formatted_city, url} failed with status {response.status}")
                return None

async def extract_all_weather_data(api_key, cities):
    """
    Extrae datos de clima para una lista de ciudades concurrentemente.
    """
    tasks = [fetch_weather_data(api_key, city["name"]) for city in cities]
    return await asyncio.gather(*tasks)

def transform_weather_data(raw_data):
    """
    Transforma los datos extraídos, seleccionando solo los campos relevantes.
    """
    if raw_data is None:
        return None
    city_name = raw_data["name"]
    return {
        "city": city_name,
        "temperature": raw_data["main"]["temp"],
        "weather_description": raw_data["weather"][0]["description"]
    }

def load_weather_data_batch(db_config, transformed_data_list):
    """
    Carga múltiples registros transformados en la base de datos PostgreSQL en una sola transacción.
    """
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

        # Crear la tabla si no existe
        create_table_query = """
        CREATE TABLE IF NOT EXISTS city_weather (
            city VARCHAR(100) PRIMARY KEY,
            temperature FLOAT,
            weather_description VARCHAR(255)
        );
        """
        cursor.execute(create_table_query)

        # Insertar múltiples registros
        insert_query = """
        INSERT INTO city_weather (city, temperature, weather_description)
        VALUES (%s, %s, %s)
        ON CONFLICT (city) DO UPDATE
        SET temperature = EXCLUDED.temperature,
            weather_description = EXCLUDED.weather_description;
        """
        psycopg2.extras.execute_batch(cursor, insert_query, [
            (data["city"], data["temperature"], data["weather_description"])
            for data in transformed_data_list if data is not None
        ])

        connection.commit()
        print(f"Batch de {len(transformed_data_list)} registros cargado exitosamente.")
    except psycopg2.Error as e:
        print(f"Error al cargar los datos en la base de datos: {e}")
    finally:
        cursor.close()
        connection.close()

def load_cities_from_json(file_path):
    """
    Carga la lista de ciudades desde un archivo JSON.
    """
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)

# Pipeline ETL
if __name__ == "__main__":
    cities = load_cities_from_json(cities_file_path)
    print(f"Ciudades cargadas: {len(cities)}")

    async def pipeline():
        # Extracción de datos concurrente
        raw_data_list = await extract_all_weather_data(api_key, cities)

        # Transformación de los datos
        transformed_data = [transform_weather_data(data) for data in raw_data_list]

        # Carga de los datos por lotes
        load_weather_data_batch(db_config, transformed_data)

    asyncio.run(pipeline())

## version: 1.0.0
##import json
##import os
##import requests
##import psycopg2
##
##api_key = "32f15c8e72c425b8e12485b8aad8b2ea"
##city = "London"
##db_config = {
##    "host": "localhost",
##    "port": 5432,
##    "database": "weather_data",
##    "user": "airflow",
##    "password": "airflow",
##}
### Ruta al archivo JSON con los nombres de las ciudades
##cities_file_path = os.path.join("./", "filtered_data.json")
##
##def format_city_name(city):
##    if ',' in city:
##        parts = city.split(', ')
##        return f"{parts[1]} {parts[0]}"  # Reorganiza como 'El Vellón'
##    return city  # Retorna sin cambios si no contiene ','
##
##def extract_weather_data(api_key, city):
##    formatted_city = format_city_name(city)
##    url = f"https://api.openweathermap.org/data/2.5/weather?q={formatted_city}&appid={api_key}"
##    response = requests.get(url)
##    if response.status_code == 200:
##        return response.json()
##    else:
##        raise Exception(f"API request failed with status code {response.status_code}")
##
##def transform_weather_data(raw_data):
##    
##    city_name = raw_data["name"].split(",")[0]
##    transformed_data = {
##        "city": city_name,
##        "temperature": raw_data["main"]["temp"],
##        "weather_description": raw_data["weather"][0]["description"]
##    }
##    return transformed_data
##
##
##def load_weather_data(db_config, weather_data):
##    print(db_config)
##    #dsn = "postgresql://airflow:airflow@localhost:5432/weather_data"
##    #print(dsn)
##    connection = psycopg2.connect(
##        host= "localhost",
##        port= 5432,
##        database= "weather_data",
##        user= "airflow",
##        password= "airflow",
##        client_encoding="UTF8"
##    )
##    #connection = psycopg2.connect(dsn)
##    #connection = psycopg2.connect(**db_config)
##    cursor = connection.cursor()
##    query = """
##        INSERT INTO city_weather (city, temperature, weather_description)
##        VALUES (%s, %s, %s);
##    """
##    cursor.execute(query, (weather_data["city"], weather_data["temperature"], weather_data["weather_description"]))
##    connection.commit()
##    cursor.close()
##    connection.close()
##    
##def load_cities_from_json(file_path):
##
##    #Carga la lista de ciudades desde un archivo JSON.
##    
##    with open(file_path, "r", encoding="utf-8") as file:
##        cities = json.load(file)
##    return cities
##
### Pipeline ETL
##if __name__ == "__main__":
##    # Cargar la lista de ciudades desde el archivo JSON
##    cities = load_cities_from_json(cities_file_path)
##    print(f"Ciudades cargadas: {cities}")
##
##    # Procesar cada ciudad
##    for city in cities:
##        print(f"Procesando la ciudad: {city}")
##        
##        # Extracción
##        raw_data = extract_weather_data(api_key, city["name"])
##        print("Acquired data...")
##        print(raw_data)
##        
##        # Transformación
##        transformed_data = transform_weather_data(raw_data)
##        print("Transformed data...")
##        print(transformed_data)
##        
##        # Carga
##        load = load_weather_data(db_config, transformed_data)
##        print("Data loaded...")
##        print(load)


##data = extract_weather_data(api_key,city)
##print("Acquired data...")
##print(data)
##
##trans = transform_weather_data(data)
##print("Transformed data...")
##print(trans)
##
##load = load_weather_data(db_config,trans)
##print("Data loaded...")
##print(load)





