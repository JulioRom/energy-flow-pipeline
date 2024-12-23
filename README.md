# ETL Energy Data Pipeline

## **Descripción del Proyecto**
Este proyecto es un pipeline ETL (Extract, Transform, Load) diseñado para extraer datos públicos sobre ventas minoristas de electricidad desde la API de Energía de EE.UU. (EIA Open Data API), transformarlos en un formato estructurado y cargarlos en una base de datos PostgreSQL para su análisis.

El pipeline está orquestado con **Apache Airflow** y utiliza contenedores Docker para garantizar portabilidad y fácil despliegue en cualquier entorno.

## **Alcance del Proyecto**
El pipeline está diseñado para:

1. **Extracción de Datos:**
   - Obtener datos de la API de EIA sobre ventas de electricidad, precios, ingresos y clientes.
   - Implementar paginación y manejo de grandes volúmenes de datos.

2. **Transformación de Datos:**
   - Filtrar registros inválidos o incompletos.
   - Normalizar los datos en un formato que cumpla con las normas de diseño relacional.

3. **Carga de Datos:**
   - Insertar los datos procesados en un esquema relacional en PostgreSQL.
   - Manejar conflictos con claves primarias y restricciones únicas mediante `ON CONFLICT`.

4. **Orquestación y Automatización:**
   - Programar la ejecución diaria del pipeline con Airflow.
   - Monitorear la ejecución y gestionar fallas mediante la interfaz gráfica de Airflow.

## **Estructura del Proyecto**

```
.
├── dags/
│   ├── electricity_retail_sales_ETL.py  # Archivo principal del DAG
├── sql/
│   ├── create_tables.sql                # Script para crear las tablas en PostgreSQL
├── .env                                 # Variables de entorno (API_KEY, DB_HOST, etc.)
├── docker-compose.yml                   # Configuración de Docker Compose
├── requirements.txt                     # Dependencias del proyecto
├── README.md                            # Documentación del proyecto
├── .gitignore                           # Archivos ignorados en el repositorio
```

## **Configuración**

### **1. Prerrequisitos**
- Docker y Docker Compose instalados.
- Clave de API válida para la API de EIA (https://www.eia.gov/opendata/).

### **2. Variables de Entorno**
Crea un archivo `.env` en el directorio raíz con las siguientes variables:

```env
API_KEY=tu_clave_de_api
DB_HOST=postgres
DB_PORT=5432
DB_NAME=energy_data
DB_USER=airflow
DB_PASSWORD=airflow
```

### **3. Configurar la Base de Datos**
Ejecuta el script SQL para crear las tablas necesarias en PostgreSQL:

```bash
docker exec -it postgres psql -U airflow -d energy_data -f /path/to/create_tables.sql
```

### **4. Levantar el Entorno con Docker**
Levanta los contenedores con Docker Compose:

```bash
docker-compose up -d
```

### **5. Acceder a Airflow**
Abre la interfaz gráfica de Airflow en `http://localhost:8080` y activa el DAG `etl_dag` para ejecutarlo.

### **6. Verificar los Datos**
Después de la ejecución, verifica los datos en PostgreSQL:

```sql
SELECT * FROM SalesData LIMIT 10;
```

## **Dependencias**
Las principales herramientas y bibliotecas utilizadas en este proyecto incluyen:

- **Apache Airflow:** Para la orquestación del pipeline.
- **PostgreSQL:** Para el almacenamiento de datos.
- **Docker Compose:** Para gestionar los contenedores.
- **Requests:** Para realizar solicitudes a la API.
- **psycopg2:** Para interactuar con PostgreSQL desde Python.

Instala las dependencias de Python con:

```bash
pip install -r requirements.txt
```

## **Alcance Futuro**
- Implementar un sistema de notificaciones en caso de fallas en el pipeline.
- Crear visualizaciones y paneles interactivos para analizar los datos cargados.
- Escalar el proyecto para incluir otras fuentes de datos relacionadas con la energía.

## **Consideraciones de Seguridad**
- Agrega `.env` al archivo `.gitignore` para evitar subir claves sensibles al repositorio.
- Considera usar gestores de secretos como AWS Secrets Manager para manejar claves de API y credenciales de bases de datos.

## **Contribuciones**
Las contribuciones son bienvenidas. Si deseas colaborar, abre un issue o crea un pull request con tus mejoras.

