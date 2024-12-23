import psycopg2
import psycopg2.extras

db_config = {
    "host": "localhost",
    "port": 5432,
    "database": "energy_data",
    "user": "airflow",
    "password": "airflow",
}

def connect_to_db():
    try:
        conn = psycopg2.connect(**db_config)
        return conn
    except psycopg2.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def run_validations():
    conn = connect_to_db()
    if not conn:
        return

    try:
        cursor = conn.cursor()

        print("Verificando configuración de tablas:")
        # Verificar la estructura de las tablas
        tables = ["States", "Sectors", "Periods", "Units", "SalesData"]
        for table in tables:
            cursor.execute(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '{table.lower()}';
            """)
            print(f"Estructura de la tabla {table}:")
            for row in cursor.fetchall():
                print(row)

        print("Contando registros en las tablas:")
        # Contar registros en cada tabla
        cursor.execute("SELECT COUNT(*) FROM States;")
        print(f"Total de estados: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM Sectors;")
        print(f"Total de sectores: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM Periods;")
        print(f"Total de períodos: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM Units;")
        print(f"Total de unidades: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM SalesData;")
        print(f"Total de datos de ventas: {cursor.fetchone()[0]}")

        print("Validando relaciones de claves foráneas:")
        # Validar relaciones con claves foráneas
        cursor.execute("""
            SELECT DISTINCT sd.state_id
            FROM SalesData sd
            LEFT JOIN States s ON sd.state_id = s.state_id
            WHERE s.state_id IS NULL;
        """)
        if cursor.rowcount > 0:
            print("Registros en SalesData con state_id inexistente en States:")
            print(cursor.fetchall())
        else:
            print("Todos los state_id en SalesData existen en States.")

        cursor.execute("""
            SELECT DISTINCT sd.sector_id
            FROM SalesData sd
            LEFT JOIN Sectors s ON sd.sector_id = s.sector_id
            WHERE s.sector_id IS NULL;
        """)
        if cursor.rowcount > 0:
            print("Registros en SalesData con sector_id inexistente en Sectors:")
            print(cursor.fetchall())
        else:
            print("Todos los sector_id en SalesData existen en Sectors.")

        cursor.execute("""
            SELECT DISTINCT sd.period_id
            FROM SalesData sd
            LEFT JOIN Periods p ON sd.period_id = p.period_id
            WHERE p.period_id IS NULL;
        """)
        if cursor.rowcount > 0:
            print("Registros en SalesData con period_id inexistente en Periods:")
            print(cursor.fetchall())
        else:
            print("Todos los period_id en SalesData existen en Periods.")

        print("Verificando valores nulos o anómalos:")
        # Verificar registros con valores nulos
        cursor.execute("""
            SELECT *
            FROM SalesData
            WHERE customers IS NULL OR price IS NULL OR revenue IS NULL OR sales IS NULL;
        """)
        if cursor.rowcount > 0:
            print("Registros con valores nulos:")
            print(cursor.fetchall())
        else:
            print("No hay registros con valores nulos en SalesData.")

        # Verificar valores extremos
        cursor.execute("""
            SELECT *
            FROM SalesData
            WHERE price < 0 OR revenue < 0 OR sales < 0;
        """)
        if cursor.rowcount > 0:
            print("Registros con valores extremos:")
            print(cursor.fetchall())
        else:
            print("No hay registros con valores extremos en SalesData.")

        print("Verificando duplicados:")
        # Verificar duplicados en SalesData
        cursor.execute("""
            SELECT state_id, sector_id, period_id, COUNT(*)
            FROM SalesData
            GROUP BY state_id, sector_id, period_id
            HAVING COUNT(*) > 1;
        """)
        if cursor.rowcount > 0:
            print("Registros duplicados en SalesData:")
            print(cursor.fetchall())
        else:
            print("No hay registros duplicados en SalesData.")

        print("Informes resumidos:")
        # Ventas totales por estado
        cursor.execute("""
            SELECT s.state_description, SUM(sd.sales) AS total_sales
            FROM SalesData sd
            JOIN States s ON sd.state_id = s.state_id
            GROUP BY s.state_description
            ORDER BY total_sales DESC;
        """)
        print("Ventas totales por estado:")
        print(cursor.fetchall())

        # Ingresos totales por sector
        cursor.execute("""
            SELECT sec.sector_name, SUM(sd.revenue) AS total_revenue
            FROM SalesData sd
            JOIN Sectors sec ON sd.sector_id = sec.sector_id
            GROUP BY sec.sector_name
            ORDER BY total_revenue DESC;
        """)
        print("Ingresos totales por sector:")
        print(cursor.fetchall())

    except Exception as e:
        print(f"Error durante las validaciones: {e}")
    finally:
        if conn:
            conn.close()
            print("Conexión a la base de datos cerrada.")

if __name__ == "__main__":
    run_validations()

