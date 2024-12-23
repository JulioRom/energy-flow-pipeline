CREATE DATABASE airflow;

-- Conectar a la base de datos energy_data y crear tablas y directrices sql
\c energy_data;

-- Habilitar extensiones útiles
CREATE EXTENSION IF NOT EXISTS pgcrypto; -- Para claves primarias generadas o UUIDs
CREATE EXTENSION IF NOT EXISTS btree_gin; -- Para índices compuestos avanzados

-- Tabla States: Información sobre los estados
CREATE TABLE States (
    state_id CHAR(2) PRIMARY KEY, -- Código único del estado (e.g., "CA")
    state_description VARCHAR(100) NOT NULL -- Nombre completo del estado
);

-- Tabla Sectors: Información sobre los sectores
CREATE TABLE Sectors (
    sector_id CHAR(3) PRIMARY KEY, -- Código único del sector (e.g., "RES")
    sector_name VARCHAR(100) NOT NULL -- Descripción del sector
);

-- Tabla Periods: Información sobre períodos de tiempo
CREATE TABLE Periods (
    period_id CHAR(7) PRIMARY KEY, -- Identificador único del período en formato "YYYY-MM"
    year SMALLINT NOT NULL CHECK (year >= 1900 AND year <= 2100), -- Año con validación de rango
    month SMALLINT NOT NULL CHECK (month >= 1 AND month <= 12), -- Mes válido
    UNIQUE (year, month) -- Restringe duplicados en año y mes
);

-- Tabla Units: Información sobre las unidades de medida
CREATE TABLE Units (
    unit_id SERIAL PRIMARY KEY, -- Identificador único de la unidad
    unit_name VARCHAR(100) UNIQUE NOT NULL -- Descripción de la unidad
);

-- Tabla SalesData: Métricas de ventas de electricidad
CREATE TABLE SalesData (
    sales_id SERIAL PRIMARY KEY, -- Identificador único para cada registro
    state_id CHAR(2) NOT NULL, -- Relación con States
    sector_id CHAR(3) NOT NULL, -- Relación con Sectors
    period_id CHAR(7) NOT NULL, -- Relación con Periods
    customers BIGINT NOT NULL CHECK (customers >= 0), -- Número de clientes
    price NUMERIC(10, 2) NOT NULL CHECK (price >= 0), -- Precio medio por kilovatio-hora
    revenue NUMERIC(15, 2) NOT NULL CHECK (revenue >= 0), -- Ingresos totales
    sales NUMERIC(15, 2) NOT NULL CHECK (sales >= 0), -- Ventas totales
    customer_unit_id INT NOT NULL, -- Relación con Units
    price_unit_id INT NOT NULL, -- Relación con Units
    revenue_unit_id INT NOT NULL, -- Relación con Units
    sales_unit_id INT NOT NULL, -- Relación con Units
    FOREIGN KEY (state_id) REFERENCES States(state_id) ON DELETE CASCADE, -- Cascada al eliminar estados
    FOREIGN KEY (sector_id) REFERENCES Sectors(sector_id) ON DELETE CASCADE, -- Cascada al eliminar sectores
    FOREIGN KEY (period_id) REFERENCES Periods(period_id) ON DELETE CASCADE, -- Cascada al eliminar períodos
    FOREIGN KEY (customer_unit_id) REFERENCES Units(unit_id) ON DELETE CASCADE, -- Cascada al eliminar unidades
    FOREIGN KEY (price_unit_id) REFERENCES Units(unit_id) ON DELETE CASCADE,
    FOREIGN KEY (revenue_unit_id) REFERENCES Units(unit_id) ON DELETE CASCADE,
    FOREIGN KEY (sales_unit_id) REFERENCES Units(unit_id) ON DELETE CASCADE,
    UNIQUE (state_id, sector_id, period_id) -- Evitar duplicados en combinación de estado, sector y período
);

-- Crear índices avanzados
CREATE INDEX idx_sales_data_compound ON SalesData (state_id, sector_id, period_id);
CREATE INDEX idx_sales_price ON SalesData (price);
CREATE INDEX idx_sales_revenue ON SalesData (revenue);
CREATE INDEX idx_period_year_month ON Periods (year, month);
