import sqlite3

conn = sqlite3.connect("distribuidora.db")
cursor = conn.cursor()

# Tabla productos
cursor.execute("""
CREATE TABLE IF NOT EXISTS productos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    descripcion TEXT,
    marca TEXT,
    cantidad INTEGER,
    fecha_ingreso TEXT,
    fecha_vencimiento TEXT,
    precio_unitario REAL
)
""")

# Tabla ventas
cursor.execute("""
CREATE TABLE IF NOT EXISTS ventas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha TEXT,
    vendedor TEXT,
    producto TEXT,
    cantidad INTEGER,
    subtotal REAL,
    total REAL,
    comision REAL
)
""")

# Tabla devoluciones
cursor.execute("""
CREATE TABLE IF NOT EXISTS devoluciones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vendedor TEXT,
    producto TEXT,
    cantidad INTEGER,
    motivo TEXT,
    fecha TEXT
)
""")

# Tabla gastos
cursor.execute("""
CREATE TABLE IF NOT EXISTS gastos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tipo TEXT,
    monto REAL,
    fecha TEXT
)
""")

conn.commit()
conn.close()
print("Base de datos creada correctamente")
