import sqlite3

conn = sqlite3.connect("distribuidora.db")
cursor = conn.cursor()

# Crear tabla ventas con la columna total
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

conn.commit()
conn.close()
print("Tabla ventas verificada/creada correctamente")
