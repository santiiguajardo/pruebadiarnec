# migrar_db.py
import sqlite3

DB_PATH = "distribuidora.db"

def add_column_if_missing(conn, table, col, coldef):
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
    if col not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coldef}")

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Asegurar columnas nuevas en productos
add_column_if_missing(conn, "productos", "precio_compra", "REAL")
add_column_if_missing(conn, "productos", "precio_venta", "REAL")
add_column_if_missing(conn, "productos", "cantidad_minima", "INTEGER DEFAULT 0")

# Si existía una columna antigua 'precio', copiamos a precio_venta (solo si está vacía)
cols = [r[1] for r in c.execute("PRAGMA table_info(productos)")]
if "precio" in cols:
    c.execute("UPDATE productos SET precio_venta = precio WHERE precio_venta IS NULL")

conn.commit()
conn.close()
print("Migración OK")
