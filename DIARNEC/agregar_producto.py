# agregar_producto.py
import sqlite3

DB_PATH = "distribuidora.db"

def insertar_producto(nombre, marca, categoria_id, precio_compra, precio_venta, cantidad, cantidad_minima):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT INTO productos (nombre, marca, categoria_id, precio_compra, precio_venta, cantidad, cantidad_minima)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (nombre, marca, categoria_id, precio_compra, precio_venta, cantidad, cantidad_minima))
    conn.commit()
    conn.close()
    print("Producto insertado OK")

if __name__ == "__main__":
    nombre = input("Nombre: ").strip()
    marca = input("Marca (opcional): ").strip() or None
    cat = input("Categoria ID (enter si no): ").strip()
    categoria_id = int(cat) if cat else None
    precio_compra = float(input("Precio compra: ").strip())
    precio_venta = float(input("Precio venta: ").strip())
    cantidad = int(input("Cantidad inicial: ").strip())
    cm = input("Cantidad mínima (enter=0): ").strip()
    cantidad_minima = int(cm) if cm else 0

    insertar_producto(nombre, marca, categoria_id, precio_compra, precio_venta, cantidad, cantidad_minima)
