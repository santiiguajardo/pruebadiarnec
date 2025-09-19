import sqlite3

DB_PATH = 'distribuidora.db'

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _cols(conn, table):
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {row["name"] for row in cur.fetchall()}

def _add_col_if_missing(conn, table, col_def):
    # col_def: "nombre_de_columna TIPO DEFAULT x"
    col_name = col_def.split()[0]
    if col_name not in _cols(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")

def init_db():
    """Crea la base de datos y tablas si no existen y MIGRA columnas faltantes."""
    conn = get_db_connection()
    cur = conn.cursor()

    # Tablas base
    cur.execute('''
    CREATE TABLE IF NOT EXISTS categorias (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL
    )''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS productos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        -- columnas nuevas se agregan por migración abajo
        categoria_id INTEGER,
        cantidad INTEGER NOT NULL DEFAULT 0,
        -- cantidad_minima puede faltar en BDs viejas, se migra abajo
        FOREIGN KEY (categoria_id) REFERENCES categorias(id)
    )''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS ventas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente TEXT NOT NULL,
        total REAL NOT NULL,
        fecha TEXT NOT NULL
    )''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS ventas_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        venta_id INTEGER NOT NULL,
        producto_id INTEGER NOT NULL,
        cantidad INTEGER NOT NULL,
        precio REAL NOT NULL,
        FOREIGN KEY (venta_id) REFERENCES ventas(id),
        FOREIGN KEY (producto_id) REFERENCES productos(id)
    )''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS devoluciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        venta_id INTEGER NOT NULL,
        producto_id INTEGER NOT NULL,
        cantidad INTEGER NOT NULL,
        motivo TEXT,
        fecha TEXT NOT NULL
    )''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS pagos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tipo TEXT NOT NULL,
        monto REAL NOT NULL,
        descripcion TEXT,
        referencia TEXT,
        fecha TEXT NOT NULL
    )''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS proveedores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL
    )''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS pagos_proveedores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proveedor_id INTEGER NOT NULL,
        monto REAL NOT NULL,
        descripcion TEXT,
        fecha TEXT NOT NULL,
        FOREIGN KEY (proveedor_id) REFERENCES proveedores(id)
    )''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS movimientos_stock (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        producto_id INTEGER NOT NULL,
        cantidad INTEGER NOT NULL,
        tipo TEXT NOT NULL,
        proveedor_id INTEGER,
        fecha_vencimiento TEXT,
        lote TEXT,
        fecha TEXT NOT NULL,
        FOREIGN KEY (producto_id) REFERENCES productos(id),
        FOREIGN KEY (proveedor_id) REFERENCES proveedores(id)
    )''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS vendedores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        telefono TEXT,
        email TEXT
        -- comision puede faltar en BDs viejas, se migra abajo
    )''')

    # ===== MIGRACIONES (agregar columnas faltantes sin perder datos) =====
    # productos: marca, precio_compra, precio_venta, cantidad_minima
    _add_col_if_missing(conn, 'productos', 'marca TEXT')
    _add_col_if_missing(conn, 'productos', 'precio_compra REAL')
    _add_col_if_missing(conn, 'productos', 'precio_venta REAL')
    _add_col_if_missing(conn, 'productos', 'cantidad_minima INTEGER DEFAULT 0')

    # vendedores: comision
    _add_col_if_missing(conn, 'vendedores', 'comision REAL DEFAULT 0')

    # (opcional) pagos_proveedores: en caso de querer defaults, no necesario
    # movimientos_stock ya tiene lote y fecha_vencimiento

    conn.commit()
    conn.close()
