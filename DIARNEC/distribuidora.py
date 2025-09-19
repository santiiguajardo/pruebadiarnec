from flask import Flask, render_template, request, redirect, url_for, flash, send_file, Response, jsonify
from datetime import datetime, timedelta
import sqlite3
from io import StringIO
import csv
from database import init_db, get_db_connection
from pdf_generator import generate_invoice_pdf, generate_price_list_pdf

app = Flask(__name__)
app.secret_key = 'distribuidora_secret_key'

# Alias de transferencia para mostrar en la factura (pie de página)
ALIAS_TRANSFERENCIA = "DIARNEC.DISTRIBUIDORA.ALIAS"

# ---------- Helpers de migración ligera ----------
def _table_cols(conn, table):
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    try:
        return {r["name"] for r in rows}
    except Exception:
        return {r[1] for r in rows}

def _add_col_if_missing(conn, table, col_def):
    col_name = col_def.split()[0]
    cols = _table_cols(conn, table)
    if col_name not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")

def ensure_schema():
    """
    Agrega columnas/tablas que la BD podría no tener (no destructivo).
    Evita errores 'no such column' y habilita FEFO, Proveedores flexible,
    comisiones por marca, pagos/bonificaciones, devoluciones (cab/items)
    y saldos por vendedor.
    """
    conn = get_db_connection()
    try:
        # productos
        _add_col_if_missing(conn, "productos", "marca TEXT")
        _add_col_if_missing(conn, "productos", "precio_compra REAL")
        _add_col_if_missing(conn, "productos", "precio_venta REAL")
        _add_col_if_missing(conn, "productos", "cantidad_minima INTEGER DEFAULT 0")

        # vendedores
        _add_col_if_missing(conn, "vendedores", "comision REAL DEFAULT 0")
        _add_col_if_missing(conn, "vendedores", "telefono TEXT")

        # movimientos_stock (para FEFO)
        _add_col_if_missing(conn, "movimientos_stock", "cantidad_restante INTEGER")
        _add_col_if_missing(conn, "movimientos_stock", "entrada_id INTEGER")

        # backfill de cantidad_restante
        conn.execute("""
            UPDATE movimientos_stock
               SET cantidad_restante = cantidad
             WHERE tipo = 'entrada' AND (cantidad_restante IS NULL OR cantidad_restante < 0)
        """)

        # pagos_proveedores (modelo flexible)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pagos_proveedores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                proveedor TEXT,
                medio_pago TEXT,
                monto_bruto REAL,
                comision_pct REAL DEFAULT 0,
                monto_neto REAL,
                descripcion TEXT,
                fecha TEXT
            )
        """)
        _add_col_if_missing(conn, "pagos_proveedores", "proveedor_id INTEGER")
        _add_col_if_missing(conn, "pagos_proveedores", "monto REAL")

        # comisiones por vendedor y marca
        conn.execute("""
            CREATE TABLE IF NOT EXISTS comisiones_vendedor_marca (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vendedor_id INTEGER NOT NULL,
                marca TEXT NOT NULL,
                comision_pct REAL NOT NULL DEFAULT 0,
                UNIQUE(vendedor_id, marca)
            )
        """)

        # devoluciones (tabla vieja, mantenemos por compatibilidad)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS devoluciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                venta_id INTEGER NOT NULL,
                producto_id INTEGER NOT NULL,
                cantidad INTEGER NOT NULL,
                motivo TEXT,
                fecha TEXT
            )
        """)

        # pagos a vendedores
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pagos_vendedores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vendedor_id INTEGER NOT NULL,
                monto REAL NOT NULL,
                fecha TEXT,
                descripcion TEXT
            )
        """)
        # columna nueva en pagos_vendedores
        _add_col_if_missing(conn, "pagos_vendedores", "medio_pago TEXT")  # efectivo | transferencia

        # bonificaciones
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bonificaciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vendedor_id INTEGER NOT NULL,
                monto REAL NOT NULL,
                fecha TEXT,
                descripcion TEXT
            )
        """)

        # devoluciones (nuevo esquema tipo ventas)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS devoluciones_cab (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vendedor_id INTEGER NOT NULL,
                vendedor_nombre TEXT,
                motivo TEXT,
                total REAL NOT NULL,
                fecha TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS devoluciones_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                devolucion_id INTEGER NOT NULL,
                producto_id INTEGER NOT NULL,
                cantidad INTEGER NOT NULL,
                precio REAL NOT NULL,
                pct REAL NOT NULL,
                subtotal REAL NOT NULL,
                FOREIGN KEY(devolucion_id) REFERENCES devoluciones_cab(id)
            )
        """)

        conn.commit()
    finally:
        conn.close()

def _norm_marca(s):
    return (s or '').strip().upper()

def _get_comision_pct(conn, vendedor_id, marca):
    """Comisión % para vendedor+marca; fallback a comisión base del vendedor; luego 0."""
    marca_norm = _norm_marca(marca)
    if vendedor_id:
        r = conn.execute(
            "SELECT comision_pct FROM comisiones_vendedor_marca WHERE vendedor_id=? AND marca=?",
            (vendedor_id, marca_norm)
        ).fetchone()
        if r and r["comision_pct"] is not None:
            try:
                return float(r["comision_pct"])
            except Exception:
                pass
        v = conn.execute("SELECT comision FROM vendedores WHERE id = ?", (vendedor_id,)).fetchone()
        if v and v["comision"] is not None:
            try:
                return float(v["comision"])
            except Exception:
                pass
    return 0.0

def consumir_stock_fefo(conn, producto_id: int, cantidad_a_vender: int):
    """Descarga por FEFO."""
    if cantidad_a_vender <= 0:
        return

    row = conn.execute("SELECT cantidad FROM productos WHERE id = ?", (producto_id,)).fetchone()
    disponible = int(row["cantidad"] if row and row["cantidad"] is not None else 0)
    if disponible < cantidad_a_vender:
        raise ValueError(f"Stock insuficiente para el producto {producto_id}. Disponible: {disponible}, requerido: {cantidad_a_vender}")

    restante = cantidad_a_vender

    lotes = conn.execute("""
        SELECT id, cantidad_restante, fecha_vencimiento
          FROM movimientos_stock
         WHERE producto_id = ? AND tipo = 'entrada' AND (cantidad_restante IS NULL OR cantidad_restante > 0)
         ORDER BY 
           CASE WHEN fecha_vencimiento IS NULL THEN 1 ELSE 0 END ASC,
           fecha_vencimiento ASC,
           id ASC
    """, (producto_id,)).fetchall()

    for lote in lotes:
        if restante <= 0:
            break
        lote_rest = int(lote["cantidad_restante"] or 0)
        if lote_rest <= 0:
            continue

        usa = min(lote_rest, restante)

        conn.execute(
            "UPDATE movimientos_stock SET cantidad_restante = cantidad_restante - ? WHERE id = ?",
            (usa, lote["id"])
        )
        conn.execute(
            """INSERT INTO movimientos_stock (producto_id, cantidad, tipo, proveedor_id, fecha_vencimiento, lote, fecha, entrada_id)
               VALUES (?, ?, 'salida', NULL, ?, NULL, ?, ?)""",
            (producto_id, usa, lote["fecha_vencimiento"], datetime.now().strftime('%Y-%m-%d'), lote["id"])
        )
        restante -= usa

    if restante > 0:
        raise ValueError(f"No se pudo completar FEFO para el producto {producto_id}. Restante: {restante}")

# ---------- Inicialización ----------
_INIT_RAN = False

@app.before_request
def initialize_database():
    global _INIT_RAN
    if not _INIT_RAN:
        init_db()
        ensure_schema()
        _INIT_RAN = True
    else:
        try:
            ensure_schema()
        except Exception:
            pass

# ----------------- Dashboard -----------------
@app.route('/')
def dashboard():
    conn = get_db_connection()
    try:
        hoy = datetime.now().strftime('%Y-%m-%d')

        total_ventas_hoy = conn.execute(
            'SELECT COALESCE(SUM(total), 0) FROM ventas WHERE fecha = ?', (hoy,)
        ).fetchone()[0] or 0

        total_productos = conn.execute('SELECT COUNT(*) FROM productos').fetchone()[0]

        productos_bajo_stock = conn.execute(
            'SELECT nombre FROM productos WHERE cantidad <= cantidad_minima'
        ).fetchall()

        ventas_mensuales = conn.execute(
            """SELECT strftime('%Y-%m', fecha) as mes, SUM(total) as total 
               FROM ventas GROUP BY mes ORDER BY mes DESC LIMIT 6"""
        ).fetchall()

        cant_ventas_hoy = conn.execute("SELECT COUNT(*) FROM ventas WHERE fecha = ?", (hoy,)).fetchone()[0]
        stock_bajo = len(productos_bajo_stock)

        # Vencimientos (para mostrar en la barra lateral también)
        venc_prox = conn.execute("""
            SELECT ms.id AS mov_id, p.nombre, p.marca, ms.fecha_vencimiento, ms.cantidad_restante
              FROM movimientos_stock ms
              JOIN productos p ON p.id = ms.producto_id
             WHERE ms.tipo='entrada'
               AND ms.fecha_vencimiento IS NOT NULL
               AND ms.cantidad_restante > 0
               AND ms.fecha_vencimiento <= date('now','+30 day')
             ORDER BY ms.fecha_vencimiento
        """).fetchall()
        vencidos = conn.execute("""
            SELECT ms.id AS mov_id, p.nombre, p.marca, ms.fecha_vencimiento, ms.cantidad_restante
              FROM movimientos_stock ms
              JOIN productos p ON p.id = ms.producto_id
             WHERE ms.tipo='entrada'
               AND ms.fecha_vencimiento IS NOT NULL
               AND ms.cantidad_restante > 0
               AND ms.fecha_vencimiento < date('now')
             ORDER BY ms.fecha_vencimiento
        """).fetchall()

    finally:
        conn.close()

    return render_template(
        'dashboard.html',
        dashboard_title="DIARNEC DISTRIBUIDORA",
        total_ventas=total_ventas_hoy,
        total_productos=total_productos,
        productos_bajo_stock=productos_bajo_stock,
        ventas_mensuales=ventas_mensuales,
        cant_ventas_hoy=cant_ventas_hoy,
        stock_bajo=stock_bajo,
        vencimientos_proximos=len(venc_prox),
        vencimientos_vencidos=len(vencidos),
        lista_vencimientos=venc_prox  # por si querés listarlos
    )

# ----------------- Datos JSON para gráficos -----------------
@app.route('/dashboard-data')
def dashboard_data():
    conn = get_db_connection()
    try:
        # Helpers de períodos (SQLite)
        semana_ini = "date('now','-' || strftime('%w','now') || ' day')"  # domingo->hoy
        mes_actual = "strftime('%Y-%m', fecha) = strftime('%Y-%m','now')"

        # --- Ventas por mes (últimos 12) ---
        vm = conn.execute("""
            SELECT strftime('%Y-%m', fecha) AS mes, SUM(total) AS total
            FROM ventas
            GROUP BY mes
            ORDER BY mes DESC
            LIMIT 12
        """).fetchall()
        ventas_mensuales = [{"mes": r["mes"], "total": float(r["total"] or 0)} for r in reversed(vm)]

        # --- Ventas por día (últimos 30) ---
        vd = conn.execute("""
            SELECT fecha, SUM(total) AS total
            FROM ventas
            WHERE fecha >= date('now','-30 day')
            GROUP BY fecha
            ORDER BY fecha
        """).fetchall()
        ventas_diarias = [{"fecha": r["fecha"], "total": float(r["total"] or 0)} for r in vd]

        # --- Ventas por semana (últimas 12) ---
        vs = conn.execute("""
            SELECT strftime('%Y-W%W', fecha) AS semana, SUM(total) AS total
            FROM ventas
            GROUP BY semana
            ORDER BY semana DESC
            LIMIT 12
        """).fetchall()
        ventas_semanales = [{"semana": r["semana"], "total": float(r["total"] or 0)} for r in reversed(vs)]

        # ========= Alertas =========
        stock_cercania = 5  # configurable
        stock_bajo = conn.execute("""
            SELECT id, nombre, marca, cantidad, cantidad_minima
            FROM productos
            WHERE cantidad <= COALESCE(cantidad_minima, 0)
            ORDER BY nombre
        """).fetchall()
        stock_cercano = conn.execute(f"""
            SELECT id, nombre, marca, cantidad, cantidad_minima
            FROM productos
            WHERE cantidad > COALESCE(cantidad_minima, 0)
              AND cantidad <= COALESCE(cantidad_minima, 0) + {stock_cercania}
            ORDER BY nombre
        """).fetchall()

        # Vencimientos
        venc_prox = conn.execute("""
            SELECT ms.id AS mov_id, p.nombre, p.marca, ms.fecha_vencimiento, ms.cantidad_restante,
                   COALESCE(p.precio_compra,0) AS precio_compra
            FROM movimientos_stock ms
            JOIN productos p ON p.id = ms.producto_id
            WHERE ms.tipo = 'entrada'
              AND ms.fecha_vencimiento IS NOT NULL
              AND ms.cantidad_restante > 0
              AND ms.fecha_vencimiento > date('now')
              AND ms.fecha_vencimiento <= date('now','+30 day')
            ORDER BY ms.fecha_vencimiento
        """).fetchall()
        vencidos = conn.execute("""
            SELECT ms.id AS mov_id, p.nombre, p.marca, ms.fecha_vencimiento, ms.cantidad_restante,
                   COALESCE(p.precio_compra,0) AS precio_compra
            FROM movimientos_stock ms
            JOIN productos p ON p.id = ms.producto_id
            WHERE ms.tipo = 'entrada'
              AND ms.fecha_vencimiento IS NOT NULL
              AND ms.cantidad_restante > 0
              AND ms.fecha_vencimiento <= date('now')
            ORDER BY ms.fecha_vencimiento
        """).fetchall()

        perdida_vencimiento = 0.0
        for r in vencidos:
            try:
                perdida_vencimiento += float(r["cantidad_restante"] or 0) * float(r["precio_compra"] or 0)
            except Exception:
                pass

        # ========= KPIs del mes =========
        ventas_mes = conn.execute(f"""
            SELECT COALESCE(SUM(total),0) AS t
            FROM ventas WHERE {mes_actual}
        """).fetchone()["t"] or 0.0

        gastos_mes = conn.execute("""
            SELECT COALESCE(SUM(monto),0) AS t
            FROM gastos WHERE strftime('%Y-%m', fecha) = strftime('%Y-%m','now')
        """).fetchone()["t"] or 0.0

        # devoluciones del mes
        dev_mes_nuevo = conn.execute("""
            SELECT COALESCE(SUM(total),0) AS t
            FROM devoluciones_cab
            WHERE strftime('%Y-%m', fecha) = strftime('%Y-%m','now')
        """).fetchone()
        dev_mes_nuevo = float(dev_mes_nuevo["t"] if dev_mes_nuevo else 0.0)
        try:
            dev_mes_viejo = conn.execute("""
                SELECT COALESCE(SUM(d.cantidad * vi.precio), 0) AS t
                FROM devoluciones d
                JOIN ventas ven      ON ven.id = d.venta_id
                JOIN ventas_items vi ON vi.venta_id = d.venta_id AND vi.producto_id = d.producto_id
                WHERE strftime('%Y-%m', d.fecha) = strftime('%Y-%m','now')
            """).fetchone()
            dev_mes_viejo = float(dev_mes_viejo["t"] if dev_mes_viejo else 0.0)
        except sqlite3.OperationalError:
            dev_mes_viejo = 0.0
        devoluciones_mes = dev_mes_nuevo + dev_mes_viejo

        bonificaciones_mes = conn.execute("""
            SELECT COALESCE(SUM(monto),0) AS t
            FROM bonificaciones
            WHERE strftime('%Y-%m', fecha) = strftime('%Y-%m','now')
        """).fetchone()["t"] or 0.0

        pagos_mes = conn.execute("""
            SELECT COALESCE(SUM(monto),0) AS t
            FROM pagos_vendedores
            WHERE strftime('%Y-%m', fecha) = strftime('%Y-%m','now')
        """).fetchone()["t"] or 0.0

        # ========= Costo de venta estimado (mes) =========
        try:
            cogs_mes = conn.execute("""
                SELECT COALESCE(SUM(vi.cantidad * COALESCE(p.precio_compra,0)),0) AS cogs
                FROM ventas v
                JOIN ventas_items vi ON vi.venta_id = v.id
                JOIN productos p     ON p.id = vi.producto_id
                WHERE strftime('%Y-%m', v.fecha) = strftime('%Y-%m','now')
            """).fetchone()["cogs"] or 0.0
        except sqlite3.OperationalError:
            cogs_mes = 0.0

        margen_bruto = float(ventas_mes) - float(cogs_mes)

        # ========= Rendimiento del mes =========
        rendimiento_total = (
            float(margen_bruto)
            - float(gastos_mes)
            - float(bonificaciones_mes)
            - float(devoluciones_mes)
            - float(perdida_vencimiento)
        )

        rendimiento_mes = {
            "ventas_netas": float(ventas_mes),
            "costo_venta_estimado": float(cogs_mes),
            "margen_bruto": float(margen_bruto),
            "gastos_mes": float(gastos_mes),
            "bonificaciones_mes": float(bonificaciones_mes),
            "devoluciones_mes": float(devoluciones_mes),
            "perdidas_vencimiento": float(perdida_vencimiento),
            "total": float(rendimiento_total)
        }

        # ========= Productos más devueltos (histórico top10) =========
        productos_mas_devueltos_hist = []
        try:
            pdev_new = conn.execute("""
                SELECT p.nombre AS producto, COALESCE(SUM(di.cantidad),0) AS unidades
                  FROM devoluciones_items di
                  JOIN productos p ON p.id = di.producto_id
              GROUP BY di.producto_id
              ORDER BY unidades DESC
                 LIMIT 10
            """).fetchall()
            if pdev_new:
                productos_mas_devueltos_hist = [
                    {"producto": r["producto"], "unidades": int(r["unidades"] or 0)}
                    for r in pdev_new
                ]
            else:
                raise sqlite3.OperationalError("sin datos en nuevo esquema")
        except sqlite3.OperationalError:
            try:
                pdev_old = conn.execute("""
                    SELECT p.nombre AS producto, SUM(d.cantidad) AS unidades
                      FROM devoluciones d
                      JOIN productos p ON p.id = d.producto_id
                  GROUP BY d.producto_id
                  ORDER BY unidades DESC
                     LIMIT 10
                """).fetchall()
                productos_mas_devueltos_hist = [
                    {"producto": r["producto"], "unidades": int(r["unidades"] or 0)}
                    for r in pdev_old
                ]
            except sqlite3.OperationalError:
                productos_mas_devueltos_hist = []

        # ========= Mejores vendedores =========
        def top_vendedores(filtro_sql):
            rows = conn.execute(f"""
                SELECT v.nombre AS vendedor, COALESCE(SUM(ven.total),0) AS total
                FROM vendedores v
                LEFT JOIN ventas ven ON ven.cliente = v.nombre
                WHERE 1=1 {filtro_sql}
                GROUP BY v.id
                ORDER BY total DESC
                LIMIT 10
            """).fetchall()
            return [{"vendedor": r["vendedor"], "total": float(r["total"] or 0)} for r in rows]

        mejores_vendedores = {
            "dia":    top_vendedores("AND ven.fecha = date('now')"),
            "semana": top_vendedores("AND ven.fecha >= " + semana_ini),
            "mes":    top_vendedores(f"AND {mes_actual.replace('fecha','ven.fecha')}"),
            "anio":   top_vendedores("AND strftime('%Y', ven.fecha) = strftime('%Y','now')")
        }

        # ========= Productos (vendidos/pedidos) =========
        def top_productos_vendidos(where_fecha_sql):
            rows = conn.execute(f"""
                SELECT p.nombre AS producto, COALESCE(SUM(vi.cantidad),0) AS unidades
                FROM ventas v
                JOIN ventas_items vi ON vi.venta_id = v.id
                JOIN productos p ON p.id = vi.producto_id
                WHERE {where_fecha_sql}
                GROUP BY vi.producto_id
                ORDER BY unidades DESC
                LIMIT 10
            """).fetchall()
            return [{"producto": r["producto"], "unidades": int(r["unidades"] or 0)} for r in rows]

        productos_mas_vendidos = {
            "semana": top_productos_vendidos(f"v.fecha >= {semana_ini}"),
            "mes":    top_productos_vendidos("strftime('%Y-%m', v.fecha) = strftime('%Y-%m','now')"),
            "anio":   top_productos_vendidos("strftime('%Y', v.fecha) = strftime('%Y','now')"),
        }
        # "Pedidos" = equivalentes a vendidos (no hay tabla de pedidos separada)
        productos_mas_pedidos = productos_mas_vendidos

        # ========= Productos más devueltos por período =========
        def top_productos_devueltos(where_fecha_sql):
            rows_new = conn.execute(f"""
                SELECT p.nombre AS producto, COALESCE(SUM(di.cantidad),0) AS unidades
                FROM devoluciones_cab dc
                JOIN devoluciones_items di ON di.devolucion_id = dc.id
                JOIN productos p ON p.id = di.producto_id
                WHERE {where_fecha_sql}
                GROUP BY di.producto_id
                ORDER BY unidades DESC
                LIMIT 10
            """).fetchall()
            out = [{"producto": r["producto"], "unidades": int(r["unidades"] or 0)} for r in rows_new]
            if not out:
                try:
                    rows_old = conn.execute(f"""
                        SELECT p.nombre AS producto, COALESCE(SUM(d.cantidad),0) AS unidades
                        FROM devoluciones d
                        JOIN productos p ON p.id = d.producto_id
                        WHERE {where_fecha_sql.replace('dc.fecha','d.fecha')}
                        GROUP BY d.producto_id
                        ORDER BY unidades DESC
                        LIMIT 10
                    """).fetchall()
                    out = [{"producto": r["producto"], "unidades": int(r["unidades"] or 0)} for r in rows_old]
                except sqlite3.OperationalError:
                    pass
            return out

        productos_mas_devueltos = {
            "semana": top_productos_devueltos(f"dc.fecha >= {semana_ini}"),
            "mes":    top_productos_devueltos("strftime('%Y-%m', dc.fecha) = strftime('%Y-%m','now')"),
            "anio":   top_productos_devueltos("strftime('%Y', dc.fecha) = strftime('%Y','now')"),
        }

        # ========= Series ventas / gastos =========
        ventas_series = {
            "semanal": [
                {"bucket": r["sem"], "total": float(r["total"] or 0)}
                for r in conn.execute("""
                    SELECT strftime('%Y-W%W', fecha) AS sem, SUM(total) AS total
                    FROM ventas
                    GROUP BY sem
                    ORDER BY sem DESC LIMIT 12
                """).fetchall()[::-1]
            ],
            "mensual": [
                {"bucket": r["mes"], "total": float(r["total"] or 0)}
                for r in conn.execute("""
                    SELECT strftime('%Y-%m', fecha) AS mes, SUM(total) AS total
                    FROM ventas
                    GROUP BY mes
                    ORDER BY mes DESC LIMIT 12
                """).fetchall()[::-1]
            ],
            "anual": [
                {"bucket": r["anio"], "total": float(r["total"] or 0)}
                for r in conn.execute("""
                    SELECT strftime('%Y', fecha) AS anio, SUM(total) AS total
                    FROM ventas
                    GROUP BY anio
                    ORDER BY anio DESC LIMIT 5
                """).fetchall()[::-1]
            ],
        }

        gastos_series = {
            "semanal": [
                {"bucket": r["sem"], "total": float(r["total"] or 0)}
                for r in conn.execute("""
                    SELECT strftime('%Y-W%W', fecha) AS sem, SUM(monto) AS total
                    FROM gastos
                    GROUP BY sem
                    ORDER BY sem DESC LIMIT 12
                """).fetchall()[::-1]
            ],
            "mensual": [
                {"bucket": r["mes"], "total": float(r["total"] or 0)}
                for r in conn.execute("""
                    SELECT strftime('%Y-%m', fecha) AS mes, SUM(monto) AS total
                    FROM gastos
                    GROUP BY mes
                    ORDER BY mes DESC LIMIT 12
                """).fetchall()[::-1]
            ],
            "anual": [
                {"bucket": r["anio"], "total": float(r["total"] or 0)}
                for r in conn.execute("""
                    SELECT strftime('%Y', fecha) AS anio, SUM(monto) AS total
                    FROM gastos
                    GROUP BY anio
                    ORDER BY anio DESC LIMIT 5
                """).fetchall()[::-1]
            ],
        }

        # Respuesta
        return jsonify({
            # existentes
            "ventas_mensuales": ventas_mensuales,
            "ventas_semanales": ventas_semanales,
            "ventas_diarias": ventas_diarias,
            "productos_mas_devueltos_hist": productos_mas_devueltos_hist,
            # alertas
            "alertas": {
                "stock_bajo": [dict(r) for r in stock_bajo],
                "stock_cercano": [dict(r) for r in stock_cercano],
                "vencimiento_proximo": [dict(r) for r in venc_prox],
                "vencido": [dict(r) for r in vencidos],
                "perdida_vencimiento": float(perdida_vencimiento)
            },
            # KPIs del mes
            "kpis_mes": {
                "ventas_mes": float(ventas_mes),
                "gastos_mes": float(gastos_mes),
                "devoluciones_mes": float(devoluciones_mes),
                "bonificaciones_mes": float(bonificaciones_mes),
                "pagos_mes": float(pagos_mes)
            },
            # rendimiento
            "rendimiento_mes": rendimiento_mes,
            # rankings
            "mejores_vendedores": mejores_vendedores,
            "productos_mas_vendidos": productos_mas_vendidos,
            "productos_mas_devueltos": productos_mas_devueltos,
            "productos_mas_pedidos": productos_mas_pedidos,
            # series
            "series": {
                "ventas": ventas_series,
                "gastos": gastos_series
            }
        })
    finally:
        conn.close()

# ---------- Saldos por vendedor ----------
def _stats_por_vendedor(conn):
    stats = {}
    vendedores = conn.execute("SELECT id, nombre FROM vendedores").fetchall()
    # Agregamos 'bonificaciones' como alias claro de 'bonificado' para el template
    for v in vendedores:
        stats[v["id"]] = {
            "retirado": 0.0,
            "devuelto": 0.0,
            "pagado": 0.0,
            "bonificado": 0.0,
            "bonificaciones": 0.0,
            "saldo": 0.0
        }

    # Retirado por ventas
    for row in conn.execute("""
        SELECT v.id AS vendedor_id, COALESCE(SUM(ven.total), 0) AS retirado
          FROM vendedores v
          LEFT JOIN ventas ven ON ven.cliente = v.nombre
         GROUP BY v.id
    """):
        stats[row["vendedor_id"]]["retirado"] = float(row["retirado"] or 0)

    # Devuelto (nuevo esquema)
    try:
        for row in conn.execute("""
            SELECT vendedor_id, COALESCE(SUM(total),0) AS devuelto
              FROM devoluciones_cab
             GROUP BY vendedor_id
        """):
            vid = row["vendedor_id"]
            if vid in stats:
                stats[vid]["devuelto"] += float(row["devuelto"] or 0)
    except sqlite3.OperationalError:
        pass

    # Compat: tabla vieja 'devoluciones'
    try:
        for row in conn.execute("""
            SELECT v.id AS vendedor_id,
                   COALESCE(SUM(dev.cantidad * vi.precio), 0) AS devuelto_old
              FROM vendedores v
              LEFT JOIN ventas ven       ON ven.cliente = v.nombre
              LEFT JOIN devoluciones dev ON dev.venta_id = ven.id
              LEFT JOIN ventas_items vi  ON vi.venta_id = dev.venta_id AND vi.producto_id = dev.producto_id
             GROUP BY v.id
        """):
            vid = row["vendedor_id"]
            if vid in stats:
                stats[vid]["devuelto"] += float(row["devuelto_old"] or 0)
    except sqlite3.OperationalError:
        pass

    # Pagos a vendedores
    try:
        for row in conn.execute("""
            SELECT vendedor_id, COALESCE(SUM(monto), 0) AS pagado
              FROM pagos_vendedores
             GROUP BY vendedor_id
        """):
            vid = row["vendedor_id"]
            if vid in stats:
                stats[vid]["pagado"] = float(row["pagado"] or 0)
    except sqlite3.OperationalError:
        pass

    # Bonificaciones (sumatoria y alias)
    try:
        for row in conn.execute("""
            SELECT vendedor_id, COALESCE(SUM(monto), 0) AS boni
              FROM bonificaciones
             GROUP BY vendedor_id
        """):
            vid = row["vendedor_id"]
            if vid in stats:
                total_boni = float(row["boni"] or 0)
                stats[vid]["bonificado"] = total_boni
                stats[vid]["bonificaciones"] = total_boni
    except sqlite3.OperationalError:
        pass

    for vid, s in stats.items():
        s["saldo"] = s["retirado"] - s["devuelto"] - s["pagado"] - s["bonificado"]

    return stats

# ----------------- Ventas -----------------
@app.route('/ventas', methods=['GET', 'POST'])
def ventas():
    if request.method == 'POST':
        conn = get_db_connection()
        try:
            vendedor_id = request.form.get('vendedor_id')
            if not vendedor_id:
                flash('Seleccioná un vendedor.')
                return redirect(url_for('ventas'))
            try:
                vendedor_id_int = int(vendedor_id)
            except Exception:
                flash('Vendedor inválido.')
                return redirect(url_for('ventas'))

            vendedor_row = conn.execute(
                'SELECT id, nombre, comision, telefono FROM vendedores WHERE id = ?',
                (vendedor_id_int,)
            ).fetchone()
            vendedor_nombre = vendedor_row['nombre'] if vendedor_row else 'Vendedor'
            vendedor_tel    = vendedor_row['telefono'] if vendedor_row else None

            # Ítems (incluye pct[])
            productos  = request.form.getlist('producto[]')
            cantidades = request.form.getlist('cantidad[]')
            precios    = request.form.getlist('precio[]')
            pct_list   = request.form.getlist('pct[]')  # puede venir vacío por ítem

            if (not productos or not cantidades or not precios or
                len(productos) != len(cantidades) or len(productos) != len(precios)):
                flash('Faltan datos de ítems.')
                return redirect(url_for('ventas'))

            total_bruto = 0.0
            total_descuento = 0.0
            items = []

            for i in range(len(productos)):
                # parseo defensivo
                try:
                    pid_raw = productos[i]
                    qty_raw = cantidades[i] if i < len(cantidades) else '0'
                    prc_raw = precios[i]    if i < len(precios)    else '0'
                    pid = int(pid_raw)
                    qty = int(qty_raw)
                    prc = float((prc_raw or '0').replace(',', '.'))
                except (TypeError, ValueError, IndexError):
                    continue

                if qty <= 0:
                    continue

                prow = conn.execute('SELECT nombre, marca FROM productos WHERE id = ?', (pid,)).fetchone()
                prod_nombre = prow['nombre'] if prow else f'Producto {pid}'
                prod_marca  = prow['marca']  if prow else None

                subtotal = qty * prc
                total_bruto += subtotal

                # % comisión manual si el input no está vacío; si no, usar la regla vendedor+marca
                pct_manual = None
                if i < len(pct_list):
                    raw_pct = (pct_list[i] or '').strip()
                    if raw_pct != '':
                        try:
                            pct_manual = float(raw_pct.replace(',', '.'))
                        except Exception:
                            pct_manual = None

                pct = pct_manual if pct_manual is not None else _get_comision_pct(conn, vendedor_id_int, prod_marca)
                total_descuento += subtotal * (pct / 100.0)

                items.append({
                    'producto': pid,
                    'cantidad': qty,
                    'precio': prc,
                    'subtotal': subtotal,
                    'nombre': prod_nombre,
                    'marca': prod_marca,
                    'pct': pct,
                })

            if not items:
                flash('No se agregó ningún ítem válido.')
                return redirect(url_for('ventas'))

            # FEFO + descarga de stock
            try:
                for it in items:
                    consumir_stock_fefo(conn, it['producto'], it['cantidad'])
                    conn.execute(
                        "UPDATE productos SET cantidad = cantidad - ? WHERE id = ?",
                        (it['cantidad'], it['producto'])
                    )
            except ValueError as e:
                conn.rollback()
                flash(str(e))
                return redirect(url_for('ventas'))

            total_neto = total_bruto - total_descuento

            venta_id = conn.execute(
                'INSERT INTO ventas (cliente, total, fecha) VALUES (?, ?, ?)',
                (vendedor_nombre, total_neto, datetime.now().strftime('%Y-%m-%d'))
            ).lastrowid

            for it in items:
                conn.execute(
                    'INSERT INTO ventas_items (venta_id, producto_id, cantidad, precio) VALUES (?, ?, ?, ?)',
                    (venta_id, it['producto'], it['cantidad'], it['precio'])
                )

            # resumen del vendedor para el PDF
            stats_all = _stats_por_vendedor(conn)
            stats_v = stats_all.get(vendedor_id_int, None)

            conn.commit()
        finally:
            conn.close()

        fecha = datetime.now().strftime('%Y-%m-%d')
        generate_invoice_pdf(
            venta_id,
            vendedor_nombre,
            items,
            total_neto,
            fecha,
            vendedor_telefono=vendedor_tel,
            vendedor_id=vendedor_id_int,
            alias_transferencia=ALIAS_TRANSFERENCIA,
            stats=stats_v
        )
        flash('Venta registrada. Comisión aplicada por marca. Descargá la factura.')
        return redirect(url_for('descargar_factura', venta_id=venta_id))

    # -------------------- GET --------------------
    conn = get_db_connection()
    try:
        productos = conn.execute(
            'SELECT id, nombre, marca, precio_venta AS precio FROM productos WHERE cantidad > 0 ORDER BY nombre'
        ).fetchall()
        vendedores = conn.execute(
            'SELECT id, nombre, comision, telefono FROM vendedores ORDER BY nombre'
        ).fetchall()
        comisiones = conn.execute(
            'SELECT vendedor_id, marca, comision_pct FROM comisiones_vendedor_marca'
        ).fetchall()
        ventas_recientes = conn.execute(
            "SELECT id, cliente, fecha, total FROM ventas ORDER BY date(fecha) DESC, id DESC LIMIT 10"
        ).fetchall()
    finally:
        conn.close()

    return render_template('ventas.html',
                           productos=productos,
                           vendedores=vendedores,
                           comisiones=comisiones,
                           ventas_recientes=ventas_recientes)

# ---- Borrar venta ----
@app.route('/ventas/delete/<int:venta_id>', methods=['POST'], endpoint='ventas_delete')
def ventas_delete(venta_id):
    conn = get_db_connection()
    try:
        try:
            conn.execute('DELETE FROM ventas_items WHERE venta_id=?', (venta_id,))
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute('DELETE FROM devoluciones WHERE venta_id=?', (venta_id,))
        except sqlite3.OperationalError:
            pass
        conn.execute('DELETE FROM ventas WHERE id=?', (venta_id,))
        conn.commit()
        flash(f'Venta {venta_id} eliminada.')
    finally:
        conn.close()
    return redirect(url_for('ventas'))

# ----------------- Devoluciones (como ventas) -----------------
@app.route('/devoluciones', methods=['GET', 'POST'])
def devoluciones():
    conn = get_db_connection()
    try:
        ensure_schema()  # asegura tablas nuevas

        if request.method == 'POST':
            vendedor_id_raw = (request.form.get('vendedor_id') or '').strip()
            if not vendedor_id_raw:
                flash('Elegí un vendedor.')
                return redirect(url_for('devoluciones'))
            try:
                vendedor_id = int(vendedor_id_raw)
            except Exception:
                flash('Vendedor inválido.')
                return redirect(url_for('devoluciones'))

            vendedor_row = conn.execute(
                "SELECT id, nombre, comision FROM vendedores WHERE id=?", (vendedor_id,)
            ).fetchone()
            vendedor_nombre = vendedor_row["nombre"] if vendedor_row else "Vendedor"
            motivo = (request.form.get('motivo') or '').strip()

            productos  = request.form.getlist('producto[]')
            cantidades = request.form.getlist('cantidad[]')
            precios    = request.form.getlist('precio[]')
            pct_list   = request.form.getlist('pct[]')

            if not productos or len(productos) != len(cantidades) or len(productos) != len(precios):
                flash('Faltan datos de ítems.')
                return redirect(url_for('devoluciones'))

            total_bruto = 0.0
            total_desc  = 0.0
            items = []

            for i in range(len(productos)):
                try:
                    pid = int(productos[i])
                    qty = int((cantidades[i] or '0'))
                    prc = float((precios[i] or '0').replace(',', '.'))
                except Exception:
                    continue
                if qty <= 0:
                    continue

                prow = conn.execute("SELECT nombre, marca FROM productos WHERE id=?", (pid,)).fetchone()
                prod_marca = prow["marca"] if prow else None

                pct_manual = None
                if i < len(pct_list):
                    raw_pct = (pct_list[i] or '').strip()
                    if raw_pct != '':
                        try:
                            pct_manual = float(raw_pct.replace(',', '.'))
                        except Exception:
                            pct_manual = None
                pct = pct_manual if pct_manual is not None else _get_comision_pct(conn, vendedor_id, prod_marca)

                subtotal = qty * prc
                desc = subtotal * (pct/100.0)

                total_bruto += subtotal
                total_desc  += desc

                items.append({
                    "producto": pid,
                    "cantidad": qty,
                    "precio": prc,
                    "pct": pct,
                    "subtotal": subtotal
                })

            if not items:
                flash('No se agregó ningún renglón válido.')
                return redirect(url_for('devoluciones'))

            total_neto = total_bruto - total_desc  # neto a considerar como "devuelto"

            devolucion_id = conn.execute(
                "INSERT INTO devoluciones_cab (vendedor_id, vendedor_nombre, motivo, total, fecha) VALUES (?, ?, ?, ?, ?)",
                (vendedor_id, vendedor_nombre, motivo, total_neto, datetime.now().strftime('%Y-%m-%d'))
            ).lastrowid

            for it in items:
                conn.execute(
                    "INSERT INTO devoluciones_items (devolucion_id, producto_id, cantidad, precio, pct, subtotal) VALUES (?, ?, ?, ?, ?, ?)",
                    (devolucion_id, it['producto'], it['cantidad'], it['precio'], it['pct'], it['subtotal'])
                )
                # Reponer stock
                conn.execute(
                    "UPDATE productos SET cantidad = cantidad + ? WHERE id = ?",
                    (it['cantidad'], it['producto'])
                )

            conn.commit()
            flash('Devolución registrada.')
            return redirect(url_for('devoluciones'))

        # ---------- GET ----------
        productos = conn.execute(
            "SELECT id, nombre, marca, precio_venta AS precio FROM productos ORDER BY nombre"
        ).fetchall()
        vendedores = conn.execute(
            "SELECT id, nombre, comision, telefono FROM vendedores ORDER BY nombre"
        ).fetchall()
        comisiones = conn.execute(
            "SELECT vendedor_id, marca, comision_pct FROM comisiones_vendedor_marca"
        ).fetchall()

        devoluciones_recientes = conn.execute("""
            SELECT id, vendedor_nombre AS vendedor, fecha, total
              FROM devoluciones_cab
          ORDER BY date(fecha) DESC, id DESC
             LIMIT 10
        """).fetchall()

        return render_template(
            'devoluciones.html',
            vendedores=vendedores,
            productos=productos,
            comisiones=comisiones,
            devoluciones_recientes=devoluciones_recientes
        )
    finally:
        conn.close()

# ---- Editar cabecera de devolución (historial) ----
@app.route('/devoluciones/update', methods=['POST'], endpoint='devoluciones_update')
def devoluciones_update():
    try:
        did = int(request.form.get('id'))
    except Exception:
        flash('ID inválido.')
        return redirect(url_for('devoluciones'))

    fecha = (request.form.get('fecha') or '').strip()
    total_raw = request.form.get('total')
    vendedor_nombre = (request.form.get('vendedor') or '').strip()

    sets, params = [], []
    if vendedor_nombre:
        sets.append("vendedor_nombre = ?")
        params.append(vendedor_nombre)
    if fecha:
        sets.append("fecha = ?")
        params.append(fecha)
    if total_raw is not None:
        try:
            total = float((total_raw or '0').replace(',', '.'))
        except Exception:
            flash('Total inválido.')
            return redirect(url_for('devoluciones'))
        sets.append("total = ?")
        params.append(total)

    if not sets:
        flash('No se enviaron cambios.')
        return redirect(url_for('devoluciones'))

    params.append(did)

    conn = get_db_connection()
    try:
        conn.execute(f"UPDATE devoluciones_cab SET {', '.join(sets)} WHERE id = ?", params)
        conn.commit()
        flash('Devolución actualizada.')
    finally:
        conn.close()

    return redirect(url_for('devoluciones'))

# ---- Borrar devolución (revierte stock) ----
@app.route('/devoluciones/delete', methods=['POST'], endpoint='devoluciones_delete')
def devoluciones_delete():
    try:
        did = int(request.form.get('id'))
    except Exception:
        flash('ID inválido.')
        return redirect(url_for('devoluciones'))

    conn = get_db_connection()
    try:
        # Revertir stock previamente repuesto
        items = conn.execute(
            "SELECT producto_id, cantidad FROM devoluciones_items WHERE devolucion_id = ?",
            (did,)
        ).fetchall()
        for it in items:
            conn.execute(
                "UPDATE productos SET cantidad = cantidad - ? WHERE id = ?",
                (int(it['cantidad'] or 0), int(it['producto_id']))
            )

        conn.execute("DELETE FROM devoluciones_items WHERE devolucion_id = ?", (did,))
        conn.execute("DELETE FROM devoluciones_cab   WHERE id = ?", (did,))
        conn.commit()
        flash('Devolución eliminada.')
    finally:
        conn.close()

    return redirect(url_for('devoluciones'))

# ----------------- Pagos a Vendedores -----------------
@app.route('/pagos', methods=['GET', 'POST'])
def pagos():
    ensure_schema()
    if request.method == 'POST':
        # Esperamos: vendedor_id, medio_pago (efectivo/transferencia), monto, fecha y descripcion
        try:
            vendedor_id = int(request.form.get('vendedor_id') or '0')
        except Exception:
            vendedor_id = 0
        if vendedor_id <= 0:
            flash('Elegí un vendedor válido.')
            return redirect(url_for('pagos'))

        medio_pago = (request.form.get('medio_pago') or 'efectivo').strip().lower()
        if medio_pago not in ('efectivo', 'transferencia'):
            medio_pago = 'efectivo'

        try:
            monto = float((request.form.get('monto') or '0').replace(',', '.'))
        except Exception:
            flash('Monto inválido.')
            return redirect(url_for('pagos'))
        if monto <= 0:
            flash('El monto debe ser mayor a 0.')
            return redirect(url_for('pagos'))

        fecha = (request.form.get('fecha') or '').strip() or datetime.now().strftime('%Y-%m-%d')
        descripcion = (request.form.get('descripcion') or '').strip()

        conn = get_db_connection()
        try:
            # Aseguramos columna medio_pago (por si venías de schema viejo)
            _add_col_if_missing(conn, "pagos_vendedores", "medio_pago TEXT")

            conn.execute(
                "INSERT INTO pagos_vendedores (vendedor_id, monto, fecha, descripcion, medio_pago) VALUES (?, ?, ?, ?, ?)",
                (vendedor_id, monto, fecha, descripcion, medio_pago)
            )
            conn.commit()
            flash('Pago registrado.')
        finally:
            conn.close()

        return redirect(url_for('pagos'))

    # GET
    conn = get_db_connection()
    try:
        vendedores = conn.execute("SELECT id, nombre FROM vendedores ORDER BY nombre").fetchall()
        pagos_list = conn.execute("""
            SELECT p.id, p.vendedor_id, v.nombre AS vendedor, p.medio_pago, p.monto, p.descripcion, p.fecha
              FROM pagos_vendedores p
              JOIN vendedores v ON v.id = p.vendedor_id
          ORDER BY date(p.fecha) DESC, p.id DESC
        """).fetchall()
        hoy = datetime.now().strftime('%Y-%m-%d')
        return render_template('pagos.html', vendedores=vendedores, pagos=pagos_list, hoy=hoy)
    finally:
        conn.close()

@app.route('/pagos/update', methods=['POST'], endpoint='pagos_update')
def pagos_update():
    try:
        pid = int(request.form.get('id'))
    except Exception:
        flash('ID inválido.')
        return redirect(url_for('pagos'))

    vendedor_id = request.form.get('vendedor_id')  # opcional
    fecha = (request.form.get('fecha') or '').strip()
    medio_pago = (request.form.get('medio_pago') or '').strip().lower()
    descripcion = (request.form.get('descripcion') or '').strip()
    monto_raw = request.form.get('monto')

    monto = None
    if monto_raw is not None:
        try:
            monto = float((monto_raw or '0').replace(',', '.'))
        except Exception:
            flash('Monto inválido.')
            return redirect(url_for('pagos'))

    sets, params = [], []

    if vendedor_id:
        try:
            sets.append("vendedor_id = ?")
            params.append(int(vendedor_id))
        except Exception:
            flash('Vendedor inválido.')
            return redirect(url_for('pagos'))

    if fecha:
        sets.append("fecha = ?")
        params.append(fecha)

    if medio_pago in ('efectivo', 'transferencia'):
        sets.append("medio_pago = ?")
        params.append(medio_pago)

    if descripcion is not None:
        sets.append("descripcion = ?")
        params.append(descripcion)

    if monto is not None:
        sets.append("monto = ?")
        params.append(monto)

    if not sets:
        flash('No se enviaron cambios.')
        return redirect(url_for('pagos'))

    params.append(pid)

    conn = get_db_connection()
    try:
        conn.execute(f"UPDATE pagos_vendedores SET {', '.join(sets)} WHERE id = ?", tuple(params))
        conn.commit()
        flash('Pago actualizado.')
    finally:
        conn.close()

    return redirect(url_for('pagos'))

@app.route('/pagos/delete', methods=['POST'], endpoint='pagos_delete')
def pagos_delete():
    try:
        pid = int(request.form.get('id'))
    except Exception:
        flash('ID inválido.')
        return redirect(url_for('pagos'))

    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM pagos_vendedores WHERE id = ?", (pid,))
        conn.commit()
        flash('Pago eliminado.')
    finally:
        conn.close()

    return redirect(url_for('pagos'))

# ----------------- Gastos -----------------
@app.route('/gastos', methods=['GET', 'POST'], endpoint='gastos')
def gastos_view():
    """
    GET: renderiza la página con el listado.
    POST: crea un gasto y redirige a /gastos (GET).
    """
    conn = get_db_connection()
    try:
        if request.method == 'POST':
            # Campos (tipo se usa como descripción visible)
            tipo = (request.form.get('tipo') or '').strip()
            # El front envía 'monto' como string decimal con punto (p.ej. "1234.56")
            try:
                monto = float((request.form.get('monto') or '0').replace(',', '.'))
            except Exception:
                monto = 0.0

            if not tipo or monto <= 0:
                flash('Completá descripción y un monto válido.')
            else:
                conn.execute(
                    'INSERT INTO gastos (tipo, monto, descripcion, fecha) VALUES (?, ?, ?, ?)',
                    (tipo, monto, '', datetime.now().strftime('%Y-%m-%d'))
                )
                conn.commit()
                flash('Gasto registrado exitosamente!')
                return redirect(url_for('gastos'))  # sólo después de crear

        # GET (o POST inválido que cae a render)
        gastos_list = conn.execute('SELECT * FROM gastos ORDER BY date(fecha) DESC, id DESC').fetchall()
        return render_template('gastos.html', gastos=gastos_list)

    finally:
        conn.close()

@app.route('/gastos/update', methods=['POST'], endpoint='gastos_update')
def gastos_update():
    """
    Actualiza un gasto (descripcion y/o monto). Siempre redirige a /gastos (GET) al terminar.
    """
    try:
        gid = int(request.form.get('id'))
    except Exception:
        flash('ID inválido.')
        return redirect(url_for('gastos'))

    tipo  = (request.form.get('tipo') or '').strip()
    monto_raw = request.form.get('monto')
    monto = None
    if monto_raw is not None:
        try:
            monto = float((monto_raw or '0').replace(',', '.'))
        except Exception:
            flash('Monto inválido.')
            return redirect(url_for('gastos'))

    if not tipo and monto is None:
        flash('No se enviaron cambios.')
        return redirect(url_for('gastos'))

    # Construimos UPDATE dinámico según lo que llegó
    sets, params = [], []
    if tipo:
        sets.append('tipo = ?')
        params.append(tipo)
    if monto is not None:
        sets.append('monto = ?')
        params.append(monto)
    params.append(gid)

    conn = get_db_connection()
    try:
        conn.execute(f"UPDATE gastos SET {', '.join(sets)} WHERE id = ?", params)
        conn.commit()
        flash('Gasto actualizado.')
    finally:
        conn.close()

    return redirect(url_for('gastos'))

@app.route('/gastos/delete', methods=['POST'], endpoint='gastos_delete')
def gastos_delete():
    """
    Elimina un gasto por ID. Redirige a /gastos (GET).
    """
    try:
        gid = int(request.form.get('id'))
    except Exception:
        flash('ID inválido.')
        return redirect(url_for('gastos'))

    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM gastos WHERE id = ?", (gid,))
        conn.commit()
        flash('Gasto eliminado.')
    finally:
        conn.close()

    return redirect(url_for('gastos'))

# ----------------- Inventario -----------------
@app.route('/inventario', methods=['GET'], endpoint='inventario')
def inventario_view():
    q = (request.args.get('q') or '').strip()
    marca_filtro = (request.args.get('marca') or '').strip()

    conn = get_db_connection()
    try:
        where = ["1=1"]
        params = []
        if marca_filtro:
            where.append("p.marca = ?")
            params.append(marca_filtro)
        if q:
            where.append("(p.nombre LIKE ? OR p.marca LIKE ?)")
            like = f"%{q}%"
            params.extend([like, like])

        productos = conn.execute(
            f"""SELECT p.id, p.nombre, p.marca, p.cantidad, p.cantidad_minima,
                         p.precio_compra, p.precio_venta
                FROM productos p
                WHERE {' AND '.join(where)}
                ORDER BY p.nombre""",
            params
        ).fetchall()

        marcas = conn.execute(
            "SELECT DISTINCT marca FROM productos WHERE marca IS NOT NULL AND TRIM(marca) <> '' ORDER BY marca"
        ).fetchall()

        productos_vencimiento = conn.execute(
            """SELECT ms.id AS mov_id, p.nombre, p.marca, ms.fecha_vencimiento, ms.cantidad_restante
               FROM movimientos_stock ms
               JOIN productos p ON p.id = ms.producto_id
               WHERE ms.tipo = 'entrada'
                 AND ms.fecha_vencimiento IS NOT NULL
                 AND ms.fecha_vencimiento <= date('now', '+30 days')
               ORDER BY ms.fecha_vencimiento"""
        ).fetchall()
    finally:
        conn.close()

    return render_template('inventario.html',
                           productos=productos,
                           productos_vencimiento=productos_vencimiento,
                           marcas=marcas,
                           q=q,
                           marca_actual=marca_filtro)

# ---- Exportar inventario a CSV ----
@app.route('/inventario/export', methods=['GET'])
def inventario_export():
    q = (request.args.get('q') or '').strip()
    marca = (request.args.get('marca') or '').strip()

    conn = get_db_connection()
    try:
        where = ["1=1"]
        params = []
        if marca:
            where.append("p.marca = ?")
            params.append(marca)
        if q:
            where.append("(p.nombre LIKE ? OR p.marca LIKE ?)")
            like = f"%{q}%"
            params.extend([like, like])

        rows = conn.execute(
            f"""SELECT p.marca, p.nombre, p.precio_compra, p.precio_venta, p.cantidad, p.cantidad_minima
                FROM productos p
                WHERE {' AND '.join(where)}
                ORDER BY p.nombre""",
            params
        ).fetchall()
    finally:
        conn.close()

    out = StringIO(newline="")
    writer = csv.writer(out)
    writer.writerow(["Marca", "Producto", "Precio Compra", "Precio Venta", "Margen %", "Cantidad", "Cant. mínima"])

    for r in rows:
        pc = float(r['precio_compra'] or 0)
        pv = float(r['precio_venta'] or 0)
        margen = ((pv - pc) / pc * 100.0) if pc else 0.0
        writer.writerow([
            r['marca'] or "",
            r['nombre'] or "",
            f"{pc:.2f}",
            f"{pv:.2f}",
            f"{margen:.1f}",
            int(r['cantidad'] or 0),
            int(r['cantidad_minima'] or 0),
        ])

    filename = f"inventario_{datetime.now().strftime('%Y-%m-%d')}.csv"
    return Response(
        out.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
# ---- Imprimir lista de precios (PDF) ----
@app.route('/inventario/print', methods=['GET'])
def inventario_print():
    q = (request.args.get('q') or '').strip()
    marca = (request.args.get('marca') or '').strip()

    conn = get_db_connection()
    try:
        where = ["1=1"]
        params = []
        if marca:
            where.append("p.marca = ?")
            params.append(marca)
        if q:
            where.append("(p.nombre LIKE ? OR p.marca LIKE ?)")
            like = f"%{q}%"
            params.extend([like, like])

        rows = conn.execute(
            f"""SELECT p.marca, p.nombre, p.precio_venta
                  FROM productos p
                 WHERE {' AND '.join(where)}
              ORDER BY p.marca, p.nombre""",
            params
        ).fetchall()

        # Convertimos sqlite3.Row -> dict para el generador de PDF
        productos = [dict(r) for r in rows]
    finally:
        conn.close()

    # Generar PDF y enviarlo
    filename = generate_price_list_pdf(
        productos,
        fecha=datetime.now().strftime("%Y-%m-%d %H:%M")
    )
    return send_file(filename, as_attachment=True)

# ----------------- Vendedores -----------------
@app.route('/vendedores', methods=['GET', 'POST'], endpoint='vendedores')
def vendedores_view():
    if request.method == 'POST':
        nombre = request.form.get('nombre', '').strip()
        telefono = request.form.get('telefono', '').strip()
        email = request.form.get('email', '').strip()
        try:
            comision = float((request.form.get('comision', '0') or '0').replace(',', '.'))
        except (TypeError, ValueError):
            flash('Comisión inválida.')
            return redirect(url_for('vendedores'))

        if not nombre:
            flash('Ingresá el nombre del vendedor.')
            return redirect(url_for('vendedores'))

        conn = get_db_connection()
        try:
            ensure_schema()
            conn.execute(
                'INSERT INTO vendedores (nombre, telefono, email, comision) VALUES (?, ?, ?, ?)',
                (nombre, telefono, email, comision)
            )
            conn.commit()
        finally:
            conn.close()

        flash('Vendedor agregado exitosamente!')
        return redirect(url_for('vendedores'))

    conn = get_db_connection()
    try:
        vendedores_list = conn.execute('SELECT * FROM vendedores ORDER BY nombre').fetchall()
        stats = _stats_por_vendedor(conn)
    finally:
        conn.close()

    return render_template('vendedores.html', vendedores=vendedores_list, stats=stats)

@app.route('/vendedores/update', methods=['POST'], endpoint='vendedores_update')
def vendedores_update():
    try:
        vendedor_id = int(request.form.get('vendedor_id'))
    except Exception:
        flash('ID de vendedor inválido.')
        return redirect(url_for('vendedores'))

    nombre = (request.form.get('nombre') or '').strip()
    telefono = (request.form.get('telefono') or '').strip()
    email = (request.form.get('email') or '').strip()
    try:
        comision = float((request.form.get('comision', '0') or '0').replace(',', '.'))
    except Exception:
        comision = 0.0

    if not nombre:
        flash('El nombre no puede estar vacío.')
        return redirect(url_for('vendedores'))

    conn = get_db_connection()
    try:
        updated = conn.execute(
            "UPDATE vendedores SET nombre=?, telefono=?, email=?, comision=? WHERE id=?",
            (nombre, telefono, email, comision, vendedor_id)
        )
        conn.commit()
        if updated.rowcount:
            flash('Vendedor actualizado.')
        else:
            flash('No se encontró el vendedor a actualizar.')
    finally:
        conn.close()

    return redirect(url_for('vendedores'))

@app.route('/vendedores/delete', methods=['POST'], endpoint='vendedores_delete')
def vendedores_delete():
    try:
        vendedor_id = int(request.form.get('vendedor_id'))
    except Exception:
        flash('ID de vendedor inválido.')
        return redirect(url_for('vendedores'))

    conn = get_db_connection()
    try:
        try:
            conn.execute("DELETE FROM comisiones_vendedor_marca WHERE vendedor_id=?", (vendedor_id,))
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("DELETE FROM pagos_vendedores WHERE vendedor_id=?", (vendedor_id,))
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("DELETE FROM bonificaciones WHERE vendedor_id=?", (vendedor_id,))
        except sqlite3.OperationalError:
            pass

        deleted = conn.execute("DELETE FROM vendedores WHERE id=?", (vendedor_id,))
        conn.commit()
        if deleted.rowcount:
            flash('Vendedor eliminado.')
        else:
            flash('No se encontró el vendedor a eliminar.')
    finally:
        conn.close()

    return redirect(url_for('vendedores'))

# ----------------- Comisiones por vendedor y marca -----------------
@app.route('/vendedores/comisiones', methods=['GET'], endpoint='vendedores_comisiones')
def vendedores_comisiones_view():
    filtro_vendedor_id = (request.args.get('vendedor_id') or '').strip()

    conn = get_db_connection()
    try:
        vendedores_all = conn.execute("SELECT id, nombre FROM vendedores ORDER BY nombre").fetchall()

        params = []
        where = []
        if filtro_vendedor_id:
            where.append("c.vendedor_id = ?")
            params.append(int(filtro_vendedor_id))

        comisiones = conn.execute(
            f"""SELECT c.id, c.vendedor_id, c.marca, c.comision_pct, v.nombre AS vendedor_nombre
                FROM comisiones_vendedor_marca c
                JOIN vendedores v ON v.id = c.vendedor_id
                {"WHERE " + " AND ".join(where) if where else ""}
                ORDER BY v.nombre, c.marca""",
            params
        ).fetchall()
    finally:
        conn.close()

    return render_template(
        'vendedores_comisiones.html',
        comisiones=comisiones,
        vendedores_all=vendedores_all,
        filtro_vendedor_id=filtro_vendedor_id
    )

@app.route('/vendedores/comisiones/add', methods=['POST'], endpoint='vendedores_comisiones_add')
def vendedores_comisiones_add():
    try:
        vendedor_id = int(request.form.get('vendedor_id'))
    except Exception:
        flash('Vendedor inválido.')
        return redirect(url_for('vendedores_comisiones'))

    marca = _norm_marca(request.form.get('marca'))
    try:
        pct = float((request.form.get('comision_pct') or '0').replace(',', '.'))
    except Exception:
        pct = 0.0

    if not marca:
        flash('Ingresá la marca.')
        return redirect(url_for('vendedores_comisiones', vendedor_id=vendedor_id))

    conn = get_db_connection()
    try:
        conn.execute(
            """INSERT INTO comisiones_vendedor_marca (vendedor_id, marca, comision_pct)
               VALUES (?, ?, ?)
               ON CONFLICT(vendedor_id, marca) DO UPDATE SET comision_pct=excluded.comision_pct
            """,
            (vendedor_id, marca, pct)
        )
        conn.commit()
        flash('Comisión guardada.')
    finally:
        conn.close()

    return redirect(url_for('vendedores_comisiones', vendedor_id=vendedor_id))

@app.route('/vendedores/comisiones/update', methods=['POST'], endpoint='vendedores_comisiones_update')
def vendedores_comisiones_update():
    try:
        cid = int(request.form.get('id'))
        pct = float((request.form.get('comision_pct') or '0').replace(',', '.'))
    except Exception:
        flash('Datos inválidos.')
        return redirect(url_for('vendedores_comisiones'))

    conn = get_db_connection()
    try:
        conn.execute("UPDATE comisiones_vendedor_marca SET comision_pct=? WHERE id=?", (pct, cid))
        conn.commit()
        flash('Comisión actualizada.')
    finally:
        conn.close()

    return redirect(url_for('vendedores_comisiones'))

@app.route('/vendedores/comisiones/delete', methods=['POST'], endpoint='vendedores_comisiones_delete')
def vendedores_comisiones_delete():
    try:
        cid = int(request.form.get('id'))
    except Exception:
        flash('ID inválido.')
        return redirect(url_for('vendedores_comisiones'))

    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM comisiones_vendedor_marca WHERE id=?", (cid,))
        conn.commit()
        flash('Comisión eliminada.')
    finally:
        conn.close()

    return redirect(url_for('vendedores_comisiones'))

# ----------------- Proveedores -----------------
@app.route('/proveedores', methods=['GET', 'POST'])
def proveedores():
    conn = get_db_connection()
    try:
        ensure_schema()

        if request.method == 'POST':
            proveedor = (request.form.get('proveedor') or '').strip()
            fecha = (request.form.get('fecha') or '').strip() or datetime.now().strftime('%Y-%m-%d')
            medio_pago = (request.form.get('medio_pago') or 'efectivo').strip().lower()

            def to_float(x):
                try:
                    return float((x or '0').replace(',', '.'))
                except Exception:
                    return 0.0

            monto_bruto = to_float(request.form.get('monto_bruto'))
            comision_pct = to_float(request.form.get('comision_pct')) if medio_pago == 'cheque' else 0.0
            monto_neto = monto_bruto - (monto_bruto * comision_pct / 100.0)
            descripcion = (request.form.get('descripcion') or '').strip()

            if not proveedor:
                flash('Ingresá el nombre del proveedor.')
                return redirect(url_for('proveedores'))

            conn.execute(
                '''INSERT INTO pagos_proveedores
                   (proveedor, medio_pago, monto_bruto, comision_pct, monto_neto, descripcion, fecha)
                   VALUES (?, ?, ?, ?, ?, ?, ?)''',
                (proveedor, medio_pago, monto_bruto, comision_pct, monto_neto, descripcion, fecha)
            )

            try:
                conn.execute('UPDATE pagos_proveedores SET monto = monto_neto WHERE rowid = last_insert_rowid()')
            except sqlite3.OperationalError:
                pass

            conn.commit()
            flash('Pago a proveedor registrado exitosamente!')
            return redirect(url_for('proveedores'))

        pagos = conn.execute(
            'SELECT * FROM pagos_proveedores ORDER BY date(fecha) DESC, id DESC'
        ).fetchall()

        hoy = datetime.now().strftime('%Y-%m-%d')
        return render_template('proveedores.html', pagos=pagos, hoy=hoy)

    finally:
        conn.close()

# ----------------- Bonificaciones -----------------
@app.route('/bonificaciones', methods=['GET', 'POST'])
def bonificaciones():
    conn = get_db_connection()
    try:
        ensure_schema()

        if request.method == 'POST':
            # ---- POST: crear bonificación ----
            try:
                vendedor_id = int(request.form.get('vendedor_id'))
            except Exception:
                flash('Vendedor inválido.')
                return redirect(url_for('bonificaciones'))

            try:
                monto = float((request.form.get('monto') or '0').replace(',', '.'))
            except Exception:
                flash('Monto inválido.')
                return redirect(url_for('bonificaciones'))

            if monto <= 0:
                flash('El monto debe ser mayor a 0.')
                return redirect(url_for('bonificaciones'))

            fecha = (request.form.get('fecha') or '').strip() or datetime.now().strftime('%Y-%m-%d')
            descripcion = (request.form.get('descripcion') or '').strip()

            conn.execute(
                "INSERT INTO bonificaciones (vendedor_id, monto, fecha, descripcion) VALUES (?, ?, ?, ?)",
                (vendedor_id, monto, fecha, descripcion)
            )
            conn.commit()
            flash('Bonificación registrada.')
            return redirect(url_for('bonificaciones'))

        # ---- GET: listar ----
        vendedores = conn.execute("SELECT id, nombre, comision FROM vendedores ORDER BY nombre").fetchall()
        productos = conn.execute("SELECT id, nombre, marca, precio_venta FROM productos ORDER BY nombre").fetchall()
        comisiones = conn.execute("SELECT vendedor_id, marca, comision_pct FROM comisiones_vendedor_marca").fetchall()
        bonis = conn.execute(
            """SELECT b.id, b.fecha, b.monto, b.descripcion,
                      v.nombre AS vendedor, v.id AS vendedor_id
               FROM bonificaciones b
               JOIN vendedores v ON v.id = b.vendedor_id
               ORDER BY date(b.fecha) DESC, b.id DESC"""
        ).fetchall()
        hoy = datetime.now().strftime('%Y-%m-%d')

        return render_template(
            'bonificaciones.html',
            vendedores=vendedores,
            productos=productos,
            comisiones=comisiones,
            bonificaciones=bonis,
            hoy=hoy
        )
    finally:
        conn.close()


# Alias para formularios que apunten a bonificaciones_add (por tu dashboard)
@app.route('/bonificaciones/add', methods=['POST'], endpoint='bonificaciones_add')
def bonificaciones_add():
    conn = get_db_connection()
    try:
        ensure_schema()

        try:
            vendedor_id = int(request.form.get('vendedor_id'))
        except Exception:
            flash('Vendedor inválido.')
            return redirect(url_for('bonificaciones'))

        try:
            monto = float((request.form.get('monto') or '0').replace(',', '.'))
        except Exception:
            flash('Monto inválido.')
            return redirect(url_for('bonificaciones'))

        if monto <= 0:
            flash('El monto debe ser mayor a 0.')
            return redirect(url_for('bonificaciones'))

        fecha = (request.form.get('fecha') or '').strip() or datetime.now().strftime('%Y-%m-%d')
        descripcion = (request.form.get('descripcion') or '').strip()

        conn.execute(
            "INSERT INTO bonificaciones (vendedor_id, monto, fecha, descripcion) VALUES (?, ?, ?, ?)",
            (vendedor_id, monto, fecha, descripcion)
        )
        conn.commit()
        flash('Bonificación registrada.')
    finally:
        conn.close()

    return redirect(url_for('bonificaciones'))


@app.route('/bonificaciones/update', methods=['POST'], endpoint='bonificaciones_update')
def bonificaciones_update():
    """
    Actualiza campos editables de una bonificación:
    - vendedor_id (opcional)
    - fecha (opcional)
    - descripcion (opcional, puede ser vacío)
    - monto (opcional, formato decimal con punto)
    """
    try:
        bid = int(request.form.get('id'))
    except Exception:
        flash('ID inválido.')
        return redirect(url_for('bonificaciones'))

    vendedor_id = request.form.get('vendedor_id')
    fecha       = (request.form.get('fecha') or '').strip()
    descripcion = (request.form.get('descripcion') or '').strip()
    monto_raw   = request.form.get('monto')

    monto = None
    if monto_raw is not None:
        try:
            monto = float((monto_raw or '0').replace(',', '.'))
        except Exception:
            flash('Monto inválido.')
            return redirect(url_for('bonificaciones'))

    sets, params = [], []

    if vendedor_id:
        try:
            sets.append("vendedor_id=?")
            params.append(int(vendedor_id))
        except Exception:
            flash('Vendedor inválido.')
            return redirect(url_for('bonificaciones'))

    if fecha:
        sets.append("fecha=?")
        params.append(fecha)

    # descripción siempre la seteamos (permitimos vacío)
    if descripcion is not None:
        sets.append("descripcion=?")
        params.append(descripcion)

    if monto is not None:
        sets.append("monto=?")
        params.append(monto)

    if not sets:
        flash('No se enviaron cambios.')
        return redirect(url_for('bonificaciones'))

    params.append(bid)

    conn = get_db_connection()
    try:
        conn.execute(f"UPDATE bonificaciones SET {', '.join(sets)} WHERE id=?", tuple(params))
        conn.commit()
        flash('Bonificación actualizada.')
    finally:
        conn.close()

    return redirect(url_for('bonificaciones'))


@app.route('/bonificaciones/delete', methods=['POST'], endpoint='bonificaciones_delete')
def bonificaciones_delete():
    try:
        bid = int(request.form.get('id'))
    except Exception:
        flash('ID inválido.')
        return redirect(url_for('bonificaciones'))

    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM bonificaciones WHERE id=?", (bid,))
        conn.commit()
        flash('Bonificación eliminada.')
    finally:
        conn.close()

    return redirect(url_for('bonificaciones'))

# ----------------- Stock -----------------
@app.route('/stock', methods=['GET', 'POST'])
def stock():
    if request.method == 'POST':
        op = (request.form.get('op') or 'ingreso').strip().lower()

        def to_int_from_input(val, field_name):
            raw = (val or '').strip()
            if not raw:
                raise ValueError(f'Falta el campo: {field_name}')
            try:
                return int(float(raw.replace(',', '.')))
            except Exception:
                raise ValueError(f'Valor inválido en "{field_name}": {raw}')

        conn = get_db_connection()
        try:
            ensure_schema()

            if op == 'ingreso':
                try:
                    producto_id = to_int_from_input(request.form.get('producto_id'), 'Producto')
                    cantidad    = to_int_from_input(request.form.get('cantidad'), 'Cantidad')
                except ValueError as e:
                    flash(str(e))
                    return redirect(url_for('stock'))

                if cantidad <= 0:
                    flash('La cantidad debe ser mayor a 0.')
                    return redirect(url_for('stock'))

                fecha_vencimiento = request.form.get('fecha_vencimiento') or None
                proveedor_id = None
                lote = None

                conn.execute(
                    'UPDATE productos SET cantidad = cantidad + ? WHERE id = ?',
                    (cantidad, producto_id)
                )
                conn.execute(
                    '''INSERT INTO movimientos_stock
                       (producto_id, cantidad, cantidad_restante, tipo, proveedor_id, fecha_vencimiento, lote, fecha)
                       VALUES (?, ?, ?, 'entrada', ?, ?, ?, ?)''',
                    (producto_id, cantidad, cantidad, proveedor_id, fecha_vencimiento, lote, datetime.now().strftime('%Y-%m-%d'))
                )
                conn.commit()
                flash('Stock actualizado exitosamente!')

            elif op == 'nuevo':
                nombre = (request.form.get('nombre') or '').strip()
                marca  = (request.form.get('marca') or '').strip()
                try:
                    precio_compra   = float((request.form.get('precio_compra') or '0').replace(',', '.'))
                    precio_venta    = float((request.form.get('precio_venta')  or '0').replace(',', '.'))
                    cantidad_minima = to_int_from_input(request.form.get('cantidad_minima', '0'), 'Cantidad mínima')
                except ValueError as e:
                    flash(str(e))
                    return redirect(url_for('stock'))

                if not nombre or not marca:
                    flash('Completá marca y nombre del artículo.')
                    return redirect(url_for('stock'))

                conn.execute(
                    '''INSERT INTO productos (nombre, marca, precio_compra, precio_venta, cantidad, cantidad_minima)
                       VALUES (?, ?, ?, ?, 0, ?)''',
                    (nombre, marca, precio_compra, precio_venta, cantidad_minima)
                )
                conn.commit()
                flash('Producto creado correctamente.')

            elif op == 'modificar':
                try:
                    producto_id_mod = to_int_from_input(request.form.get('producto_id_mod'), 'Producto a modificar')
                except ValueError as e:
                    flash(str(e))
                    return redirect(url_for('stock'))

                nombre = (request.form.get('nombre') or '').strip()
                marca  = (request.form.get('marca') or '').strip()
                try:
                    precio_compra   = float((request.form.get('precio_compra') or '0').replace(',', '.'))
                    precio_venta    = float((request.form.get('precio_venta')  or '0').replace(',', '.'))
                    cantidad_minima = to_int_from_input(request.form.get('cantidad_minima', '0'), 'Cantidad mínima')
                except ValueError as e:
                    flash(str(e))
                    return redirect(url_for('stock'))

                if not nombre or not marca:
                    flash('Completá marca y nombre del artículo.')
                    return redirect(url_for('stock'))

                updated = conn.execute(
                    '''UPDATE productos
                       SET nombre = ?, marca = ?, precio_compra = ?, precio_venta = ?, cantidad_minima = ?
                       WHERE id = ?''',
                    (nombre, marca, precio_compra, precio_venta, cantidad_minima, producto_id_mod)
                )
                conn.commit()
                flash('Producto modificado correctamente.' if updated.rowcount else 'No se encontró el producto a modificar.')

            elif op == 'eliminar':
                # id del producto a eliminar (desde inventario.html: producto_id_del)
                try:
                    producto_id_del = to_int_from_input(request.form.get('producto_id_del'), 'Producto a eliminar')
                except ValueError as e:
                    flash(str(e))
                    return redirect(url_for('stock'))

                # Verificar existencia
                row = conn.execute(
                    "SELECT id, nombre FROM productos WHERE id = ?",
                    (producto_id_del,)
                ).fetchone()
                if not row:
                    flash('No se encontró el producto a eliminar.')
                    return redirect(url_for('stock'))

                # Evitar borrar si el producto tiene referencias en ventas/devoluciones
                refs = 0
                try:
                    c = conn.execute(
                        "SELECT COUNT(*) FROM ventas_items WHERE producto_id = ?",
                        (producto_id_del,)
                    ).fetchone()
                    refs += int((c[0] if isinstance(c, tuple) else c["COUNT(*)"]) if c is not None else 0)
                except sqlite3.OperationalError:
                    pass
                try:
                    c = conn.execute(
                        "SELECT COUNT(*) FROM devoluciones_items WHERE producto_id = ?",
                        (producto_id_del,)
                    ).fetchone()
                    refs += int((c[0] if isinstance(c, tuple) else c["COUNT(*)"]) if c is not None else 0)
                except sqlite3.OperationalError:
                    pass

                if refs > 0:
                    flash('No se puede eliminar: el producto tiene movimientos (ventas/devoluciones) asociados.')
                    return redirect(url_for('stock'))

                # Borrar movimientos/lotes del producto (si existieran)
                try:
                    conn.execute("DELETE FROM movimientos_stock WHERE producto_id = ?", (producto_id_del,))
                except sqlite3.OperationalError:
                    pass

                # Borrar el producto
                conn.execute("DELETE FROM productos WHERE id = ?", (producto_id_del,))
                conn.commit()
                flash('Producto eliminado correctamente.')

            else:
                flash('Operación inválida.')

        finally:
            conn.close()

        return redirect(url_for('stock'))

    # GET
    ensure_schema()
    conn = get_db_connection()
    try:
        productos = conn.execute(
            'SELECT id, nombre, marca, precio_compra, precio_venta FROM productos ORDER BY nombre'
        ).fetchall()
    finally:
        conn.close()

    return render_template('stock.html', productos=productos)

# ----------------- Descarga de Factura -----------------
@app.route('/descargar_factura/<int:venta_id>')
def descargar_factura(venta_id):
    filename = f"facturas/factura_{venta_id}.pdf"
    return send_file(filename, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
