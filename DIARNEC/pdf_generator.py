# pdf_generator.py
from __future__ import annotations

import os
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, KeepTogether
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle


def _fmt_money(n: float) -> str:
    try:
        v = float(n or 0)
    except Exception:
        v = 0.0
    s = f"{v:,.2f}"
    return "$ " + s.replace(",", "_").replace(".", ",").replace("_", ".")

def _pct(n: float) -> str:
    try:
        v = float(n or 0)
    except Exception:
        v = 0.0
    return f"{v:.1f}%"

def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def generate_invoice_pdf(
    venta_id: int,
    vendedor_nombre: str,
    items: list[dict],
    total_neto: float,
    fecha: str,
    vendedor_telefono: str | None = None,
    vendedor_id: int | None = None,     # compat
    alias_transferencia: str | None = None,  # aceptado pero NO mostrado
    stats: dict | None = None,
) -> str:
    _ensure_dir("facturas")
    filename = os.path.join("facturas", f"factura_{venta_id}.pdf")

    styles = getSampleStyleSheet()
    title = ParagraphStyle("title", parent=styles["Title"], fontName="Helvetica-Bold",
                           fontSize=18, leading=22, spaceAfter=2*mm, alignment=0)
    subtitle = ParagraphStyle("subtitle", parent=styles["Normal"], fontName="Helvetica",
                              fontSize=10.5, textColor=colors.grey, spaceAfter=1*mm)
    hsmall = ParagraphStyle("hsmall", parent=styles["Normal"], fontName="Helvetica-Bold",
                            fontSize=10.5, spaceAfter=0)
    normal = ParagraphStyle("normal", parent=styles["Normal"], fontName="Helvetica",
                            fontSize=9.5, leading=12)

    doc = SimpleDocTemplate(
        filename, pagesize=A4,
        rightMargin=12*mm, leftMargin=12*mm, topMargin=12*mm, bottomMargin=14*mm,
        title=f"Factura {venta_id}", author="DIARNEC DISTRIBUIDORA",
    )

    flow = []
    # Encabezado
    flow.append(Paragraph("DIARNEC DISTRIBUIDORA", title))
    flow.append(Paragraph("NECOCHEA Y ZONA", subtitle))

    vend_line = f"Vendedor: <b>{(vendedor_nombre or '—')}</b>"
    if vendedor_telefono:
        vend_line += f" &nbsp;&nbsp;|&nbsp;&nbsp; Tel: {vendedor_telefono}"
    flow.append(Paragraph(vend_line, normal))
    flow.append(Spacer(1, 3*mm))

    # Cabecera derecha con Nº / Fecha / Hora
    fecha_str = fecha or datetime.now().strftime("%Y-%m-%d")
    hora_str = datetime.now().strftime("%H:%M")
    head_tbl = Table(
        [["Comprobante Nº", f"{venta_id}"],
         ["Fecha", fecha_str],
         ["Hora", hora_str]],
        colWidths=[35*mm, 35*mm],
        hAlign="RIGHT",
    )
    head_tbl.setStyle(TableStyle([
        ("BOX", (0,0), (-1,-1), 0.25, colors.black),
        ("INNERGRID", (0,0), (-1,-1), 0.25, colors.black),
        ("BACKGROUND", (0,0), (-1,0), colors.whitesmoke),
        ("ALIGN", (1,0), (-1,-1), "RIGHT"),
        ("FONTNAME", (0,0), (-1,-1), "Helvetica"),
        ("FONTSIZE", (0,0), (-1,-1), 9.5),
        ("TOPPADDING", (0,0), (-1,-1), 3),
        ("BOTTOMPADDING", (0,0), (-1,-1), 3),
    ]))
    flow.append(head_tbl)
    flow.append(Spacer(1, 4*mm))

    # Tabla ítems — encabezado visible (negrita, fondo blanco, bordes negros)
    headers = [
        Paragraph("Marca", hsmall),
        Paragraph("Artículo", hsmall),
        Paragraph("Cantidad", hsmall),
        Paragraph("% Restado", hsmall),
        Paragraph("Precio unitario", hsmall),
        Paragraph("Precio final", hsmall),
    ]
    data = [headers]
    total_mercaderia = 0.0
    col_widths = [25*mm, 62*mm, 22*mm, 22*mm, 28*mm, 28*mm]

    for it in items or []:
        marca = it.get("marca") or ""
        nombre = it.get("nombre") or it.get("producto") or ""
        qty = float(it.get("cantidad") or 0)
        pu = float(it.get("precio") or 0)
        pct = float(it.get("pct") or 0)

        pu_final = pu * (1.0 - pct/100.0)
        subtotal = qty * pu_final
        total_mercaderia += subtotal
        qty_str = f"{int(qty)}" if float(qty).is_integer() else f"{qty:g}"

        data.append([
            Paragraph(str(marca), normal),
            Paragraph(str(nombre), normal),
            Paragraph(qty_str, normal),
            Paragraph(_pct(pct), normal),
            Paragraph(_fmt_money(pu), normal),
            Paragraph(_fmt_money(pu_final), normal),
        ])

    table = Table(data, colWidths=col_widths, repeatRows=1)
    table.setStyle(TableStyle([
        # Encabezado: negrita, fondo blanco y línea inferior negra
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE", (0,0), (-1,0), 10),
        ("TEXTCOLOR", (0,0), (-1,0), colors.black),
        ("BACKGROUND", (0,0), (-1,0), colors.white),
        ("LINEBELOW", (0,0), (-1,0), 0.8, colors.black),

        # Cuerpo
        ("FONTNAME", (0,1), (-1,-1), "Helvetica"),
        ("FONTSIZE", (0,1), (-1,-1), 9.5),
        ("ALIGN", (2,1), (2,-1), "RIGHT"),
        ("ALIGN", (3,1), (5,-1), "RIGHT"),

        # Bordes/grilla negros
        ("GRID", (0,0), (-1,-1), 0.25, colors.black),

        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("TOPPADDING", (0,0), (-1,-1), 4),
        ("BOTTOMPADDING", (0,0), (-1,-1), 4),
    ]))
    flow.append(table)
    flow.append(Spacer(1, 3*mm))

    # Total mercadería
    tot_tbl = Table([["Total mercadería retirada", _fmt_money(total_mercaderia)]],
                    colWidths=[80*mm, 30*mm], hAlign="RIGHT")
    tot_tbl.setStyle(TableStyle([
        ("BOX", (0,0), (-1,-1), 0.6, colors.black),
        ("BACKGROUND", (0,0), (0,0), colors.whitesmoke),
        ("ALIGN", (0,0), (0,0), "RIGHT"),
        ("ALIGN", (1,0), (1,0), "RIGHT"),
        ("FONTNAME", (0,0), (-1,-1), "Helvetica-Bold"),
        ("FONTSIZE", (0,0), (-1,-1), 10),
        ("TOPPADDING", (0,0), (-1,-1), 4),
        ("BOTTOMPADDING", (0,0), (-1,-1), 4),
    ]))
    flow.append(tot_tbl)
    flow.append(Spacer(1, 4*mm))

    # Resumen del vendedor
    if stats:
        retirado = float(stats.get("retirado") or 0.0)
        devuelto = float(stats.get("devuelto") or 0.0)
        bonif = float(stats.get("bonificado") or stats.get("bonificaciones") or 0.0)
        pagado = float(stats.get("pagado") or 0.0)
        saldo  = float(stats.get("saldo") or (retirado - devuelto - bonif - pagado))

        resumen_tbl = Table(
            [["Retirado", _fmt_money(retirado)],
             ["Devuelto", _fmt_money(devuelto)],
             ["Bonificaciones", _fmt_money(bonif)],
             ["Pagos", _fmt_money(pagado)],
             ["Saldo", _fmt_money(saldo)]],
            colWidths=[60*mm, 35*mm],
        )
        resumen_tbl.setStyle(TableStyle([
            ("BOX", (0,0), (-1,-1), 0.4, colors.black),
            ("INNERGRID", (0,0), (-1,-1), 0.25, colors.black),
            ("BACKGROUND", (0,0), (-1,-2), colors.whitesmoke),
            ("BACKGROUND", (0,-1), (-1,-1), colors.Color(0.95, 0.98, 1)),
            ("FONTNAME", (0,0), (-1,-1), "Helvetica"),
            ("FONTNAME", (0,-1), (-1,-1), "Helvetica-Bold"),
            ("ALIGN", (1,0), (1,-1), "RIGHT"),
            ("FONTSIZE", (0,0), (-1,-1), 9.5),
            ("TOPPADDING", (0,0), (-1,-1), 3),
            ("BOTTOMPADDING", (0,0), (-1,-1), 3),
        ]))
        flow.append(Paragraph("Resumen del vendedor", hsmall))
        flow.append(KeepTogether(resumen_tbl))

    # Sin pie
    doc.build(flow)
    return filename
# -------- lista de precios --------
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import mm
from datetime import datetime
import os

def generate_price_list_pdf(productos, fecha=None, titulo="LISTA DE PRECIOS"):
    """
    Genera un PDF A4 con columnas: Marca | Producto | Precio (precio_venta).
    productos: iterable de dict/row con claves: 'marca', 'nombre', 'precio_venta'
    Devuelve la ruta del archivo generado.
    """
    _ensure_dir("reportes")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join("reportes", f"lista_precios_{ts}.pdf")

    styles = getSampleStyleSheet()
    h1 = ParagraphStyle("h1", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=18, leading=22, spaceAfter=2*mm)
    sub = ParagraphStyle("sub", parent=styles["Normal"], fontName="Helvetica", fontSize=10.5, textColor=colors.grey)
    th  = ParagraphStyle("th",  parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=10)
    td  = ParagraphStyle("td",  parent=styles["Normal"], fontName="Helvetica", fontSize=9.5)

    doc = SimpleDocTemplate(
        filename, pagesize=A4,
        leftMargin=12*mm, rightMargin=12*mm, topMargin=12*mm, bottomMargin=14*mm,
        title="Lista de precios", author="DIARNEC DISTRIBUIDORA"
    )

    flow = []
    flow.append(Paragraph("DIARNEC DISTRIBUIDORA", h1))
    flow.append(Paragraph("NECOCHEA Y ZONA", sub))
    flow.append(Paragraph(titulo, th))
    flow.append(Paragraph(f"Fecha: {fecha or datetime.now().strftime('%Y-%m-%d %H:%M')}", sub))
    flow.append(Spacer(1, 4*mm))

    # Encabezados
    data = [
        [Paragraph("Marca", th), Paragraph("Producto", th), Paragraph("Precio", th)]
    ]

    # Ordenar por marca y producto
    rows = sorted(
        [(r.get("marca") or "", r.get("nombre") or "", float(r.get("precio_venta") or 0.0)) for r in productos],
        key=lambda x: (x[0].upper(), x[1].upper())
    )
    for marca, nombre, precio in rows:
        data.append([Paragraph(marca or "-", td), Paragraph(nombre, td), Paragraph(_fmt_money(precio), td)])

    table = Table(data, colWidths=[40*mm, 95*mm, 30*mm], repeatRows=1)
    table.setStyle(TableStyle([
        ("BOX", (0,0), (-1,-1), 0.6, colors.black),
        ("GRID", (0,0), (-1,-1), 0.25, colors.grey),
        ("BACKGROUND", (0,0), (-1,0), colors.white),   # fondo blanco (encabezados)
        ("TEXTCOLOR", (0,0), (-1,0), colors.black),
        ("ALIGN", (2,1), (2,-1), "RIGHT"),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("TOPPADDING", (0,0), (-1,-1), 4),
        ("BOTTOMPADDING", (0,0), (-1,-1), 4),
    ]))

    flow.append(table)
    doc.build(flow)
    return filename
