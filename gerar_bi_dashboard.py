# -*- coding: utf-8 -*-
"""
Dashboard BI Profissional — Sicoob Credfaz
Powered by dados reais: OSs 2024.xlsx (10.995 registros)
Paleta: azul marinho, branco, cinza | Ícones: escudo + banco de dados
Resolução: 16:9 (1920x1080 eq.)
"""
import sys, io, os, warnings
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
from matplotlib.gridspec import GridSpec

# ═══════════════════════════════════════════════════════════════════════════
#  1. LEITURA E AGREGAÇÃO DOS DADOS REAIS
# ═══════════════════════════════════════════════════════════════════════════
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
XLSX_PATH = os.path.join(BASE_DIR, "OSs 2024.xlsx")
OUT_PATH  = os.path.join(BASE_DIR, "dashboard_prints", "03_bi_dashboard_professional.png")
os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

print("[...] Carregando OSs 2024.xlsx...")
df = pd.read_excel(XLSX_PATH, sheet_name="CONSOLIDADO", engine="openpyxl")
df.columns = df.columns.str.strip()
print(f"[OK]  {len(df):,} registros carregados.")

# ── SLA ──────────────────────────────────────────────────────────────────
sla_counts    = df["SLA"].value_counts()
total_os      = len(df)
dentro_prazo  = int(sla_counts.get("Dentro do prazo estimado", 0))
fora_prazo    = int(sla_counts.get("Fora do prazo", 0))
pct_sla       = dentro_prazo / total_os

# ── Categorias (limpa encoding mojibake p/ exibição) ─────────────────────
ENCODING_MAP = {
    "��": "Ç", "��": "Ã", "��": "Â", "Ã‡": "Ç", "Ã‰": "É",
    "Ã›": "Û", "Ã¢": "â", "OPERA��O": "OPERAÇÃO",
    "CR�DITO": "CRÉDITO", "ANTECIPA��O": "ANTECIPAÇÃO",
    "RECEB�VEIS": "RECEBÍVEIS", "ASSOCIA��O": "ASSOCIAÇÃO",
    "COBRAN�A": "COBRANÇA", "PORTABILIDADE": "PORTABILIDADE",
    "DESLIGAMENTO": "DESLIGAMENTO",
}

def fix_enc(s):
    if not isinstance(s, str):
        return str(s)
    for bad, good in ENCODING_MAP.items():
        s = s.replace(bad, good)
    return s

cat_raw     = df["Categoria"].value_counts().head(7)
CAT_LABELS  = [fix_enc(c) for c in cat_raw.index]
CAT_VALS    = cat_raw.values.tolist()

# ── Prioridade ────────────────────────────────────────────────────────────
PRIO_MAP = {
    "1 - Alta - 8 horas úteis":      "Alta (8h)",
    "2 - Média - 16 horas úteis":    "Média (16h)",
    "3 - Normal - 24 horas úteis":   "Normal (24h)",
    "4 - Baixa - 40 horas úteis":    "Baixa (40h)",
    "5 - Ampla - 120 horas":         "Ampla (120h)",
}
prio_raw = df["PRIORIDADE"].value_counts()
PRIO_LABELS = []
PRIO_VALS   = []
for raw_key in prio_raw.index:
    label = fix_enc(raw_key)
    for k, v in PRIO_MAP.items():
        if any(tok in raw_key for tok in ["Alta","Média","Normal","Baixa","Ampla"]):
            pass  # keep
    # Simplifica
    if "Alta"   in raw_key: short = "Alta\n(8h)"
    elif "Média" in raw_key or "Media" in raw_key: short = "Média\n(16h)"
    elif "Normal" in raw_key: short = "Normal\n(24h)"
    elif "Baixa" in raw_key: short = "Baixa\n(40h)"
    elif "Ampla" in raw_key: short = "Ampla\n(120h)"
    else: short = fix_enc(raw_key)[:12]
    PRIO_LABELS.append(short)
    PRIO_VALS.append(int(prio_raw[raw_key]))

# ── Canal ─────────────────────────────────────────────────────────────────
canal_raw   = df["Canal de Atendimento"].value_counts()
CANAL_LABELS = [fix_enc(c) for c in canal_raw.index]
CANAL_VALS   = canal_raw.values.tolist()

# ── Status ────────────────────────────────────────────────────────────────
STATUS_OK  = int(df["STATUS"].value_counts().get("Finalizado com sucesso", 0))
STATUS_NOK = int(df["STATUS"].value_counts().get("Finalizado sem sucesso", 0))
STATUS_OPEN = total_os - STATUS_OK - STATUS_NOK

# ── Mensal ────────────────────────────────────────────────────────────────
df["_dt"] = pd.to_datetime(df["data_abertura"], errors="coerce")
mensal      = df.groupby(df["_dt"].dt.to_period("M")).size()
MES_LABELS  = [str(p) for p in mensal.index]
MES_LABS_SHORT = [m[-2:] + "/"+m[2:4] for m in [l.replace("-","") for l in MES_LABELS]]
MES_VALS    = mensal.values.tolist()

# ── Setores ───────────────────────────────────────────────────────────────
setor_raw    = df["Setor"].value_counts().head(5)
SETOR_LABELS = [fix_enc(s)[:22] for s in setor_raw.index]
SETOR_VALS   = setor_raw.values.tolist()

print(f"[OK]  SLA: {dentro_prazo:,} dentro ({pct_sla*100:.1f}%) | {fora_prazo:,} fora")

# ═══════════════════════════════════════════════════════════════════════════
#  2. PALETA & ESTILOS
# ═══════════════════════════════════════════════════════════════════════════
NAVY        = "#0D1B2A"
NAVY2       = "#102030"
CARD_BG     = "#142035"
CARD_BRD    = "#1E3A5F"
BLUE_ACC    = "#1E78C8"
BLUE_MID    = "#2D6FAB"
BLUE_LIGHT  = "#5BA4E6"
GREEN_ACC   = "#27AE60"
GREEN_MID   = "#2ECC71"
RED_ACC     = "#E74C3C"
AMBER_ACC   = "#F39C12"
WHITE       = "#FFFFFF"
GRAY_L      = "#B0BEC5"
GRAY_M      = "#607D8B"
SEP_COL     = "#1A3550"
BAR_BG      = "#1A3550"

PRIO_COLS   = [AMBER_ACC, "#3498DB", BLUE_ACC, "#1A5276", "#117A65"]
CAT_COLS    = [BLUE_ACC, BLUE_MID, BLUE_LIGHT, "#1A5276", "#2874A6", "#4A235A", "#117A65"]
CANAL_COLS  = [BLUE_ACC, GREEN_ACC, AMBER_ACC, RED_ACC, BLUE_LIGHT, GRAY_M]

# ═══════════════════════════════════════════════════════════════════════════
#  3. HELPERS DE DESENHO
# ═══════════════════════════════════════════════════════════════════════════
def card_bg(ax, x, y, w, h, accent=BLUE_ACC, alpha_border=0.6):
    r = FancyBboxPatch((x, y), w, h,
                       boxstyle="round,pad=0.008",
                       linewidth=0.6, edgecolor=CARD_BRD,
                       facecolor=CARD_BG, zorder=3,
                       transform=ax.transAxes)
    ax.add_patch(r)
    l = FancyBboxPatch((x, y), 0.004, h,
                       boxstyle="round,pad=0",
                       linewidth=0, facecolor=accent,
                       zorder=4, transform=ax.transAxes)
    ax.add_patch(l)


def kpi_card(ax, x, y, w, h, title, value, sub="", accent=BLUE_ACC, val_col=WHITE):
    card_bg(ax, x, y, w, h, accent)
    cx = x + w / 2
    ax.text(cx, y+h*0.82, title, transform=ax.transAxes,
            ha="center", va="center", fontsize=7.5, color=GRAY_L)
    ax.text(cx, y+h*0.46, value, transform=ax.transAxes,
            ha="center", va="center", fontsize=16, color=val_col, fontweight="bold")
    if sub:
        ax.text(cx, y+h*0.13, sub, transform=ax.transAxes,
                ha="center", va="center", fontsize=6.5, color=GRAY_M)


def gauge(ax, pct, cx, cy, r_out=0.35, r_in=0.22):
    """Gauge semicircular (180°→0° = 0%→100%)."""
    # trilho cinza
    for ang in np.linspace(180, 0, 220):
        r = np.radians(ang)
        for ri, ro in [(r_in, r_out)]:
            ax.fill_between(
                [cx + ri*np.cos(r), cx + ro*np.cos(r)],
                [cy + ri*np.sin(r), cy + ro*np.sin(r)],
                color=BAR_BG, linewidth=0, transform=ax.transAxes, zorder=3)

    # arco colorido
    end = 180 - pct * 180
    n = max(3, int(pct * 180))
    for a0, a1 in zip(np.linspace(180, end, n)[:-1],
                      np.linspace(180, end, n)[1:]):
        r0, r1 = np.radians(a0), np.radians(a1)
        xs = [cx + r_in*np.cos(r0), cx + r_out*np.cos(r0),
              cx + r_out*np.cos(r1), cx + r_in*np.cos(r1)]
        ys = [cy + r_in*np.sin(r0), cy + r_out*np.sin(r0),
              cy + r_out*np.sin(r1), cy + r_in*np.sin(r1)]
        col = GREEN_ACC if pct >= 0.5 else AMBER_ACC
        ax.fill(xs, ys, color=col, transform=ax.transAxes, zorder=4)

    # textos
    ax.text(cx, cy - 0.02, f"{pct*100:.1f}%",
            transform=ax.transAxes, ha="center", va="center",
            fontsize=20, fontweight="bold",
            color=GREEN_ACC if pct >= 0.5 else AMBER_ACC, zorder=5)
    ax.text(cx, cy - 0.14, "SLA Cumprido",
            transform=ax.transAxes, ha="center", va="center",
            fontsize=7.5, color=GRAY_L, zorder=5)
    ax.text(cx - r_out - 0.02, cy - 0.05, "0%",
            transform=ax.transAxes, ha="center", fontsize=6, color=GRAY_M)
    ax.text(cx + r_out + 0.02, cy - 0.05, "100%",
            transform=ax.transAxes, ha="center", fontsize=6, color=GRAY_M)


def shield(ax, cx, cy, sz=0.07):
    pts = np.array([[0,.9],[.5,.75],[1,.9],[1,.35],[.5,0],[0,.35]])
    pts[:,0] = cx - sz + pts[:,0]*sz*2
    pts[:,1] = cy - sz*.5 + pts[:,1]*sz*1.15
    poly = plt.Polygon(pts, closed=True,
                       facecolor=BLUE_ACC, edgecolor=BLUE_LIGHT,
                       linewidth=1.5, zorder=6, transform=ax.transAxes)
    ax.add_patch(poly)
    # checkmark
    cx_arr = [cx-sz*.3, cx-sz*.05, cx+sz*.38]
    cy_arr = [cy+sz*.08, cy-sz*.15, cy+sz*.45]
    ax.plot(cx_arr, cy_arr, color=WHITE, lw=2.2,
            solid_capstyle="round", transform=ax.transAxes, zorder=7)


def database_icon(ax, cx, cy, w=0.065, h=0.085):
    body = FancyBboxPatch((cx-w/2, cy-h/2), w, h,
                          boxstyle="round,pad=0.005",
                          facecolor=BLUE_MID, edgecolor=BLUE_LIGHT,
                          linewidth=1, zorder=6, transform=ax.transAxes)
    ax.add_patch(body)
    for yp in [cy+h/2, cy, cy-h/2]:
        e = mpatches.Ellipse((cx, yp), w, h*0.2,
                              facecolor=BLUE_ACC, edgecolor=BLUE_LIGHT,
                              linewidth=0.7, zorder=7, transform=ax.transAxes)
        ax.add_patch(e)
    ax.text(cx, cy-h*0.12, "DB", transform=ax.transAxes,
            ha="center", va="center", fontsize=5.5, color=WHITE,
            fontweight="bold", zorder=8)


# ═══════════════════════════════════════════════════════════════════════════
#  4. LAYOUT DA FIGURA
# ═══════════════════════════════════════════════════════════════════════════
fig = plt.figure(figsize=(19.2, 10.8), facecolor=NAVY, dpi=100)
ax  = fig.add_axes([0, 0, 1, 1])
ax.set_xlim(0, 1); ax.set_ylim(0, 1)
ax.set_facecolor(NAVY); ax.axis("off")

# gradiente sutil
for i, y in enumerate(np.linspace(0, 1, 50)):
    ax.axhspan(y, y+1/50, facecolor=BLUE_MID, alpha=0.025*(1-y), zorder=0)

# ── HEADER ────────────────────────────────────────────────────────────────
ax.add_patch(FancyBboxPatch((0,0.905), 1, 0.095,
    boxstyle="square,pad=0", facecolor=NAVY2, edgecolor=SEP_COL, lw=0.8, zorder=2))
ax.axhspan(0.985, 1.0, facecolor=BLUE_ACC, zorder=3)

ax.text(0.5, 0.956, "SICOOB CREDFAZ  ——  SLA Performance Dashboard",
        ha="center", va="center", fontsize=16, fontweight="bold", color=WHITE, zorder=5)
ax.text(0.5, 0.924,
        f"Base completa: {total_os:,} Ordens de Serviço  |  Jan – Dez 2024  |  "
        "Pipeline: MariaDB (Legacy)  →  Python ETL  →  Power BI",
        ha="center", va="center", fontsize=8.5, color=GRAY_L, zorder=5)
ax.text(0.013, 0.951, "SLA", ha="left", va="center",
        fontsize=13, fontweight="bold", color=BLUE_LIGHT, zorder=5)
ax.text(0.013, 0.924, "ANALYTICS", ha="left", va="center",
        fontsize=6.2, color=GRAY_M, zorder=5)
ax.text(0.987, 0.951, "Fev / 2026", ha="right", va="center",
        fontsize=8, color=GRAY_L, zorder=5)
ax.text(0.987, 0.924, "25/02/2026", ha="right", va="center",
        fontsize=7, color=GRAY_M, zorder=5)

# ── SEPARADORES VERTICAIS ──────────────────────────────────────────────────
for xv in [0.305, 0.64]:
    ax.plot([xv, xv], [0.065, 0.900], color=SEP_COL, lw=0.8, zorder=3)

# ═══ COLUNA ESQUERDA (x: 0.01 – 0.295) ════════════════════════════════════
LX, LW = 0.010, 0.285

# Gauge card
ax.add_patch(FancyBboxPatch((LX, 0.545), LW, 0.345,
    boxstyle="round,pad=0.01", facecolor=CARD_BG, edgecolor=CARD_BRD, lw=0.7, zorder=3))
ax.text(LX+LW/2, 0.874, "SLA GERAL — Cumprimento de Prazo",
        ha="center", fontsize=8.5, color=GRAY_L, fontweight="bold")
gauge(ax, pct_sla, cx=LX+LW/2, cy=0.695, r_out=0.095, r_in=0.060)

# KPI cards
kpi_h = 0.115
kpi_data = [
    ("Total de OSs",       f"{total_os:,}",        "Jan-Dez 2024",      BLUE_ACC,  WHITE),
    ("Dentro do Prazo",    f"{dentro_prazo:,}",     f"{pct_sla*100:.1f}% do total", GREEN_ACC, GREEN_MID),
    ("Fora do Prazo",      f"{fora_prazo:,}",       f"{(1-pct_sla)*100:.1f}% do total", RED_ACC, RED_ACC),
    ("Finalizados OK",     f"{STATUS_OK:,}",        "com sucesso", BLUE_MID, WHITE),
]
for i, (tit, val, sub, acc, vc) in enumerate(kpi_data):
    ky = 0.395 - i*(kpi_h+0.012)
    kpi_card(ax, LX, ky, LW, kpi_h, tit, val, sub, acc, vc)


# ═══ COLUNA CENTRAL (x: 0.31 – 0.63) ══════════════════════════════════════
CX_MID = 0.472

# ── Ícones ──
shield(ax, cx=CX_MID, cy=0.798, sz=0.065)
database_icon(ax, cx=CX_MID, cy=0.688, w=0.060, h=0.075)

ax.text(CX_MID, 0.643, "Governança & Indicadores de SLA",
        ha="center", fontsize=8.5, color=GRAY_L, fontweight="bold")
ax.text(CX_MID, 0.618, "Dados extraídos do sistema legado MariaDB",
        ha="center", fontsize=7.5, color=GRAY_M)

# Pipeline flow
for xi, lbl, col in [
    (CX_MID-0.11, "MariaDB",    GRAY_M),
    (CX_MID,      "Python ETL", BLUE_LIGHT),
    (CX_MID+0.11, "Power BI",   GREEN_MID),
]:
    ax.text(xi, 0.583, lbl, ha="center", fontsize=7.5, color=col, fontweight="bold")
    if xi < CX_MID+0.11:
        ax.annotate("", xy=(xi+0.07, 0.586), xytext=(xi+0.025, 0.586),
                    xycoords="axes fraction", textcoords="axes fraction",
                    arrowprops=dict(arrowstyle="->", color=BLUE_ACC, lw=1.2))

# ── Evolução Mensal (gráfico de linha) ───────────────────────────────────
line_card_x, line_card_y = 0.313, 0.065
line_card_w, line_card_h = 0.318, 0.45

ax.add_patch(FancyBboxPatch((line_card_x, line_card_y),
    line_card_w, line_card_h,
    boxstyle="round,pad=0.01", facecolor=CARD_BG, edgecolor=CARD_BRD, lw=0.7, zorder=3))
ax.text(line_card_x+line_card_w/2, line_card_y+line_card_h-0.022,
        "Evolução Mensal de OSs Abertas (2024)",
        ha="center", fontsize=8, color=GRAY_L, fontweight="bold")

# Normaliza pontos para o espaço do card
mn  = np.array(MES_VALS, dtype=float)
xs  = np.linspace(line_card_x+0.025, line_card_x+line_card_w-0.018, len(mn))
mn_min, mn_max = mn.min(), mn.max()
def ny(v): return line_card_y + 0.055 + (v - mn_min)/(mn_max - mn_min + 1) * (line_card_h - 0.1)
ys = np.array([ny(v) for v in mn])

# Área preenchida
ax.fill_between(xs, [line_card_y+0.055]*len(xs), ys,
                color=BLUE_ACC, alpha=0.15, zorder=4,
                transform=ax.transAxes)
ax.plot(xs, ys, color=BLUE_LIGHT, lw=2.0, zorder=5, transform=ax.transAxes)
ax.scatter(xs, ys, color=WHITE, s=28, zorder=6, transform=ax.transAxes,
           edgecolors=BLUE_LIGHT, linewidths=1.2)

# Labels valores e meses
for x, y, v, m in zip(xs, ys, mn, MES_LABS_SHORT):
    ax.text(x, y+0.022, str(int(v)), ha="center", va="bottom",
            fontsize=5.8, color=BLUE_LIGHT, fontweight="bold",
            transform=ax.transAxes)
    ax.text(x, line_card_y+0.028, m, ha="center", va="center",
            fontsize=5.5, color=GRAY_M, transform=ax.transAxes)


# ═══ COLUNA DIREITA (x: 0.648 – 0.99) ═════════════════════════════════════
RX, RW = 0.648, 0.342

# ── Barras Horizontais — Top 7 Categorias ────────────────────────────────
cat_card_x, cat_card_y = RX, 0.47
cat_card_w, cat_card_h = RW, 0.425

ax.add_patch(FancyBboxPatch((cat_card_x, cat_card_y),
    cat_card_w, cat_card_h,
    boxstyle="round,pad=0.01", facecolor=CARD_BG, edgecolor=CARD_BRD, lw=0.7, zorder=3))
ax.text(cat_card_x+cat_card_w/2, cat_card_y+cat_card_h-0.022,
        "Volume por Categoria (Top 7)",
        ha="center", fontsize=8, color=GRAY_L, fontweight="bold")

max_cat   = max(CAT_VALS)
bar_x0    = cat_card_x + 0.10
bar_max_w = cat_card_w - 0.13
bar_h_c   = 0.034
bars_start= cat_card_y + cat_card_h - 0.065

for i, (cat, val) in enumerate(zip(CAT_LABELS, CAT_VALS)):
    by = bars_start - i*(bar_h_c + 0.01)
    bw = (val/max_cat) * bar_max_w - 0.003
    # bg
    ax.add_patch(FancyBboxPatch((bar_x0, by), bar_max_w, bar_h_c,
        boxstyle="round,pad=0.002", facecolor=BAR_BG, edgecolor="none", zorder=4,
        transform=ax.transAxes))
    # valor
    ax.add_patch(FancyBboxPatch((bar_x0, by), bw, bar_h_c,
        boxstyle="round,pad=0.002", facecolor=CAT_COLS[i % len(CAT_COLS)],
        edgecolor="none", zorder=5, transform=ax.transAxes))
    # label cat
    cat_short = cat[:20].replace("OPERAÇÃO DE CRÉDITO - ", "CRED. ")
    ax.text(bar_x0-0.006, by+bar_h_c/2, cat_short,
            ha="right", va="center", fontsize=5.8, color=GRAY_L, transform=ax.transAxes)
    ax.text(bar_x0+bw+0.006, by+bar_h_c/2, f"{val:,}",
            ha="left", va="center", fontsize=6.5, color=WHITE,
            fontweight="bold", transform=ax.transAxes)


# ── Canal de Atendimento (donut) + Prioridade (barras verticais) ──────────
bottom_x, bottom_y = RX, 0.065
bottom_w, bottom_h = RW, 0.385

ax.add_patch(FancyBboxPatch((bottom_x, bottom_y),
    bottom_w, bottom_h,
    boxstyle="round,pad=0.01", facecolor=CARD_BG, edgecolor=CARD_BRD, lw=0.7, zorder=3))
ax.text(RX+RW/2, bottom_y+bottom_h-0.022,
        "Distribuição por Prioridade",
        ha="center", fontsize=8, color=GRAY_L, fontweight="bold")

# Barras de prioridade
n_prios  = len(PRIO_VALS)
pb_w     = (bottom_w - 0.06) / n_prios - 0.01
pb_x0    = bottom_x + 0.03
pb_base  = bottom_y + 0.055
pb_max_h = bottom_h - 0.115
max_prio = max(PRIO_VALS)

for i, (lbl, val) in enumerate(zip(PRIO_LABELS, PRIO_VALS)):
    px   = pb_x0 + i*(pb_w + 0.01)
    ph   = (val/max_prio)*pb_max_h
    col  = PRIO_COLS[i % len(PRIO_COLS)]
    # barra bg
    ax.add_patch(FancyBboxPatch((px, pb_base), pb_w, pb_max_h,
        boxstyle="round,pad=0.003", facecolor=BAR_BG, edgecolor="none",
        alpha=0.7, zorder=4, transform=ax.transAxes))
    # barra valor
    ax.add_patch(FancyBboxPatch((px, pb_base), pb_w, ph,
        boxstyle="round,pad=0.003", facecolor=col,
        edgecolor="none", alpha=0.90, zorder=5, transform=ax.transAxes))
    ax.text(px+pb_w/2, pb_base+ph+0.015, f"{val:,}",
            ha="center", fontsize=6.5, color=WHITE, fontweight="bold",
            transform=ax.transAxes)
    ax.text(px+pb_w/2, pb_base-0.022, lbl,
            ha="center", fontsize=5.8, color=GRAY_L,
            transform=ax.transAxes, linespacing=1.1)

# Linha % no topo de cada barra
for i, val in enumerate(PRIO_VALS):
    px  = pb_x0 + i*(pb_w + 0.01)
    pct = val/total_os*100
    ax.text(pb_x0 + i*(pb_w+0.01) + pb_w/2,
            pb_base - 0.038,
            f"{pct:.1f}%",
            ha="center", fontsize=5.5, color=GRAY_M,
            transform=ax.transAxes)


# ── RODAPÉ ────────────────────────────────────────────────────────────────
ax.add_patch(FancyBboxPatch((0,0), 1, 0.063,
    boxstyle="square,pad=0", facecolor=NAVY2, edgecolor=SEP_COL, lw=0.5, zorder=2))
ax.text(0.5, 0.032,
        "Sicoob Credfaz  •  Analista de Dados: Matheus Benvindo  •  "
        "Stack: MariaDB Legacy  →  Python (Pandas/Matplotlib)  →  Power BI",
        ha="center", va="center", fontsize=7.5, color=GRAY_M, zorder=5)
ax.text(0.987, 0.032, "data-analytics-sla-sicoob  |  github.com/MatheusBenvindo",
        ha="right", fontsize=6.5, color=SEP_COL, va="center", zorder=5)

# Grade decorativa
for y in np.linspace(0.065, 0.900, 10):
    ax.axhline(y, color=SEP_COL, lw=0.2, alpha=0.4, zorder=1)

# ═══════════════════════════════════════════════════════════════════════════
plt.savefig(OUT_PATH, dpi=150, bbox_inches="tight", facecolor=NAVY, pad_inches=0)
plt.close()
print(f"[OK]  Dashboard salvo: {OUT_PATH}")
