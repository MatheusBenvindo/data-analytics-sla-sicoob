# -*- coding: utf-8 -*-
import sys, io
# Force UTF-8 output on Windows so emoji/special chars print correctly
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

"""
=============================================================================
  ETL - Otimização de Indicadores de SLA | Sicoob Credfaz
=============================================================================
  Autor   : Matheus Benvindo
  Data    : Fevereiro de 2026
  Projeto : data-analytics-sla-sicoob

  Descrição:
    Este script simula o pipeline ETL utilizado para transformar dados brutos
    extraídos do sistema legado MariaDB em indicadores de SLA prontos para
    visualização no Power BI.

    Etapas:
      1. EXTRAÇÃO   → Lê raw_data_os.csv (dados sujos do sistema legado)
      2. TRANSFORMAÇÃO → Corrige encoding, padroniza datas, aplica lógica SLA
      3. CARGA       → Salva CSV tratado + gera dashboard PNG + preview XLSX
=============================================================================
"""

import os
import re
import textwrap
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
from matplotlib import font_manager
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
#  CONFIGURAÇÕES GLOBAIS DE PATHS
# ─────────────────────────────────────────────
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
RAW_PATH       = os.path.join(BASE_DIR, "data", "raw", "raw_data_os.csv")
PROCESSED_PATH = os.path.join(BASE_DIR, "data", "processed", "os_tratadas.csv")
PRINT_DIR      = os.path.join(BASE_DIR, "dashboard_prints")
XLSX_PATH      = os.path.join(BASE_DIR, "OSs 2024.xlsx")

os.makedirs(os.path.dirname(PROCESSED_PATH), exist_ok=True)
os.makedirs(PRINT_DIR, exist_ok=True)

# ─────────────────────────────────────────────
#  PALETA DE CORES (Sicoob — verde institucional)
# ─────────────────────────────────────────────
SICOOB_GREEN   = "#006F44"
SICOOB_LIGHT   = "#4CAF50"
SICOOB_GOLD    = "#F5A623"
SICOOB_RED     = "#D94F3D"
SICOOB_BLUE    = "#1976D2"
SICOOB_DARK    = "#1B2B22"
BG_COLOR       = "#F4F7F5"
CARD_COLOR     = "#FFFFFF"

PALETTE = [SICOOB_GREEN, SICOOB_GOLD, SICOOB_RED, SICOOB_BLUE,
           SICOOB_LIGHT, "#8E44AD", "#E67E22", "#16A085"]


# =============================================================================
#  FASE 1 — EXTRAÇÃO
# =============================================================================
def extrair_dados(caminho: str) -> pd.DataFrame:
    """
    Lê o CSV bruto com encoding latin-1 para capturar corretamente os
    caracteres corrompidos provenientes do MariaDB.
    """
    print("\n" + "="*65)
    print("  FASE 1 — EXTRAÇÃO")
    print("="*65)

    df = pd.read_csv(caminho, encoding="latin-1", dtype=str)
    print(f"  ✅ {len(df)} registros carregados de: {os.path.basename(caminho)}")
    print(f"  ℹ  Colunas encontradas: {list(df.columns)}")

    # Mostra amostra dos problemas para evidenciar a necessidade do ETL
    print("\n  ⚠  Amostra de problemas identificados:")
    print(f"     • Encoding  : '{df.iloc[0]['Respons vel']}'  →  esperado: 'Responsável'")
    print(f"     • Data mista: '{df.iloc[2]['data_abertura']}' (formato MM-DD-YYYY)")
    print(f"     • Nulo      : data_encerramento = '{df.iloc[3]['data_encerramento']}'")
    return df


# =============================================================================
#  FASE 2 — TRANSFORMAÇÃO
# =============================================================================

# Mapa de substituição para reparar mojibake gerado pelo MariaDB latin-1
ENCODING_FIX = {
    "Respons vel": "Responsável",
    "AG NCIA":     "AGÊNCIA",
    "CR DITO":     "CRÉDITO",
    "AMORTIZA  O": "AMORTIZAÇÃO",
    "PREJU ZO":    "PREJUÍZO",
    "INSTALA  O":  "INSTALAÇÃO",
    "CONFIGURA  O":"CONFIGURAÇÃO",
    "TRANSFER NCIA":"TRANSFERÊNCIA",
    "BANC RIA":    "BANCÁRIA",
    "EMISS O":     "EMISSÃO",
    "BLOQUEIO DE CART O": "BLOQUEIO DE CARTÃO",
    "CART O":      "CARTÃO",
    "CONTRATA  O": "CONTRATAÇÃO",
    "COBRAN A":    "COBRANÇA",
    "ATUALIZA  O": "ATUALIZAÇÃO",
    "INFORMA  ES": "INFORMAÇÕES",
    "OPERA  O":    "OPERAÇÃO",
    "SEGURAN A":   "SEGURANÇA",
    "T CNICO":     "TÉCNICO",
    "CR TICO":     "CRÍTICO",
    " teis":       " úteis",
    " til,":       " útil,",
    "Jo o Luiz":   "João Luiz",
    "Cr dito":     "Crédito",
    "FINANCIAMENTO": "FINANCIAMENTO",
}

def corrigir_encoding(df: pd.DataFrame) -> pd.DataFrame:
    """Corrige caracteres corrompidos em todas as colunas de texto."""
    print("\n  → Corrigindo encoding (mojibake do MariaDB)...")

    # Renomeia a coluna corrompida para o nome correto
    df = df.rename(columns={"Respons vel": "Responsável"})

    for col in df.select_dtypes(include="object").columns:
        for errado, correto in ENCODING_FIX.items():
            df[col] = df[col].str.replace(errado, correto, regex=False)

    print("     ✅ Encoding corrigido em todas as colunas de texto.")
    return df


FORMATOS_DATA = [
    "%d/%m/%Y %H:%M",   # 02/01/2024 08:36
    "%Y-%m-%d %H:%M:%S",# 2024-01-17 13:15:14
    "%m-%d-%Y %H:%M",   # 01-02-2024 09:19  (formato MM-DD errado)
    "%d/%m/%Y",
    "%Y-%m-%d",
]

def parsear_data(valor: str):
    """Tenta parsear uma string de data em múltiplos formatos."""
    if pd.isna(valor) or str(valor).strip() == "":
        return pd.NaT
    for fmt in FORMATOS_DATA:
        try:
            return datetime.strptime(str(valor).strip(), fmt)
        except ValueError:
            continue
    return pd.NaT


def padronizar_datas(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza data_abertura e data_encerramento para datetime."""
    print("\n  → Padronizando colunas de data...")

    antes = df["data_abertura"].tolist()
    df["data_abertura"]      = df["data_abertura"].apply(parsear_data)
    df["data_encerramento"]  = df["data_encerramento"].apply(parsear_data)

    # Preenche datas de encerramento faltantes com data_abertura + SLA
    mask_nulo = df["data_encerramento"].isna()
    df.loc[mask_nulo, "data_encerramento"] = (
        df.loc[mask_nulo, "data_abertura"] +
        pd.to_timedelta(df.loc[mask_nulo, "prioridade_horas"].astype(float) * 2, unit="h")
    )

    n_corrigidos = mask_nulo.sum()
    print(f"     ✅ {n_corrigidos} data(s) de encerramento inferidas para OSs em aberto.")
    print(f"     ✅ {len(df) - n_corrigidos} datas validadas e padronizadas para ISO 8601.")
    return df


def calcular_tempo_resolucao(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula o tempo real de resolução em horas úteis."""
    df["tempo_resolucao_horas"] = (
        (df["data_encerramento"] - df["data_abertura"])
        .dt.total_seconds() / 3600
    ).round(2)
    return df


def aplicar_regra_sla(df: pd.DataFrame) -> pd.DataFrame:
    """
    Regra de Negócio — Cálculo de Status SLA:
      • Dentro do Prazo : tempo_resolucao_horas <= prioridade_horas
      • Atrasada        : tempo_resolucao_horas  > prioridade_horas

    A coluna 'prioridade_horas' define o SLA contratual por prioridade:
      1 - Crítico  →  1h  útil
      2 - Urgente  →  4h  úteis
      3 - Normal   → 24h  úteis
      4 - Baixa    → 72h  úteis
    """
    print("\n  → Aplicando regra de negócio: cálculo de Status SLA...")

    df["prioridade_horas"] = pd.to_numeric(df["prioridade_horas"], errors="coerce").fillna(24.0)
    df = calcular_tempo_resolucao(df)

    df["status_sla"] = df.apply(
        lambda r: "Dentro do Prazo"
        if r["tempo_resolucao_horas"] <= r["prioridade_horas"]
        else "Atrasada",
        axis=1,
    )

    dentro   = (df["status_sla"] == "Dentro do Prazo").sum()
    atrasada = (df["status_sla"] == "Atrasada").sum()
    print(f"     ✅ Regra aplicada: {dentro} Dentro do Prazo | {atrasada} Atrasadas")
    return df


def padronizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza nomes de colunas para snake_case."""
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    return df


def transformar_dados(df: pd.DataFrame) -> pd.DataFrame:
    print("\n" + "="*65)
    print("  FASE 2 — TRANSFORMAÇÃO")
    print("="*65)

    df = corrigir_encoding(df)
    df = padronizar_datas(df)
    df = aplicar_regra_sla(df)

    # Preenche STATUS nulos
    df["STATUS"] = df["STATUS"].fillna("Não Informado")

    df = padronizar_colunas(df)

    print(f"\n  ✅ Transformação concluída. Shape final: {df.shape}")
    return df


# =============================================================================
#  FASE 3 — CARGA
# =============================================================================
def salvar_csv(df: pd.DataFrame, caminho: str):
    df.to_csv(caminho, index=False, encoding="utf-8-sig")
    print(f"\n  ✅ CSV tratado salvo: {os.path.relpath(caminho)}")


# =============================================================================
#  VISUALIZAÇÃO 1 — PREVIEW DO XLSX (dados brutos estilizados)
# =============================================================================
def gerar_preview_xlsx(caminho_xlsx: str, pasta_saida: str):
    """
    Gera uma imagem estilizada que simula a visualização do arquivo XLSX
    original, mostrando os dados brutos com os problemas de qualidade
    evidenciados visualmente (células destacadas em laranja/vermelho).
    """
    print("\n  → Gerando preview do arquivo XLSX original...")

    try:
        df_raw = pd.read_excel(caminho_xlsx, sheet_name="CONSOLIDADO", nrows=12)
    except Exception:
        print("     ⚠  XLSX não acessível, usando raw_data_os.csv para preview.")
        df_raw = pd.read_csv(RAW_PATH, encoding="latin-1", nrows=12)

    # Seleciona colunas mais representativas para o print
    cols_show = [
        c for c in ["id", "titulo", "data_abertura", "data_encerramento",
                    "Responsável", "Setor Inicial", "SLA", "PRIORIDADE",
                    "STATUS", "Categoria"]
        if c in df_raw.columns
    ]

    # Fallback para colunas do raw csv
    if not cols_show:
        cols_show = list(df_raw.columns[:8])

    df_show = df_raw[cols_show].head(10).copy()

    # Trunca textos longos para caber na tabela
    for col in df_show.columns:
        df_show[col] = df_show[col].astype(str).apply(
            lambda x: x[:28] + "…" if len(x) > 28 else x
        )

    n_cols = len(df_show.columns)
    n_rows = len(df_show) + 1  # +1 header

    fig_w = max(18, n_cols * 2.2)
    fig_h = n_rows * 0.6 + 2.5

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    fig.patch.set_facecolor(SICOOB_DARK)
    ax.set_facecolor(SICOOB_DARK)
    ax.axis("off")

    # Título do painel
    fig.text(
        0.5, 0.97,
        "📋  Preview — OSs 2024.xlsx  |  Dados Brutos (Sistema Legado MariaDB)",
        ha="center", va="top",
        fontsize=13, fontweight="bold", color="white",
        fontfamily="monospace",
    )
    fig.text(
        0.5, 0.93,
        "⚠  Células em laranja indicam problemas de qualidade detectados pelo ETL",
        ha="center", va="top",
        fontsize=10, color=SICOOB_GOLD, fontstyle="italic",
    )

    # Constrói tabela matplotlib
    col_labels = list(df_show.columns)
    cell_data  = df_show.values.tolist()

    # Cria a table
    table = ax.table(
        cellText=cell_data,
        colLabels=col_labels,
        cellLoc="center",
        loc="center",
        bbox=[0, 0, 1, 0.82],
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8.5)

    # Estilização do header
    for j in range(n_cols):
        cell = table[0, j]
        cell.set_facecolor(SICOOB_GREEN)
        cell.set_text_props(color="white", fontweight="bold")
        cell.set_edgecolor("#2E5C40")

    # Palavras-chave que indicam problemas de qualidade no dado bruto
    problemas_keywords = [
        "Fora", "NaT", "nan", "None", " vel", "NCIA", " O ", "NCIA",
        "Em andamento", "Não Informado", "�",
    ]

    for i in range(len(cell_data)):
        for j in range(n_cols):
            cell  = table[i + 1, j]
            valor = str(cell_data[i][j])

            # Cor zebra base
            bg = "#FAFAFA" if i % 2 == 0 else "#EEF2EE"

            tem_problema = any(kw in valor for kw in problemas_keywords)
            if tem_problema:
                bg = "#FFF3CD"   # amarelo suave — dado suspeito
                cell.set_text_props(color="#8B4513", fontweight="bold")
            elif valor in ("nan", "NaT", "None", ""):
                bg = "#FFDDDD"   # rosa — valor nulo
                cell.set_text_props(color=SICOOB_RED, fontweight="bold")

            cell.set_facecolor(bg)
            cell.set_edgecolor("#CCCCCC")

    # Legenda
    patch_ok  = mpatches.Patch(color="#FAFAFA",  label="Dado normal")
    patch_sus = mpatches.Patch(color="#FFF3CD",  label="Encoding / formato suspeito")
    patch_nul = mpatches.Patch(color="#FFDDDD",  label="Valor nulo / ausente")
    ax.legend(
        handles=[patch_ok, patch_sus, patch_nul],
        loc="lower center",
        ncol=3,
        fontsize=9,
        framealpha=0.9,
        bbox_to_anchor=(0.5, -0.08),
    )

    saida = os.path.join(pasta_saida, "01_raw_data_preview.png")
    plt.tight_layout(rect=[0, 0.02, 1, 0.91])
    plt.savefig(saida, dpi=160, bbox_inches="tight", facecolor=SICOOB_DARK)
    plt.close()
    print(f"     ✅ Preview XLSX salvo: {os.path.relpath(saida)}")


# =============================================================================
#  VISUALIZAÇÃO 2 — DASHBOARD SLA (dados tratados)
# =============================================================================
def gerar_dashboard(df: pd.DataFrame, pasta_saida: str):
    """
    Gera dashboard de 4 painéis com os dados já tratados pelo ETL:
      1. Pizza  — SLA Geral (% Dentro do Prazo vs Atrasada)
      2. Barras — Top 5 Categorias por volume de OSs
      3. Linha  — Evolução Mensal de OSs abertas
      4. Barras — Distribuição por Prioridade
    """
    print("\n  → Gerando dashboard de indicadores SLA...")

    # ── Preparação de dados ──────────────────────────────────────
    # Colunas padronizadas (snake_case) pelo ETL
    col_sla      = "status_sla"
    col_cat      = next((c for c in df.columns if "categ" in c), None)
    col_abertura = next((c for c in df.columns if "abertura" in c), None)
    col_prio     = next((c for c in df.columns if "prioridade" in c and "hora" not in c), None)

    sla_counts = df[col_sla].value_counts()

    top_cat = (
        df[col_cat].value_counts().head(5)
        if col_cat else pd.Series(dtype=int)
    )
    top_cat.index = [
        textwrap.fill(str(i), 22) for i in top_cat.index
    ]

    if col_abertura and pd.api.types.is_datetime64_any_dtype(df[col_abertura]):
        df["_mes"] = df[col_abertura].dt.to_period("M").astype(str)
        mensal = df.groupby("_mes").size()
    else:
        mensal = pd.Series(dtype=int)

    prio_counts = (
        df[col_prio].value_counts()
        if col_prio else pd.Series(dtype=int)
    )
    prio_counts.index = [str(i)[:30] for i in prio_counts.index]

    # ── Layout ───────────────────────────────────────────────────
    fig = plt.figure(figsize=(20, 13), facecolor=BG_COLOR)
    gs  = gridspec.GridSpec(
        2, 2,
        figure=fig,
        left=0.06, right=0.97,
        top=0.88,  bottom=0.08,
        hspace=0.45, wspace=0.35,
    )

    # Cabeçalho
    fig.text(
        0.5, 0.96,
        "SICOOB CREDFAZ  —  Dashboard de Indicadores de SLA",
        ha="center", va="center",
        fontsize=18, fontweight="bold", color=SICOOB_DARK,
    )
    fig.text(
        0.5, 0.915,
        f"Pipeline ETL concluído em {datetime.now().strftime('%d/%m/%Y %H:%M')}  •  "
        f"{len(df)} Ordens de Serviço processadas  •  Fonte: MariaDB → ETL → Power BI",
        ha="center", va="center",
        fontsize=10, color="#555555", fontstyle="italic",
    )

    # Linha separadora do header
    fig.add_artist(
        plt.Line2D([0.04, 0.96], [0.905, 0.905],
                   transform=fig.transFigure,
                   color=SICOOB_GREEN, lw=2)
    )

    # ── Gráfico 1 — Pizza SLA ────────────────────────────────────
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.set_facecolor(CARD_COLOR)

    cores_pizza = [SICOOB_GREEN if "Dentro" in str(l) else SICOOB_RED
                   for l in sla_counts.index]
    wedges, texts, autotexts = ax1.pie(
        sla_counts.values,
        labels=sla_counts.index,
        autopct="%1.1f%%",
        colors=cores_pizza,
        startangle=90,
        wedgeprops={"edgecolor": "white", "linewidth": 2},
        textprops={"fontsize": 10},
        pctdistance=0.75,
    )
    for at in autotexts:
        at.set_fontweight("bold")
        at.set_fontsize(12)
        at.set_color("white")

    ax1.set_title(
        "① SLA Geral — Cumprimento de Prazo",
        fontsize=12, fontweight="bold", color=SICOOB_DARK, pad=12,
    )

    # KPI central
    pct_prazo = sla_counts.get("Dentro do Prazo", 0) / sla_counts.sum() * 100
    ax1.text(
        0, 0, f"{pct_prazo:.0f}%\ndentro\ndo prazo",
        ha="center", va="center",
        fontsize=11, fontweight="bold", color=SICOOB_DARK,
    )

    # ── Gráfico 2 — Top 5 Categorias ─────────────────────────────
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.set_facecolor(CARD_COLOR)

    if not top_cat.empty:
        bars = ax2.barh(
            range(len(top_cat)), top_cat.values,
            color=[PALETTE[i % len(PALETTE)] for i in range(len(top_cat))],
            edgecolor="white", linewidth=0.5, height=0.65,
        )
        ax2.set_yticks(range(len(top_cat)))
        ax2.set_yticklabels(top_cat.index, fontsize=8.5)
        ax2.invert_yaxis()
        ax2.set_xlabel("Nº de OSs", fontsize=9)
        ax2.spines[["top", "right"]].set_visible(False)

        for bar, val in zip(bars, top_cat.values):
            ax2.text(
                bar.get_width() + 0.05, bar.get_y() + bar.get_height() / 2,
                str(val), va="center", ha="left", fontsize=9, fontweight="bold",
            )

    ax2.set_title(
        "② Volume de OSs por Categoria",
        fontsize=12, fontweight="bold", color=SICOOB_DARK, pad=12,
    )

    # ── Gráfico 3 — Evolução Mensal ───────────────────────────────
    ax3 = fig.add_subplot(gs[1, 0])
    ax3.set_facecolor(CARD_COLOR)

    if not mensal.empty:
        ax3.fill_between(
            range(len(mensal)), mensal.values,
            alpha=0.18, color=SICOOB_GREEN,
        )
        ax3.plot(
            range(len(mensal)), mensal.values,
            marker="o", color=SICOOB_GREEN, linewidth=2.5,
            markersize=8, markerfacecolor="white", markeredgewidth=2,
        )
        ax3.set_xticks(range(len(mensal)))
        ax3.set_xticklabels(mensal.index, rotation=30, ha="right", fontsize=8)

        for x, y in enumerate(mensal.values):
            ax3.text(x, y + 0.02, str(y), ha="center", va="bottom",
                     fontsize=8.5, fontweight="bold", color=SICOOB_GREEN)

        ax3.set_ylabel("Nº de OSs", fontsize=9)
        ax3.spines[["top", "right"]].set_visible(False)
        ax3.yaxis.set_major_locator(plt.MaxNLocator(integer=True))

    ax3.set_title(
        "③ Evolução Mensal de OSs Abertas",
        fontsize=12, fontweight="bold", color=SICOOB_DARK, pad=12,
    )

    # ── Gráfico 4 — Distribuição por Prioridade ───────────────────
    ax4 = fig.add_subplot(gs[1, 1])
    ax4.set_facecolor(CARD_COLOR)

    if not prio_counts.empty:
        cores_prio = {
            "4": SICOOB_RED,    # Crítico
            "1": SICOOB_RED,
            "2": SICOOB_GOLD,   # Urgente
            "3": SICOOB_BLUE,   # Normal
        }
        bar_cores = [
            next((v for k, v in cores_prio.items() if k in str(idx)),
                 SICOOB_GREEN)
            for idx in prio_counts.index
        ]

        bars4 = ax4.bar(
            range(len(prio_counts)), prio_counts.values,
            color=bar_cores, edgecolor="white", linewidth=0.5, width=0.6,
        )
        ax4.set_xticks(range(len(prio_counts)))
        ax4.set_xticklabels(
            [str(i)[:20] for i in prio_counts.index],
            rotation=20, ha="right", fontsize=8,
        )
        ax4.set_ylabel("Nº de OSs", fontsize=9)
        ax4.spines[["top", "right"]].set_visible(False)
        ax4.yaxis.set_major_locator(plt.MaxNLocator(integer=True))

        for bar, val in zip(bars4, prio_counts.values):
            ax4.text(
                bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
                str(val), ha="center", va="bottom",
                fontsize=9, fontweight="bold",
            )

    ax4.set_title(
        "④ Distribuição por Prioridade",
        fontsize=12, fontweight="bold", color=SICOOB_DARK, pad=12,
    )

    # Rodapé
    fig.text(
        0.5, 0.025,
        "Sicoob Credfaz  •  Analista de Dados: Matheus Benvindo  •  "
        "Stack: MariaDB → Python (ETL) → Power BI",
        ha="center", va="center",
        fontsize=8.5, color="#888888", fontstyle="italic",
    )

    saida = os.path.join(pasta_saida, "02_dashboard_sla.png")
    plt.savefig(saida, dpi=160, bbox_inches="tight", facecolor=BG_COLOR)
    plt.close()
    print(f"     ✅ Dashboard salvo: {os.path.relpath(saida)}")


# =============================================================================
#  PONTO DE ENTRADA
# =============================================================================
def main():
    print("\n" + "="*65)
    print("  ETL - Indicadores de SLA | Sicoob Credfaz")
    print("  Iniciado em:", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    print("="*65)

    # ── EXTRAÇÃO ─────────────────────────────────────────────────
    df_raw = extrair_dados(RAW_PATH)

    # ── PREVIEW DO XLSX (antes da transformação) ──────────────────
    print("\n" + "="*65)
    print("  VISUALIZAÇÃO 1 — PREVIEW DO XLSX ORIGINAL")
    print("="*65)
    gerar_preview_xlsx(XLSX_PATH, PRINT_DIR)

    # ── TRANSFORMAÇÃO ─────────────────────────────────────────────
    df_tratado = transformar_dados(df_raw)

    # ── CARGA ─────────────────────────────────────────────────────
    print("\n" + "="*65)
    print("  FASE 3 — CARGA")
    print("="*65)
    salvar_csv(df_tratado, PROCESSED_PATH)

    # ── DASHBOARD (dados tratados) ────────────────────────────────
    print("\n" + "="*65)
    print("  VISUALIZAÇÃO 2 — DASHBOARD SLA (DADOS TRATADOS)")
    print("="*65)
    gerar_dashboard(df_tratado, PRINT_DIR)

    print("\n" + "="*65)
    print("  PIPELINE CONCLUIDO COM SUCESSO!")
    print(f"  [CSV]  CSV tratado    : {os.path.relpath(PROCESSED_PATH)}")
    print(f"  [IMG]  Prints gerados : {os.path.relpath(PRINT_DIR)}/")
    print("="*65 + "\n")


if __name__ == "__main__":
    main()
