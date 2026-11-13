"""Gráfico del experimento E0-E2 a partir de evidencias/benchmark_bigdata.csv."""
import os
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

B = os.path.join(os.path.dirname(__file__), "..", "evidencias")
df = pd.read_csv(os.path.join(B, "benchmark_bigdata.csv"))
df["serie"] = df.apply(lambda r: "pandas · 1 nodo" if r.implementacion.startswith("A") else f"PySpark · {r.workers} worker{'s' if r.workers > 1 else ''}", axis=1)
COL = {"pandas · 1 nodo": "#2a78d6", "PySpark · 1 worker": "#eb6834", "PySpark · 2 workers": "#1baf7a"}
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"
fig, axes = plt.subplots(1, 2, figsize=(12, 4.6), facecolor="#fcfcfb")
for ax, col, tit in [(axes[0], "mediana_s", "Tiempo del pipeline (s, escala log)"), (axes[1], "registros_por_s", "Rendimiento (registros por segundo, escala log)")]:
    ax.set_facecolor("#fcfcfb")
    for s, g in df.groupby("serie"):
        g = g.sort_values("registros")
        ax.plot(g.escala, g[col], color=COL[s], lw=2, marker="o", ms=6, label=s)
        for x, y in zip(g.escala, g[col]):
            ax.annotate(f"{y:,.0f}".replace(",", " ") if col != "mediana_s" else f"{y:g}", (x, y), textcoords="offset points",
                        xytext=(0, 7), ha="center", fontsize=8, color=INK)
    ax.set_yscale("log"); ax.set_title(tit, loc="left", fontsize=10.5, fontweight="bold", color=INK)
    ax.grid(axis="y", color=GRID, lw=0.6); ax.set_axisbelow(True)
    for sp in ("top", "right", "left"): ax.spines[sp].set_visible(False)
    ax.spines["bottom"].set_color(GRID); ax.tick_params(length=0, colors=INK2); ax.set_xlabel("Escala del dataset DS-3", color=INK2)
axes[0].legend(frameon=False, fontsize=8.5, loc="upper left")
fig.tight_layout(); fig.savefig(os.path.join(B, "benchmark_bigdata.png"), dpi=170)
print("ok")
