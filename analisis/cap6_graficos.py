"""ETAC - Gráficos del Capítulo VI (vinculados a los objetivos). Salida: evidencias/cap6/*.png"""
import json, os, re
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
OUT = os.path.join(BASE, "evidencias", "cap6")
R = json.load(open(os.path.join(OUT, "resultados.json")))
SURF, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
S1, S2, S3 = "#2a78d6", "#eb6834", "#1baf7a"
BASEL = "#b9b8b3"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9, "axes.edgecolor": GRID, "axes.labelcolor": INK2,
                     "xtick.color": INK2, "ytick.color": INK2, "axes.titlesize": 10, "axes.titleweight": "bold",
                     "axes.titlecolor": INK, "axes.titlelocation": "left", "figure.facecolor": SURF, "axes.facecolor": SURF})


def ejes(ax, g="x"):
    for s in ("top", "right", "left"): ax.spines[s].set_visible(False)
    ax.grid(axis=g, color=GRID, lw=0.6); ax.set_axisbelow(True); ax.tick_params(length=0)


# ---------- G1: tiempos de proceso: línea base vs solución vs meta (Objetivo general / OE-Experimentación)
fig, axes = plt.subplots(1, 3, figsize=(12, 2.9))
casos = [("K5 Pre-matrícula (min)", (30, 50), R["desc"]["min_prematricula"]["media"], 10, 60),
         ("K6 Registro de matrícula (min)", (15, 20), R["desc"]["min_matricula"]["media"], 5, 25),
         ("K11 Consolidación de asistencia (h)", (6, 12), 0.0, 0.0, 14)]
for ax, (tit, (lo, hi), sol, meta, xmax) in zip(axes, casos):
    ejes(ax)
    ax.barh(["Línea base\n(manual)"], [hi - lo], left=[lo], color=BASEL, height=0.5)
    ax.text(hi + xmax * 0.02, 0, f"{lo}–{hi}", va="center", fontsize=9, color=INK)
    ax.barh(["Solución\n(DS-3)"], [max(sol, xmax * 0.004)], color=S1, height=0.5)
    ax.text(max(sol, 0) + xmax * 0.02, 1, f"{sol:.1f}".replace(".", ","), va="center", fontsize=9, color=INK, fontweight="bold")
    if meta > 0:
        ax.axvline(meta, color=INK2, ls="--", lw=1); ax.text(meta, 1.45, f"meta < {meta}", fontsize=7.5, color=INK2, ha="center")
    else:
        ax.text(xmax * 0.35, 1.45, "meta: 0 al concluir la clase", fontsize=7.5, color=INK2)
    ax.set_xlim(0, xmax); ax.set_title(tit); ax.set_ylim(-0.6, 1.7)
fig.tight_layout(); fig.savefig(os.path.join(OUT, "g1_tiempos_vs_linea_base.png"), dpi=170); plt.close(fig)

# ---------- G2: calidad de datos del Excel histórico, raw vs curated (OE-Big Data / H1-d)
c = [x for x in R["calidad_excel"] if x["problema"] != "Sede escrita de forma no estándar"]
fig, ax = plt.subplots(figsize=(10, 3.6)); ejes(ax)
y = np.arange(len(c)); h = 0.38
ax.barh(y + h / 2, [x["raw"] for x in c], height=h, color=S2, label="Zona raw (antes)")
ax.barh(y - h / 2, [x["curated"] for x in c], height=h, color=S1, label="Zona curated (después)")
for i, x in enumerate(c):
    ax.text(x["raw"] + 2, i + h / 2, f"{x['raw']} ({x['raw_pct']:.1f} %)".replace(".", ","), va="center", fontsize=8, color=INK)
    ax.text(x["curated"] + 2, i - h / 2, str(x["curated"]), va="center", fontsize=8, color=INK)
ax.set_yticks(y, [x["problema"] for x in c]); ax.invert_yaxis()
ax.set_xlim(0, max(x["raw"] for x in c) * 1.25)
ax.legend(frameon=False, loc="lower right", fontsize=8.5)
ax.set_title(f"Problemas de calidad en el Excel histórico ({R['excel_filas'][0]} filas raw → {R['excel_filas'][1]} filas curated)")
fig.tight_layout(); fig.savefig(os.path.join(OUT, "g2_calidad_raw_vs_curated.png"), dpi=170); plt.close(fig)

# ---------- G3: distribución de K5 y K6 frente a la meta (OE-BI / H1-a, H1-b)
import re as _re
TOK = _re.compile(r"(-?\d+\.\d+)\)$")
def minutos(tabla):
    vals, dentro = [], False
    for l in open(os.path.join(BASE, "sql", "02_datos_etac_oltp.sql"), encoding="utf-8-sig"):
        if l.startswith(f"INSERT INTO dbo.{tabla} "): dentro = True; continue
        if l.startswith("SET IDENTITY_INSERT") and "OFF" in l: dentro = False
        if dentro and l.startswith("    ("):
            m = TOK.search(l.rstrip().rstrip(","))
            if m: vals.append(float(m.group(1)))
    return vals
pre, mat = minutos("prematricula"), minutos("matricula")
fig, axes = plt.subplots(1, 2, figsize=(11, 3.3))
for ax, v, meta, tit, col in [(axes[0], pre, 10, "K5 · pre-matrícula", S1), (axes[1], mat, 5, "K6 · registro de matrícula", S1)]:
    ejes(ax, "y")
    ax.hist(v, bins=18, color=col, edgecolor=SURF, linewidth=1.5)
    ax.axvline(meta, color=S2, lw=2); ax.text(meta, ax.get_ylim()[1] * 0.92, f" meta < {meta} min", color=INK, fontsize=8.5)
    ax.axvline(np.mean(v), color=INK2, lw=1, ls="--")
    ax.text(np.mean(v), ax.get_ylim()[1] * 0.78, f" media {np.mean(v):.2f}".replace(".", ","), color=INK2, fontsize=8)
    dentro = np.mean(np.array(v) < meta) * 100
    ax.set_title(f"{tit} (n = {len(v)}; {dentro:.1f} % en meta)".replace(".", ",", 1)); ax.set_xlabel("minutos"); ax.set_ylabel("casos")
fig.tight_layout(); fig.savefig(os.path.join(OUT, "g3_distribucion_k5_k6.png"), dpi=170); plt.close(fig)

# ---------- G4: crecimiento del tiempo por cada ×10 de volumen (OE-Big Data / H2)
cr = R["crecimiento_x10"]
fig, ax = plt.subplots(figsize=(9, 3.2)); ejes(ax, "y")
labels = ["pandas · 1 nodo", "PySpark · 1 worker", "PySpark · 2 workers"]; keys = ["pandas", "spark1", "spark2"]; cols = [S1, S2, S3]
x = np.arange(2); w = 0.25
for i, (k, l, col) in enumerate(zip(keys, labels, cols)):
    vals = [cr[k]["x1_x10"], cr[k]["x10_x100"]]
    ax.bar(x + (i - 1) * w, vals, width=w - 0.02, color=col, label=l)
    for xi, v in zip(x, vals): ax.text(xi + (i - 1) * w, v + 0.15, f"{v:g}×".replace(".", ","), ha="center", fontsize=8.5, color=INK)
ax.axhline(10, color=INK2, ls="--", lw=1); ax.text(1.45, 10.2, "límite de aceptación: ×10", fontsize=8, color=INK2, ha="right")
ax.set_xticks(x, ["de ×1 a ×10", "de ×10 a ×100"]); ax.set_ylim(0, 11.5); ax.set_ylabel("veces que crece el tiempo")
ax.legend(frameon=False, fontsize=8.5, loc="upper left", bbox_to_anchor=(0, 0.86)); ax.set_title("Crecimiento del tiempo del pipeline cuando el volumen se multiplica por 10")
fig.tight_layout(); fig.savefig(os.path.join(OUT, "g4_crecimiento_por_volumen.png"), dpi=170); plt.close(fig)

# ---------- G5: demanda (prospectos de chats) vs oferta (vacantes) por sede (OE-BI + OE-Big Data)
txt = open(os.path.join(BASE, "sql", "02_datos_etac_oltp.sql"), encoding="utf-8-sig").read()
def filas(tabla):
    b = txt[txt.index(f"INSERT INTO dbo.{tabla} "):txt.index(f"SET IDENTITY_INSERT dbo.{tabla} OFF")]
    return [re.findall(r"N?'(?:[^']|'')*'|NULL|-?\d+(?:\.\d+)?", l) for l in b.splitlines() if l.startswith("    (")]
sede = {int(f[0]): f[1].strip("N'") for f in filas("sede")}
grupo = {int(f[0]): (sede[int(f[6])], int(f[4])) for f in filas("grupo")}
ocup = {}
for f in filas("matricula"):
    if f[5] == "N'Matriculado'": ocup[int(f[2])] = ocup.get(int(f[2]), 0) + 1
vac = {}
for gid, (s, aforo) in grupo.items(): vac[s] = vac.get(s, 0) + aforo - ocup.get(gid, 0)
dem = R["prospecto_sede"]
sedes = ["Sede Norte", "Sede Centro", "Sede Sur"]
fig, ax = plt.subplots(figsize=(9, 3.2)); ejes(ax, "y")
x = np.arange(3); w = 0.36
d = [dem.get(s, 0) for s in sedes]; v = [vac.get(s, 0) for s in sedes]
ax.bar(x - w / 2, d, width=w - 0.02, color=S1, label="Prospectos interesados (chats curados)")
ax.bar(x + w / 2, v, width=w - 0.02, color=S3, label="Vacantes disponibles (base transaccional)")
for xi, a, b in zip(x, d, v):
    ax.text(xi - w / 2, a + 1, str(a), ha="center", fontsize=8.5, color=INK); ax.text(xi + w / 2, b + 1, str(b), ha="center", fontsize=8.5, color=INK)
ax.set_xticks(x, sedes); ax.legend(frameon=False, fontsize=8.5, loc="upper right"); ax.set_ylim(0, max(d + v) * 1.3)
ax.set_title(f"Demanda frente a oferta por sede ({dem.get('No indicada', 0)} prospectos no indicaron sede)")
fig.tight_layout(); fig.savefig(os.path.join(OUT, "g5_demanda_vs_vacantes.png"), dpi=170); plt.close(fig)
R["demanda_oferta"] = {s: dict(prospectos=dem.get(s, 0), vacantes=vac.get(s, 0)) for s in sedes}
json.dump(R, open(os.path.join(OUT, "resultados.json"), "w"), indent=1, ensure_ascii=False, default=str)
print("ok", R["demanda_oferta"])
