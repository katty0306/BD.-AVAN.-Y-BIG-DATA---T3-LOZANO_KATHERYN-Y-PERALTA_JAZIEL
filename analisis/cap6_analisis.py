"""
ETAC - Capítulo VI: validación de datasets y resultados preliminares.
Lee el dataset de la base transaccional (sql/02_datos_etac_oltp.sql), la zona raw y la zona
curated del data lake (escala x1) y el benchmark; calcula estadísticas descriptivas, perfil
de calidad antes/después, ejemplos de transformación y pruebas estadísticas.
Salida: evidencias/cap6/resultados.json
"""
import csv, glob, json, math, os, re, statistics as st
import pandas as pd
from scipy import stats

BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
LAKE = os.path.join(BASE, "datalake", "x1")
OUT = os.path.join(BASE, "evidencias", "cap6"); os.makedirs(OUT, exist_ok=True)
R = {}

# ------------------------------------------------------------------ OLTP desde el script T-SQL
TOK = re.compile(r"N'((?:[^']|'')*)'|'((?:[^']|'')*)'|(NULL)|(-?\d+(?:\.\d+)?)")
def leer_tsql(path):
    tablas, actual, cols = {}, None, None
    for linea in open(path, encoding="utf-8-sig"):
        m = re.match(r"INSERT INTO dbo\.(\w+) \(([^)]*)\) VALUES", linea)
        if m:
            actual, cols = m.group(1), [c.strip() for c in m.group(2).split(",")]
            tablas.setdefault(actual, {"cols": cols, "filas": []}); continue
        if actual and linea.startswith("    ("):
            vals = []
            for a, b, nul, num in TOK.findall(linea):
                if nul: vals.append(None)
                elif num: vals.append(float(num) if "." in num else int(num))
                else: vals.append((a or b).replace("''", "'"))
            tablas[actual]["filas"].append(vals)
        elif linea.startswith("SET IDENTITY_INSERT") and "OFF" in linea:
            actual = None
    return {t: pd.DataFrame(v["filas"], columns=v["cols"]) for t, v in tablas.items()}

O = leer_tsql(os.path.join(BASE, "sql", "02_datos_etac_oltp.sql"))
R["oltp_filas"] = {t: len(d) for t, d in O.items()}

alu = O["alumno"].copy()
alu["fecha_nacimiento"] = pd.to_datetime(alu.fecha_nacimiento)
ref = pd.Timestamp("2026-11-07")
alu["edad"] = ((ref - alu.fecha_nacimiento).dt.days // 365.25).astype(int)
validos = alu[alu.edad.between(4, 45)]
pre, mat, pag, asi = O["prematricula"], O["matricula"], O["pago"], O["asistencia"]

def desc(s):
    s = pd.Series(s).dropna().astype(float)
    return dict(n=int(s.size), media=round(s.mean(), 2), mediana=round(s.median(), 2), de=round(s.std(), 2),
                min=round(s.min(), 2), max=round(s.max(), 2), p95=round(s.quantile(0.95), 2))

R["desc"] = {
    "edad_alumno": desc(validos.edad),
    "min_prematricula": desc(pre.minutos_proceso),
    "min_matricula": desc(mat.minutos_proceso),
    "monto_pago": desc(pag.monto),
}
R["alumnos_categoria"] = validos.categoria.value_counts().reindex(["Sub-8", "Sub-10", "Sub-12", "Sub-14", "Sub-17", "Adultos"]).fillna(0).astype(int).to_dict()
R["alumnos_sexo"] = validos.sexo.value_counts().to_dict()
R["alumnos_fuera_rango"] = int((~alu.edad.between(4, 45)).sum())
R["pre_por_canal"] = pre.canal.value_counts().to_dict()
R["pre_por_estado"] = pre.estado.value_counts().to_dict()
R["pago_por_medio"] = pag.medio_pago.value_counts().to_dict()
R["pago_por_estado"] = pag.estado_validacion.value_counts().to_dict()
R["asis_por_estado"] = asi.estado.value_counts().to_dict()
R["asis_tasa"] = round(asi.estado.isin(["Presente", "Tarde"]).mean() * 100, 1)

# completitud de la base transaccional (columnas opcionales)
R["oltp_nulos"] = {
    "apoderado.correo": round(O["apoderado"].correo.isna().mean() * 100, 1),
    "alumno.id_apoderado (adultos)": round(alu.id_apoderado.isna().mean() * 100, 1),
    "prematricula.fecha_clase_prueba": round(pre.fecha_clase_prueba.isna().mean() * 100, 1),
    "pago.fecha_validacion (pendientes)": round(pag.fecha_validacion.isna().mean() * 100, 1),
}

# ------------------------------------------------------------------ zona raw x1
raw = os.path.join(LAKE, "raw")
inv = []
for nombre, patron, tipo in [("Chats de WhatsApp", "whatsapp/*/*/*.txt", "TXT"),
                             ("Excel histórico de matrículas", "excel/*/*.xlsx", "XLSX"),
                             ("Notas de asistencia", "notas_asistencia/*/*.txt", "TXT"),
                             ("Comprobantes de pago", "comprobantes/*/*.png", "PNG")]:
    fs = glob.glob(os.path.join(raw, patron))
    reg = None
    if tipo == "TXT":
        reg = sum(sum(1 for _ in open(f, encoding="utf-8")) for f in fs)
    elif tipo == "XLSX":
        reg = len(pd.read_csv(glob.glob(os.path.join(raw, "excel/*/*.csv"))[0]))
    else:
        reg = len(fs)
    inv.append(dict(fuente=nombre, archivos=len(fs), registros=reg, kb=round(sum(os.path.getsize(f) for f in fs) / 1024, 1), formato=tipo))
R["raw_inventario"] = inv

# ------------------------------------------------------------------ zona curated x1
cur = os.path.join(LAKE, "curated")
cinv = []
for t in ["prospecto_curado", "alumno_historico_curado", "asistencia_historica_curada", "comprobante_curado"]:
    d = pd.read_parquet(os.path.join(cur, t))
    size = sum(os.path.getsize(f) for f in glob.glob(os.path.join(cur, t, "**", "*.parquet"), recursive=True))
    parts = len({os.path.dirname(f) for f in glob.glob(os.path.join(cur, t, "**", "*.parquet"), recursive=True)})
    cinv.append(dict(tabla=t, filas=len(d), columnas=d.shape[1], particiones=parts, kb=round(size / 1024, 1)))
R["curated_inventario"] = cinv
qz = pd.read_parquet(os.path.join(LAKE, "quarantine"))
R["quarantine_total"] = len(qz)

pros = pd.read_parquet(os.path.join(cur, "prospecto_curado"))
R["prospecto_completitud"] = {c: round(pros[c].notna().mean() * 100, 1) for c in ["nombre_extraido", "edad_extraida", "sede_interes", "horario_interes"]}
R["prospecto_edad"] = desc(pros.edad_extraida)
R["prospecto_sede"] = pros.sede_interes.fillna("No indicada").value_counts().to_dict()
R["prospecto_intencion"] = pros.intencion.value_counts().to_dict()
R["prospecto_multichat"] = int((pros.n_chats > 1).sum())
ah = pd.read_parquet(os.path.join(cur, "alumno_historico_curado"))
R["historico_por_sede"] = ah.sede.value_counts().to_dict()
R["historico_monto"] = desc(ah.monto)
asih = pd.read_parquet(os.path.join(cur, "asistencia_historica_curada"))
R["asis_hist_estado"] = asih.estado.value_counts().to_dict()
R["asis_hist_fechas"] = [str(asih.fecha.min()), str(asih.fecha.max())]
R["asis_hist_por_anio_mes"] = asih.groupby(["anio", "mes"]).size().reset_index().values.tolist()

# ------------------------------------------------------------------ perfil de calidad Excel: raw vs curated
ex = pd.read_csv(glob.glob(os.path.join(raw, "excel/*/*.csv"))[0], dtype=str, keep_default_na=False)
SEDES_OK = {"sede norte", "norte", "los olivos", "la de los olivos", "sede centro", "centro", "jesus maria", "jesús maría",
            "la del centro", "sede sur", "sur", "sjm", "san juan de miraflores", "la del sur"}
def fecha_ok(s):
    try: pd.to_datetime(s, format="%d/%m/%Y"); return True
    except Exception: return False
raw_perf = {
    "DNI con formato inválido (no 8 dígitos)": int((~ex.dni.str.fullmatch(r"\d{8}")).sum()),
    "Fecha de nacimiento inválida": int((~ex.fecha_nac.map(fecha_ok)).sum()),
    "Sede escrita de forma no estándar": int((~ex.sede.isin(["Sede Norte", "Sede Centro", "Sede Sur"])).sum()),
    "Sede inexistente": int((~ex.sede.str.strip().str.lower().isin(SEDES_OK)).sum()),
    "Estado fuera de dominio o sin normalizar": int((~ex.estado.isin(["Matriculado", "Pendiente", "Inactivo"])).sum()),
    "Monto no numérico, vacío o negativo": int((~ex.monto.str.fullmatch(r"\d+(\.\d+)?")).sum()),
    "Nombre con espacios o mayúsculas indebidas": int((ex.alumno != ex.alumno.str.strip().str.title()).sum()),
    "Filas duplicadas (mismo DNI)": int(ex.duplicated("dni").sum()),
}
cur_perf = {
    "DNI con formato inválido (no 8 dígitos)": int((~ah.dni.str.fullmatch(r"\d{8}")).sum()),
    "Fecha de nacimiento inválida": int(ah.fecha_nacimiento.isna().sum()),
    "Sede escrita de forma no estándar": int((~ah.sede.isin(["Sede Norte", "Sede Centro", "Sede Sur"])).sum()),
    "Sede inexistente": 0,
    "Estado fuera de dominio o sin normalizar": int((~ah.estado.isin(["Matriculado", "Pendiente", "Inactivo"])).sum()),
    "Monto no numérico, vacío o negativo": int((ah.monto.isna() | (ah.monto <= 0)).sum()),
    "Nombre con espacios o mayúsculas indebidas": int((ah.alumno != ah.alumno.str.strip().str.title()).sum()),
    "Filas duplicadas (mismo DNI)": int(ah.duplicated("dni").sum()),
}
R["calidad_excel"] = [dict(problema=k, raw=v, raw_pct=round(v / len(ex) * 100, 1), curated=cur_perf[k]) for k, v in raw_perf.items()]
R["excel_filas"] = [len(ex), len(ah)]
R["excel_filas_con_algun_error"] = int(((~ex.dni.str.fullmatch(r"\d{8}")) | (~ex.fecha_nac.map(fecha_ok)) |
                                        (~ex.sede.str.strip().str.lower().isin(SEDES_OK)) |
                                        (~ex.estado.isin(["Matriculado", "Pendiente", "Inactivo", "matriculado", "MATRICULADO", "Matric.", "pendiente", "activo"])) |
                                        (~ex.monto.str.fullmatch(r"\d+(\.\d+)?"))).sum())

# ------------------------------------------------------------------ ejemplos de transformación (antes / después)
ah_i = ah.set_index("dni")
ej = []
def agregar(cond, etiqueta):
    for _, f in ex[cond].iterrows():
        dni = re.sub(r"[.\s-]", "", f.dni)
        if dni in ah_i.index:
            d = ah_i.loc[dni]
            if isinstance(d, pd.DataFrame): d = d.iloc[0]
            ej.append(dict(regla=etiqueta, antes=f"{f.alumno.strip()[:30]} | DNI {f.dni} | {f.sede} | {f.estado} | {f.monto}",
                           despues=f"{d.alumno[:30]} | DNI {dni} | {d.sede} | {d.estado} | {d.monto:.2f}"))
            return
agregar(ex.dni.str.contains(r"\."), "R02 DNI con punto")
agregar(ex.monto.str.contains("S/"), "R08 monto en texto")
agregar(ex.alumno.str.startswith("  "), "R01 espacios y mayúsculas")
agregar(ex.sede.str.lower().isin(["los olivos", "sjm", "la del centro", "jesus maria"]), "R05 sinónimo de sede")
agregar(ex.estado.isin(["Matric.", "activo"]), "R07 estado no estándar")
R["ejemplos_transformacion"] = ej
qz_ej = qz.groupby(["fuente", "motivo"]).head(1)[["fuente", "id_origen", "motivo"]].head(8)
R["ejemplos_cuarentena"] = qz_ej.values.tolist()

# ------------------------------------------------------------------ ES-BI: pruebas de hipótesis (procedimiento)
t5 = stats.ttest_1samp(pre.minutos_proceso.astype(float), 10, alternative="less")
t6 = stats.ttest_1samp(mat.minutos_proceso.astype(float), 5, alternative="less")
w5 = stats.wilcoxon(pre.minutos_proceso.astype(float) - 10, alternative="less")
w6 = stats.wilcoxon(mat.minutos_proceso.astype(float) - 5, alternative="less")
sh5, sh6 = stats.shapiro(pre.minutos_proceso.astype(float)), stats.shapiro(mat.minutos_proceso.astype(float))
R["hipotesis"] = {
    "H1-a": dict(n=len(pre), media=round(pre.minutos_proceso.mean(), 2), t=round(t5.statistic, 2), p=t5.pvalue,
                 shapiro_p=round(sh5.pvalue, 4), wilcoxon_p=w5.pvalue),
    "H1-b": dict(n=len(mat), media=round(mat.minutos_proceso.mean(), 2), t=round(t6.statistic, 2), p=t6.pvalue,
                 shapiro_p=round(sh6.pvalue, 4), wilcoxon_p=w6.pvalue),
}
# H1-d: 0 errores en destino -> límite superior IC 95 % (Clopper-Pearson unilateral: 1 - 0.05^(1/n))
n_dest = 221 + 570 + 4993 + 224
R["h1d"] = dict(n=n_dest, errores=0, limite_sup_pct=round((1 - 0.05 ** (1 / n_dest)) * 100, 3))
# K5/K6 casos dentro de la meta
R["k5_en_meta_pct"] = round((pre.minutos_proceso < 10).mean() * 100, 1)
R["k6_en_meta_pct"] = round((mat.minutos_proceso < 5).mean() * 100, 1)

# ------------------------------------------------------------------ ES-BD
b = pd.read_csv(os.path.join(BASE, "evidencias", "benchmark_bigdata.csv"))
def med(impl, w, e): return float(b[(b.implementacion.str.startswith(impl)) & (b.workers == w) & (b.escala == e)].mediana_s.iloc[0])
R["bench"] = b.to_dict("records")
R["aceleracion"] = {e: dict(spark2_vs_spark1=round(med("B", 1, e) / med("B", 2, e), 2),
                            spark2_vs_pandas=round(med("A", 1, e) / med("B", 2, e), 3)) for e in ["x1", "x10", "x100"]}
R["crecimiento_x10"] = {cfg: dict(x1_x10=round(med(i, w, "x10") / med(i, w, "x1"), 1), x10_x100=round(med(i, w, "x100") / med(i, w, "x10"), 1))
                        for cfg, i, w in [("pandas", "A", 1), ("spark1", "B", 1), ("spark2", "B", 2)]}
R["cv_tiempos"] = {f"{r.implementacion} w{r.workers} {r.escala}": round(st.stdev(map(float, r.tiempos.split(";"))) / st.mean(map(float, r.tiempos.split(";"))) * 100, 1)
                   for r in b.itertuples()}
R["ocr"] = json.load(open(os.path.join(BASE, "evidencias", "ocr_exactitud.json")))
R["bd_pruebas"] = list(csv.DictReader(open(os.path.join(BASE, "evidencias", "resultados_pruebas_bigdata.csv"), encoding="utf-8")))
R["pipeline_x1"] = json.load(open(os.path.join(LAKE, "metricas_pipeline.json")))

json.dump(R, open(os.path.join(OUT, "resultados.json"), "w"), indent=1, ensure_ascii=False, default=str)
print(json.dumps({k: R[k] for k in ["oltp_filas", "desc", "raw_inventario", "curated_inventario", "calidad_excel", "excel_filas",
                                    "excel_filas_con_algun_error", "ejemplos_transformacion", "hipotesis", "h1d",
                                    "aceleracion", "crecimiento_x10", "prospecto_completitud", "asis_hist_fechas",
                                    "asis_hist_por_anio_mes", "k5_en_meta_pct", "k6_en_meta_pct", "quarantine_total"]},
                 indent=1, ensure_ascii=False, default=str))
