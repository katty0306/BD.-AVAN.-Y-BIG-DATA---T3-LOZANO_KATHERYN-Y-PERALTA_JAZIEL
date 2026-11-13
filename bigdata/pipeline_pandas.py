"""
ETAC - Pipeline equivalente en un solo nodo con pandas (implementación A, línea de comparación)
Mismas reglas R01-R13 que pipeline_spark.py (sin OCR), para el experimento E0-E2.
"""
import glob, hashlib, os, re, time
import pandas as pd
import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import reglas as R

SEDES = {R.sin_tildes(k): v for k, v in R.SEDES.items()}
HORAS = {R.sin_tildes(k): v for k, v in R.HORAS.items()}


def chats(raw):
    filas = []
    rx = re.compile(R.RX_LINEA)
    for f in glob.glob(os.path.join(raw, "whatsapp", "*", "*", "*.txt")):
        with open(f, encoding="utf-8") as fh:
            for linea in fh:
                m = rx.match(linea.rstrip("\n"))
                if m: filas.append((f, *m.groups()))
    df = pd.DataFrame(filas, columns=["archivo", "fecha", "hora", "autor", "texto"])
    df = df[(df.autor != "ETAC") & ~df.texto.str.contains("<Multimedia")].copy()
    df["ts"] = pd.to_datetime(df.fecha + " " + df.hora, format="%d/%m/%y %H:%M")
    g = df.groupby("autor").agg(fecha_primer_contacto=("ts", "min"), txt=("texto", " | ".join),
                                n_chats=("archivo", "nunique")).reset_index()
    tl = g.txt.map(lambda s: R.sin_tildes(s).lower())
    ne = g.txt.str.extract(R.RX_NOMBRE_EDAD)
    out = pd.DataFrame({
        "telefono_hash": g.autor.str.replace(r"\D", "", regex=True).map(lambda s: hashlib.sha256(s.encode()).hexdigest()),
        "nombre_extraido": ne[0].str.capitalize(), "edad_extraida": pd.to_numeric(ne[1]),
        "sede_interes": tl.str.extract(R.sin_tildes(R.RX_SEDE))[0].map(SEDES),
        "horario_interes": tl.str.extract(R.sin_tildes(R.RX_HORA))[0].map(HORAS),
        "intencion": tl.str.contains("matricul|lo lleno|formulario").map({True: "alta", False: "media"}),
        "fecha_primer_contacto": g.fecha_primer_contacto, "n_chats": g.n_chats})
    return out


def excel(raw):
    df = pd.concat(pd.read_csv(f, dtype=str, keep_default_na=False) for f in glob.glob(os.path.join(raw, "excel", "*", "*.csv")))
    df["alumno"] = df.alumno.str.replace(r"\s+", " ", regex=True).str.strip().str.title()
    df["dni"] = df.dni.str.replace(r"[.\s-]", "", regex=True)
    df["telefono"] = "+51" + df.telefono.str.replace(r"\D", "", regex=True)
    df["fecha_nacimiento"] = pd.to_datetime(df.fecha_nac, format="%d/%m/%Y", errors="coerce")
    df["sede"] = df.sede.str.strip().map(lambda s: SEDES.get(R.sin_tildes(s).lower()))
    df["estado"] = df.estado.str.strip().str.lower().map(R.ESTADOS)
    df["monto"] = pd.to_numeric(df.monto.str.replace(r"S/\s*", "", regex=True).str.replace(",", "."), errors="coerce")
    mot = pd.Series(None, index=df.index, dtype=object)
    for cond, txt in [(df.monto.isna() | (df.monto <= 0), "R08"), (df.estado.isna(), "R07"), (df.sede.isna(), "R05"),
                      (df.fecha_nacimiento.isna(), "R04"), (~df.dni.str.match(r"^\d{8}$"), "R02")]:
        mot[cond] = txt
    df["motivo"] = mot
    val = df[df.motivo.isna()].sort_values("nro")
    dup = val.duplicated("dni")
    cuar = pd.concat([df[df.motivo.notna()], val[dup].assign(motivo="R06")])
    return val[~dup], cuar


def notas(raw):
    filas = []
    for f in glob.glob(os.path.join(raw, "notas_asistencia", "*", "*.txt")):
        grupo = fecha = None
        with open(f, encoding="utf-8") as fh:
            for l in fh:
                l = l.rstrip("\n")
                if l.startswith("## "):
                    grupo, fecha = l[3:].rsplit(" ", 1)
                elif " - " in l:
                    a, m = l.rsplit(" - ", 1); filas.append((grupo, fecha + "/2025", a.strip(), m.strip()))
    df = pd.DataFrame(filas, columns=["grupo", "fecha_txt", "alumno", "marca"])
    df["fecha"] = pd.to_datetime(df.fecha_txt, format="%d/%m/%Y", errors="coerce")
    df["estado"] = df.marca.str.lower().map(R.ASISTENCIA)
    inval = df.estado.isna()
    ok = df[~inval].sort_values("marca")
    dup = ok.duplicated(["grupo", "fecha", "alumno"])
    return ok[~dup], pd.concat([df[inval], ok[dup]])


def ejecutar(lake, escribir=True):
    raw, cur = os.path.join(lake, "raw"), os.path.join(lake, "curated_pandas")
    t0, et = time.perf_counter(), {}
    t = time.perf_counter(); p = chats(raw); et["chats_parseo"] = time.perf_counter() - t
    t = time.perf_counter(); a, qa = excel(raw); et["excel_limpieza"] = time.perf_counter() - t
    t = time.perf_counter(); s, qs = notas(raw); et["notas_parseo"] = time.perf_counter() - t
    if escribir:
        t = time.perf_counter(); os.makedirs(cur, exist_ok=True)
        p.to_parquet(os.path.join(cur, "prospecto.parquet"), compression="snappy")
        a.to_parquet(os.path.join(cur, "alumno.parquet"), compression="snappy")
        s.to_parquet(os.path.join(cur, "asistencia.parquet"), compression="snappy")
        et["escritura"] = time.perf_counter() - t
    return dict(tiempo_total_s=round(time.perf_counter() - t0, 2), etapas={k: round(v, 2) for k, v in et.items()},
                conteos=dict(prospectos=len(p), alumnos=len(a), asistencias=len(s), cuarentena=len(qa) + len(qs)))


if __name__ == "__main__":
    import json; print(json.dumps(ejecutar(sys.argv[1] if len(sys.argv) > 1 else "datalake/x1"), indent=2))
