"""
ETAC - Experimento E0-E2: pipeline A (pandas, 1 nodo) vs pipeline B (PySpark con 1 y 2 workers locales)
Escalas x1, x10, x100 · 3 repeticiones · se reporta la mediana. El arranque de la sesión Spark
se mide aparte y no se suma al tiempo del pipeline. Sin OCR (se evalúa por separado).
Salida: evidencias/benchmark_bigdata.csv
"""
import csv, os, statistics, sys, time, resource
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pipeline_pandas as PA, pipeline_spark as PS

ESCALAS, REPS = [1, 10, 100], 3
OUT = os.path.join(os.path.dirname(__file__), "..", "evidencias", "benchmark_bigdata.csv")
filas = []


def registros(lake):
    n = 0
    for root, _, files in os.walk(os.path.join(lake, "raw")):
        for f in files:
            if f.endswith((".txt", ".csv")):
                with open(os.path.join(root, f), encoding="utf-8") as fh: n += sum(1 for _ in fh)
    return n


for e in ESCALAS:
    lake = f"datalake/x{e}"; n = registros(lake)
    rs = [PA.ejecutar(lake) for _ in range(REPS)]
    ts = [r["tiempo_total_s"] for r in rs]
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    filas.append(dict(escala=f"x{e}", implementacion="A pandas (1 nodo)", workers=1, registros=n,
                      tiempos=";".join(map(str, ts)), mediana_s=statistics.median(ts),
                      registros_por_s=round(n / statistics.median(ts)), arranque_s=0, mem_pico_mb=round(rss),
                      chats_s=statistics.median(r["etapas"]["chats_parseo"] for r in rs)))
    print(filas[-1], flush=True)

for w in [1, 2]:
    t = time.perf_counter(); spark = PS.spark_session(f"local[{w}]"); spark.sparkContext.setLogLevel("ERROR")
    arranque = round(time.perf_counter() - t, 2)
    for e in ESCALAS:
        lake = f"datalake/x{e}"; n = registros(lake)
        if e == 1: PS.ejecutar(spark, lake, con_ocr=False)  # calentamiento de la JVM, no se cuenta
        rs = [PS.ejecutar(spark, lake, con_ocr=False) for _ in range(REPS)]
        ts = [r["tiempo_total_s"] for r in rs]
        filas.append(dict(escala=f"x{e}", implementacion="B PySpark", workers=w, registros=n,
                          tiempos=";".join(map(str, ts)), mediana_s=statistics.median(ts),
                          registros_por_s=round(n / statistics.median(ts)), arranque_s=arranque, mem_pico_mb="",
                          chats_s=statistics.median(r["etapas"]["chats_parseo"] for r in rs)))
        print(filas[-1], flush=True)
    spark.stop()

with open(OUT, "w", newline="", encoding="utf-8") as fh:
    wr = csv.DictWriter(fh, fieldnames=list(filas[0])); wr.writeheader(); wr.writerows(filas)
print("[OK]", OUT)
