"""
ETAC - Pipeline Big Data con PySpark (implementación B)
raw -> (validación de esquema) -> parseo / OCR -> limpieza R01-R13 -> curated (Parquet snappy)
                                                                   -> quarantine (registros rechazados)
Uso:
  python pipeline_spark.py --lake datalake/x1 [--master local[2]] [--sin-ocr]
En GCP el mismo script corre en Dataproc Serverless cambiando --lake por gs://etac-datalake
"""
import argparse, hashlib, json, os, re, subprocess, tempfile, time, uuid, glob
from datetime import datetime
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import reglas as R

TILDES, SIN = "áéíóúÁÉÍÓÚ", "aeiouAEIOU"


def spark_session(master="local[2]"):
    spark = (SparkSession.builder.appName("etac-pipeline").master(master)
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.files.openCostInBytes", "8192")              # agrupa archivos pequeños por partición
            .config("spark.sql.files.maxPartitionBytes", str(16 * 1024 * 1024))
            .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")      # fecha imposible -> NULL
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # reejecución idempotente
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.ui.enabled", "false").getOrCreate())
    # distribuye los módulos a los workers (equivale a --py-files en Dataproc)
    aqui = os.path.dirname(os.path.abspath(__file__))
    for m in ("reglas.py", "pipeline_spark.py"):
        spark.sparkContext.addPyFile(os.path.join(aqui, m))
    return spark


def mapa(dic):
    """Convierte un diccionario de reglas en una expresión map de Spark (broadcast implícito)."""
    return F.create_map(*[F.lit(x) for kv in dic.items() for x in kv])


# --------------------------------------------------------------------- chats
def procesar_chats(spark, raw, ing):
    df = (spark.read.text(os.path.join(raw, "whatsapp", "*", "*", "*.txt"))
          .withColumn("archivo", F.input_file_name())).cache()
    p = df.select("archivo", *[F.regexp_extract("value", R.RX_LINEA, i).alias(c)
                               for i, c in [(1, "fecha"), (2, "hora"), (3, "autor"), (4, "texto")]])
    malformadas = p.filter(F.col("fecha") == "").count()
    msg = (p.filter((F.col("fecha") != "") & (F.col("autor") != "ETAC") & ~F.col("texto").contains("<Multimedia"))
             .withColumn("ts", F.to_timestamp(F.concat_ws(" ", "fecha", "hora"), "dd/MM/yy HH:mm"))
             .withColumn("t", F.lower(F.translate("texto", TILDES, SIN))))
    # R06: un prospecto = un teléfono (resolución de entidades entre chats)
    g = (msg.groupBy("autor").agg(F.min("ts").alias("fecha_primer_contacto"),
                                  F.concat_ws(" | ", F.collect_list("texto")).alias("txt"),
                                  F.concat_ws(" | ", F.collect_list("t")).alias("tl"),
                                  F.countDistinct("archivo").alias("n_chats")))
    sedes, horas = mapa({R.sin_tildes(k): v for k, v in R.SEDES.items()}), mapa({R.sin_tildes(k): v for k, v in R.HORAS.items()})
    out = (g.select(
        F.sha2(F.regexp_replace("autor", r"\D", ""), 256).alias("telefono_hash"),          # R12
        F.initcap(F.regexp_extract("txt", R.RX_NOMBRE_EDAD, 1)).alias("nombre_extraido"),
        F.regexp_extract("txt", R.RX_NOMBRE_EDAD, 2).cast("int").alias("edad_extraida"),
        sedes[F.regexp_extract("tl", R.sin_tildes(R.RX_SEDE), 1)].alias("sede_interes"),
        horas[F.regexp_extract("tl", R.sin_tildes(R.RX_HORA), 1)].alias("horario_interes"),
        F.when(F.col("tl").rlike("matricul|lo lleno|formulario"), "alta").otherwise("media").alias("intencion"),
        "fecha_primer_contacto", "n_chats")
        .withColumn("id_prospecto", F.substring(F.col("telefono_hash"), 1, 12))
        .withColumn("id_ingesta", F.lit(ing))
        .withColumn("anio", F.year("fecha_primer_contacto")).withColumn("mes", F.month("fecha_primer_contacto")))
    return out.cache(), malformadas


# --------------------------------------------------------------------- excel histórico
ESQUEMA_EXCEL = T.StructType([T.StructField(c, T.StringType(), True) for c in
                              ["nro", "alumno", "dni", "fecha_nac", "telefono", "sede", "horario", "estado", "monto"]])


def procesar_excel(spark, raw, ing):
    df = spark.read.csv(os.path.join(raw, "excel", "*", "*.csv"), header=True, schema=ESQUEMA_EXCEL,  # esquema forzado
                        mode="PERMISSIVE")
    df = df.cache(); leidos = df.count()
    sedes, estados = mapa({R.sin_tildes(k): v for k, v in R.SEDES.items()}), mapa(R.ESTADOS)
    c = (df.withColumn("alumno", F.initcap(F.trim(F.regexp_replace("alumno", r"\s+", " "))))              # R01
           .withColumn("dni", F.regexp_replace("dni", r"[.\s-]", ""))                                        # R02
           .withColumn("telefono", F.concat(F.lit("+51"), F.regexp_replace("telefono", r"\D", "")))        # R03
           .withColumn("fecha_nacimiento", F.to_date("fecha_nac", "dd/MM/yyyy"))                            # R04
           .withColumn("sede", sedes[F.lower(F.translate(F.trim("sede"), TILDES, SIN))])                    # R05
           .withColumn("estado", estados[F.lower(F.trim("estado"))])                                        # R07
           .withColumn("monto", F.regexp_replace(F.regexp_replace("monto", r"S/\s*", ""), ",", ".").cast("double"))  # R08
           .withColumn("motivo", F.when(~F.col("dni").rlike(r"^\d{8}$"), "R02 documento inválido")
                       .when(F.col("fecha_nacimiento").isNull(), "R04 fecha inválida")
                       .when(F.col("sede").isNull(), "R05 sede fuera de catálogo")
                       .when(F.col("estado").isNull(), "R07 estado fuera de dominio")
                       .when(F.col("monto").isNull() | (F.col("monto") <= 0), "R08 monto inválido")))
    validos = c.filter(F.col("motivo").isNull())
    w = Window.partitionBy("dni").orderBy("nro")
    dedup = validos.withColumn("rn", F.row_number().over(w))                                                # R06
    dup = dedup.filter("rn > 1").withColumn("motivo", F.lit("R06 duplicado por documento"))
    cur = (dedup.filter("rn = 1").select("dni", "alumno", "fecha_nacimiento", "telefono", "sede", "horario", "estado", "monto")
           .withColumn("id_ingesta", F.lit(ing)))
    cuar = (c.filter(F.col("motivo").isNotNull()).unionByName(dup.drop("rn"))
            .select(F.lit("excel_matriculas").alias("fuente"), F.col("nro").alias("id_origen"), "motivo")
            .withColumn("id_ingesta", F.lit(ing)))
    return cur, cuar, leidos


# --------------------------------------------------------------------- notas de asistencia
def procesar_notas(spark, raw, ing):
    sc = spark.sparkContext
    rdd = sc.newAPIHadoopFile(os.path.join(raw, "notas_asistencia", "*", "*.txt"),
                              "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                              "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text",
                              conf={"textinputformat.record.delimiter": "## "})   # 1 registro = 1 sesión

    def parse(bloque):
        lineas = [l for l in bloque[1].split("\n") if l.strip()]
        if not lineas: return []
        cab = lineas[0].rsplit(" ", 1)
        return [(cab[0], cab[1] + "/2025", *[x.strip() for x in l.rsplit(" - ", 1)]) for l in lineas[1:] if " - " in l]
    df = rdd.flatMap(parse).toDF(["grupo", "fecha_txt", "alumno", "marca"]).cache()
    leidos = df.count()
    est = mapa(R.ASISTENCIA)
    c = (df.withColumn("fecha", F.to_date("fecha_txt", "dd/MM/yyyy"))
           .withColumn("estado", est[F.lower("marca")])
           .withColumn("motivo", F.when(F.col("estado").isNull(), "R10 estado de asistencia inválido")))
    w = Window.partitionBy("grupo", "fecha", "alumno").orderBy("marca")
    c = c.withColumn("rn", F.row_number().over(w))
    cur = (c.filter(F.col("motivo").isNull() & (F.col("rn") == 1))
           .select("alumno", "grupo", "fecha", "estado").withColumn("fuente", F.lit("notas_profesor"))
           .withColumn("id_ingesta", F.lit(ing)).withColumn("anio", F.year("fecha")).withColumn("mes", F.month("fecha")))
    cuar = (c.filter(F.col("motivo").isNotNull() | (F.col("rn") > 1))
            .select(F.lit("notas_asistencia").alias("fuente"),
                    F.concat_ws("|", "grupo", "fecha_txt", "alumno").alias("id_origen"),
                    F.coalesce("motivo", F.lit("R10 asistencia duplicada")).alias("motivo"))
            .withColumn("id_ingesta", F.lit(ing)))
    return cur, cuar, leidos


# --------------------------------------------------------------------- OCR de comprobantes (distribuido)
def ocr_particion(filas):
    for f in filas:
        nombre = os.path.basename(f.path)
        h = hashlib.sha256(f.content).hexdigest()
        try:
            with tempfile.NamedTemporaryFile(suffix=".png") as tmp:
                tmp.write(f.content); tmp.flush()
                r = subprocess.run(["tesseract", tmp.name, "stdout", "-l", "spa"], capture_output=True, text=True, errors="replace", timeout=60)
            if r.returncode != 0: raise RuntimeError("archivo de imagen corrupto o ilegible")
            txt = r.stdout
            m = re.search(r"S/\s*([\d]+[.,]\d{2})", txt); fe = re.search(r"(\d{2}/\d{2}/\d{4})", txt)
            nro = re.search(r"\b(\d{8})\b", txt)
            medio = "Billetera digital" if "Billetera" in txt else ("Transferencia bancaria" if "Transferencia" in txt else None)
            campos = [m, fe, nro, medio]
            conf = sum(x is not None for x in campos) / 4
            yield (nombre, h, float(m.group(1).replace(",", ".")) if m else None,
                   datetime.strptime(fe.group(1), "%d/%m/%Y").date() if fe else None,
                   nro.group(1) if nro else None, medio, conf, None)
        except Exception as e:                                 # archivo corrupto / ilegible -> cuarentena
            yield (nombre, h, None, None, None, None, 0.0, f"OCR fallido: {str(e)[:80]}")


ESQ_OCR = T.StructType([T.StructField("archivo", T.StringType()), T.StructField("hash_archivo", T.StringType()),
                        T.StructField("monto", T.DoubleType()), T.StructField("fecha_pago", T.DateType()),
                        T.StructField("nro_operacion", T.StringType()), T.StructField("medio_pago", T.StringType()),
                        T.StructField("confianza_ocr", T.DoubleType()), T.StructField("error", T.StringType())])


def procesar_comprobantes(spark, raw, ing):
    b = spark.read.format("binaryFile").option("pathGlobFilter", "*.png").load(os.path.join(raw, "comprobantes", "*"))
    o = spark.createDataFrame(b.select("path", "content").repartition(4).rdd.mapPartitions(ocr_particion), ESQ_OCR).cache()
    w = Window.partitionBy("hash_archivo").orderBy("archivo")
    o = o.withColumn("rn", F.row_number().over(w))                                                   # R09
    ok = (F.col("error").isNull()) & (F.col("confianza_ocr") == 1.0) & (F.col("rn") == 1)
    cur = (o.filter(ok).select(F.substring("hash_archivo", 1, 12).alias("id_comprobante"), "hash_archivo", "monto", "fecha_pago",
                               F.concat(F.lit("****"), F.substring("nro_operacion", -4, 4)).alias("nro_operacion_enmascarado"),  # R12
                               "medio_pago", "confianza_ocr", F.lit("Pendiente de validación").alias("estado_validacion"),
                               F.concat(F.lit("raw/comprobantes/"), "archivo").alias("ruta_original"))
           .withColumn("id_ingesta", F.lit(ing)))
    cuar = (o.filter(~ok).select(F.lit("comprobantes").alias("fuente"), F.col("archivo").alias("id_origen"),
                                 F.when(F.col("error").isNotNull(), F.col("error"))
                                  .when(F.col("rn") > 1, "R09 comprobante duplicado (hash)")
                                  .otherwise(F.concat(F.lit("Confianza OCR "), F.col("confianza_ocr"), F.lit(" < 1: revisión manual"))).alias("motivo"))
            .withColumn("id_ingesta", F.lit(ing)))
    return cur, cuar, o.count(), o.select("archivo", "monto", "fecha_pago", "nro_operacion", "medio_pago", "confianza_ocr")


# --------------------------------------------------------------------- orquestación
def manifiesto(raw, ing):
    filas = []
    for f in sorted(glob.glob(os.path.join(raw, "**", "*.*"), recursive=True)):
        if "/whatsapp/" in f and len(filas) > 50:      # muestra: los chats se registran por lote
            continue
        with open(f, "rb") as fh:
            filas.append(dict(archivo=os.path.relpath(f, raw), bytes=os.path.getsize(f),
                              sha256=hashlib.sha256(fh.read()).hexdigest(), id_ingesta=ing))
    return filas


def ejecutar(spark, lake, con_ocr=True, escribir=True):
    raw, cur_dir, q_dir = os.path.join(lake, "raw"), os.path.join(lake, "curated"), os.path.join(lake, "quarantine")
    ing = datetime.now().strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]
    met, t0 = {"id_ingesta": ing, "etapas": {}}, time.perf_counter()

    def etapa(nombre, fn):
        t = time.perf_counter(); r = fn(); met["etapas"][nombre] = round(time.perf_counter() - t, 2); return r

    pros, malf = etapa("chats_parseo", lambda: procesar_chats(spark, raw, ing))
    alu, q1, l1 = etapa("excel_limpieza", lambda: procesar_excel(spark, raw, ing))
    asi, q2, l2 = etapa("notas_parseo", lambda: procesar_notas(spark, raw, ing))
    cuar = [q1, q2]
    if escribir:
        etapa("escritura_prospecto", lambda: pros.write.mode("overwrite").partitionBy("anio", "mes").parquet(os.path.join(cur_dir, "prospecto_curado")))
        etapa("escritura_alumno", lambda: alu.write.mode("overwrite").partitionBy("sede").parquet(os.path.join(cur_dir, "alumno_historico_curado")))
        etapa("escritura_asistencia", lambda: asi.write.mode("overwrite").partitionBy("anio", "mes").parquet(os.path.join(cur_dir, "asistencia_historica_curada")))
    met["conteos"] = {"chats_lineas_malformadas": malf, "prospectos_curados": pros.count(),
                      "excel_leidos": l1, "alumnos_curados": alu.count(),
                      "notas_leidas": l2, "asistencias_curadas": asi.count()}
    if con_ocr:
        cmp_, q3, n_img, detalle = etapa("ocr_comprobantes", lambda: procesar_comprobantes(spark, raw, ing))
        cuar.append(q3)
        cmp_.write.mode("overwrite").parquet(os.path.join(cur_dir, "comprobante_curado"))
        detalle.toPandas().to_csv(os.path.join(lake, "ocr_detalle.csv"), index=False)
        met["conteos"].update(comprobantes_leidos=n_img, comprobantes_curados=cmp_.count())
    qa = cuar[0]
    for x in cuar[1:]: qa = qa.unionByName(x)
    if escribir:
        etapa("escritura_cuarentena", lambda: qa.write.mode("overwrite").partitionBy("fuente").parquet(q_dir))
        met["cuarentena_por_motivo"] = {f"{r['fuente']} | {r['motivo']}": r["n"] for r in
                                        spark.read.parquet(q_dir).groupBy("fuente", "motivo").agg(F.count("*").alias("n")).collect()}
        with open(os.path.join(lake, "manifiesto_ingesta.json"), "w") as fh: json.dump(manifiesto(raw, ing), fh, indent=1)
    met["tiempo_total_s"] = round(time.perf_counter() - t0, 2)
    return met


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--lake", default="datalake/x1")
    ap.add_argument("--master", default="local[2]"); ap.add_argument("--sin-ocr", action="store_true")
    a = ap.parse_args()
    spark = spark_session(a.master); spark.sparkContext.setLogLevel("ERROR")
    m = ejecutar(spark, a.lake, con_ocr=not a.sin_ocr)
    with open(os.path.join(a.lake, "metricas_pipeline.json"), "w") as fh: json.dump(m, fh, indent=2, ensure_ascii=False)
    print(json.dumps(m, indent=2, ensure_ascii=False))
    spark.stop()
