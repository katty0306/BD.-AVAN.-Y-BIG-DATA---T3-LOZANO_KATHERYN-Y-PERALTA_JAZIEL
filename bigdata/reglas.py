"""Catálogos y expresiones compartidas por los pipelines (reglas R01-R13)."""
import unicodedata

SEDES = {  # R05: tabla de sinónimos -> catálogo
    "sede norte": "Sede Norte", "norte": "Sede Norte", "los olivos": "Sede Norte", "la de los olivos": "Sede Norte",
    "sede centro": "Sede Centro", "centro": "Sede Centro", "jesus maria": "Sede Centro", "la del centro": "Sede Centro",
    "sede sur": "Sede Sur", "sur": "Sede Sur", "sjm": "Sede Sur", "san juan de miraflores": "Sede Sur", "la del sur": "Sede Sur",
}
ESTADOS = {  # R07
    "matriculado": "Matriculado", "matric.": "Matriculado", "activo": "Matriculado",
    "pendiente": "Pendiente", "inactivo": "Inactivo",
}
ASISTENCIA = {"p": "Presente", "f": "Falta", "falto": "Falta", "t": "Tarde", "j": "Justificada"}  # R10
HORAS = {  # normalización de horario mencionado en el chat
    "4pm": "16:00", "4:00 pm": "16:00", "5:30": "17:30", "5 y media": "17:30",
    "7pm": "19:00", "7 de la noche": "19:00", "8am": "08:00", "8 de la mañana": "08:00",
}
RX_LINEA = r"^(\d{2}/\d{2}/\d{2}), (\d{2}:\d{2}) - ([^:]+): (.*)$"
RX_NOMBRE_EDAD = r"(?:hijo|para|inscribir a)\s+([A-ZÁÉÍÓÚ][a-záéíóúñ]+)\D{0,15}?(\d{1,2})\s*a[ñn]"
RX_SEDE = r"(sede norte|sede centro|sede sur|los olivos|jesus maria|san juan de miraflores|sjm|la de los olivos|la del centro|la del sur|norte|centro|sur)"
RX_HORA = r"(4:00 pm|4pm|5 y media|5:30|7 de la noche|7pm|8 de la mañana|8am)"


def sin_tildes(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
