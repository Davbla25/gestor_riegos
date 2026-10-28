import io
import shutil
import sys
import threading
import time

import db_manager

# Leyenda del estado "fuera de servicio" (la guía de corrección la escribe en inglés: "Out of Service").
# Cámbiala aquí si el profesor la quiere de otra forma.
LEYENDA_PARADA = "Fuera de Servicio"

# Códigos ANSI
RESET = "\033[0m"
NEGRITA = "\033[1m"
LIMPIAR = "\033[H\033[J"          # cursor al inicio y borrar hasta el final de la pantalla
FONDO_VERDE = "\033[42m"
FONDO_VERDE_CLARO = "\033[102m"   # el "parpadeo" alterna entre este y el verde normal
FONDO_ROJO = "\033[41m"
FONDO_NARANJA = "\033[48;5;208m"  # el naranja no existe en los 16 colores básicos: usamos la paleta de 256
FONDO_GRIS = "\033[100m"
FONDO_AMARILLO = "\033[43m"
TEXTO_NEGRO = "\033[30m"
TEXTO_BLANCO = "\033[97m"

# estado -> (fondo, color del texto, leyenda que se muestra)
ESTILOS = {
    "DISPONIBLE":   (FONDO_VERDE,    TEXTO_NEGRO,  "Disponible"),
    "REGANDO":      (FONDO_VERDE,    TEXTO_NEGRO,  "Regando"),
    "FUGA":         (FONDO_ROJO,     TEXTO_BLANCO, "Fuga"),
    "PARADA":       (FONDO_NARANJA,  TEXTO_NEGRO,  LEYENDA_PARADA),
    "DESCONECTADA": (FONDO_GRIS,     TEXTO_BLANCO, "Desconectada"),
    "SOLICITADA":   (FONDO_AMARILLO, TEXTO_NEGRO,  "Solicitada"),
}

ANCHO_MAX = 100            # ancho máximo de cada fila coloreada
MAX_LINEAS_VOLCADO = 30    # cuántos mensajes acumulados se muestran al salir del panel


def _ancho_pantalla():
    # Por SSH o docker attach el terminal puede ser más estrecho que 100 columnas. Si una fila es más ancha
    # que la pantalla, se parte en dos líneas y el panel se desmonta, así que nos adaptamos al terminal.
    columnas = shutil.get_terminal_size((ANCHO_MAX, 24)).columns
    return max(60, min(ANCHO_MAX, columnas - 1))


def _fila(estacion, parpadeo, ancho):
    id_est, ubicacion, estado, operario, caudal, volumen = estacion
    fondo, texto, leyenda = ESTILOS.get(estado, ("", "", str(estado)))

    # Regando = verde parpadeante: alternamos dos tonos de verde en cada refresco
    if estado == "REGANDO" and parpadeo:
        fondo = FONDO_VERDE_CLARO

    # Detalle: caudal | volumen acumulado | operario ("Op"). Está pensado para caber en un terminal de 80 columnas
    if estado == "REGANDO":
        detalle = f"{caudal or 0} L/min | {volumen or 0} L | Op: {operario or '-'}"
    elif estado == "SOLICITADA":
        detalle = f"Esperando autorización | Op: {operario or '-'}"
    else:
        detalle = ""

    linea = f" {str(id_est)[:8]:<8} {str(ubicacion or '-')[:18]:<18} {leyenda:<17} {detalle}"
    linea = linea[:ancho]
    return f"{fondo}{texto}{linea:<{ancho}}{RESET}"


def _componer_frame():
    parpadeo = int(time.time()) % 2 == 0
    ancho = _ancho_pantalla()   # se recalcula en cada refresco: si redimensionas el terminal, se adapta
    lineas = [
        f"{NEGRITA}PANEL DE MONITORIZACIÓN - WM_CENTRAL{RESET}   {time.strftime('%H:%M:%S')}",
        "Pulsa ENTER para volver al menú",
        "",
        f" {'ID':<8} {'UBICACIÓN':<18} {'ESTADO':<17} DETALLE"[:ancho],
        "-" * ancho,
    ]
    try:
        filas = sorted(db_manager.listar_estaciones(), key=lambda f: f[0])
    except Exception as e:
        lineas.append(f" Error al leer la base de datos: {e}")
        return "\n".join(lineas)

    if not filas:
        lineas.append(" No hay estaciones registradas aún.")
    for fila in filas:
        lineas.append(_fila(fila, parpadeo, ancho))
    return "\n".join(lineas)


def mostrar_panel():
    """Panel en vivo: se redibuja cada segundo hasta que se pulsa ENTER.
    Mientras está abierto, los print del resto de hilos se acumulan en un buffer para no estropear la pantalla;
    al salir se muestran los últimos."""
    salir = threading.Event()

    def esperar_enter():
        try:
            input()
        except EOFError:
            pass
        salir.set()

    threading.Thread(target=esperar_enter, daemon=True).start()

    salida = sys.stdout            # el panel escribe aquí, en la pantalla real
    buffer = io.StringIO()
    sys.stdout = buffer            # a partir de ahora, los print de los demás hilos van al buffer
    try:
        while True:
            salida.write(LIMPIAR + _componer_frame() + "\n")
            salida.flush()
            if salir.wait(1):      # espera 1 s, o menos si se pulsa ENTER
                break
    finally:
        sys.stdout = salida        # restauramos siempre la salida normal
        salida.write(LIMPIAR)
        salida.flush()

    mensajes = buffer.getvalue().splitlines()
    if mensajes:
        print(f"[PANEL] Mensajes recibidos mientras el panel estaba abierto ({len(mensajes)}):")
        omitidos = len(mensajes) - MAX_LINEAS_VOLCADO
        if omitidos > 0:
            print(f"  (... {omitidos} mensajes anteriores omitidos)")
        for linea in mensajes[-MAX_LINEAS_VOLCADO:]:
            print(linea)