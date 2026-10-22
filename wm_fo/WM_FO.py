import os
import sys
import threading
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

# ---------------------------------------------------------
# PARÁMETROS: línea de comandos (prioritaria) o variables de entorno
#   python WM_FO.py <broker host:puerto> <ID_OPERARIO> [fichero_servicios]
# ---------------------------------------------------------
KAFKA_SERVER = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("KAFKA_SERVER", "localhost:9092")
ID_OPERARIO = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("FO_ID", "FO_DEFAULT")
FICHERO = sys.argv[3] if len(sys.argv) > 3 else os.environ.get("FO_FICHERO")  # si existe, se ejecuta al arrancar
NOMBRE = os.environ.get("FO_NAME", ID_OPERARIO)
DURACION_RIEGO = float(os.environ.get("FO_DURACION_RIEGO", 10))    # s de riego en modo fichero
PLAZO_RESPUESTA = float(os.environ.get("FO_PLAZO_RESPUESTA", 10))  # s sin respuesta => Central inalcanzable
ESPERA_ENTRE_SERVICIOS = 4                                         # s, según la especificación

TOPIC_PETICIONES = "peticiones_operarios"
TOPIC_RESPUESTAS = "respuesta_central"

productor = None
consumidor = None

# Eventos que rellena el hilo que escucha y que consumen los servicios
evento_registro = threading.Event()   # la Central confirmó el alta del operario
evento_respuesta = threading.Event()  # la Central ha contestado algo (está viva)
evento_inicio = threading.Event()     # el riego ha arrancado
evento_fin = threading.Event()        # el servicio ha concluido (éxito o fracaso)
salir = threading.Event()


#########################################################################
# Recepción
#########################################################################

def cierra_servicio(tipo, texto):
    """Cierran un servicio cualquier ERROR y el resumen final del riego."""
    return tipo == "ERROR" or (tipo == "INFO" and texto.startswith("Riego finalizado"))


def procesar(texto):
    partes = texto.split('#', 2)
    if len(partes) < 3:
        return  # mensaje mal formado: se descarta sin romper el hilo
    tipo, destinatario, contenido = partes

    if destinatario == "ALL":
        print(f"\n[ALERTA GLOBAL] {contenido}")
        return
    if destinatario != ID_OPERARIO:
        return  # no es para este operario

    evento_respuesta.set()
    if tipo == "ERROR":
        print(f"\n[ERROR CENTRAL] {contenido}")
    elif tipo == "INFO":
        etiqueta = "MÉTRICAS" if contenido.startswith("Métricas") else "INFO CENTRAL"
        print(f"\n[{etiqueta}] {contenido}")
        if "dado de alta" in contenido:
            evento_registro.set()
        if contenido.startswith("Riego iniciado"):
            evento_inicio.set()
    if cierra_servicio(tipo, contenido):
        evento_fin.set()


def sondear(timeout_ms):
    try:
        lote = consumidor.poll(timeout_ms=timeout_ms)
        for registros in lote.values():
            for r in registros:
                procesar(r.value)
    except Exception as e:
        print(f"\n[ERROR] Imposible leer de Kafka: {e}")
        time.sleep(1)


def escuchar():
    while not salir.is_set():
        sondear(500)


def esperar_asignacion(plazo=20):
    """Espera a que el consumidor tenga partición y posición de lectura ANTES de registrarnos,
    para no perder la respuesta de la Central al REGISTRO_FO."""
    limite = time.monotonic() + plazo
    while time.monotonic() < limite:
        sondear(200)
        if consumidor.assignment():
            for tp in consumidor.assignment():
                consumidor.position(tp)
            return True
    return False


#########################################################################
# Envío
#########################################################################

def enviar(mensaje):
    try:
        productor.send(TOPIC_PETICIONES, key=ID_OPERARIO, value=mensaje).get(timeout=5)
        return True
    except KafkaError as e:
        print(f"[ERROR] Imposible conectar con Kafka: {e}")
        return False


def registrar():
    while not salir.is_set():
        evento_registro.clear()
        enviado = enviar(f"REGISTRO_FO#{ID_OPERARIO}#{NOMBRE}")
        if enviado and evento_registro.wait(PLAZO_RESPUESTA):
            return True
        if not enviado:
            time.sleep(2)
        print("[ERROR] Imposible conectar con WM_Central: sin respuesta al registro. Reintentando...")
    return False


def id_valido(destino):
    return bool(destino) and '#' not in destino


#########################################################################
# Servicios
#########################################################################

def ejecutar_servicio(destino):
    """Pide un riego y espera su conclusión. En modo fichero para el riego tras DURACION_RIEGO s."""
    evento_respuesta.clear()
    evento_inicio.clear()
    evento_fin.clear()

    print(f"\n[SERVICIO] Solicitando riego en {destino}")
    if not enviar(f"PETICION_RIEGO#{ID_OPERARIO}#{destino}"):
        return
    if not evento_respuesta.wait(PLAZO_RESPUESTA):
        print("[ERROR] Imposible conectar con WM_Central: sin respuesta a la petición")
        return

    # Esperamos a que el riego arranque o el servicio se cierre (denegado, error...)
    limite = time.monotonic() + PLAZO_RESPUESTA
    while not (evento_inicio.is_set() or evento_fin.is_set()):
        if salir.is_set() or time.monotonic() > limite:
            print(f"[ERROR] La estación {destino} no ha respondido a tiempo")
            return
        time.sleep(0.1)
    if evento_fin.is_set():
        print(f"[SERVICIO] Servicio en {destino} concluido")
        return

    # Riego en marcha: se corta pasado el tiempo (o antes, si lo aborta una fuga, un bloqueo...)
    if not evento_fin.wait(DURACION_RIEGO):
        print(f"[SERVICIO] Tiempo de riego cumplido ({DURACION_RIEGO}s): solicitando parada")
        if not enviar(f"PETICION_PARADA#{ID_OPERARIO}#{destino}"):
            return
        if not evento_fin.wait(PLAZO_RESPUESTA):
            print("[ERROR] No se ha recibido el resumen del riego (¿Central o estación caídas?)")
            return
    print(f"[SERVICIO] Servicio en {destino} concluido")


def ejecutar_fichero(ruta):
    try:
        with open(ruta, encoding="utf-8") as f:
            servicios = [l.strip() for l in f if l.strip() and not l.startswith("#")]
    except OSError as e:
        print(f"[ERROR] No se puede leer el fichero {ruta}: {e}")
        return
    print(f"[FICHERO] {len(servicios)} servicios en {ruta}")
    for i, destino in enumerate(servicios, 1):
        if salir.is_set():
            return
        print(f"\n[FICHERO] Servicio {i}/{len(servicios)}")
        if id_valido(destino):
            ejecutar_servicio(destino)
        else:
            print(f"[ERROR] Línea no válida en el fichero: {destino!r}")
        if i < len(servicios):
            print(f"[FICHERO] Esperando {ESPERA_ENTRE_SERVICIOS}s hasta el siguiente servicio...")
            time.sleep(ESPERA_ENTRE_SERVICIOS)
    print("\n[FICHERO] Todos los servicios del fichero han concluido")


#########################################################################
# Menú
#########################################################################

def menu():
    hilo_fichero = None
    while True:
        print("\n--------OPCIONES--------")
        print(" 1. INICIAR RIEGO\n 2. PARAR RIEGO\n 3. EJECUTAR SERVICIOS DEL FICHERO\n 4. SALIR")
        try:
            opcion = input("¿QUÉ QUIERES HACER? ").strip()
        except EOFError:
            # sin terminal interactiva (p. ej. contenedor sin -it): seguimos escuchando
            while not salir.is_set():
                time.sleep(1)
            return

        if opcion in ("1", "2"):
            destino = input("¿DÓNDE? ").strip()
            if not id_valido(destino):
                print("[ERROR] El ID de la estación no puede estar vacío ni contener '#'")
                continue
            comando = "PETICION_RIEGO" if opcion == "1" else "PETICION_PARADA"
            if enviar(f"{comando}#{ID_OPERARIO}#{destino}"):
                print(f"[ENVIADO] {comando} a {destino}")
        elif opcion == "3":
            if hilo_fichero and hilo_fichero.is_alive():
                print("[AVISO] Ya hay una ejecución de fichero en curso")
                continue
            ruta = input(f"Fichero [{FICHERO or 'servicios.txt'}]: ").strip() or FICHERO or "servicios.txt"
            hilo_fichero = threading.Thread(target=ejecutar_fichero, args=(ruta,), daemon=True)
            hilo_fichero.start()
        elif opcion == "4":
            return
        else:
            print("[ERROR] Opción no válida")


#########################################################################
# Arranque
#########################################################################

def main():
    global productor, consumidor
    print("--- INICIANDO APLICACIÓN DE OPERARIO ---")
    print(f"ID: {ID_OPERARIO} | Nombre: {NOMBRE} | Servidor Kafka: {KAFKA_SERVER}")
    print("-" * 40)

    try:
        productor = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            key_serializer=lambda k: k.encode('utf-8'),   # misma clave => orden garantizado por operario
            value_serializer=lambda v: v.encode('utf-8'),
            max_block_ms=5000,
            retries=3,
        )
        # group_id propio y estable: si el FO cae, al volver recibe lo que se le envió mientras estaba caído
        consumidor = KafkaConsumer(
            TOPIC_RESPUESTAS,
            bootstrap_servers=[KAFKA_SERVER],
            group_id=f"fo-{ID_OPERARIO}",
            auto_offset_reset='latest',
            value_deserializer=lambda x: x.decode('utf-8', errors='replace'),
        )
    except NoBrokersAvailable:
        print(f"[ERROR] Imposible conectar con el broker Kafka en {KAFKA_SERVER}")
        return

    if not esperar_asignacion():
        print("[ERROR] Imposible conectar con Kafka: no se ha podido suscribir a las respuestas de la Central")
        return

    hilo_escucha = threading.Thread(target=escuchar, daemon=True)
    hilo_escucha.start()

    try:
        if not registrar():
            return
        if FICHERO:
            threading.Thread(target=ejecutar_fichero, args=(FICHERO,), daemon=True).start()
        menu()
    except KeyboardInterrupt:
        pass
    finally:
        salir.set()
        hilo_escucha.join(timeout=3)
        try:
            productor.flush(timeout=5)
            productor.close()
            consumidor.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()