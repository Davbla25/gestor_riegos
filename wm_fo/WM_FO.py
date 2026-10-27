import os
import threading
import time
import argparse
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError


# PARÁMETROS: línea de comandos (prioritaria) o variables de entorno
#   python WM_FO.py <broker host:puerto> <ID_OPERARIO> [fichero_servicios]
# ---------------------------------------------------------
# Se rellenan en configurar(), a partir de los argumentos
KAFKA_SERVER = ID_OPERARIO = FICHERO = NOMBRE = None
DURACION_RIEGO = PLAZO_RESPUESTA = 0.0
ESPERA_ENTRE_SERVICIOS = 4  # s, según la especificación                                      
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

def leer_argumentos():
    p = argparse.ArgumentParser(
        prog="WM_FO",
        description="Aplicación del operario de campo: solicita y supervisa riegos a través de WM_Central.")
    p.add_argument("--broker", default=os.environ.get("KAFKA_SERVER", "localhost:9092"), type=host_puerto,
                   help="host:puerto del broker Kafka (o KAFKA_SERVER).")
    p.add_argument("--id", default=os.environ.get("FO_ID"), type=campo,
                   help="ID único del operario (o FO_ID). Obligatorio.")
    p.add_argument("--nombre", default=os.environ.get("FO_NAME"), type=campo,
                   help="Nombre del operario (o FO_NAME). Por defecto, su ID.")
    p.add_argument("--fichero", default=os.environ.get("FO_FICHERO"),
                   help="Fichero de servicios a ejecutar al arrancar (o FO_FICHERO).")
    p.add_argument("--duracion-riego", default=os.environ.get("FO_DURACION_RIEGO", "10"), type=numero_positivo,
                   help="Segundos de riego en modo fichero antes de pedir la parada.")
    p.add_argument("--plazo-respuesta", default=os.environ.get("FO_PLAZO_RESPUESTA", "10"), type=numero_positivo,
                   help="Segundos sin respuesta tras los que se da por caída a la Central.")
    args = p.parse_args()
    if not args.id:
        p.error("falta el ID del operario: usa --id o la variable FO_ID")
    return args


def configurar(args):
    global KAFKA_SERVER, ID_OPERARIO, FICHERO, NOMBRE, DURACION_RIEGO, PLAZO_RESPUESTA
    host, puerto = args.broker
    KAFKA_SERVER = f"{host}:{puerto}"
    ID_OPERARIO = args.id
    NOMBRE = args.nombre or args.id
    FICHERO = args.fichero
    DURACION_RIEGO = args.duracion_riego
    PLAZO_RESPUESTA = args.plazo_respuesta

#########################################################
# VALIDADORES argparse
#########################################################

def puerto_valido(texto):
    try:
        valor = int(texto)
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{texto}' no es un puerto")
    if not 1 <= valor <= 65535:
        raise argparse.ArgumentTypeError(f"puerto fuera de rango (1-65535): {valor}")
    return valor

def host_puerto(texto):
    host, sep, puerto = texto.rpartition(":")
    if not sep or not host:
        raise argparse.ArgumentTypeError(f"'{texto}' no tiene el formato host:puerto")
    return host, puerto_valido(puerto)


def campo(texto):
    """ID, ubicación o nombre: no vacío y sin '#', que es el separador del protocolo."""
    texto = texto.strip()
    if not texto or "#" in texto:
        raise argparse.ArgumentTypeError("no puede estar vacío ni contener '#'")
    return texto


def numero_positivo(texto):
    try:
        valor = float(texto)
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{texto}' no es un número")
    if valor <= 0:
        raise argparse.ArgumentTypeError("debe ser mayor que 0")
    return valor

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
                v_deco = r.value.decode('utf-8', errors='replace')
                procesar(v_deco)
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
        mensaje_nuevo = mensaje.encode('utf-8')
        id_operario = ID_OPERARIO.encode('utf-8')
        productor.send(TOPIC_PETICIONES, key=id_operario, value=mensaje_nuevo).get(timeout=5)
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
    configurar(leer_argumentos())
    print("--- INICIANDO APLICACIÓN DE OPERARIO ---")
    print(f"ID: {ID_OPERARIO} | Nombre: {NOMBRE} | Servidor Kafka: {KAFKA_SERVER}")
    print("-" * 40)

    try:
        productor = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            max_block_ms=5000,
            retries=3,
        )
        # group_id propio y estable: si el FO cae, al volver recibe lo que se le envió mientras estaba caído
        consumidor = KafkaConsumer(
            TOPIC_RESPUESTAS,
            bootstrap_servers=[KAFKA_SERVER],
            # Añadir int(time.time()) para crear un grupo nuevo en cada arranque
            group_id=f"fo-{ID_OPERARIO}-{int(time.time())}", 
            auto_offset_reset='latest',
        )
    except KafkaError:
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
        os._exit(0) # <-- Añadir esto para matar el proceso y destrabar el input()


if __name__ == "__main__":
    main()