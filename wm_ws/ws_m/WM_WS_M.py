import socket
import time
import common.protocol
import os
import argparse

INTERVALO_SALUD = 1.0   # s entre comprobaciones de salud al Engine
TIMEOUT_SALUD = 3.0     # s sin respuesta del Engine => se considera caído
PLAZO_ACK = 1.5         # s que esperamos el ACK de la Central en cada intento
TICK = 0.2              # timeout del socket de la Central (ritmo del bucle)

#######################################################
# Lectura de los argumentos
#######################################################

def leer_argumentos():
    p = argparse.ArgumentParser(
        prog="WM_WS_M",
        description="Monitor de una Watering Station: se registra en WM_Central y supervisa al Engine.")
    p.add_argument("--id", default=os.environ.get("STATION_ID"), type=campo,
                   help="ID único de la WS (o variable STATION_ID). Obligatorio.")
    p.add_argument("--ubicacion", default=os.environ.get("STATION_LOCATION", "Zona Centro"), type=campo,
                   help="Ubicación de la WS (o STATION_LOCATION).")
    p.add_argument("--central",
                   default=f"{os.environ.get('CENTRAL_IP', '127.0.0.1')}:{os.environ.get('CENTRAL_PORT', '5000')}",
                   type=host_puerto,
                   help="host:puerto de WM_Central (o CENTRAL_IP y CENTRAL_PORT).")
    p.add_argument("--puerto-local", default=os.environ.get("MONITOR_PORT", "5001"), type=puerto_valido,
                   help="Puerto donde escucha al Engine (o MONITOR_PORT).")
    p.add_argument("--ip-local", default=os.environ.get("MONITOR_IP", "127.0.0.1"),
                   help="IP donde escucha al Engine (o MONITOR_IP). Usa 0.0.0.0 en contenedores separados.")
    args = p.parse_args()
    if not args.id:
        p.error("falta el ID de la estación: usa --id o la variable STATION_ID")
    return args

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

#########################################################################
# Funciones auxiliares
#########################################################################

def enviar_a_central(cliente, mensaje, cola):
    """Envía una trama a la Central y espera su ACK (3 intentos).
    Las tramas de datos que lleguen mientras esperamos se acusan y se guardan en `cola`."""
    paquete = common.protocol.empaquetar(mensaje)
    try:
        for _ in range(3):
            cliente.send(paquete)
            limite = time.monotonic() + PLAZO_ACK
            while True:
                restante = limite - time.monotonic()
                if restante <= 0:
                    break  # se acabó el plazo: el for reintenta
                cliente.settimeout(max(restante, 0.05))
                try:
                    trama = common.protocol.leer_trama(cliente)
                except socket.timeout:
                    break
                if trama == common.protocol.ACK:
                    return True
                if trama == common.protocol.NACK:
                    print("[ALERTA] La Central rechazó la trama (NACK), reintentando...")
                    break
                if trama is None:
                    raise ConnectionError("La Central ha cerrado la conexión")
                # Trama de datos (p. ej. una orden): la acusamos ya y la dejamos en cola
                try:
                    recibido = common.protocol.desempaquetar(trama)
                except (ValueError, IndexError) as e:
                    print(f"[ERROR DE PROTOCOLO] Trama dañada de la Central: {e}")
                    cliente.send(common.protocol.NACK)
                    continue
                cliente.send(common.protocol.ACK)
                cola.append(recibido)
        print("[ERROR DE RED] La Central no confirmó el mensaje tras 3 intentos")
        return False
    finally:
        cliente.settimeout(TICK)


def leer_del_engine(conexion):
    """Devuelve el mensaje del Engine, o None si no hay nada que leer ahora.
    Lanza ConnectionError si el Engine ha cerrado o reseteado la conexión."""
    try:
        trama = common.protocol.leer_trama(conexion)
    except BlockingIOError:
        return None
    if trama is None:
        raise ConnectionError("El Engine ha cerrado la conexión")
    if trama in (common.protocol.ACK, common.protocol.NACK):
        return None
    try:
        return common.protocol.desempaquetar(trama)
    except (ValueError, IndexError) as e:
        print(f"[ERROR LOCAL] Trama dañada del Engine: {e}")
        return None


def reenviar_al_engine(conexion, mensaje):
    """True si la orden se entregó al Engine; False si no hay Engine o el socket está roto."""
    if conexion is None:
        return False
    try:
        conexion.send(common.protocol.empaquetar(mensaje))
        return True
    except OSError:
        return False  # el paso 2 del bucle detectará la caída y limpiará la conexión


#########################################################################
# Bucle principal
#########################################################################

def WM_WS_M(args):

    IP_CENTRAL, PUERTO_CENTRAL = args.central
    IP_LOCAL, PUERTO_LOCAL = args.ip_local, args.puerto_local
    ID_ESTACION, UBICACION = args.id, args.ubicacion
    ##########################################
    # PARTE QUE ACTÚA COMO CLIENTE (C <= M)  #
    ##########################################
    cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    ###########################################
    # PARTE QUE ACTÚA COMO SERVIDOR (M <= E)  #
    ###########################################
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind((IP_LOCAL, PUERTO_LOCAL))
    servidor.listen(1)
    servidor.setblocking(False)
    print(f"[INFO] Monitor escuchando al Engine en {IP_LOCAL}:{PUERTO_LOCAL}")

    try:
        # --- Conexión y registro en la Central ---
        cliente.connect((IP_CENTRAL, PUERTO_CENTRAL))
        cliente.send(common.protocol.ENQ)
        confirmacion = common.protocol.leer_trama(cliente)
        if confirmacion != common.protocol.ACK:
            print("[ERROR DE PROTOCOLO] La central rechazó la conexión (no se recibió <ACK>)")
            return

        cliente.send(common.protocol.empaquetar(f"REGISTRO#{ID_ESTACION}#{UBICACION}"))
        respuesta = common.protocol.leer_trama(cliente)
        if respuesta == common.protocol.ACK:
            trama_recibida = common.protocol.leer_trama(cliente)
            mensaje_recibido = common.protocol.desempaquetar(trama_recibida)
            print(f"MENSAJE DE LA CENTRAL:{mensaje_recibido}")
            cliente.send(common.protocol.ACK)

        cliente.settimeout(TICK)

        conexion_engine = None
        averia = False
        bloqueada = False
        estado_enviado = None
        cola_ordenes = []   # tramas de la Central recibidas (y ya acusadas) mientras esperábamos un ACK
        t_salud = t_respuesta = t_latido = 0.0

        # --- Bucle de conexión persistente ---
        while True:
            ahora = time.monotonic()

            # 1. Aceptar al Engine si no hay ninguno (también tras una caída)
            if conexion_engine is None:
                try:
                    conexion_engine, direccion = servidor.accept()
                    conexion_engine.setblocking(False)
                    t_respuesta = ahora
                    print(f"[INFO] Engine conectado desde {direccion}")
                    if bloqueada:
                        reenviar_al_engine(conexion_engine, "BLOQUEAR")
                except BlockingIOError:
                    pass

            # 2. Engine: leer lo que sube y vigilar su salud
            para_central = None
            if conexion_engine is not None:
                try:
                    mensaje_engine = leer_del_engine(conexion_engine)
                    if mensaje_engine:
                        t_respuesta = ahora
                        partes = mensaje_engine.split('#')
                        if partes[0] == "SALUD":
                            averia = len(partes) < 3 or partes[2] != "OK"
                        else:
                            para_central = mensaje_engine  # AUTORIZA/DENIEGA/METRICAS/FIN_RIEGO

                    if ahora - t_salud >= INTERVALO_SALUD:
                        t_salud = ahora
                        try:
                            conexion_engine.send(common.protocol.empaquetar(f"COMPROBAR_SALUD#{ID_ESTACION}"))
                        except BlockingIOError:
                            pass
                    if ahora - t_respuesta > TIMEOUT_SALUD:
                        raise ConnectionError("El Engine no responde a las comprobaciones de salud")
                except ConnectionError as e:
                    print(f"[ENGINE CAÍDO] {e}")
                    conexion_engine.close()
                    conexion_engine = None
                    averia = True

            # Reenvío a la Central, fuera del try anterior para no confundir sus errores con los del Engine
            if para_central:
                print(f"[PUENTE] Reenviando a Central: {para_central}")
                enviar_a_central(cliente, para_central, cola_ordenes)

            # 3. Estado hacia la Central
            estado = "FUGA" if averia else "OK"
            if estado == "OK" and estado_enviado == "FUGA":
                # La Central solo saca la estación de FUGA con un REGISTRO nuevo
                print("[RECUPERACIÓN] Avería resuelta: se vuelve a registrar la estación")
                if enviar_a_central(cliente, f"REGISTRO#{ID_ESTACION}#{UBICACION}", cola_ordenes):
                    estado_enviado = "OK"
                    t_latido = ahora
                    # el REGISTRO borra también PARADA: sincronizamos el Engine
                    bloqueada = False
                    reenviar_al_engine(conexion_engine, "ACTIVAR")
            elif estado != estado_enviado or ahora - t_latido >= 1.0:
                if estado != estado_enviado:
                    print(f"[ESTADO] Engine {estado}")
                enviar_a_central(cliente, f"ESTADO#{ID_ESTACION}#{estado}", cola_ordenes)
                estado_enviado = estado
                t_latido = ahora

            # 4. Órdenes de la Central: primero las que quedaron en cola, después el socket
            if cola_ordenes:
                mensaje = cola_ordenes.pop(0)
            else:
                try:
                    trama_recibida = common.protocol.leer_trama(cliente)
                except socket.timeout:
                    continue
                if not trama_recibida:
                    print("[DESCONEXIÓN] La Central ha cerrado la conexión de forma inesperada.")
                    break
                if trama_recibida in (common.protocol.ACK, common.protocol.NACK):
                    continue  # rezagado de un cruce de tramas: se ignora
                try:
                    mensaje = common.protocol.desempaquetar(trama_recibida)
                except (ValueError, IndexError) as e:
                    print(f"[ERROR DE PROTOCOLO] Trama dañada de la Central: {e}")
                    cliente.send(common.protocol.NACK)
                    continue
                cliente.send(common.protocol.ACK)  # acusamos antes de actuar: así la Central ya ha anotado su estado

            accion = mensaje.split('#')[0]
            if accion == "INICIAR_RIEGO":
                print("[ORDEN RECIBIDA] Iniciando el riego por petición de la central")
                if not reenviar_al_engine(conexion_engine, "INICIAR_RIEGO"):
                    print("[AVISO] El Engine no está disponible: se deniega el riego")
                    enviar_a_central(cliente, f"DENIEGA_RIEGO#{ID_ESTACION}#Engine no disponible", cola_ordenes)
            elif accion == "FIN_RIEGO":
                print("[ORDEN RECIBIDA] Finalizando el riego por petición de la central")
                if not reenviar_al_engine(conexion_engine, "FIN_RIEGO"):
                    print("[AVISO] El Engine no está disponible: el Monitor confirma el fin del riego")
                    enviar_a_central(cliente, f"FIN_RIEGO#{ID_ESTACION}", cola_ordenes)
            elif accion == "BLOQUEAR":
                bloqueada = True
                print("[ORDEN RECIBIDA] Estación FUERA DE SERVICIO por orden de la Central")
                reenviar_al_engine(conexion_engine, "BLOQUEAR")
            elif accion == "ACTIVAR":
                bloqueada = False
                print("[ORDEN RECIBIDA] Estación ACTIVADA por orden de la Central")
                reenviar_al_engine(conexion_engine, "ACTIVAR")
            else:
                print(f"[CENTRAL] {mensaje}")  # tramas informativas de la Central

    except ConnectionRefusedError as e:
        print(f"[ERROR DE CONEXIÓN] La central no está disponible. Motivo:{e}")
    except OSError as e:
        print(f"[ERROR DE CONEXIÓN] Imposible conectar con WM_Central: {e}")
    finally:
        cliente.close()
        servidor.close()


if __name__ == "__main__":
    WM_WS_M(leer_argumentos())