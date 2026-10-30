import socket
import time
import common.protocol
import os
import argparse
import sys

# Códigos de colores ANSI para la consola
VERDE = "\033[92m"
ROJO = "\033[91m"
AMARILLO = "\033[93m"
AZUL = "\033[94m"
RESET = "\033[0m"

INTERVALO_SALUD = 1.5  
TIMEOUT_SALUD = 5.0     
PLAZO_ACK = 3.0        
TICK = 0.2            

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
    texto = texto.strip()
    if not texto or "#" in texto:
        raise argparse.ArgumentTypeError("no puede estar vacío ni contener '#'")
    return texto

def enviar_a_central(cliente, mensaje, cola):
    paquete = common.protocol.empaquetar(mensaje)
    try:
        for _ in range(3):
            cliente.send(paquete)
            limite = time.monotonic() + PLAZO_ACK
            while True:
                restante = limite - time.monotonic()
                if restante <= 0:
                    break  
                cliente.settimeout(max(restante, 0.05))
                try:
                    trama = common.protocol.leer_trama(cliente)
                except socket.timeout:
                    break
                if trama == common.protocol.ACK:
                    return True
                if trama == common.protocol.NACK:
                    print(f"{ROJO}[ALERTA] La Central rechazó la trama (NACK), reintentando...{RESET}")
                    break
                if trama is None:
                    raise ConnectionError("La Central ha cerrado la conexión")
                
                try:
                    recibido = common.protocol.desempaquetar(trama)
                except (ValueError, IndexError) as e:
                    print(f"{ROJO}[ERROR DE PROTOCOLO] Trama dañada de la Central: {e}{RESET}")
                    cliente.send(common.protocol.NACK)
                    continue
                cliente.send(common.protocol.ACK)
                cola.append(recibido)
        print(f"{ROJO}[ERROR DE RED] La Central no confirmó el mensaje tras 3 intentos{RESET}")
        return False
    finally:
        cliente.settimeout(TICK)

def leer_del_engine(conexion):
    try:
        trama = common.protocol.leer_trama(conexion)
    except socket.timeout:
        return None
    if trama is None:
        raise ConnectionError("El Engine ha cerrado la conexión")
    if trama in (common.protocol.ACK, common.protocol.NACK):
        return None
    try:
        return common.protocol.desempaquetar(trama)
    except (ValueError, IndexError) as e:
        print(f"{ROJO}[ERROR LOCAL] Trama dañada del Engine: {e}{RESET}")
        return None

def reenviar_al_engine(conexion, mensaje):
    if conexion is None:
        return False
    try:
        conexion.send(common.protocol.empaquetar(mensaje))
        return True
    except OSError:
        return False  

def WM_WS_M(args):
    IP_CENTRAL, PUERTO_CENTRAL = args.central
    IP_LOCAL, PUERTO_LOCAL = args.ip_local, args.puerto_local
    ID_ESTACION, UBICACION = args.id, args.ubicacion

    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind((IP_LOCAL, PUERTO_LOCAL))
    servidor.listen(1)
    servidor.setblocking(False)
    print(f"{AZUL}[INFO] Monitor escuchando al Engine en {IP_LOCAL}:{PUERTO_LOCAL}{RESET}")

    conexion_engine = None
    cliente = None
    central_conectada = False
    
    averia = False
    bloqueada = False
    estado_enviado = None
    cola_ordenes = []
    t_salud = t_respuesta = t_latido = 0.0

    try:
        while True:
            ahora = time.monotonic()

            # --- 1. RECONEXIÓN A LA CENTRAL ---
            if not central_conectada:
                if cliente:
                    cliente.close()
                cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                cliente.settimeout(TICK)
                try:
                    cliente.connect((IP_CENTRAL, PUERTO_CENTRAL))
                    cliente.send(common.protocol.ENQ)
                    if common.protocol.leer_trama(cliente) == common.protocol.ACK:
                        cliente.send(common.protocol.empaquetar(f"REGISTRO#{ID_ESTACION}#{UBICACION}"))
                        if common.protocol.leer_trama(cliente) == common.protocol.ACK:
                            _ = common.protocol.leer_trama(cliente) 
                            cliente.send(common.protocol.ACK)
                            print(f"{VERDE}[INFO] Conectado y registrado en WM_Central{RESET}")
                            central_conectada = True
                            estado_enviado = None 
                except OSError:
                    pass 

            # --- 2. ACEPTAR ENGINE (Local) ---
            if conexion_engine is None:
                try:
                    conexion_engine, direccion = servidor.accept()
                    conexion_engine.settimeout(1.0)
                    t_respuesta = ahora
                    print(f"{VERDE}[INFO] Engine conectado desde {direccion}{RESET}")
                    if bloqueada:
                        reenviar_al_engine(conexion_engine, "BLOQUEAR")
                except BlockingIOError: # <-- AQUÍ ESTABA EL FALLO (Debe ser BlockingIOError, no socket.timeout)
                    pass

            # --- 3. GESTIONAR ENGINE (Local) ---
            para_central = None
            if conexion_engine is not None:
                try:
                    mensaje_engine = leer_del_engine(conexion_engine)
                    if mensaje_engine:
                        t_respuesta = ahora
                        partes = mensaje_engine.split('#')
                        if partes[0] == "SALUD":
                            nueva_averia = (len(partes) < 3 or partes[2] != "OK")
                            if nueva_averia != averia:
                                averia = nueva_averia
                                if averia:
                                    print(f"{ROJO}[ALERTA LOCAL] El Engine reporta AVERÍA (Fuga){RESET}")
                                else:
                                    print(f"{VERDE}[INFO LOCAL] El Engine reporta estado OK{RESET}")
                        else:
                            para_central = mensaje_engine
                            # Añadir estos print para reflejar visualmente la actividad local
                            if partes[0] == "INICIAR_RIEGO":
                                print(f"{VERDE}[ACCIÓN LOCAL] El Engine ha iniciado el riego manualmente{RESET}")
                            elif partes[0] == "FIN_RIEGO":
                                print(f"{AZUL}[ACCIÓN LOCAL] El Engine ha detenido el riego manualmente{RESET}")
                            elif partes[0] == "METRICAS":
                                print(f"{AZUL}[MÉTRICAS] Reenviando métricas: Caudal {partes[2]} L/m - Vol: {partes[3]} L{RESET}")
                    if ahora - t_salud >= INTERVALO_SALUD:
                        t_salud = ahora
                        try:
                            conexion_engine.send(common.protocol.empaquetar(f"COMPROBAR_SALUD#{ID_ESTACION}"))
                        except socket.timeout:
                            pass
                    if ahora - t_respuesta > TIMEOUT_SALUD:
                        raise ConnectionError("El Engine no responde a las comprobaciones de salud")
                except ConnectionError as e:
                    print(f"{ROJO}[ENGINE CAÍDO] {e}{RESET}")
                    conexion_engine.close()
                    conexion_engine = None
                    averia = True

            # --- 4. COMUNICACIÓN CON CENTRAL (Solo si está conectada) ---
            if central_conectada:
                try:
                    # Enviar tramas pendientes
                    if para_central:
                        print(f"{AZUL}[PUENTE] Reenviando a Central: {para_central}{RESET}")
                        if not enviar_a_central(cliente, para_central, cola_ordenes):
                            raise ConnectionError("Fallo al enviar trama a la Central")

                    # Enviar estados
                    estado = "FUGA" if (averia or conexion_engine is None) else "OK"
                    if estado == "OK" and estado_enviado == "FUGA":
                        print(f"{VERDE}[RECUPERACIÓN] Avería resuelta: registrando estación{RESET}")
                        if enviar_a_central(cliente, f"REGISTRO#{ID_ESTACION}#{UBICACION}", cola_ordenes):
                            estado_enviado = "OK"
                            t_latido = ahora
                            bloqueada = False
                            reenviar_al_engine(conexion_engine, "ACTIVAR")
                    elif estado != estado_enviado:
                        print(f"{VERDE if estado == 'OK' else ROJO}[ESTADO] Engine {estado}{RESET}")
                        enviar_a_central(cliente, f"ESTADO#{ID_ESTACION}#{estado}", cola_ordenes)
                        estado_enviado = estado
                        t_latido = ahora

                    # Leer órdenes
                    if cola_ordenes:
                        mensaje = cola_ordenes.pop(0)
                    else:
                        try:
                            trama_recibida = common.protocol.leer_trama(cliente)
                            if not trama_recibida:
                                raise ConnectionError("Conexión cerrada por la Central")
                            if trama_recibida not in (common.protocol.ACK, common.protocol.NACK):
                                mensaje = common.protocol.desempaquetar(trama_recibida)
                                cliente.send(common.protocol.ACK)
                            else:
                                mensaje = None
                        except socket.timeout:
                            mensaje = None
                        except (ValueError, IndexError) as e:
                            print(f"{ROJO}[ERROR DE PROTOCOLO] Trama dañada de la Central: {e}{RESET}")
                            cliente.send(common.protocol.NACK)
                            mensaje = None

                    # Ejecutar órdenes
                    if mensaje:
                        accion = mensaje.split('#')[0]
                        if accion == "INICIAR_RIEGO":
                            print(f"{AMARILLO}[ORDEN RECIBIDA] Iniciando riego por orden de Central{RESET}")
                            if not reenviar_al_engine(conexion_engine, "INICIAR_RIEGO"):
                                enviar_a_central(cliente, f"DENIEGA_RIEGO#{ID_ESTACION}#Engine no disponible", cola_ordenes)
                        elif accion == "FIN_RIEGO":
                            print(f"{AMARILLO}[ORDEN RECIBIDA] Finalizando riego por orden de Central{RESET}")
                            if not reenviar_al_engine(conexion_engine, "FIN_RIEGO"):
                                enviar_a_central(cliente, f"FIN_RIEGO#{ID_ESTACION}", cola_ordenes)
                        elif accion == "BLOQUEAR":
                            bloqueada = True
                            print(f"{AMARILLO}[ORDEN RECIBIDA] Estación FUERA DE SERVICIO por orden de la Central{RESET}")
                            reenviar_al_engine(conexion_engine, "BLOQUEAR")
                        elif accion == "ACTIVAR":
                            bloqueada = False
                            print(f"{VERDE}[ORDEN RECIBIDA] Estación ACTIVADA por orden de la Central{RESET}")
                            reenviar_al_engine(conexion_engine, "ACTIVAR")
                        else:
                            print(f"{AZUL}[CENTRAL] {mensaje}{RESET}") 

                except (ConnectionError, OSError) as e:
                    print(f"{ROJO}[CAÍDA DE CENTRAL] Se perdió conexión con WM_Central: {e}{RESET}")
                    central_conectada = False 
                    
            time.sleep(0.05) 
            
    finally:
        if cliente: cliente.close()
        servidor.close()

if __name__ == "__main__":
    try:
        WM_WS_M(leer_argumentos())
    except KeyboardInterrupt:
        print(f"\n{AMARILLO}[CIERRE] Deteniendo el Monitor (Ctrl+C){RESET}")
        sys.exit(0)