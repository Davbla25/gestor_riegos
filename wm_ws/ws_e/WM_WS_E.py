import socket
import threading
import time
import common.protocol
import os
import argparse

TICK = 0.2  # timeout del socket; los envíos periódicos se miden con el reloj

ko = False  # avería simulada: se alterna desde el teclado

#######################################################
# Lectura de los argumentos
#######################################################
def leer_argumentos():
    p = argparse.ArgumentParser(
        prog="WM_WS_E",
        description="Engine de una Watering Station: controla la válvula y el caudalímetro simulados.")
    p.add_argument("--id", default=os.environ.get("STATION_ID"), type=campo,
                   help="ID de la WS (o STATION_ID). Debe coincidir con el del Monitor.")
    p.add_argument("--monitor",
                   default=f"{os.environ.get('MONITOR_IP', '127.0.0.1')}:{os.environ.get('MONITOR_PORT', '5001')}",
                   type=host_puerto,
                   help="host:puerto del Monitor (o MONITOR_IP y MONITOR_PORT).")
    p.add_argument("--broker", default=os.environ.get("KAFKA_SERVER"), type=host_puerto,
                   help="host:puerto del broker Kafka (o KAFKA_SERVER). Se acepta por la especificación; esta versión no lo usa.")
    p.add_argument("--caudal", default=os.environ.get("CAUDAL_LMIN", "5.0"), type=numero_positivo,
                   help="Caudal simulado en L/min (o CAUDAL_LMIN).")
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


def numero_positivo(texto):
    try:
        valor = float(texto)
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{texto}' no es un número")
    if valor <= 0:
        raise argparse.ArgumentTypeError("debe ser mayor que 0")
    return valor

#########################################################
# FUNCIONES auxiliares
#########################################################

def enviar(conexion, mensaje):
    conexion.send(common.protocol.empaquetar(mensaje))


def escuchar_teclado():
    global ko
    print("[ENGINE] Pulsa ENTER para alternar entre OK y KO (simular fuga)")
    while True:
        input()
        ko = not ko
        print(f"[ENGINE] Estado simulado: {'KO (fuga)' if ko else 'OK'}")

#########################################################
# Bucle principal
#########################################################
def WM_WS_E(args):
    IP_LOCAL, PUERTO_LOCAL = args.monitor
    ID_ESTACION = args.id
    CAUDAL = args.caudal
    
    if args.broker:
        print("[ENGINE] Broker recibido, pero esta versión del Engine no usa Kafka")
        
    threading.Thread(target=escuchar_teclado, daemon=True).start()
    
    regando = False
    bloqueada = False
    volumen = 0.0
    t_ultimo = t_metrica = 0.0

    while True:
        cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            cliente.connect((IP_LOCAL, PUERTO_LOCAL))
            cliente.settimeout(TICK)
            print("[ENGINE] Conectado al Monitor")
            
            # Bucle de tramas (el original)
            while True:
                mensaje = None
                try:
                    trama = common.protocol.leer_trama(cliente)
                    if not trama:
                        print("[DESCONEXIÓN] El Monitor ha cerrado la conexión.")
                        break # Rompe este bucle interno, y el try finaliza para volver a conectar
                    if trama not in (common.protocol.ACK, common.protocol.NACK):
                        mensaje = common.protocol.desempaquetar(trama)
                except socket.timeout:
                    pass
                except (ValueError, IndexError) as e:
                    print(f"[ERROR DE PROTOCOLO] {e}")

                ahora = time.monotonic()
                
                if mensaje:
                    accion = mensaje.split('#')[0]
                    if accion == "COMPROBAR_SALUD":
                        enviar(cliente, f"SALUD#{ID_ESTACION}#{'KO' if ko else 'OK'}")
                    elif accion == "INICIAR_RIEGO":
                        if bloqueada:
                            enviar(cliente, f"DENIEGA_RIEGO#{ID_ESTACION}#Estación fuera de servicio")
                        elif ko:
                            enviar(cliente, f"DENIEGA_RIEGO#{ID_ESTACION}#Estación averiada")
                        else:
                            regando = True
                            volumen = 0.0
                            t_ultimo = t_metrica = ahora
                            print("[RIEGO] Válvula abierta")
                            enviar(cliente, f"AUTORIZA_RIEGO#{ID_ESTACION}")
                    elif accion == "FIN_RIEGO":
                        if regando:
                            volumen += CAUDAL / 60 * (ahora - t_ultimo)
                            enviar(cliente, f"METRICAS#{ID_ESTACION}#{CAUDAL}#{round(volumen, 2)}")
                            print(f"[RIEGO] Válvula cerrada. Total regado: {round(volumen, 2)} L")
                        regando = False
                        enviar(cliente, f"FIN_RIEGO#{ID_ESTACION}")
                    elif accion == "BLOQUEAR":
                        regando = False
                        bloqueada = True
                        print("[ENGINE] FUERA DE SERVICIO")
                    elif accion == "ACTIVAR":
                        bloqueada = False
                        print("[ENGINE] Estación activada: disponible")

                if regando and ahora - t_metrica >= 1.0:
                    volumen += CAUDAL / 60 * (ahora - t_ultimo)
                    t_ultimo = t_metrica = ahora
                    enviar(cliente, f"METRICAS#{ID_ESTACION}#{CAUDAL}#{round(volumen, 2)}")

        except ConnectionRefusedError:
            print("[ERROR] El Monitor no está disponible. Reintentando...")
        except OSError as e:
            print(f"[ERROR] Conexión perdida con el Monitor: {e}")
        finally:
            cliente.close()
            # Pausa de 2 segundos antes de reintentar para no saturar la red
            time.sleep(2)


if __name__ == "__main__":
    WM_WS_E(leer_argumentos())