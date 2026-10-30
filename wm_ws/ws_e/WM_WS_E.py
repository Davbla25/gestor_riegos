import socket
import threading
import time
import common.protocol
import os
import argparse
import queue # Añadido para comunicar el menú con el socket

# Códigos de colores ANSI para la consola
VERDE = "\033[92m"
ROJO = "\033[91m"
AMARILLO = "\033[93m"
AZUL = "\033[94m"
RESET = "\033[0m"

TICK = 1.0  # timeout del socket; los envíos periódicos se miden con el reloj

ko = False  # avería simulada: se alterna desde el teclado
cola_comandos = queue.Queue() # Cola para pasar los comandos del menú al hilo principal

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
    time.sleep(1) # Pausa inicial para que no se pise con los logs de arranque
    while True:
        print(f"\n--- MENÚ ENGINE ({ROJO + 'KO' + RESET if ko else VERDE + 'OK' + RESET}) ---")
        print(" 1. Alternar avería simulada (Fuga)")
        print(" 2. Iniciar riego manual")
        print(" 3. Parar riego manual")
        opcion = input("Selecciona una opción: ").strip()
        
        if opcion == "1":
            ko = not ko
            print(f"[ENGINE] Estado simulado: {ROJO + 'KO (fuga)' + RESET if ko else VERDE + 'OK' + RESET}")
        elif opcion == "2":
            cola_comandos.put("INICIAR_RIEGO")
        elif opcion == "3":
            cola_comandos.put("FIN_RIEGO")
        else:
            print("[ERROR] Opción no válida")

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
    estado_salud_anterior = None # <-- AÑADE ESTO AQUÍ


    estado_salud_anterior = None
    while True:
        cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            cliente.connect((IP_LOCAL, PUERTO_LOCAL))
            cliente.settimeout(TICK)
            print("[ENGINE] Conectado al Monitor")
            
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

                estado_actual = "KO" if ko else "OK"
                if estado_actual != estado_salud_anterior:
                    enviar(cliente, f"SALUD#{ID_ESTACION}#{estado_actual}")
                    estado_salud_anterior = estado_actual
                
                # --- Procesar órdenes desde el Monitor ---
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
                            print(f"{VERDE}[RIEGO] Válvula abierta por petición remota{RESET}")
                            enviar(cliente, f"AUTORIZA_RIEGO#{ID_ESTACION}")
                    elif accion == "FIN_RIEGO":
                        if regando:
                            volumen += CAUDAL / 60 * (ahora - t_ultimo)
                            enviar(cliente, f"METRICAS#{ID_ESTACION}#{CAUDAL}#{round(volumen, 2)}")
                            print(f"{AZUL}[RIEGO] Válvula cerrada. Total regado: {round(volumen, 2)} L{RESET}")
                        regando = False
                        enviar(cliente, f"FIN_RIEGO#{ID_ESTACION}")
                    elif accion == "BLOQUEAR":
                        regando = False
                        bloqueada = True
                        print(f"{AMARILLO}[ENGINE] FUERA DE SERVICIO{RESET}")
                    elif accion == "ACTIVAR":
                        bloqueada = False
                        print(f"{VERDE}[ENGINE] Estación activada: disponible{RESET}")

                # --- Procesar órdenes desde el teclado (Riego Manual) ---
                try:
                    comando_manual = cola_comandos.get_nowait()
                    if comando_manual == "INICIAR_RIEGO":
                        if bloqueada or ko or regando:
                            print(f"{AMARILLO}[AVISO] No se puede iniciar: estación bloqueada, en fuga o ya regando{RESET}")
                        else:
                            regando = True
                            volumen = 0.0
                            t_ultimo = t_metrica = ahora
                            print(f"{VERDE}[RIEGO MANUAL] Válvula abierta desde la propia estación{RESET}")
                            enviar(cliente, f"INICIAR_RIEGO#{ID_ESTACION}")
                    elif comando_manual == "FIN_RIEGO":
                        if regando:
                            volumen += CAUDAL / 60 * (ahora - t_ultimo)
                            enviar(cliente, f"METRICAS#{ID_ESTACION}#{CAUDAL}#{round(volumen, 2)}")
                            print(f"{AZUL}[RIEGO MANUAL] Válvula cerrada. Total regado: {round(volumen, 2)} L{RESET}")
                            regando = False
                            enviar(cliente, f"FIN_RIEGO#{ID_ESTACION}")
                        else:
                            print(f"{AMARILLO}[AVISO] No hay riego en curso para detener{RESET}")
                except queue.Empty:
                    pass

                # --- Envío de métricas periódicas ---
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
            # Reiniciar variables de estado para la reconexión
            regando = False 
            bloqueada = False
            time.sleep(2)

if __name__ == "__main__":
    WM_WS_E(leer_argumentos())