import socket
import threading
import time
import common.protocol
import os

IP_LOCAL = os.environ.get("MONITOR_IP", "127.0.0.1")
PUERTO_LOCAL = int(os.environ.get("MONITOR_PORT", 5001))
ID_ESTACION = os.environ.get("STATION_ID", "WS_DEFAULT")
UBICACION = os.environ.get("STATION_LOCATION", "Zona Centro")
CAUDAL = float(os.environ.get("CAUDAL_LMIN", 5.0))  # caudal simulado en L/min
TICK = 0.2  # timeout del socket; los envíos periódicos se miden con el reloj

ko = False  # avería simulada: se alterna desde el teclado


def enviar(conexion, mensaje):
    conexion.send(common.protocol.empaquetar(mensaje))


def escuchar_teclado():
    global ko
    print("[ENGINE] Pulsa ENTER para alternar entre OK y KO (simular fuga)")
    while True:
        input()
        ko = not ko
        print(f"[ENGINE] Estado simulado: {'KO (fuga)' if ko else 'OK'}")


def WM_WS_E():
    cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        cliente.connect((IP_LOCAL, PUERTO_LOCAL))
        cliente.settimeout(TICK)
        threading.Thread(target=escuchar_teclado, daemon=True).start()

        regando = False
        bloqueada = False
        volumen = 0.0
        t_ultimo = 0.0    # último instante en el que se acumuló volumen
        t_metrica = 0.0   # último envío de métricas

        while True:
            mensaje = None
            try:
                trama = common.protocol.leer_trama(cliente)
                if not trama:
                    print("[DESCONEXIÓN] El Monitor ha cerrado la conexión.")
                    break
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
                        volumen = 0.0            # cada riego empieza desde cero
                        t_ultimo = t_metrica = ahora
                        print("[RIEGO] Válvula abierta")
                        enviar(cliente, f"AUTORIZA_RIEGO#{ID_ESTACION}")

                elif accion == "FIN_RIEGO":
                    if regando:
                        # última lectura, para que el resumen de la Central sea exacto
                        volumen += CAUDAL / 60 * (ahora - t_ultimo)
                        enviar(cliente, f"METRICAS#{ID_ESTACION}#{CAUDAL}#{round(volumen, 2)}")
                        print(f"[RIEGO] Válvula cerrada. Total regado: {round(volumen, 2)} L")
                    regando = False
                    enviar(cliente, f"FIN_RIEGO#{ID_ESTACION}")

                elif accion == "BLOQUEAR":
                    regando = False
                    bloqueada = True
                    print("[ENGINE] FUERA DE SERVICIO por orden de la Central")

                elif accion == "ACTIVAR":
                    bloqueada = False
                    print("[ENGINE] Estación activada: disponible")

            # Métricas cada segundo, aunque estén llegando otras tramas
            if regando and ahora - t_metrica >= 1.0:
                volumen += CAUDAL / 60 * (ahora - t_ultimo)
                t_ultimo = t_metrica = ahora
                print(f"[RIEGO] Caudal: {CAUDAL} L/min | Volumen: {round(volumen, 2)} L")
                enviar(cliente, f"METRICAS#{ID_ESTACION}#{CAUDAL}#{round(volumen, 2)}")

    except ConnectionRefusedError as e:
        print(f"[ERROR DE CONEXIÓN] El Monitor no está disponible. Motivo: {e}")
    except OSError as e:
        print(f"[ERROR DE CONEXIÓN] Se ha perdido la conexión con el Monitor: {e}")
    finally:
        cliente.close()


if __name__ == "__main__":
    WM_WS_E()