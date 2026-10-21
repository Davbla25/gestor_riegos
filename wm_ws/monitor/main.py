import socket
import time 
import common.protocol
import os

IP_CENTRAL = os.environ.get("CENTRAL_IP", "127.0.0.1")
PUERTO_CENTRAL =int(os.environ.get("CENTRAL_PORT", 5000))
ID_ESTACION = os.environ.get("STATION_ID", "WS_DEFAULT")
UBICACION = os.environ.get("STATION_LOCATION", "Zona Centro")

IP_LOCAL = os.environ.get("MONITOR_IP", "127.0.0.1")
PUERTO_LOCAL = int(os.environ.get("MONITOR_PORT", 5001))

#########################################################################
# Bucle principal
#########################################################################

def WM_WS_M():
    ##########################################
    # PARTE QUE ACTÚA COMO CLIENTE (C <= M)  #
    ##########################################
    #Creamos el socket
    cliente = socket.socket(socket.AF_INET,socket.SOCK_STREAM)


    ###########################################
    # PARTE QUE ACTÚA COMO SERVIDOR (M <= E)  #
    ###########################################
    #Creamos el socket
    servidor = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    servidor.bind((IP_LOCAL,PUERTO_LOCAL))
    servidor.listen(1) #Para que escuche de 1 en 1

    # IMPORTANTE: Que no bloquee el programa
    servidor.setblocking(False) 
    print(f"[INFO] Monitor escuchando al Engine en {IP_LOCAL}:{PUERTO_LOCAL}")

    #Intentamos conectarnos a la central
    try:
        #Conexión
        cliente.connect((IP_CENTRAL, PUERTO_CENTRAL))
        #Saludo
        cliente.send(common.protocol.ENQ)
        confirmacion = common.protocol.leer_trama(cliente)
        #Bloque para comprobar que se establece la conexión
        if confirmacion != common.protocol.ACK:
            print("[ERROR DE PROTOCOLO] La central rechazó la conexión (no se recibió <ACK>)")
            cliente.close()
            return #Detención de la función

        trama_registro = common.protocol.empaquetar(f"REGISTRO#{ID_ESTACION}#{UBICACION}")
        cliente.send(trama_registro)
        respuesta = common.protocol.leer_trama(cliente)
        #Comprobamos el ACK
        if respuesta == common.protocol.ACK:
            #Leemos la trama
            trama_recibida = common.protocol.leer_trama(cliente)
            mensaje_recibido = common.protocol.desempaquetar(trama_recibida)
            print(f"MENSAJE DE LA CENTRAL:{mensaje_recibido}")
            #Mandamos confirmación
            cliente.send(common.protocol.ACK)
        
        #Configuración del timeout
        cliente.settimeout(1.0)

        #Variable para guardar al engine
        conexion_engine = None 
        #Bucle de conexión persistente
        while True:

            try:
                #Comprobamos conexión
                if conexion_engine is None:
                    conexion_engine, direccion = servidor.accept()
                    print(f"[INFO] Engine conectado desde {direccion}")
                    # Hacemos que esta nueva conexión tampoco bloquee el programa
                    conexion_engine.setblocking(False)


            except BlockingIOError:
                # Si entra aquí, es que el Engine no está intentando conectarse en este milisegundo.
                # No pasa nada, seguimos nuestro camino.
                pass

            # ESCUCHAR LO QUE SUBE DEL ENGINE
            try:
                if conexion_engine is not None:
                    trama_engine = common.protocol.leer_trama(conexion_engine)
                    
                    # Si el Engine nos ha enviado algo (como AUTORIZA_RIEGO o METRICAS)
                    if trama_engine:
                        mensaje_engine = common.protocol.desempaquetar(trama_engine)
                        print(f"[PUENTE] Reenviando a Central: {mensaje_engine}")
                        
                        # 1. Le devolvemos el ACK local al Engine para liberarlo
                        conexion_engine.send(common.protocol.ACK)
                        
                        # 2. Re-empaquetamos el mensaje y lo lanzamos hacia la red externa
                        paquete_central = common.protocol.empaquetar(mensaje_engine)
                        cliente.send(paquete_central)
                        
                        # 3. Esperamos que la Central confirme que lo ha recibido
                        confirmacion = common.protocol.leer_trama(cliente)
                        if confirmacion != common.protocol.ACK:
                            print("[ERROR DE RED] La Central no confirmó el mensaje del Engine.")
            except BlockingIOError:
                # Si el Engine no está diciendo nada en este milisegundo, simplemente seguimos
                pass  

            try:
                #Comprobamos si debemos iniciar el riego
                trama_recibida = common.protocol.leer_trama(cliente)

                if not trama_recibida:
                    print("[DESCONEXIÓN] La Central ha cerrado la conexión de forma inesperada.")
                    break

                mensaje = common.protocol.desempaquetar(trama_recibida)
                
                #separamos la información, que viene marcada entre #
                argumentos = mensaje.split('#')
                #Guardamos el comando que nos ha llegado
                accion = argumentos[0]

                if accion == "INICIAR_RIEGO":
                    print("[ORDEN RECIBIDA] Iniciando el riego por petición de la central")
                    
                    # Comprobamos que el Engine está conectado antes de enviarle nada
                    if conexion_engine is not None:
                        paquete = common.protocol.empaquetar("INICIAR_RIEGO")
                        conexion_engine.send(paquete)
                    else:
                        print("[AVISO] La Central pide regar, pero el Engine no está conectado.")

                if accion == "FIN_RIEGO": 
                    print("[ORDEN RECIBIDA] Finalizando el riego por petición de la central")
                    
                    if conexion_engine is not None:
                        paquete = common.protocol.empaquetar("FIN_RIEGO")
                        conexion_engine.send(paquete)
                    else:
                        print("[AVISO] La Central pide parar, pero el Engine no está conectado.")
                
                # Liberamos a la Central
                cliente.send(common.protocol.ACK)

                
            except socket.timeout:
                # El timeout saltará cada segundo si la Central no manda órdenes
                #Enviamos el estado
                estado_engine = "OK"
                trama_estado = common.protocol.empaquetar(f"ESTADO#{ID_ESTACION}#{estado_engine}")
                #Nos aseguramos que recibimos la confirmación
                cliente.send(trama_estado)
                confirmacion = common.protocol.leer_trama(cliente)
                #Esto nos permite depurar cortes de red
                if confirmacion != common.protocol.ACK:
                    print("[ERROR DE RED] El latido de salud no llegó correctamente (no se recibió <ACK>)")

                pass
    except ConnectionRefusedError as e:
        print(f"[ERROR DE CONEXIÓN] La central no está disponible. Motivo:{e}")
    finally:
        cliente.close()
    
if __name__ == "__main__":
    WM_WS_M()
                


