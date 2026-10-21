import socket
import time 
import common.protocol
import os

IP_LOCAL = os.environ.get("MONITOR_IP", "127.0.0.1")
PUERTO_LOCAL = int(os.environ.get("MONITOR_PORT", 5001))
ID_ESTACION = os.environ.get("STATION_ID", "WS_DEFAULT")
UBICACION = os.environ.get("STATION_LOCATION", "Zona Centro")
CAUDAL = 5.0

#########################################################################
# Bucle principal
#########################################################################

def WM_WS_E():

    #Creamos el socket
    cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        #Conexion
        cliente.connect((IP_LOCAL, PUERTO_LOCAL))

        #Configuración del timeout
        cliente.settimeout(1.0)

        #Implementamos un par de variables de control para saber el estado
        regando = False
        volumen_acumulado = 0
        while True:
            try:
                trama_recibida = common.protocol.leer_trama(cliente)
                if not trama_recibida:
                    print("[DESCONEXIÓN] La Central ha cerrado la conexión de forma inesperada.")
                    break

                mensaje = common.protocol.desempaquetar(trama_recibida)
                
                #separamos la información, que viene marcada entre #
                argumentos = mensaje.split('#')
                #Guardamos el comando que nos ha llegado
                accion = argumentos[0]
                #Opciones que se pueden recibir 
                if accion == "INICIAR_RIEGO":
                    regando = True
                    comprobar_ack(cliente,f"AUTORIZA_RIEGO#{ID_ESTACION}")

                if accion == "FIN_RIEGO":
                    regando = False
                    cliente.send(common.protocol.ACK)

            except socket.timeout:
                if regando:
                    volumen_acumulado += CAUDAL
                    paquete = common.protocol.empaquetar(f"METRICAS#{ID_ESTACION}#{CAUDAL}#{volumen_acumulado}")
                    cliente.send(paquete)
                    confirmacion = common.protocol.leer_trama(cliente)
                    if confirmacion != common.protocol.ACK:
                        print("[ERROR DE PROTOCOLO] La central rechazó la conexión (no se recibió <ACK>)")
                        cliente.close()
                        return 


    except ConnectionRefusedError as e:
        print(f"[ERROR DE CONEXIÓN] El Monitor no está disponible. Motivo: {e}")
    finally:
        cliente.close()

if __name__ == "__main__":
    WM_WS_E()

#función que vamos a usar para comprobar que el mensaje se ha enviado correctamente
def comprobar_ack(conexion, mensaje):
    #convertiemoms el mensaje de string a byte
    paquete = common.protocol.empaquetar(mensaje)
    #vamos a dar 3 intentos de reenvio en caso de fallo
    for i in range(3):
        conexion.send(paquete)
        respuesta = common.protocol.leer_trama(conexion)
        if respuesta == common.protocol.ACK:
            return True 
        if respuesta ==common.protocol.NACK:
            print("[ALERTA] conexión rechazada, reintentando...")
    return False
