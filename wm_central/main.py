import sys
import socket
import threading
import common.protocol


#########################################################################
# Bucle principal
#########################################################################
def WM_Central():
    #convertinmos a numeros el puerto
    puerto_escucha = int(sys.argv[1])
    ip_kafka = sys.argv[2]

    #Creamos un socket servidor usano AF_INET y SOCK_STREAM
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #Hacemos que el socket se enlace con la ip del pc y el puerto donde escuchar
    servidor.bind((socket.gethostname(), puerto_escucha)) #el argumento de bind es una tupla!!!
    #ponemos el servidor a escuchar en ese puerto. Puede tener en cola 5 peticiones
    servidor.listen(5)

    #Bucle principal del servidor donde despacha las peticines de los clientes a diferentes hilos
    while True:
        #aceptamos la conexión
        (socket_cliente, direccion) = servidor.accept()
        #creamos un hilo para que se maneje a parte la petición y el servidor vuelva a escuchar
        hilo = threading.Thread(target=manejar_hilo, args=(socket_cliente, direccion)) #target debe ser solo referencia de la función, args debe ser iterable
        hilo.start()


#########################################################################
# Tratamiento del hilo
#########################################################################
def manejar_hilo(conexion, direccion):
    # Leemos el primer mensaje para el saludo inicial
    trama_inicial = conexion.recv(1024)

    #Si no seguimos el protocolo hay que cancelar conexion
    if trama_inicial != common.protocol.ENQ:
        # Mensaje claro por consola indicando el rechazo
        print(f"[ERROR DE PROTOCOLO] Conexión de {direccion} rechazada. Se esperaba <ENQ>, recibido: {trama_inicial}")
        #enviamos un NACK al cliente
        conexion.send(common.protocol.NACK)
        conexion.close()
        return  # Salimos de la función para matar este hilo

    #en caso de seguir el protocolo, enviamos ACK y empezamos el bucle de la conexión
    conexion.send(common.protocol.ACK)
    print(f"[NUEVA CONEXIÓN] Establecimiento exitoso con {direccion}")

    while trama_recibida := conexion.recv(1024): #mientras se recivan datos
        try:
            #desempaquetamos la trama y devolvemos ACK
            mensaje_recibido = common.protocol.desempaquetar(trama_recibida)
            conexion.send(common.protocol.ACK)
        #Manejamos las posibles excepciones que salten devolviendo NACK
        except ValueError as e:
            print(f"([ERROR DE PROTOCOLO] {e})")
            conexion.send(common.protocol.NACK)
        except IndexError as e:
            print(f"([ERROR DE PROTOCOLO] {e})")
            conexion.send(common.protocol.NACK)
    #Al acabar la conexión de repente, cerramos el socket.
    print(f"([DESCONEXIÓN] El cliente {direccion} ha cerrado conexión)")
    conexion.close()
