import sys
import socket
import threading
import common.protocol
import db_manager

#########################################################################
# Bucle principal de red
#########################################################################
def WM_Central():
    #vamos a cargar las estaciones que hay guardadas en la base de datos
    estaciones = db_manager.iniciar_bd()
    if estaciones: 
        print("[SISTEMA] Cargando estaciones previas:")
        #Imprimimos los datos de cada estación en la lista
        for estacion in estaciones:
            print(f" -> {estacion[0]} en {estacion[1]}  DESCONECTADA")
    else: 
        print("[SISTEMA] No hay estaciones registradas aun")


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
            #desempaquetamos la trama, devolvemos ACK y pasamos a analizar el mensaje
            mensaje_recibido = common.protocol.desempaquetar(trama_recibida)
            print(f"[MENSAJE de {direccion}]: {mensaje_recibido}")
            conexion.send(common.protocol.ACK)
            tratar_mensaje(mensaje_recibido, direccion, conexion)
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


#########################################################################
# Tratamiento de los mensajes recibidos
#########################################################################
def tratar_mensaje(mensaje, direccion, conexion):
    #separamos la información, que viene marcada entre #
    argumentos = mensaje.split('#')
    #Guardamos el comando que nos ha llegado
    accion = argumentos[0]

    match accion:
        case "ESTADO":
            print(f"[ESTADO] la estación {argumentos[1]} se encuentra en el estado: {argumentos[2]}")
       
        case "REGISTRO":
            db_manager.registrar_estacion(argumentos[1], argumentos[2])
            print(f"[REGISTRO]: anotada la estación {argumentos[1]} en {argumentos[2]}")
            conexion.send(common.protocol.empaquetar("Estación correctamente dada de alta en el sistema"))
        
        case "INICIAR_RIEGO":
            print(f"[INICIO RIEGO]: la estación {argumentos[1]} comienza el riego")
        
        case "METRICAS":
            print(f"[MÉTRICAS] la estación: {argumentos[1]} tiene un caudal de:{argumentos[2]} y un volumen de {argumentos[3]}")
        
        case "FIN_RIEGO":
            print(f"[FINALIZACIÓN DE RIEGO] la estación: {argumentos[1]} ha finalizado el regado ")
        
        case _:
            print(f"([ERROR DE MENSAJE] el mensaje: {mensaje} no sigue la convención especificada")
