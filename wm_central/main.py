import sys
import socket
import threading
import common.protocol
import db_manager
from kafka import KafkaConsumer


#Diccionario donde iremos apuntando las conexiones abiertas con las WS para usar en Kafka
conexiones_abiertas = {}

#########################################################################
# Bucle Kafka
#########################################################################
def iniciar_kafka(ip_kafka):
    print(f"[KAFKA] Iniciando escucha en el broker {ip_kafka}...")

    #Nos ponemos en el canal de peticiones_operarios, traduciendo los bytes a texto
    consumidor = KafkaConsumer ( 'peticiones_operarios', bootstrap_servers= [ip_kafka], 
                                value_deserializer=lambda m: m.decode('utf-8'))
    #bucle donde estamos escuchando las peticiones
    for mensaje in consumidor:
        texto_recibido = mensaje.value
        print(f"[KAFKA] Petición recibida: {texto_recibido}")
        tratar_mensaje_operario(mensaje)


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

    #creamos un hilo para manejar kafka y lo iniciamos
    hilo_kafka = threading.Thread(target=iniciar_kafka, args=(ip_kafka,))
    hilo_kafka.start()

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
            tratar_mensaje_estacion(mensaje_recibido, direccion, conexion)
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
def tratar_mensaje_estacion(mensaje, direccion, conexion):
    #separamos la información, que viene marcada entre #
    argumentos = mensaje.split('#')
    #Guardamos el comando que nos ha llegado
    accion = argumentos[0]

    match accion:
        case "ESTADO":
            conexion.send(common.protocol.ACK)
            print(f"[ESTADO] la estación {argumentos[1]} se encuentra en el estado: {argumentos[2]}")
       
        case "REGISTRO":
            db_manager.registrar_estacion(argumentos[1], argumentos[2])
            print(f"[REGISTRO]: anotada la estación {argumentos[1]} en {argumentos[2]}")
            comprobar_ack(conexion, "Estación correctamente dada de alta en el sistema")
            #Nos guardamos la conexión en un diccionario para después poder acceder a ella desde la sección de Kafka
            conexiones_abiertas[argumentos[1]]= conexion  

        case "INICIAR_RIEGO":
            print(f"[INICIO RIEGO]: la estación {argumentos[1]} comienza el riego")
        
        case "METRICAS":
            print(f"[MÉTRICAS] la estación: {argumentos[1]} tiene un caudal de:{argumentos[2]} y un volumen de {argumentos[3]}")
        
        case "FIN_RIEGO":
            print(f"[FINALIZACIÓN DE RIEGO] la estación: {argumentos[1]} ha finalizado el regado ")
        
        case _:
            print(f"([ERROR DE MENSAJE] el mensaje: {mensaje} enviado por: {direccion}, no sigue la convención especificada")

def tratar_mensaje_operario(mensaje):
    #separamos la información, que viene marcada entre #
     argumentos = mensaje.split('#')
     #Guardamos el comando que nos ha llegado
     accion = argumentos[0]
 
     match accion:
         case "PETICION_RIEGO":
             #primero comprobamos que el operario esté dado de alta
            if not db_manager.existe_operario(argumentos[1]):
                print(f"[PETICION DE RIEGO] El operario {argumentos[1]}, no está dado de alta, debes hacerlo antes de hacer peticiones")
            #Comprobamos que este dada de alta la estacion
            elif not (estado:= db_manager.obtener_estado(argumentos[2])):
                print(f"[PETICION DE RIEGO] La estación {argumentos[2]}, no está dada de alta, debes hacerlo antes de hacer peticiones")
            else: 
                if estado == "DISPONIBLE":
                    conexion_estacion = conexiones_abiertas[argumentos[2]]
                    comprobar_ack (conexion_estacion,f"INICIAR_RIEGO#{argumentos[2]}")
                    print(f"[PETICION DE RIEGO] La estación {argumentos[2]}, ha empezado a regar")

         case "PETICION_PARADA":
             print(f"[INICIO RIEGO]: la estación {argumentos[1]} comienza el riego")
         
         case "REGISTRO_FO":
             db_manager.registrar_operario(argumentos[1], argumentos[2])
             print(f"[ALTA DE OPERARIO] El operario: {argumentos[1]} se ha dado de alta con el nombre: {argumentos[2]} ")
         
         case _:
             print(f"([ERROR DE MENSAJE] el mensaje: {mensaje} enviado por: {argumentos[1]}, no sigue la convención especificada")

#función que vamos a usar para comprobar que el mensaje se ha enviado correctamente
def comprobar_ack(conexion, mensaje):
    #convertiemoms el mensaje de string a byte
    paquete = common.protocol.empaquetar(mensaje)
    #vamos a dar 3 intentos de reenvio en caso de fallo
    for i in range(3):
        conexion.send(paquete)
        respuesta = conexion.recv(1024)
        if respuesta == common.protocol.ACK:
            return True 
        if respuesta ==common.protocol.NACK:
            print("[ALERTA] conexión rechazada, reintentando...")
    return False