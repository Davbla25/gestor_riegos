import sys
import socket
import threading
import common.protocol
import db_manager
from kafka import KafkaConsumer, KafkaProducer
import queue


#Diccionario donde iremos apuntando las conexiones abiertas con las WS para usar en Kafka
coleccion_buzones = {}
#Variable donde guardamos la IP de Kafka
productor_respuestas = None

#########################################################################
# Consumidor Kafka
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
        tratar_mensaje_operario(texto_recibido) 


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

    #creamos el productor de kafka para contestar a los usuarios
    global productor_respuestas 
    productor_respuestas = KafkaProducer('respuesta_central', bootstrap_servers= [ip_kafka],value_serializer=lambda m: m.encode('utf-8'))

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
    #creamos una cola donde kafka guardará los mensajes para que sean enviados a la estación conveniente
    buzon_salida = queue.Queue()
    # Leemos el primer mensaje para el saludo inicial
    trama_inicial = common.protocol.leer_trama(conexion)

    #Bloque para comprobar que seguimos el protocolo 
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

    #vamos a establecer un tiempo limite de espera de un segundo
    conexion.settimeout(1)

    #Bucle principal donde escuchamos el socket de la estación
    while True: 
        try:
            trama_recibida = common.protocol.leer_trama(conexion)
            #Si no llega información salimos del bucle (desconectamos)
            if not trama_recibida : break
            #desempaquetamos la trama, devolvemos ACK y pasamos a analizar el mensaje
            mensaje_recibido = common.protocol.desempaquetar(trama_recibida)
            print(f"[MENSAJE de {direccion}]: {mensaje_recibido}")
            conexion.send(common.protocol.ACK)
            tratar_mensaje_estacion(mensaje_recibido, direccion, conexion,  buzon_salida)
        #Manejamos las posibles excepciones que salten
        except socket.timeout:
            #si pasado un segundo no recivimos nada, continúa la ejecución
            pass
        except ValueError as e:
            print(f"([ERROR DE PROTOCOLO] {e})")
            conexion.send(common.protocol.NACK)
        except IndexError as e:
            print(f"([ERROR DE PROTOCOLO] {e})")
            conexion.send(common.protocol.NACK)
        except ConnectionResetError as e:
            print(f"([CONEXIÓN CORTADA ABRUPTAMENTE] {e})")
            break


        #bloque en el que manejamos la cola para Kafka
        try:
            #sacamos los argumentos del recado de Kafka
            argumentos_kafka = buzon_salida.get_nowait()
            #asignamos la accion que está en el primer bloque y mandamos a la función que toque
            accion = argumentos_kafka[0]
            if accion == "PETICION_RIEGO":
                # Le pasamos los argumentos correctos a la función
                iniciar_riego(argumentos_kafka[2], conexion, argumentos_kafka[1])
                    
            elif accion == "PETICION_PARADA":
                fin_riego(argumentos_kafka[2], conexion, argumentos_kafka[1]) 

        except queue.Empty:
            pass
    #Al acabar la conexión de repente, cerramos el socket.
    print(f"([DESCONEXIÓN] El cliente {direccion} ha cerrado conexión)")
    conexion.close()


#########################################################################
# Tratamiento de los mensajes recibidos
#########################################################################
def tratar_mensaje_estacion(mensaje, direccion, conexion, buzon_salida):
    #separamos la información, que viene marcada entre #
    argumentos = mensaje.split('#')
    #Guardamos el comando que nos ha llegado
    accion = argumentos[0]

    match accion:
        case "ESTADO":
            if argumentos[2] == "FUGA":
                print(f"[ALERTA] la estación {argumentos[1]} Ha detectado una fuga")
                db_manager.cambiar_estado_fuga(argumentos[1])
                fin_riego(argumentos[1])

            print(f"[ESTADO] la estación {argumentos[1]} se encuentra en el estado: {argumentos[2]}")
            
        case "REGISTRO":
            db_manager.registrar_estacion(argumentos[1], argumentos[2])
            print(f"[REGISTRO]: anotada la estación {argumentos[1]} en {argumentos[2]}")
            comprobar_ack(conexion, "Estación correctamente dada de alta en el sistema")
            #Nos guardamos la cola en un diccionario para después poder acceder a ella desde la sección de Kafka
            coleccion_buzones[argumentos[1]]= buzon_salida  

        case "INICIAR_RIEGO":
            estado = db_manager.obtener_estado(argumentos[1])
            if estado == "DISPONIBLE":
                print(f"[INICIO_RIEGO] la estación: {argumentos[1]} nos avisa de que comienza a regar")
                db_manager.cambiar_estado_riego(argumentos[1])
                if not comprobar_ack(conexion, "Inicio de riego correctamente anotado"):
                    print(f"[ERROR DE CONEXIÓN] La estación {argumentos[1]} no responde")

            else:
                if not comprobar_ack(conexion, "La estación no se encuentra disponible en estos momentos"):
                    print(f"[ERROR DE CONEXIÓN] La estación {argumentos[1]} no responde")
                else:
                    print(f"[INICIO_RIEGO] la estación: {argumentos[1]} no está disponible")



        case "AUTORIZA_RIEGO":
            db_manager.cambiar_estado_riego(argumentos[1])
            print(f"INFO#{argumentos[1]}#Riego iniciado con éxito")

        case "DENIEGA_RIEGO":
            print(f"INFO#{argumentos[1]}#Solicitud de riego rechazada, motivo: {argumentos[2]}")
            
        case "METRICAS":
            print(f"[MÉTRICAS] la estación: {argumentos[1]} tiene un caudal de:{argumentos[2]} y un volumen de {argumentos[3]}")
        
        case "FIN_RIEGO":
            estado = db_manager.obtener_estado(argumentos[1])
            if estado == "REGANDO":
                print(f"[FIN_RIEGO] la estación: {argumentos[1]} nos avisa de que acabar de regar")
                db_manager.cambiar_estado_disponible(argumentos[1])
                comprobar_ack(conexion, "Fin de riego correctamente anotado")

            else:
                print(f"[FIN_RIEGO] la estación: {argumentos[1]} nos avisa de que acaba de regar, pero no está regando")
                comprobar_ack(conexion, "La estación no se encuentra regando en estos momentos")
        case _:
            print(f"([ERROR DE MENSAJE] el mensaje: {mensaje} enviado por: {direccion}, no sigue la convención especificada")
            comprobar_ack(conexion, f"([ERROR DE MENSAJE] el mensaje: {mensaje}, no sigue la convención especificada")



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
                notificar_operario(f"[PETICION DE RIEGO] El operario {argumentos[1]}, no está dado de alta, debes hacerlo antes de hacer peticiones")
            else:
                if argumentos[2] in coleccion_buzones:
                    buzon = coleccion_buzones[argumentos[2]]
                    buzon.put(argumentos)


         case "PETICION_PARADA":
            #primero comprobamos que el operario esté dado de alta
            if not db_manager.existe_operario(argumentos[1]):
                print(f"[PETICION DE PARADA] El operario {argumentos[1]}, no está dado de alta, debes hacerlo antes de hacer peticiones")
                notificar_operario(f"[PETICION DE PARADA] El operario {argumentos[1]}, no está dado de alta, debes hacerlo antes de hacer peticiones")
            else:
                if argumentos[2] in coleccion_buzones:
                    buzon = coleccion_buzones[argumentos[2]]
                    buzon.put(argumentos)

         case "REGISTRO_FO":
             db_manager.registrar_operario(argumentos[1], argumentos[2])
             print(f"[ALTA DE OPERARIO] El operario: {argumentos[1]} se ha dado de alta con el nombre: {argumentos[2]} ")
             notificar_operario(f"[ALTA DE OPERARIO] El operario: {argumentos[1]} se ha dado de alta con el nombre: {argumentos[2]} ")
         
         case _:
             print(f"([ERROR DE MENSAJE] el mensaje: {mensaje} enviado por: {argumentos[1]}, no sigue la convención especificada")
             notificar_operario(f"([ERROR DE MENSAJE] el mensaje: {mensaje} enviado por: {argumentos[1]}, no sigue la convención especificada")

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

#función usada para enviar mensajes por el productor de Kafka
def notificar_operario(mensaje):
    #traemos la variable global del productor
    global productor_respuestas
    #enviamos el mensaje por el canal de respuestas
    productor_respuestas.send('respuesta_central', mensaje)
    productor_respuestas.flush()


#########################################################################
# Funciones directas a la estación
#########################################################################

#Función que hace las comprobaciones necesarias para iniciar el riego
def iniciar_riego(id_estacion, conexion,  id_operario = None):
    #Comprobamos que este dada de alta la estacion
    if not (estado:= db_manager.obtener_estado(id_estacion)):
        print(f"[PETICION DE RIEGO] La estación {id_estacion}, no está dada de alta, debes hacerlo antes de hacer peticiones")
        if id_operario: notificar_operario(f"INFO#{id_operario}#Estación no dada de alta ")
    else: 
        #si la estación se encutra disponible realizamos conexión con ella
        if estado == "DISPONIBLE":
            #si la conexión tiene éxito, le cambiamos el estado a regando
            if comprobar_ack (conexion,f"INICIAR_RIEGO#{id_estacion}"):
                print(f"[PETICION DE RIEGO] Se ha enviado la petición a la estación {id_estacion}")
                if id_operario: notificar_operario(f"INFO#{id_operario}#Petición enviada, esperando respuesta...")

            else:
                #si falla la conexión desconectamos la estación
                print(f"[ERROR DE CONEXIÓN] La estación {id_estacion}, está fuera de conexión, inténtalo más tarde")
                if id_operario: notificar_operario(f"INFO#{id_operario}#Estación fuera de conexión")
                db_manager.cambiar_estado_desconectada(id_estacion)

                
        else:
            print(f"[PETICION DE RIEGO] La estación {id_estacion}, no se encuentra disponible en estos momentos")
            if id_operario: notificar_operario(f"INFO#{id_operario}#Estación no disponible actualmente ")


#Función que detiene el riego, detecta si se pide porque hay una fuga o simplemente porque se desea detener el riego
def fin_riego(id_estacion, conexion, id_operario = None):
    
    #Comprobamos que este dada de alta la estacion
    if not (estado:= db_manager.obtener_estado(id_estacion)):
        print(f"[PETICION DE PARADA] La estación {id_estacion}, no está dada de alta, debes hacerlo antes de hacer peticiones")
        if id_operario: notificar_operario(f"INFO#{id_operario}#Estación no dada de alta ")

    else: 
        #si la estación ya está parada, lo advertimos
        if estado == "REGANDO":
            #si la conexión es exitosa la  ponemos en estado disponible
            if comprobar_ack (conexion,f"FIN_RIEGO#{id_estacion}"):
                print(f"[PETICION DE PARADA] Se ha envido la petición de parada a la estación {id_estacion}")
                if id_operario: notificar_operario(f"INFO#{id_operario}# Petición enviada, esperando respuesta...")

            else:
                #si falla la conexión desconectamos la estación
                print(f"[ERROR DE CONEXIÓN] La estación {id_estacion}, está fuera de conexión, inténtalo más tarde")
                if id_operario: notificar_operario(f"INFO#{id_operario}#Estación fuera de conexión")
                db_manager.cambiar_estado_desconectada(id_estacion)
        #si se nos manda parar porque hay una fuga, bloqueamos la estación
        elif estado == "FUGA":
            db_manager.bloquear_estacion(id_estacion)
        else:
            print(f"[PETICIÓN DE PARADA] La estación {id_estacion}, No se encuentra regando")
            if id_operario: notificar_operario(f"INFO#{id_operario}#Estación no está regando actualmente")
            
