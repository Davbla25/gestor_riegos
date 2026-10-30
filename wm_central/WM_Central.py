import sys
import socket
import threading
import common.protocol
import db_manager
from kafka import KafkaConsumer, KafkaProducer
import queue
import time
import panel

# Códigos de colores ANSI para la consola
VERDE = "\033[92m"
ROJO = "\033[91m"
AMARILLO = "\033[93m"
AZUL = "\033[94m"
RESET = "\033[0m"

#Tiempo máximo (en segundos) que esperamos el ACK en cada intento de envío
PLAZO_ACK = 1.5

#Diccionario donde iremos apuntando los buzones de las estaciones conectadas para usar en Kafka y en el menú
coleccion_buzones = {}
#Variable donde guardamos el productor de Kafka
productor_respuestas = None


#Excepción que usamos cuando una estación no acusa recibo de una ORDEN (iniciar, parar, bloquear, activar).
#Hereda de ConnectionError para que el bucle de la estación la trate como una conexión perdida y salga por el finally
class EstacionInalcanzable(ConnectionError):
    pass
#########################################################################
# Utilidades para los resúmenes de riego
#########################################################################
def formatear_duracion(segundos):
    segundos = int(segundos)
    minutos, segundos = divmod(segundos, 60)
    return f"{minutos} min {segundos} s" if minutos else f"{segundos} s"
 
 
def texto_resumen(riego):
    #riego es el diccionario que devuelve db_manager.cerrar_riego
    return f"{riego['volumen']:.1f} Litros en {formatear_duracion(riego['duracion'])}"
 
 
def volumen_del_mensaje(argumentos, posicion):
    #Devuelve el volumen que viene en el mensaje de la estación (campo opcional), o None si no viene o no es un número
    try:
        return float(argumentos[posicion])
    except (IndexError, ValueError):
        return None
 

#########################################################################
# Consumidor Kafka
#########################################################################
def iniciar_kafka(ip_kafka):
    print(f"[KAFKA] Iniciando escucha en el broker {ip_kafka}...")
    while True:
        consumidor = None
        #Nos ponemos en el canal de peticiones_operarios (los bytes los traducimos nosotros dentro del try)
        try:
            consumidor = KafkaConsumer('peticiones_operarios', bootstrap_servers=[ip_kafka])
            #bucle donde estamos escuchando las peticiones
            for mensaje in consumidor:
                #el try va DENTRO del bucle: un mensaje defectuoso no debe parar la escucha de los siguientes
                try:
                    texto_recibido = mensaje.value.decode('utf-8')
                    print(f"[KAFKA] Petición recibida: {texto_recibido}")
                    tratar_mensaje_operario(texto_recibido)
                except Exception as e:
                    print(f"[ERROR KAFKA] No se pudo tratar el mensaje {mensaje.value!r}: {e}")
        except Exception as e:
            print(f"[ERROR KAFKA] El consumidor de Kafka se ha detenido: {e}, reconectamos en 2s")
       #cerramos el consumidor si se ha llegado a asignar
        finally:
            try:
                if consumidor is not None:
                    consumidor.close()
            except Exception as e:
                print(f"[ERROR KAFKA] Fallo al cerrar el consumidor: {e}")
        time.sleep(2)


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

    #convertimos a numeros el puerto
    puerto_escucha = int(sys.argv[1])
    ip_kafka = sys.argv[2]

    #creamos el productor de kafka para contestar a los usuarios
    global productor_respuestas
    productor_respuestas = KafkaProducer(bootstrap_servers=[ip_kafka], value_serializer=lambda m: m.encode('utf-8'), max_block_ms=5000)

    #creamos un hilo para manejar kafka y lo iniciamos
    hilo_kafka = threading.Thread(target=iniciar_kafka, args=(ip_kafka,), daemon=True)
    hilo_kafka.start()

    #Creamos un socket servidor usando AF_INET y SOCK_STREAM
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #Habilitamos la opción de poder reutilizar un puerto por si relanzamos varias veces la central
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #Hacemos que el socket se enlace con todas las interfaces del pc y el puerto donde escuchar
    servidor.bind(('', puerto_escucha))  #el argumento de bind es una tupla!!!
    #ponemos el servidor a escuchar en ese puerto. Puede tener en cola 5 peticiones
    servidor.listen(5)

    # Creamos e iniciamos el hilo del menú interactivo en segundo plano que sea un Daemon (para que se corte el proceso si hacemos Ctrl+C)
    hilo_menu = threading.Thread(target=menu_central, daemon=True)
    hilo_menu.start()

    #Bucle principal del servidor donde despacha las peticiones de los clientes a diferentes hilos
    while True:
        #aceptamos la conexión
        (socket_cliente, direccion) = servidor.accept()
        #creamos un hilo para que se maneje a parte la petición y el servidor vuelva a escuchar
        hilo = threading.Thread(target=manejar_hilo, args=(socket_cliente, direccion), daemon=True)
        hilo.start()


#########################################################################
# Tratamiento del hilo
#########################################################################
def manejar_hilo(conexion, direccion):
    #creamos una cola donde kafka y el menú guardarán las órdenes para que sean enviadas a la estación conveniente
    buzon_salida = queue.Queue()
    #creamos una variable para guardar el id de la estación por si se corta la conexión
    id_estacion_actual = None
    try:
        #vamos a establecer un tiempo limite de espera de un segundo y medio
        conexion.settimeout(1.5)
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
        #Bucle principal donde escuchamos el socket de la estación
        while True:
            try:
                trama_recibida = common.protocol.leer_trama(conexion)
                #Si no llega información salimos del bucle (desconectamos)
                if not trama_recibida:
                    break
                # si recibimos un ACK o NACK suelto, no hacemos nada
                if trama_recibida in (common.protocol.ACK, common.protocol.NACK):
                    continue
                #desempaquetamos la trama, devolvemos ACK y pasamos a analizar el mensaje
                mensaje_recibido = common.protocol.desempaquetar(trama_recibida)
                print(f"[MENSAJE de {direccion}]: {mensaje_recibido}")
                conexion.send(common.protocol.ACK)
                #guardamos el id de la estación si manda registro
                if mensaje_recibido.startswith("REGISTRO#"):
                    argumentos = mensaje_recibido.split('#')
                    id_estacion_actual = argumentos[1]
                tratar_mensaje_estacion(mensaje_recibido, direccion, conexion, buzon_salida)
            #Manejamos las posibles excepciones que salten
            except socket.timeout:
                # Si han pasado más de 5 segundos sin recibir NADA, la damos por muerta
                pass
            except (ValueError, IndexError) as e:
                print(f"[ERROR DE PROTOCOLO] {e}")
                conexion.send(common.protocol.NACK)
            except OSError as e:
                #cubre ConnectionResetError, BrokenPipeError, ConnectionAbortedError y EstacionInalcanzable
                print(f"[CONEXIÓN PERDIDA] {e}")
                break
            except Exception as e:
                #un fallo interno (por ejemplo de la BD) no debe tumbar la conexión de una estación sana
                print(f"[ERROR INTERNO] {type(e).__name__}: {e}")

            #Despachamos las órdenes que hay en el buzón (de Kafka o del menú)
            try:
                orden = buzon_salida.get_nowait()
                despachar_orden(orden, direccion, conexion, buzon_salida)
            except queue.Empty:
                pass
            except OSError as e:
                #la estación no acusó una orden o se cortó la conexión: salimos por el finally
                print(f"[ESTACIÓN INALCANZABLE] {e}")
                break
            except Exception as e:
                print(f"[ERROR INTERNO] {type(e).__name__}: {e}")
    finally:
        try:
            print(f"[DESCONEXIÓN] El cliente {direccion} ha cerrado conexión")
            #Solo limpiamos si el buzón registrado para esa estación es el de ESTE hilo. Si la estación ya se
            #ha vuelto a registrar con otra conexión, el buzón y el estado pertenecen a la conexión nueva
            if id_estacion_actual and coleccion_buzones.get(id_estacion_actual) is buzon_salida:
                #leemos el operario ANTES de cambiar el estado: cambiar_estado_desconectada lo borra
                operario = db_manager.obtener_operario(id_estacion_actual)
                #cerramos el riego (si lo había) guardando lo que se había regado hasta el corte
                riego = db_manager.cerrar_riego(id_estacion_actual, db_manager.MOTIVO_DESCONEXION)
                db_manager.cambiar_estado_desconectada(id_estacion_actual)
                coleccion_buzones.pop(id_estacion_actual, None)
                if operario:
                    detalle = f" Regado hasta el corte: {texto_resumen(riego)}." if riego else ""
                    notificar_operario(f"ERROR#{operario}#Se ha perdido la conexión física con la estación {id_estacion_actual}.{detalle}")
        finally:
            conexion.close()


def despachar_orden(orden, direccion, conexion, buzon):
    accion, origen, id_est = orden[0], orden[1], orden[2]
    if accion == "PETICION_RIEGO":
        pendientes = iniciar_riego(id_est, conexion, origen)
    elif accion == "PETICION_PARADA":
        pendientes = fin_riego(id_est, conexion, origen)
    elif accion == "BLOQUEAR":
        pendientes = bloquear(id_est, conexion)
    elif accion == "ACTIVAR":
        pendientes = activar(id_est, conexion)
    else:
        print(f"[ORDEN DESCONOCIDA] {orden}")
        return
    procesar_pendientes(pendientes, direccion, conexion, buzon)


def procesar_pendientes(pendientes, direccion, conexion, buzon):
    for trama in pendientes:
        try:
            mensaje = common.protocol.desempaquetar(trama)
        except (ValueError, IndexError):
            conexion.send(common.protocol.NACK)
            continue
        conexion.send(common.protocol.ACK)
        tratar_mensaje_estacion(mensaje, direccion, conexion, buzon)


#########################################################################
# Tratamiento de los mensajes recibidos
#########################################################################
def tratar_mensaje_estacion(mensaje, direccion, conexion, buzon_salida):
    #separamos la información, que viene marcada entre #
    argumentos = mensaje.split('#')
    #Guardamos el comando que nos ha llegado
    accion = argumentos[0]
    #lista para guardar posibles tramas que se pierden al esperar un ACK
    lista_pendientes = []

    match accion:
        case "ESTADO":
            if argumentos[2] == "FUGA":
                #solo avisamos de la fuga si es la primera vez
                if (estado_previo := db_manager.obtener_estado(argumentos[1])) == "FUGA":
                    return

                #Leemos cómo estaban las cosas antes de la fuga
                operario_afectado = db_manager.obtener_operario(argumentos[1])

                #Cambiamos el estado a FUGA y quitamos su operario
                db_manager.cambiar_estado_fuga(argumentos[1])

                 #Cerramos el riego en curso (si lo había) guardando lo regado hasta ahora
                riego = db_manager.cerrar_riego(argumentos[1], db_manager.MOTIVO_FUGA)

                #Si la estación estaba soltando agua, le mandamos el cierre de emergencia
                if estado_previo in ["REGANDO", "SOLICITADA"]:
                    print(f"[EMERGENCIA] Cerrando válvula de {argumentos[1]} por fuga...")
                    if not comprobar_ack(conexion, f"FIN_RIEGO#{argumentos[1]}", lista_pendientes):
                        print(f"[EMERGENCIA] ¡La estación {argumentos[1]} no ha confirmado el cierre de la válvula!")

                print(f"[ALERTA] la estación {argumentos[1]} ha detectado una fuga")
                #Avisamos al operario al que le hemos cortado el riego y lanzamos la alerta general
                if operario_afectado:
                    detalle = f" Regado hasta el corte: {texto_resumen(riego)}." if riego else ""
                    notificar_operario(f"ERROR#{operario_afectado}#La estación {argumentos[1]} ha detectado una fuga y aborta el riego.{detalle}")
                notificar_operario(f"ALERTA#ALL#¡FUGA detectada en la estación {argumentos[1]}!")
            else:
                print(f"[ESTADO] la estación {argumentos[1]} se encuentra en el estado: {argumentos[2]}")

        case "REGISTRO":
            id_estacion = argumentos[1]
            ubicacion = argumentos[2]

            #comprobamos que la estación no está ya dada de alta para evitar duplicados
            if id_estacion in coleccion_buzones:
                #comprobamos que no sea una estaión registrándose de nuevo
                if buzon_salida == coleccion_buzones[id_estacion] : pass
                else:
                    print(f"[ALERTA] Intento de registro duplicado. La estación {id_estacion} ya está conectada.")
                    # Avisamos a la estación intrusa
                    comprobar_ack(conexion, "ERROR#ID de estación ya en uso", lista_pendientes)
                    # Levantamos un error de red para forzar a manejar_hilo a cerrar este socket intruso
                    raise ConnectionAbortedError(f"Intento de duplicar la estación {id_estacion}")

             #Si se registra de nuevo con un riego abierto, el registro lo corta: lo cerramos para no perder lo regado
            #(registrar_estacion sobrescribe la fila de la estación y borraría el operario y el volumen)
            operario_previo = db_manager.obtener_operario(id_estacion)
            riego_previo = db_manager.cerrar_riego(id_estacion, db_manager.MOTIVO_INTERRUMPIDO)
 
            db_manager.registrar_estacion(id_estacion, ubicacion)
            print(f"[REGISTRO]: anotada la estación {id_estacion} en {ubicacion}")
            #Nos guardamos la cola en un diccionario para después poder acceder a ella desde Kafka y el menú
            coleccion_buzones[argumentos[1]] = buzon_salida
            if riego_previo and operario_previo:
                notificar_operario(f"ERROR#{operario_previo}#La estación {id_estacion} se ha vuelto a registrar y ha interrumpido el riego. Regado: {texto_resumen(riego_previo)}.")
            comprobar_ack(conexion, "Estación correctamente dada de alta en el sistema", lista_pendientes)

        #Este caso se da si se pide iniciar el riego desde la propia estación
        case "INICIAR_RIEGO":
            estado = db_manager.obtener_estado(argumentos[1])
            if estado == "DISPONIBLE":
                print(f"[INICIO_RIEGO] la estación: {argumentos[1]} nos avisa de que comienza a regar")
                db_manager.cambiar_estado_riego(argumentos[1])
                #registramos el riego (no hay operario: lo ha iniciado la propia estación)
                db_manager.abrir_riego(argumentos[1], "ESTACION")
                if not comprobar_ack(conexion, "Inicio de riego correctamente anotado", lista_pendientes):
                    print(f"[ERROR DE CONEXIÓN] La estación {argumentos[1]} no responde")

            else:
                if not comprobar_ack(conexion, "La estación no se encuentra disponible en estos momentos", lista_pendientes):
                    print(f"[ERROR DE CONEXIÓN] La estación {argumentos[1]} no responde")
                else:
                    print(f"[INICIO_RIEGO] la estación: {argumentos[1]} no está disponible")

        case "AUTORIZA_RIEGO":
            estado = db_manager.obtener_estado(argumentos[1])
            if estado != "SOLICITADA":
                # Mensaje fuera de orden: lo descartamos para no pisar el estado actual
                print(f"[AVISO] AUTORIZA_RIEGO de {argumentos[1]} ignorado: la estación está en {estado}, no en SOLICITADA")
            else:
                db_manager.cambiar_estado_riego(argumentos[1])
                operario = db_manager.obtener_operario(argumentos[1])
                 #registramos el inicio del riego
                db_manager.abrir_riego(argumentos[1], operario)
                print(f"[RIEGO AUTORIZADO] la estación {argumentos[1]} ha iniciado con éxito")
                if operario:
                    notificar_operario(f"INFO#{operario}#Riego iniciado con éxito")

        case "DENIEGA_RIEGO":
            estado = db_manager.obtener_estado(argumentos[1])
            if estado != "SOLICITADA":
                print(f"[AVISO] DENIEGA_RIEGO de {argumentos[1]} ignorado: la estación está en {estado}, no en SOLICITADA")
            else:
                id_est = argumentos[1]
                motivo = argumentos[2] if len(argumentos) > 2 else "Sin motivo especificado"
                # Leemos el operario antes de liberar la estación
                operario = db_manager.obtener_operario(id_est)
                db_manager.cambiar_estado_disponible(id_est)
                print(f"[RIEGO DENEGADO] la estación {id_est} deniega por el motivo: {motivo}")
                if operario:
                    notificar_operario(f"ERROR#{operario}#No se pudo iniciar el riego, motivo: {motivo}")

        case "METRICAS":
            #Esto es por si recibimos métricas tras una caída
            if db_manager.obtener_estado(argumentos[1]) in ("DISPONIBLE", "DESCONECTADA"):
                print(f"[RECUPERACIÓN] La estación {argumentos[1]} emite métricas tras una caída. Restaurando riego...")
                db_manager.recuperar_estado_riego(argumentos[1])

            #Guardamos en la BD para que no se pierdan
            db_manager.actualizar_metricas(argumentos[1], argumentos[2], argumentos[3])
            db_manager.actualizar_volumen_riego(argumentos[1], argumentos[3])
            #Se lo enviamos al operario
            if operario := db_manager.obtener_operario(argumentos[1]):
                notificar_operario(f"INFO#{operario}#Métricas actuales - Caudal: {argumentos[2]} L/m, Volumen: {argumentos[3]} L")

        case "FIN_RIEGO":
            estado = db_manager.obtener_estado(argumentos[1])
            volumen_final = volumen_del_mensaje(argumentos, 2)
            if estado == "REGANDO":
                #Leemos las estadísticas y el operario ANTES de cambiar el estado (cambiar_estado_disponible los borra)
                operario = db_manager.obtener_operario(argumentos[1])
                #Cerramos el riego: de aquí salen el volumen total y la duración del resumen
                riego = db_manager.cerrar_riego(argumentos[1], db_manager.MOTIVO_NORMAL, volumen_final)
                resumen = texto_resumen(riego) if riego else f"{db_manager.obtener_volumen(argumentos[1])} Litros consumidos"
                print(f"[FIN_RIEGO] la estación: {argumentos[1]} nos avisa de que acaba de regar. Resumen: {resumen}")
                db_manager.cambiar_estado_disponible(argumentos[1])
                if operario:
                    notificar_operario(f"INFO#{operario}#Riego finalizado con éxito. Resumen: {resumen}")
                comprobar_ack(conexion, "Fin de riego correctamente anotado", lista_pendientes)
            else:
                # recuperación tardía
                resumen_recuperado = None
                if volumen_final is not None:
                    resumen_recuperado = db_manager.corregir_volumen_ultimo_riego(argumentos[1], volumen_final)
                
                if resumen_recuperado:
                    res = texto_resumen(resumen_recuperado)
                    print(f"[FIN_RIEGO] la estación {argumentos[1]} informa del volumen final de su último riego (histórico corregido)")
                    
                    db_manager.cambiar_estado_disponible(argumentos[1])
                    
                    # Notificamos al operario que no recibió su aviso por la caída
                    if op := resumen_recuperado.get("operario"):
                        notificar_operario(f"INFO#{op}#Riego finalizado (tras recuperación de la Central). Resumen: {res}")
                else:
                    print(f"[FIN_RIEGO] la estación: {argumentos[1]} nos avisa de que acaba de regar, pero no está regando")
                comprobar_ack(conexion, "La estación no se encuentra regando en estos momentos", lista_pendientes)
 
        case _:
            print(f"[ERROR DE MENSAJE] el mensaje: {mensaje} enviado por: {direccion}, no sigue la convención especificada")
            comprobar_ack(conexion, f"[ERROR DE MENSAJE] el mensaje: {mensaje}, no sigue la convención especificada", lista_pendientes)
    #Ahora tratamos las posibles tramas olvidadas
    procesar_pendientes(lista_pendientes, direccion, conexion, buzon_salida)


def tratar_mensaje_operario(mensaje):
    #separamos la información, que viene marcada entre #
    argumentos = mensaje.split('#')
    #Guardamos el comando que nos ha llegado
    accion = argumentos[0]

    #Comprobamos que el mensaje tenga sentido
    if len(argumentos) < 2:
        print(f"[KAFKA ERROR] Mensaje no reconocible: {mensaje}")
        return
    if len(argumentos) < 3:
        notificar_operario(f"ERROR#{argumentos[1]}#El mensaje no tiene los argumentos suficientes")
        return

    match accion:
        case "PETICION_RIEGO":
            #primero comprobamos que el operario esté dado de alta
            if not db_manager.existe_operario(argumentos[1]):
                print(f"[PETICION DE RIEGO] El operario {argumentos[1]}, no está dado de alta, debes hacerlo antes de hacer peticiones")
                notificar_operario(f"ERROR#{argumentos[1]}#no está dado de alta, debes hacerlo antes de hacer peticiones")
            else:
                if argumentos[2] in coleccion_buzones:
                    buzon = coleccion_buzones[argumentos[2]]
                    buzon.put(argumentos)
                else:
                    notificar_operario(f"ERROR#{argumentos[1]}#La estación {argumentos[2]} no está online en estos momentos")

        case "PETICION_PARADA":
            #primero comprobamos que el operario esté dado de alta
            if not db_manager.existe_operario(argumentos[1]):
                print(f"[PETICION DE PARADA] El operario {argumentos[1]}, no está dado de alta, debes hacerlo antes de hacer peticiones")
                notificar_operario(f"ERROR#{argumentos[1]}#no está dado de alta, debes hacerlo antes de hacer peticiones")
            else:
                if argumentos[2] in coleccion_buzones:
                    buzon = coleccion_buzones[argumentos[2]]
                    buzon.put(argumentos)
                else:
                    notificar_operario(f"ERROR#{argumentos[1]}#La estación {argumentos[2]} no está online en estos momentos")

        case "REGISTRO_FO":
            db_manager.registrar_operario(argumentos[1], argumentos[2])
            print(f"[ALTA DE OPERARIO] El operario: {argumentos[1]} se ha dado de alta con el nombre: {argumentos[2]} ")
            notificar_operario(f"INFO#{argumentos[1]}#Se ha dado de alta con el nombre: {argumentos[2]} ")

        case "LISTAR_ESTACIONES":
            # 1. Comprobamos que el operario esté dado de alta
            if not db_manager.existe_operario(argumentos[1]):
                notificar_operario(f"ERROR#{argumentos[1]}#No estás dado de alta, hazlo antes de listar estaciones")
            else:
                # 2. Consultamos todas las estaciones registradas en la base de datos
                try:
                    estaciones = db_manager.listar_estaciones()
                    if not estaciones:
                        notificar_operario(f"INFO#{argumentos[1]}#No hay estaciones registradas en el sistema.")
                    else:
                        # 3. Formateamos la lista de estaciones para enviarla por Kafka
                        # Cada estación en la BD devuelve: (id_estacion, ubicacion, estado, operario, caudal, volumen)
                        lista_formateada = " / ".join([f"[{est[0]}] - Ubicación: {est[1]} - Estado: {est[2]}" for est in estaciones])
                        notificar_operario(f"INFO#{argumentos[1]}#Estaciones disponibles: {lista_formateada}")
                except Exception as e:
                    notificar_operario(f"ERROR#{argumentos[1]}#Error al recuperar las estaciones: {e}")
                    
        case _:
            print(f"[ERROR DE MENSAJE] el mensaje: {mensaje} enviado por: {argumentos[1]}, no sigue la convención especificada")
            notificar_operario(f"ERROR#{argumentos[1]}#El mensaje: {mensaje} enviado por: {argumentos[1]}, no sigue la convención especificada")


#función que vamos a usar para comprobar que el mensaje se ha enviado correctamente
def comprobar_ack(conexion, mensaje, pendientes):
    # convertimos el mensaje de string a byte
    paquete = common.protocol.empaquetar(mensaje)
    #guardamos el timeout original del socket para restaurarlo al terminar
    timeout_original = conexion.gettimeout()

    try:
        # vamos a dar 3 intentos de reenvio en caso de fallo
        for i in range(3):
            conexion.send(paquete)
            #cada intento tiene un plazo fijo: lleguen las tramas que lleguen, no esperamos más de PLAZO_ACK
            limite = time.monotonic() + PLAZO_ACK

            # Bucle interno: nos quedamos esperando el ACK hasta que se acabe el plazo.
            # Si llega otra cosa, la guardamos y seguimos esperando.
            while True:
                restante = limite - time.monotonic()
                if restante <= 0:
                    break  # Se acabó el plazo de este intento: el for reenvía
                conexion.settimeout(max(restante, 0.1))
                try:
                    respuesta = common.protocol.leer_trama(conexion)
                except socket.timeout:
                    break  # Si salta el timeout, salimos del while para que el for haga el reintento

                if respuesta == common.protocol.ACK:
                    return True

                if respuesta == common.protocol.NACK:
                    print("[ALERTA] trama rechazada (NACK), reintentando...")
                    break  # Salimos del while para que el for reintente

                if respuesta is None:
                    return False  # Se cortó la conexión abruptamente

                # Si no es ACK, ni NACK, ni None es una trama de datos
                # La guardamos en la lista y el bucle while vuelve a girar para seguir esperando nuestro ACK
                pendientes.append(respuesta)

        return False
    finally:
        conexion.settimeout(timeout_original)


#función usada para enviar mensajes por el productor de Kafka
def notificar_operario(mensaje):
    argumentos = mensaje.split('#')
    #comprobamos que el mensaje al menos tenga 2 argumentos
    if len(argumentos) < 2:
        print(f'[ERROR] El mensaje {mensaje} no tiene los argumentos suficientes')
        return
    #los avisos para CENTRAL no se publican en Kafka (ya se imprimen por consola en cada caso)
    if argumentos[1] == 'CENTRAL':
        return
    #enviamos el mensaje por el canal de respuestas
    try:
        productor_respuestas.send('respuesta_central', mensaje)
    except Exception as e:
        print(f'[ERROR] No se pudo enviar al operario el mensaje: {mensaje}. Error: {e}')


#########################################################################
# Funciones directas a la estación
#########################################################################

#Función que hace las comprobaciones necesarias para iniciar el riego
def iniciar_riego(id_estacion, conexion, id_operario=None):
    #lista donde guardamos posibles tramas perdidas al esperar un ACK
    lista_pendientes = []
    #Comprobamos que este dada de alta la estacion
    if not (estado := db_manager.obtener_estado(id_estacion)):
        print(f"[PETICION DE RIEGO] La estación {id_estacion}, no está dada de alta, debes hacerlo antes de hacer peticiones")
        if id_operario:
            notificar_operario(f"ERROR#{id_operario}#Estación no dada de alta ")
    else:
        #si la estación se encuentra disponible le mandamos la petición
        if estado == "DISPONIBLE":
            if comprobar_ack(conexion, f"INICIAR_RIEGO#{id_estacion}", lista_pendientes):
                #primero lo que cambia la BD (SOLICITADA y operario), después el aviso por Kafka
                db_manager.asignar_operario(id_estacion, id_operario)
                print(f"[PETICION DE RIEGO] Se ha enviado la petición a la estación {id_estacion}")
                if id_operario:
                    notificar_operario(f"INFO#{id_operario}#Petición enviada, esperando respuesta...")
            else:
                #la estación no acusa la orden: la damos por inalcanzable (el finally del hilo la marca DESCONECTADA)
                print(f"[ERROR DE CONEXIÓN] La estación {id_estacion}, está fuera de conexión, inténtalo más tarde")
                if id_operario:
                    notificar_operario(f"ERROR#{id_operario}#Estación fuera de conexión")
                raise EstacionInalcanzable(f"La estación {id_estacion} no acusó INICIAR_RIEGO")
        else:
            print(f"[PETICION DE RIEGO] La estación {id_estacion}, no se encuentra disponible en estos momentos")
            if id_operario:
                notificar_operario(f"ERROR#{id_operario}#Estación no disponible actualmente ")
    return lista_pendientes


#Función que detiene el riego, detecta si se pide porque hay una fuga o simplemente porque se desea detener el riego
def fin_riego(id_estacion, conexion, id_operario=None):
    #lista donde guardamos posibles tramas perdidas al esperar un ACK
    lista_pendientes = []
    #Comprobamos que este dada de alta la estacion
    if not (estado := db_manager.obtener_estado(id_estacion)):
        print(f"[PETICION DE PARADA] La estación {id_estacion}, no está dada de alta, debes hacerlo antes de hacer peticiones")
        if id_operario:
            notificar_operario(f"ERROR#{id_operario}#Estación no dada de alta ")

    else:
        if estado == "REGANDO":
            if comprobar_ack(conexion, f"FIN_RIEGO#{id_estacion}", lista_pendientes):
                print(f"[PETICION DE PARADA] Se ha enviado la petición de parada a la estación {id_estacion}")
                if id_operario:
                    notificar_operario(f"INFO#{id_operario}#Petición enviada, esperando respuesta...")
            else:
                #la estación no acusa la orden: la damos por inalcanzable
                print(f"[ERROR DE CONEXIÓN] La estación {id_estacion}, está fuera de conexión, inténtalo más tarde")
                #al salir, el finally avisará al operario que estaba regando; solo avisamos aquí a quien pidió la parada si es otro
                duenio = db_manager.obtener_operario(id_estacion)
                if id_operario and id_operario != duenio:
                    notificar_operario(f"ERROR#{id_operario}#Estación fuera de conexión")
                raise EstacionInalcanzable(f"La estación {id_estacion} no acusó FIN_RIEGO")
        elif estado == "FUGA":
            print(f"[PETICIÓN DE PARADA] La estación {id_estacion} ya se encuentra bloqueada por FUGA")
            if id_operario:
                notificar_operario(f"ERROR#{id_operario}#La estación está bloqueada por protocolo de fuga")
        else:
            print(f"[PETICIÓN DE PARADA] La estación {id_estacion}, No se encuentra regando")
            if id_operario:
                notificar_operario(f"ERROR#{id_operario}#Estación no está regando actualmente")
    return lista_pendientes


def bloquear(id_estacion, conexion):
    pendientes = []
    estado = db_manager.obtener_estado(id_estacion)
    # Solo tiene sentido bloquear una estación operativa
    if estado not in ("DISPONIBLE", "REGANDO"):
        print(f"[BLOQUEO] {id_estacion} está en {estado}, no se puede bloquear")
        return pendientes

    # Leemos ANTES de bloquear: bloquear_estacion borra el operario
    operario = db_manager.obtener_operario(id_estacion)

    if comprobar_ack(conexion, f"BLOQUEAR#{id_estacion}", pendientes):
        db_manager.bloquear_estacion(id_estacion)
        #cerramos el riego (si lo había) guardando lo regado hasta el bloqueo
        riego = db_manager.cerrar_riego(id_estacion, db_manager.MOTIVO_BLOQUEO)
        print(f"[BLOQUEO] La estación {id_estacion} está fuera de servicio")
        if estado == "REGANDO" and operario:
            detalle = f" Regado: {texto_resumen(riego)}." if riego else ""
            notificar_operario(f"ERROR#{operario}#Riego interrumpido por CENTRAL (estación fuera de servicio).{detalle}")
 
    else:
        print(f"[ERROR DE CONEXIÓN] La estación {id_estacion} no responde")
        #el finally del hilo la marcará DESCONECTADA y avisará al operario si estaba regando
        raise EstacionInalcanzable(f"La estación {id_estacion} no acusó BLOQUEAR")
    return pendientes


def activar(id_estacion, conexion):
    pendientes = []
    # Solo se activa una estación bloqueada
    if db_manager.obtener_estado(id_estacion) != "PARADA":
        print(f"[ACTIVAR] La estación {id_estacion} no está bloqueada")
        return pendientes

    if comprobar_ack(conexion, f"ACTIVAR#{id_estacion}", pendientes):
        db_manager.cambiar_estado_disponible(id_estacion)
        print(f"[ACTIVAR] La estación {id_estacion} vuelve a estar disponible")
    else:
        print(f"[ERROR DE CONEXIÓN] La estación {id_estacion} no responde")
        raise EstacionInalcanzable(f"La estación {id_estacion} no acusó ACTIVAR")
    return pendientes


#########################################################################
# Menú interactivo de la Central
#########################################################################

#Orden de menú -> acción que entiende despachar_orden
ACCIONES_MENU = {
    "2": "PETICION_RIEGO",
    "3": "PETICION_PARADA",
    "4": "BLOQUEAR",
    "5": "ACTIVAR",
}


def orden_menu(accion, id_estacion):
    #'*' significa "todas las estaciones conectadas"
    if id_estacion == "*":
        #list para que los hilos de las estaciones pueden añadir o quitar buzones mientras recorremos sin petar
        destinos = list(coleccion_buzones.items())
        if not destinos:
            print(f"{AMARILLO}[AVISO]{RESET} No hay estaciones conectadas.")
            return
        for id, buzon in destinos:
            buzon.put((accion, "CENTRAL", id))
        print(f"{VERDE}[OK]{RESET} Orden {accion} enviada a {len(destinos)} estación(es) conectada(s)")
    else:
        buzon = coleccion_buzones.get(id_estacion)
        if buzon is None:
            print(f"{ROJO}[ERROR]{RESET} La estación {id_estacion} no está conectada.")
            return
        buzon.put((accion, "CENTRAL", id_estacion))
        print(f"{VERDE}[OK]{RESET} Orden {accion} enviada a {id_estacion}")

def menu_central():
    # Pausa inicial para no solaparse con los mensajes de arranque del servidor
    time.sleep(1.5)

    while True:
        print("\n========================================")
        print("         PANEL DE CONTROL CENTRAL       ")
        print("========================================")
        print("1. Panel de estaciones en vivo")
        print("2. Iniciar riego       (ID de estación, o * para todas)")
        print("3. Detener riego       (ID de estación, o * para todas)")
        print("4. Bloquear estación   (ID de estación, o * para todas)")
        print("5. Activar estación    (ID de estación, o * para todas)")
        print("========================================")

        opcion = input("Selecciona una opción:\n").strip()

        match opcion:
            case "1":
                panel.mostrar_panel()
                
            case "2" | "3" | "4" | "5":
                id_estacion = input("ID de la estación (* = todas las conectadas): ").strip()
                orden_menu(ACCIONES_MENU[opcion], id_estacion)

            case _:
                print(f"{ROJO}[ERROR]{RESET} Opción no válida. Inténtalo de nuevo.")


if __name__ == "__main__":
    WM_Central()