import socket
import time 
import common.protocol
import os
import threading
from kafka import KafkaProducer, KafkaConsumer

# ---------------------------------------------------------
# VARIABLES DE ENTORNO Y CONFIGURACIÓN
# ---------------------------------------------------------
ID_OPERARIO = os.environ.get("FO_ID","FO_DEFAULT")
UBICACION = os.environ.get("STATION_LOCATION", "Zona Centro")
KAFKA_SERVER = os.environ.get("KAFKA_SERVER", "localhost:9092")

# Definimos los "tablones" (topics) que vamos a usar
TOPIC_PETICIONES = "peticiones_operarios"
TOPIC_RESPUESTAS = "respuesta_central"

print(f"--- INICIANDO APLICACIÓN DE OPERARIO ---")
print(f"ID: {ID_OPERARIO} | Zona: {UBICACION} | Servidor Kafka: {KAFKA_SERVER}")
print("-" * 40)

def WM_FO():

    productor = KafkaProducer(
        bootstrap_servers = [KAFKA_SERVER],
        value_serializer = lambda v : v.encode('utf-8')
    )

    productor.send(TOPIC_PETICIONES,f"REGISTRO_FO#{ID_OPERARIO}#{UBICACION}")

    while True:

        print("--------OPCIONES--------")
        print("\n 1.INICIAR RIEGO \n 2.PARAR RIEGO \n 3.SALIR")

        opcion = input("¿QUÉ QUIERES HACER?")
        if opcion == '1':
            destino = input("¿DÓNDE?")
            productor.send(TOPIC_PETICIONES,f"PETICION_RIEGO#{ID_OPERARIO}#{destino}")
        elif opcion == '2':
            destino = input("¿DÓNDE?")
            productor.send(TOPIC_PETICIONES,f"PETICION_PARADA#{ID_OPERARIO}#{destino}")
        elif opcion == '3':
            break
        else:
            pass
           

    productor.flush()
    productor.close()

def escuchar_mensajes():
    consumidor = KafkaConsumer(
        TOPIC_RESPUESTAS,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset='latest',
        value_deserializer=lambda x: x.decode('utf-8')
    )

    for mensaje in consumidor:
        texto_recibido = mensaje.value
        argumentos = texto_recibido.split('#')
        
        tipo = argumentos[0]
        destinatario = argumentos[1]
        texto_libre = argumentos[2]

        #Tipos de mensajes que recibe el consumidor
        if destinatario == ID_OPERARIO:
            if tipo == "ERROR":
                print(f"\n[ERROR CENTRAL] {texto_libre}")
            elif tipo == "INFO":
                print(f"\n[INFO CENTRAL] {texto_libre}")
        elif destinatario == "ALL":
            print(f"\n[ALERTA GLOBAL] {texto_libre}")
        else:
            pass



if __name__ == "__main__":
    # 1. Arrancamos el "oído" en un hilo en segundo plano
    # OJO: Se pasa el nombre de la función SIN paréntesis en 'target'
    hilo = threading.Thread(target=escuchar_mensajes)
    hilo.daemon = True # Esto hace que el hilo muera automáticamente si cerramos el menú
    hilo.start()
    
    # 2. Arrancamos la "boca" (el menú interactivo) en el hilo principal
    WM_FO()
