#########################################################################
# Variables de control
#########################################################################

ENQ = b'\x05' #petición conexión
ACK = b'\x06' #Acuse de recibido positivo
NACK = b'\x15' #Acuse de recibido negativo
STX = b'\x02' #Inicio trama de datos
ETX = b'\x03' #Fin trama de datos
EOT = b'\x04' #Cierre de la comunicación

########################################################################


#########################################################################
# Tratamiento de mensajes
#########################################################################

def calcular_lrc(datos_bytes): #función que calcula la integridad del mensaje
    lrc = 0
    for byte in datos_bytes: #aplicamos XOR a cada byte. El resultado debe ser el mismo en el origen
        lrc ^= byte
    return bytes([lrc])


def empaquetar(mensaje_string): #recibe una cadena y la transforma al formato estándar
    mensaje = mensaje_string.encode('utf-8') #transformamos en bytes
    lrc = calcular_lrc(mensaje) #calculo LRC
    return STX+mensaje+ETX+lrc #concateno hasta tener el formato deseado


def desempaquetar(trama_bytes): #recibe una trama y la decodifica, comprobando la integridad del mensaje
        if len(trama_bytes) < 4: #Compruebo que la trama tiene al menos las 4 componentes básicas de mensaje, si no error
             raise IndexError("La trama es demasiado pequeña")
        if not (trama_bytes[0:1] == STX and trama_bytes[-2:-1] == ETX): #comprobamos que la trama cumpla el formato deseado
             raise ValueError("La tama no tiene el formato estandar <STX><DATA><ETX><LRC>")
        lrc = calcular_lrc(trama_bytes[1:-2])
        if not lrc == trama_bytes[-1:]: #si el LRC no coincide se ha corrompido el mensaje por el camino
             raise ValueError("Mensaje dañado, el LRC no coincide")
        return trama_bytes[1:-2].decode('utf-8') #devolcemos un string con el mensajes