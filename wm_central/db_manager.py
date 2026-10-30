import sqlite3
import time
ruta_bd="water_management-db"

MOTIVO_NORMAL = "NORMAL"                #fin de riego pedido desde la estación, un operario o CENTRAL
MOTIVO_FUGA = "FUGA"                    #la estación ha detectado una fuga
MOTIVO_BLOQUEO = "BLOQUEO"              #CENTRAL ha puesto la estación fuera de servicio
MOTIVO_DESCONEXION = "DESCONEXION"      #se ha perdido la conexión con la estación
MOTIVO_REINICIO = "REINICIO_CENTRAL"    #la central se reinició con el riego abierto
MOTIVO_INTERRUMPIDO = "INTERRUMPIDO"    #la estación se registró de nuevo (o se abrió otro riego) con este abierto

def iniciar_bd():
    #Hacemos conexión con la base de datos, si no existe la creamos
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()

    #Creamos la tabla de las estaciones, donde se guarda su id, ubicación y estado
    cursor.execute('''CREATE TABLE IF NOT EXISTS estaciones
    (id TEXT PRIMARY KEY ,
    ubicacion TEXT,
    operario_actual TEXT,
    caudal TEXT,
    volumen REAL,
    estado TEXT CHECK( estado IN ('DISPONIBLE', 'SOLICITADA', 'FUGA', 'PARADA', 'REGANDO', 'DESCONECTADA')))''' )
    #El estado solo puede estar en determinados valores indicados en la documentación

    cursor.execute('''CREATE TABLE IF NOT EXISTS operarios
        (id TEXT PRIMARY KEY ,
        nombre TEXT )''' )

    #Registro permanente de cada riego. Un riego está ABIERTO mientras su columna fin está vacía (NULL).
    #inicio y fin son horas (time.time()), así que la duración es fin - inicio.
    #actualizado guarda la hora de la última métrica: si la central cae con un riego abierto, es la mejor hora de fin que conocemos
    cursor.execute('''CREATE TABLE IF NOT EXISTS riegos
        (id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_estacion TEXT NOT NULL,
        operario TEXT,
        inicio REAL NOT NULL,
        actualizado REAL NOT NULL,
        fin REAL,
        volumen REAL DEFAULT 0,
        motivo_fin TEXT)''' )

     #si la central se paró con riegos abiertos, los cerramos con la hora de su última actualización
    cursor.execute("UPDATE riegos SET fin = actualizado, motivo_fin = ? WHERE fin IS NULL", (MOTIVO_REINICIO,))
    huerfanos = cursor.rowcount
 
    #al iniciar la central, debe mostrar todas las estaciones registradas. El estado no lo conoce, por los borramos
    cursor.execute("UPDATE estaciones SET  estado = 'DESCONECTADA', operario_actual = NULL ")
    #guardamos la lista de estaciones
    cursor.execute('''SELECT id, ubicacion FROM estaciones ''')
    estaciones = cursor.fetchall()
    #cerramos conexión
    cursor.close()
    conexion.commit()
    conexion.close()
    #devolvemos la lista de las estaciones
    return estaciones


def registrar_estacion(id_estacion, ubicacion):
    #Hacemos conexión con la base de datos, si no existe la creamos
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()
    #insertamos la estación, si existe simplemente modificamos los datos
    cursor.execute("INSERT OR REPLACE INTO  estaciones (id, ubicacion, estado) VALUES (?, ?, ?)",
                   (id_estacion, ubicacion, 'DISPONIBLE'))
    #cerramos conexión
    cursor.close()
    conexion.commit()
    conexion.close()
    
def listar_estaciones():
    # Consulta de solo lectura para el panel de control de la Central
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()
    cursor.execute("SELECT id, ubicacion, estado, operario_actual, caudal, volumen FROM estaciones")
    filas = cursor.fetchall()
    cursor.close()
    conexion.close()
    return filas
########################################################################
# FUNCIONES DE MÉTRICAS
#########################################################################
def actualizar_metricas(id_estacion, caudal, volumen):
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()
    cursor.execute("UPDATE estaciones SET  caudal = ?, volumen = ? WHERE id = ?", (caudal, volumen, id_estacion))
    cursor.close()
    conexion.commit()
    conexion.close()

def obtener_volumen(id_estacion):
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()
    cursor.execute("SELECT volumen FROM estaciones WHERE id = ?", (id_estacion,))
    resultado = cursor.fetchone()
    cursor.close()
    conexion.close()
    # Devolvemos el volumen si existe, o 0 por defecto
    return resultado[0] if resultado and resultado[0] else 0
#########################################################################
# FUNCIONES DE RIEGOS (registro permanente)
#########################################################################
def abrir_riego(id_estacion, operario):
    #Registra el inicio de un riego. Una estación solo puede tener un riego abierto a la vez
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()
    ahora = time.time()
    #red de seguridad: si por cualquier motivo quedó un riego abierto, lo cerramos antes de abrir el nuevo
    cursor.execute("UPDATE riegos SET fin = actualizado, motivo_fin = ? WHERE id_estacion = ? AND fin IS NULL",
                   (MOTIVO_INTERRUMPIDO, id_estacion))
    cursor.execute("INSERT INTO riegos (id_estacion, operario, inicio, actualizado, volumen) VALUES (?, ?, ?, ?, 0)",
                   (id_estacion, operario, ahora, ahora))
    cursor.close()
    conexion.commit()
    conexion.close()
 
def actualizar_volumen_riego(id_estacion, volumen):
    #La estación envía el volumen ACUMULADO, así que se sustituye, no se suma
    try:
        volumen = float(volumen)
    except (TypeError, ValueError):
        return
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()
    cursor.execute("UPDATE riegos SET volumen = ?, actualizado = ? WHERE id_estacion = ? AND fin IS NULL",
                   (volumen, time.time(), id_estacion))
    cursor.close()
    conexion.commit()
    conexion.close()
 
def cerrar_riego(id_estacion, motivo, volumen_final=None):
    #Cierra el riego abierto de la estación. Devuelve un diccionario con el resumen, o None si no había ninguno abierto
    #(así se puede llamar desde varios sitios sin miedo a cerrar dos veces)
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()
    cursor.execute("SELECT id, operario, inicio, volumen FROM riegos WHERE id_estacion = ? AND fin IS NULL", (id_estacion,))
    fila = cursor.fetchone()
    if fila is None:
        cursor.close()
        conexion.close()
        return None
    id_riego, operario, inicio, volumen = fila
    #si la estación nos da el volumen final, manda sobre la última métrica
    if volumen_final is not None:
        volumen = volumen_final
    fin = time.time()
    #el AND fin IS NULL evita cerrar dos veces el mismo riego si dos hilos coinciden
    cursor.execute("UPDATE riegos SET fin = ?, volumen = ?, motivo_fin = ? WHERE id = ? AND fin IS NULL",
                   (fin, volumen, motivo, id_riego))
    cerrado = cursor.rowcount == 1
    cursor.close()
    conexion.commit()
    conexion.close()
    if not cerrado:
        return None
    return {"operario": operario, "volumen": volumen or 0, "duracion": fin - inicio, "motivo": motivo}
 
def corregir_volumen_ultimo_riego(id_estacion, volumen):
    #Para el FIN_RIEGO tardío de una estación que se recupera: si el volumen que informa es mayor que el que
    #teníamos guardado en su último riego (ya cerrado), lo corregimos. Devuelve True si se ha actualizado
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()
    cursor.execute("SELECT id, volumen FROM riegos WHERE id_estacion = ? ORDER BY id DESC LIMIT 1", (id_estacion,))
    fila = cursor.fetchone()
    corregido = False
    if fila is not None and volumen > (fila[1] or 0):
        cursor.execute("UPDATE riegos SET volumen = ? WHERE id = ?", (volumen, fila[0]))
        corregido = True
    cursor.close()
    conexion.commit()
    conexion.close()
    return corregido
 

#########################################################################
# FUNCIONES DE OPERARIO
#########################################################################
def registrar_operario(id_operario, ubicacion):
     #Hacemos conexión con la base de datos, si no existe la creamos
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()
    #insertamos el operario, si existe simplemente modificamos los datos
    cursor.execute("INSERT OR REPLACE INTO  operarios (id, nombre) VALUES (?, ?)",
                   (id_operario, ubicacion))
    #cerramos conexión
    cursor.close()
    conexion.commit()
    conexion.close()   

def existe_operario(id_operario):
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()
    cursor.execute("SELECT id FROM operarios WHERE id=  ?", (id_operario,))
    resultado = cursor.fetchone()
    cursor.close()
    conexion.close()
    return resultado is not None

def asignar_operario(id_estacion, id_operario):
    #Guarda el operario en la BD cuando mandamos la petición de riego
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()
    cursor.execute("UPDATE estaciones SET operario_actual = ?, caudal = 0, volumen = 0, estado = 'SOLICITADA' WHERE id = ?", (id_operario, id_estacion))
    conexion.commit()
    conexion.close()

def obtener_operario(id_estacion):
    #Lee quién es el operario actual de una estación para poder notificarle por Kafka
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()
    cursor.execute("SELECT operario_actual FROM estaciones WHERE id = ?", (id_estacion,))
    resultado = cursor.fetchone()
    conexion.close()
    
    # Devolvemos el nombre si existe, o None si la columna estaba vacía/NULL
    return resultado[0] if resultado and resultado[0] else None

#########################################################################
# FUNCIONES DE CAMBIO DE ESTADO
#########################################################################
def obtener_estado(id_estacion):
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()
    cursor.execute("SELECT estado FROM estaciones WHERE id = ?", (id_estacion,))
    resultado = cursor.fetchone()
    cursor.close()
    conexion.close()
    if resultado:
        return resultado[0]
    else:
        return None

def bloquear_estacion(id_estacion):
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()
    cursor.execute("UPDATE estaciones SET  estado = 'PARADA', operario_actual = NULL WHERE id = ?", (id_estacion,))
    cursor.close()
    conexion.commit()
    conexion.close()

def cambiar_estado_riego(id_estacion):
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()
    cursor.execute("UPDATE estaciones SET estado = 'REGANDO' WHERE id = ?", (id_estacion, ))
    cursor.close()
    conexion.commit()
    conexion.close()

def cambiar_estado_desconectada(id_estacion):
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()
    cursor.execute("UPDATE estaciones SET  estado = 'DESCONECTADA', operario_actual = NULL, caudal = 0, volumen = 0 WHERE id = ?", (id_estacion,))
    cursor.close()
    conexion.commit()
    conexion.close()

def cambiar_estado_disponible(id_estacion):
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()
    cursor.execute("UPDATE estaciones SET  estado = 'DISPONIBLE', caudal = 0, volumen = 0, operario_actual = NULL WHERE id = ?", (id_estacion,))
    cursor.close()
    conexion.commit()
    conexion.close()

def cambiar_estado_fuga(id_estacion):
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()
    cursor.execute("UPDATE estaciones SET  estado = 'FUGA', operario_actual = NULL WHERE id = ?", (id_estacion,))
    cursor.close()
    conexion.commit()
    conexion.close()