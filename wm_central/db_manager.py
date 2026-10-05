import sqlite3

ruta_bd="water_management-db"

def iniciar_bd():
    #Hacemos conexión con la base de datos, si no existe la creamos
    conexion = sqlite3.connect(ruta_bd)
    cursor = conexion.cursor()

    #Creamos la tabla de las estaciones, donde se guarda su id, ubicación y estado
    cursor.execute('''CREATE TABLE IF NOT EXISTS estaciones
    (id TEXT PRIMARY KEY ,
    ubicacion TEXT,
    estado TEXT CHECK( estado IN ('DISPONIBLE', 'FUGA', 'PARADA', 'REGANDO', 'DESCONECTADA')))''' )
    #El estado solo puede estar en determinados valores indicados en la documentación

    cursor.execute('''CREATE TABLE IF NOT EXISTS operarios
        (id TEXT PRIMARY KEY ,
        nombre TEXT )''' )
    #al iniciar la central, debe mostrar todas las estaciones registradas. El estado no lo conoce, por los borramos
    cursor.execute("UPDATE estaciones SET  estado = 'DESCONECTADA' ")
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