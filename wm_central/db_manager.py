import sqlite3

def iniciar_bd(ruta_bd="water_management-db"):
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
    cursor.close()
    conexion.commit()
    conexion.close()
