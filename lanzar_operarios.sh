#chmod +x lanzar_operarios.sh
# ./lanzar_operarios.sh

#!/bin/bash

echo "🔨 1. Construyendo imagen Docker local para el Operario..."
# Construye únicamente la imagen del FO para ganar tiempo
docker build -t wm_fo -f wm_fo/Dockerfile . || { echo "Fallo al construir Operario (FO)"; exit 1; }

echo "🌐 2. Configurando red de Docker..."
docker network create red-ws01 2>/dev/null || true

# Configura la IP y puerto de tu broker de Kafka
KAFKA_BROKER="44.221.249.166:9092"
NUM_OPERARIOS=2 

echo "🧹 3. Limpiando contenedores de operarios anteriores..."
# Elimina todos los contenedores que contengan "-fo" en el nombre
docker rm -f $(docker ps -aq --filter name="-fo") 2>/dev/null || true

echo "🚀 4. Abriendo ventanas para $NUM_OPERARIOS operarios..."

for i in $(seq 1 $NUM_OPERARIOS); do
    # Genera IDs con el formato OP_01, OP_02...
    ID=$(printf "OP_%02d" $i)
    
    # Lanza el Operario (FO) y pausa la ventana si hay error o al salir
    gnome-terminal --title="Operario $ID" -- bash -c "docker run -it --rm --name ${ID}-fo \
        --network red-ws01 wm_fo --id $ID --nombre 'Operario $i' \
        --broker $KAFKA_BROKER; echo '[!] Proceso finalizado.'; read -p 'Pulsa ENTER para salir'"
    
    # Pequeña pausa para no saturar el gestor de ventanas
    sleep 0.5 
done

echo "✅ Despliegue de operarios completado."