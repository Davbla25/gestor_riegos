#chmod +x lanzar_estaciones.sh
# ./lanzar_estaciones.sh
#!/bin/bash

echo "🔨 1. Construyendo imágenes Docker locales..."
# Si el ordenador del aula exige permisos de administrador para Docker, 
# tendrás que ejecutar este script con: sudo ./lanzar_aula.sh

docker build -t wm_ws_m -f wm_ws/ws_m/Dockerfile . || { echo "Fallo al construir Monitor"; exit 1; }
docker build -t wm_ws_e -f wm_ws/ws_e/Dockerfile . || { echo "Fallo al construir Engine"; exit 1; }
docker build -t wm_fo -f wm_fo/Dockerfile . || { echo "Fallo al construir Operario (FO)"; exit 1; }

echo "🌐 2. Configurando red de Docker..."
docker network create red-ws01 2>/dev/null || true

CENTRAL_IP_PORT="44.221.249.166:8080"
NUM_ESTACIONES=3 

echo "🚀 3. Abriendo ventanas para $NUM_ESTACIONES estaciones..."

echo "🧹 Limpiando contenedores anteriores..."
# Elimina todos los contenedores que contengan "monitor" o "engine" en el nombre
docker rm -f $(docker ps -aq --filter name="-monitor") 2>/dev/null || true
docker rm -f $(docker ps -aq --filter name="-engine") 2>/dev/null || true

for i in $(seq 1 $NUM_ESTACIONES); do
    ID=$(printf "WS_%02d" $i)
    PUERTO=$((5000 + i))
    
    # Lanza el Monitor y pausa la ventana si hay error
    gnome-terminal --title="Monitor $ID" -- bash -c "docker run -it --rm --name ${ID}-monitor \
        --network red-ws01 wm_ws_m --id $ID --ubicacion 'Lab $i' \
        --central $CENTRAL_IP_PORT --puerto-local $PUERTO; echo '[!] Proceso finalizado.'; read -p 'Pulsa ENTER para salir'"
    
    sleep 0.5 

    # Lanza el Engine y pausa la ventana si hay error
    gnome-terminal --title="Engine $ID" -- bash -c "docker run -it --rm --name ${ID}-engine \
        --network red-ws01 wm_ws_e --id $ID --monitor ${ID}-monitor:${PUERTO}; echo '[!] Proceso finalizado.'; read -p 'Pulsa ENTER para salir'"
done

echo "✅ Despliegue completado."
