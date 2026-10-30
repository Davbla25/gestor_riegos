#chmod +x lanzar_estaciones.sh
# ./lanzar_estaciones.sh
#!/bin/bash

docker network create red-ws01 2>/dev/null || true
CENTRAL_IP_PORT="44.221.249.166:8080"

# OJO: Si pones 20, se abrirán 40 ventanas de terminal de golpe. 
# Te recomiendo probar con 3 o 4 para la defensa.
NUM_ESTACIONES=3 

echo "🚀 Abriendo ventanas para $NUM_ESTACIONES estaciones..."

for i in $(seq 1 $NUM_ESTACIONES); do
    ID=$(printf "WS_%02d" $i)
    PUERTO=$((5000 + i))
    
    # Lanza el Monitor en una nueva ventana de terminal (-it en lugar de -d)
    gnome-terminal --title="Monitor $ID" -- docker run -it --rm --name ${ID}-monitor \
        --network red-ws01 wm_ws_m --id $ID --ubicacion "Parque $i" \
        --central $CENTRAL_IP_PORT --puerto-local $PUERTO
    
    sleep 0.5 

    # Lanza el Engine en otra nueva ventana de terminal
    gnome-terminal --title="Engine $ID" -- docker run -it --rm --name ${ID}-engine \
        --network red-ws01 wm_ws_e --id $ID --monitor ${ID}-monitor:${PUERTO}
done

echo "✅ Terminales lanzadas. Revisa tus ventanas."