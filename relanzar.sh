#!/bin/bash
# Uso: ./relanzar.sh <numero> <m|e|ambos>
#   ./relanzar.sh 2 m       -> relanza solo el Monitor de WS_02
#   ./relanzar.sh 2 e       -> relanza solo el Engine de WS_02
#   ./relanzar.sh 2 ambos   -> relanza la estación completa

N=$1
PARTE=${2:-ambos}
[ -z "$N" ] && { echo "Uso: $0 <numero> <m|e|ambos>"; exit 1; }

ID=$(printf "WS_%02d" "$((10#$N))")
PUERTO=$((5000 + 10#$N))
CENTRAL_IP_PORT="44.221.249.166:8080"

docker network create red-ws01 2>/dev/null || true

lanzar_monitor() {
    docker rm -f ${ID}-monitor 2>/dev/null
    gnome-terminal --title="Monitor $ID" -- bash -c "docker run -it --rm --name ${ID}-monitor \
        --network red-ws01 wm_ws_m --id $ID --ubicacion 'Lab $((10#$N))' \
        --central $CENTRAL_IP_PORT --puerto-local $PUERTO; echo '[!] Proceso finalizado.'; read -p 'Pulsa ENTER para salir'"
}

lanzar_engine() {
    docker rm -f ${ID}-engine 2>/dev/null
    gnome-terminal --title="Engine $ID" -- bash -c "docker run -it --rm --name ${ID}-engine \
        --network red-ws01 wm_ws_e --id $ID --monitor ${ID}-monitor:${PUERTO}; echo '[!] Proceso finalizado.'; read -p 'Pulsa ENTER para salir'"
}

case "$PARTE" in
    m)    lanzar_monitor ;;
    e)    lanzar_engine ;;
    ambos) lanzar_monitor; sleep 1; lanzar_engine ;;
    *)    echo "Parte no válida: usa m, e o ambos"; exit 1 ;;
esac