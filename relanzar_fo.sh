#!/bin/bash
# Uso: ./relanzar_fo.sh <ID_OPERARIO> [fichero_servicios]
#   ./relanzar_fo.sh FO_01
#   ./relanzar_fo.sh FO_02 servicios.txt

ID=$1
FICHERO=$2
[ -z "$ID" ] && { echo "Uso: $0 <ID_OPERARIO> [fichero]"; exit 1; }

# Pon aquí la dirección PÚBLICA del Kafka de Railway (host:puerto)
BROKER="TU_HOST_KAFKA:PUERTO"

docker rm -f ${ID}-fo 2>/dev/null

if [ -n "$FICHERO" ]; then
    # Monta el fichero dentro del contenedor (solo lectura)
    ARGS_FICHERO="-v $(realpath $FICHERO):/app/servicios.txt:ro"
    OPC_FICHERO="--fichero /app/servicios.txt"
fi

gnome-terminal --title="Operario $ID" -- bash -c "docker run -it --rm --name ${ID}-fo \
    $ARGS_FICHERO wm_fo --broker $BROKER --id $ID --nombre '$ID' $OPC_FICHERO; \
    echo '[!] Proceso finalizado.'; read -p 'Pulsa ENTER para salir'"