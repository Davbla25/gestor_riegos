#chmod +x lanzar_operarios.sh
# ./lanzar_operarios.sh

#!/bin/bash
BROKER_IP="44.221.249.166:9092"
NUM_OPERARIOS=3

echo "Abriendo terminales para $NUM_OPERARIOS operarios..."

for i in $(seq 1 $NUM_OPERARIOS); do
    ID="FO-$i"
    gnome-terminal --title="Operario $ID" -- docker run -it --rm --name fo-$i \
        --network red-ws01 wm_fo --id $ID --broker $BROKER_IP
done