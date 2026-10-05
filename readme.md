### Catálogo de Mensajes de Sockets

| Comando base | Sentido de red | Formato exacto (Payload) | Propósito y reacción esperada |
| :--- | :--- | :--- | :--- |
| **REGISTRO** | Monitor -> Central | `REGISTRO#<ID_WS>#<Ubicación>` | El Monitor de la estación solicita unirse a la red al arrancar. Central verifica y autoriza. |
| **ESTADO** | Monitor -> Central | `ESTADO#<ID_WS>#<OK\|FUGA>` | Latido de salud enviado cada segundo. Si envía `FUGA`, Central bloquea la estación y cambia la vista a ROJO. |
| **INICIAR_RIEGO**| Central -> Engine | `INICIAR_RIEGO#<ID_WS>` | La Central ordena a la estación que abra la electroválvula tras validar que está disponible. |
| **METRICAS** | Engine -> Central | `METRICAS#<ID_WS>#<Caudal>#<Volumen>` | El Engine lo envía cada segundo durante el riego para que Central muestre los litros/minuto y el volumen acumulado. |
| **FIN_RIEGO** | Engine -> Central | `FIN_RIEGO#<ID_WS>` | El Engine notifica que el riego ha terminado, ya sea por alcanzar el límite de tiempo o por parada manual. |

## Protocolo de Comunicación: Operarios (WM_FO) -> Central (WM_Central)

La comunicación desde la aplicación de los operarios hacia la Central se realiza de forma asíncrona mediante Apache Kafka, publicando eventos en un *topic* dedicado (ej. `peticiones_operarios`). 

Los mensajes viajan en formato texto y los parámetros se separan mediante el carácter `#`. La estructura general es: `COMANDO#ID_OPERARIO#PARAMETROS`.

### Mensajes Soportados

| Comando | Formato | Ejemplo | Descripción |
|---|---|---|---|
| **PETICION_RIEGO** | `PETICION_RIEGO#<ID_OPERARIO>#<ID_ESTACION>` | `PETICION_RIEGO#FO-12#WS-04` | El operario solicita abrir el agua en una estación concreta. |
| **PETICION_PARADA** | `PETICION_PARADA#<ID_OPERARIO>#<ID_ESTACION>` | `PETICION_PARADA#FO-12#WS-04` | El operario solicita detener el riego o bloquear una estación. |
| **REGISTRO_FO** | `REGISTRO_FO#<ID_OPERARIO>#<UBICACION>` | `REGISTRO_FO#FO-12#River Park` | (Opcional) El operario notifica a la Central a qué parque acaba de llegar. |