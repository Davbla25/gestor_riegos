### Catálogo de Mensajes de Sockets

| Comando base | Sentido de red | Formato exacto (Payload) | Propósito y reacción esperada |
| :--- | :--- | :--- | :--- |
| **REGISTRO** | Monitor -> Central | `REGISTRO#<ID_WS>#<Ubicación>` | El Monitor de la estación solicita unirse a la red al arrancar. Central verifica y autoriza. |
| **ESTADO** | Monitor -> Central | `ESTADO#<ID_WS>#<OK\|FUGA>` | Latido de salud enviado cada segundo. Si envía `FUGA`, Central bloquea la estación y cambia la vista a ROJO. |
| **INICIAR_RIEGO**| Central -> Engine | `INICIAR_RIEGO#<ID_WS>` | La Central ordena a la estación que abra la electroválvula tras validar que está disponible. |
| **METRICAS** | Engine -> Central | `METRICAS#<ID_WS>#<Caudal>#<Volumen>` | El Engine lo envía cada segundo durante el riego para que Central muestre los litros/minuto y el volumen acumulado. |
| **FIN_RIEGO** | Engine -> Central | `FIN_RIEGO#<ID_WS>` | El Engine notifica que el riego ha terminado, ya sea por alcanzar el límite de tiempo o por parada manual. |