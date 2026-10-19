# Especificación del Protocolo de Comunicaciones (Water Management System)

El sistema utiliza un protocolo de capa de aplicación basado en texto plano delimitado por el carácter `#`. Las comunicaciones por Sockets van encapsuladas en tramas binarias con control de errores (LRC) byte a byte. Las comunicaciones por Kafka envían el texto directamente.

## 1. Capa de Enlace TCP (Control de Tramas por Sockets)
Estos bytes se utilizan para gestionar el saludo inicial de la conexión y garantizar que los mensajes no se fragmenten ni se corrompan en la red (mediante el cálculo de LRC).

| Byte | Hex | Significado y uso |
| :--- | :--- | :--- |
| `<ENQ>` | `0x05` | Petición de conexión. Enviado por la estación al iniciar el Socket. |
| `<ACK>` | `0x06` | Acuse de recibo positivo. Indica que la trama llegó completa y el LRC es correcto. |
| `<NACK>` | `0x15` | Acuse de recibo negativo. Indica que la trama llegó dañada o el LRC no coincide. |
| `<STX>` | `0x02` | Inicio de trama de datos. Precede al texto del mensaje. |
| `<ETX>` | `0x03` | Fin de trama de datos. Indica que el texto ha terminado y el siguiente byte es el LRC. |
| `<EOT>` | `0x04` | Cierre de la comunicación (Desconexión controlada). |

---

## 2. Catálogo de Mensajes de Sockets (Monitor/Engine <-> Central)

### Comandos Base (Originales)
| Comando base | Sentido de red | Formato exacto (Payload) | Propósito y reacción esperada |
| :--- | :--- | :--- | :--- |
| **REGISTRO** | Monitor -> Central | `REGISTRO#<ID_WS>#<Ubicación>` | El Monitor de la estación solicita unirse a la red al arrancar. Central verifica y autoriza. |
| **ESTADO** | Monitor -> Central | `ESTADO#<ID_WS>#<OK\|FUGA>` | Latido de salud enviado periódicamente. Si envía `FUGA`, Central bloquea la estación y cambia la vista a ROJO. |
| **INICIAR_RIEGO**| Central -> Engine | `INICIAR_RIEGO#<ID_WS>` | La Central ordena a la estación que abra la electroválvula tras validar que está disponible. |
| **METRICAS** | Engine -> Central | `METRICAS#<ID_WS>#<Caudal>#<Volumen>` | El Engine lo envía cada segundo durante el riego para que Central muestre los litros/minuto y el volumen acumulado. |
| **FIN_RIEGO** | Engine -> Central | `FIN_RIEGO#<ID_WS>` | El Engine notifica que el riego ha terminado, ya sea por alcanzar el límite de tiempo o por parada manual. |

### Comandos de Autorización de Aplicación [NUEVOS]
Se utilizan para que la Central sepa si la orden enviada por TCP se ha ejecutado físicamente con éxito.

| Comando base | Sentido de red | Formato exacto (Payload) | Propósito y reacción esperada |
| :--- | :--- | :--- | :--- |
| **AUTORIZA_RIEGO** | Engine -> Central | `AUTORIZA_RIEGO#<ID_WS>` | Confirma que la válvula física se ha abierto con éxito. La Central pasa a `REGANDO`. |
| **DENIEGA_RIEGO** | Engine -> Central | `DENIEGA_RIEGO#<ID_WS>#<Motivo>` | Informa que no se pudo iniciar el riego (ej. válvula atascada, sin presión de agua). |

---

## 3. Protocolo de Comunicación: Operarios (WM_FO) <-> Central (WM_Central)

La comunicación desde y hacia la aplicación de los operarios se realiza de forma asíncrona mediante Apache Kafka, utilizando diferentes *topics*.

### Peticiones del Operario (Topic: `peticiones_operarios`)
La estructura general es: `COMANDO#ID_OPERARIO#PARAMETROS`.

| Comando | Formato | Ejemplo | Descripción |
|---|---|---|---|
| **PETICION_RIEGO** | `PETICION_RIEGO#<ID_OP>#<ID_WS>` | `PETICION_RIEGO#FO-12#WS-04` | El operario solicita abrir el agua en una estación concreta. |
| **PETICION_PARADA** | `PETICION_PARADA#<ID_OP>#<ID_WS>` | `PETICION_PARADA#FO-12#WS-04` | El operario solicita detener el riego o bloquear una estación. |
| **REGISTRO_FO** | `REGISTRO_FO#<ID_OP>#<UBICACION>` | `REGISTRO_FO#FO-12#Parque Sur` | El operario notifica a la Central a qué parque acaba de llegar. |

### Respuestas de la Central [NUEVOS] (Topic: `respuesta_central`)
Para que cada tablet/app de operario pueda filtrar fácilmente qué mensajes son para él y cuáles debe ignorar, la Central responderá con una estructura jerárquica: `<TIPO>#<DESTINATARIO>#<TEXTO_LIBRE>`.

| Tipo | Formato exacto | Propósito |
| :--- | :--- | :--- |
| **INFO** | `INFO#<ID_OPERARIO>#<Texto>` | Confirmaciones e información de seguimiento exclusiva para ese operario (ej. "Riego iniciado con éxito"). |
| **ERROR** | `ERROR#<ID_OPERARIO>#<Texto>` | Avisos de fallos o denegaciones para ese operario (ej. "Estación no disponible actualmente"). |
| **ALERTA**| `ALERTA#ALL#<Texto>` | Aviso global urgente que todos los operarios de la red deben ver obligatoriamente (ej. "¡FUGA detectada en WS-01!"). |