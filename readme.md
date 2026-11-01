# Especificación del Protocolo de Comunicaciones (Water Management System)

El sistema usa un protocolo de capa de aplicación basado en texto plano (UTF-8) cuyos campos se separan con el carácter `#`.

- **Sockets TCP** (Estación ↔ Central): los mensajes viajan dentro de tramas binarias con control de errores (LRC).
- **Kafka** (Operarios ↔ Central): el texto se envía directamente, sin tramas.

**Convenciones del documento**
- Todo lo descrito en este documento está **implementado y funcional** en la versión final de despliegue.
- Ningún campo puede contener el carácter `#`, ni siquiera la ubicación o el nombre del operario.

---

## 1. Modelo de conexión

- Para CENTRAL, el **Monitor y el Engine de una estación son un único elemento**: "la estación". CENTRAL solo habla con el Monitor, que abre **una única conexión TCP** por estación.
- CENTRAL guarda un buzón interno por cada conexión registrada, y es el único hilo que lee y escribe en ese socket.
- Los operarios (`WM_FO`) solo hablan con CENTRAL por Kafka.
- La Central cuenta con una base de datos SQLite multihilo que garantiza la persistencia del estado en caso de caída o reinicio abrupto.

---

## 2. Capa de enlace (tramas por sockets)

### 2.1 Bytes de control

| Byte | Hex | Significado y uso |
| :--- | :--- | :--- |
| `<ENQ>` | `0x05` | Petición de conexión. Lo envía el Monitor nada más abrir el socket. |
| `<ACK>` | `0x06` | Acuse positivo: la trama llegó completa y el LRC es correcto. |
| `<NACK>` | `0x15` | Acuse negativo: trama dañada, formato incorrecto o LRC erróneo. |
| `<STX>` | `0x02` | Inicio de trama de datos. |
| `<ETX>` | `0x03` | Fin del texto. El byte siguiente es siempre el LRC. |
| `<EOT>` | `0x04` | Cierre ordenado de la comunicación. CENTRAL lo trata como una desconexión y limpia la base de datos. |

### 2.2 Formato de la trama de datos
```text
<STX> <DATA> <ETX> <LRC>
```
- `DATA`: texto del mensaje codificado en UTF-8.
- `LRC`: **un único byte** por trama, resultado del XOR de todos los bytes de `DATA`.
- Una trama con menos de 4 bytes, sin `STX`/`ETX` en su sitio o con LRC incorrecto se rechaza con `NACK`.

### 2.3 Handshake, fiabilidad
1. El Monitor abre el socket y envía `<ENQ>`. CENTRAL responde `<ACK>`.
2. Cada trama de datos recibida se acusa con `<ACK>` (correcta) o `<NACK>` (dañada).
3. CENTRAL espera el `<ACK>` un máximo de **1,5 s** y reintenta hasta **3 veces**. Si se agotan los intentos, marca la estación como `DESCONECTADA`.


---

## 3. Catálogo de mensajes por sockets (Estación ↔ Central)

### 3.1 Registro y salud

| Mensaje | Sentido | Formato | Propósito y reacción de CENTRAL |
| :--- | :--- | :--- | :--- |
| **REGISTRO** | Estación → Central | `REGISTRO#<ID_WS>#<Ubicación>` | Da de alta la estación. Si la Central acababa de recuperarse de una caída y el riego seguía abierto, la Central detecta las métricas posteriores y recupera al operario de forma transparente. |
| **ESTADO** | Estación → Central | `ESTADO#<ID_WS>#<OK\|FUGA>` | `FUGA`: CENTRAL envía `FIN_RIEGO`, aborta el servicio, avisa al operario y publica un `ALERTA` global. CENTRAL procesa la fuga solo la primera vez para evitar saturación (spam). |

### 3.2 Flujo de riego

El riego continúa de forma indefinida hasta que se corta manualmente (menú de la estación, petición FO o menú CENTRAL).

| Mensaje | Sentido | Formato | Propósito y reacción esperada |
| :--- | :--- | :--- | :--- |
| **INICIAR_RIEGO** | Central → Estación | `INICIAR_RIEGO#<ID_WS>` | Petición tras validar disponibilidad. La estación debe contestar `AUTORIZA_RIEGO` o `DENIEGA_RIEGO`. |
| **INICIAR_RIEGO** | Estación → Central | `INICIAR_RIEGO#<ID_WS>` | Riego iniciado desde el menú manual del Engine. En el panel de control se asignará al operario `LOCAL`. |
| **AUTORIZA_RIEGO** | Estación → Central | `AUTORIZA_RIEGO#<ID_WS>` | La válvula se ha abierto. Pasa a `REGANDO`. |
| **DENIEGA_RIEGO** | Estación → Central | `DENIEGA_RIEGO#<ID_WS>#<Motivo>` | No se pudo iniciar. El operario recibe un `ERROR` en Kafka con el motivo. |
| **METRICAS** | Estación → Central | `METRICAS#<ID_WS>#<Caudal>#<Volumen>` | Envío periódico (1 vez por segundo). **Silenciado en consola/Kafka**: se guarda directamente en SQLite para que el Panel Visual lo consuma sin hacer spam. |
| **FIN_RIEGO** | Estación → Central | `FIN_RIEGO#<ID_WS>` | El riego terminó. Si llega tras una avería del hardware, CENTRAL actualiza el volumen histórico de SQLite corrigiendo la métrica final y avisa al operario. |
| **FIN_RIEGO** | Central → Estación | `FIN_RIEGO#<ID_WS>` | Orden de parada remota. |

### 3.3 Bloqueo de estaciones (Fuera de Servicio)
Implementado al 100% en el menú interactivo de la CENTRAL y soportado por el Engine.

| Mensaje | Sentido | Formato | Reacción esperada |
| :--- | :--- | :--- | :--- |
| **BLOQUEAR** | Central → Estación | `BLOQUEAR#<ID_WS>` | Cierra el riego (si lo hay) y pasa a `PARADA`. La estación muestra color naranja. |
| **ACTIVAR** | Central → Estación | `ACTIVAR#<ID_WS>` | Solo válido desde `PARADA`: pasa a `DISPONIBLE` (verde). |

---

## 4. Estados de una estación (Interfaz Visual)
El módulo `panel.py` de la Central refleja en tiempo real el estado del sistema adaptándose a las dimensiones de la terminal mediante la siguiente paleta ANSI:

| Estado | Color ANSI | Significado |
| :--- | :--- | :--- |
| `DESCONECTADA` | Gris + Texto Blanco | Sin conexión con CENTRAL (Caída abrupta o apagado). |
| `DISPONIBLE` | Verde + Texto Negro | Operativa y a la espera de una petición. |
| `SOLICITADA` | Amarillo + Texto Negro | CENTRAL ha pedido el riego y espera respuesta de la válvula. |
| `REGANDO` | **Verde Parpadeante** | Regando. Se muestran Op, L/min y L totales actualizados por SQLite. |
| `FUGA` | Rojo + Texto Blanco | Fuga detectada desde el Engine. |
| `PARADA` | Naranja + Texto Negro | Fuera de servicio por orden del panel de CENTRAL. |

---

## 5. Operarios (WM_FO) ↔ Central por Kafka

### 5.1 Peticiones del operario (topic `peticiones_operarios`)

| Comando | Formato | Descripción |
| :--- | :--- | :--- |
| **REGISTRO_FO** | `REGISTRO_FO#<ID_OP>#<NOMBRE>` | Da de alta al operario (obligatorio). |
| **LISTAR_ESTACIONES**| `LISTAR_ESTACIONES#<ID_OP>#CONSULTA` | Consulta a la Base de Datos para listar todas las estaciones y su estado. |
| **PETICION_RIEGO** | `PETICION_RIEGO#<ID_OP>#<ID_WS>` | Solicita abrir válvula. CENTRAL valida disponibilidad y conexión. |
| **PETICION_PARADA** | `PETICION_PARADA#<ID_OP>#<ID_WS>` | Solicita detener un riego en curso. |

### 5.2 Respuestas de CENTRAL (topic `respuesta_central`)

Los consumidores FO utilizan grupos dinámicos (`group_id=fo-<ID>-<TIMESTAMP>`) para leer siempre los mensajes más recientes al reiniciar.

| Tipo | Formato | Propósito |
| :--- | :--- | :--- |
| **INFO** | `INFO#<ID_OPERARIO>#<Texto>` | Confirmaciones y resúmenes finales (ej. "Riego finalizado con éxito. Resumen: 120.0 Litros..."). |
| **ERROR** | `ERROR#<ID_OPERARIO>#<Texto>` | Avisos de fallos (ej. "Estación fuera de conexión", "Riego abortado por fuga"). Cierra el servicio del FO automáticamente. |
| **ALERTA**| `ALERTA#ALL#<Texto>` | Aviso global urgente. Todos los operarios lo imprimen en pantalla al instante. |

*Nota: Las notificaciones constantes de `METRICAS` fueron suprimidas del canal de Kafka para evitar la saturación del broker y del operario. El consumo de agua se entrega consolidado en el mensaje `INFO` al concluir el riego.*