# Especificación del Protocolo de Comunicaciones (Water Management System)

El sistema usa un protocolo de capa de aplicación basado en texto plano (UTF-8) cuyos campos se separan con el carácter `#`.

- **Sockets TCP** (Estación ↔ Central): los mensajes viajan dentro de tramas binarias con control de errores (LRC).
- **Kafka** (Operarios ↔ Central): el texto se envía directamente, sin tramas.

**Convenciones del documento**

- Sin etiqueta: implementado y funcionando en `WM_Central`.
- **[PROPUESTO]**: diseñado, pendiente de implementar o de acordar con el resto del equipo.
- Ningún campo puede contener el carácter `#`, ni siquiera la ubicación o el nombre del operario.

---

## 1. Modelo de conexión

- Para CENTRAL, el **Monitor y el Engine de una estación son un único elemento**: "la estación". CENTRAL solo habla con el Monitor, que abre **una única conexión TCP** por estación. Cómo se comunican internamente Monitor y Engine queda fuera de este protocolo.
- CENTRAL guarda un buzón interno por cada conexión registrada, y es el único hilo que lee y escribe en ese socket.
- Los operarios (`WM_FO`) solo hablan con CENTRAL por Kafka.

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
| `<EOT>` | `0x04` | Cierre ordenado de la comunicación. Hoy CENTRAL lo trata igual que una desconexión (pasa la estación a `DESCONECTADA`). |

### 2.2 Formato de la trama de datos

```
<STX> <DATA> <ETX> <LRC>
```

- `DATA`: texto del mensaje codificado en UTF-8.
- `LRC`: **un único byte** por trama, resultado del XOR de todos los bytes de `DATA` (sin incluir `STX` ni `ETX`).
- El LRC puede coincidir con el valor de `ETX` (`0x03`), por lo que **siempre se lee como el byte que sigue a `ETX`**, sin interpretarlo.
- Una trama con menos de 4 bytes, sin `STX`/`ETX` en su sitio o con LRC incorrecto se rechaza con `NACK`.

### 2.3 Handshake y fiabilidad

1. El Monitor abre el socket y envía `<ENQ>`.
2. CENTRAL responde `<ACK>`. Si el primer byte no es `<ENQ>`, responde `<NACK>` y cierra la conexión.
3. Cada trama de datos recibida se acusa con `<ACK>` (correcta) o `<NACK>` (dañada).
4. Cuando CENTRAL envía una trama, espera el `<ACK>` un máximo de **1,5 s** y reintenta hasta **3 veces** si recibe `<NACK>` o salta el timeout. Si se agotan los intentos, o la conexión se cierra, CENTRAL marca la estación como `DESCONECTADA`.
5. Mientras CENTRAL espera un `<ACK>`, pueden llegar tramas de datos de la estación (por ejemplo `METRICAS`). CENTRAL las conserva, las procesa y las acusa después.
6. **El `<ACK>` solo significa "trama recibida correctamente", nunca "orden ejecutada".** Que una orden se ha ejecutado se comunica con mensajes de aplicación (`AUTORIZA_RIEGO`, `DENIEGA_RIEGO`, `FIN_RIEGO`...).

---

## 3. Catálogo de mensajes por sockets (Estación ↔ Central)

### 3.1 Registro y salud

| Mensaje | Sentido | Formato | Propósito y reacción de CENTRAL |
| :--- | :--- | :--- | :--- |
| **REGISTRO** | Estación → Central | `REGISTRO#<ID_WS>#<Ubicación>` | Da de alta la estación (o la actualiza si ya existía), la deja en `DISPONIBLE` y asocia su buzón a esa conexión. **Es también la forma de recuperarse de una fuga** (ver 3.5). |
| **ESTADO** | Estación → Central | `ESTADO#<ID_WS>#<OK\|FUGA>` | `FUGA`: la estación pasa a `FUGA` (rojo). Si había riego en curso o solicitado, CENTRAL envía `FIN_RIEGO` a la estación, avisa al operario afectado y publica un `ALERTA` global. `OK`: informativo, **no** saca la estación de `FUGA`. |

### 3.2 Flujo de riego

El riego **no tiene duración predefinida**: continúa de forma indefinida hasta que se corta manualmente (menú de la estación, `PETICION_PARADA` de un operario o parada desde el menú de CENTRAL).

| Mensaje | Sentido | Formato | Propósito y reacción esperada |
| :--- | :--- | :--- | :--- |
| **INICIAR_RIEGO** | Central → Estación | `INICIAR_RIEGO#<ID_WS>` | Petición de riego tras validar que la estación está `DISPONIBLE`. La estación pasa a `SOLICITADA` y debe contestar con `AUTORIZA_RIEGO` o `DENIEGA_RIEGO`. |
| **INICIAR_RIEGO** | Estación → Central | `INICIAR_RIEGO#<ID_WS>` | Riego iniciado desde el menú de la propia estación. Ver 3.6. |
| **AUTORIZA_RIEGO** | Estación → Central | `AUTORIZA_RIEGO#<ID_WS>` | La válvula se ha abierto. Pasa de `SOLICITADA` a `REGANDO` y se avisa al operario. |
| **DENIEGA_RIEGO** | Estación → Central | `DENIEGA_RIEGO#<ID_WS>#<Motivo>` | No se pudo iniciar el riego (válvula atascada, sin presión...). La estación vuelve de `SOLICITADA` a `DISPONIBLE` y el operario recibe un `ERROR` con el motivo. |
| **METRICAS** | Estación → Central | `METRICAS#<ID_WS>#<Caudal>#<Volumen>` | Cada segundo durante el riego. Caudal en L/min y volumen acumulado en L. CENTRAL los guarda y los reenvía al operario. |
| **FIN_RIEGO** | Estación → Central | `FIN_RIEGO#<ID_WS>` | El riego terminó por parada manual en la estación. Si estaba `REGANDO`, pasa a `DISPONIBLE` y el operario recibe el resumen. |
| **FIN_RIEGO** | Central → Estación | `FIN_RIEGO#<ID_WS>` | Orden de parada (petición de un operario, del menú de CENTRAL, o cierre de emergencia por fuga). La estación debe **cerrar la válvula y contestar con su propio `FIN_RIEGO`**: CENTRAL espera ese mensaje para volver a `DISPONIBLE` y generar el resumen. |

### 3.3 Bloqueo de estaciones **[PROPUESTO]**

CENTRAL puede poner una estación fuera de servicio (punto 11 de la mecánica). Las opciones ya existen en el menú de CENTRAL; falta el tratamiento de estas órdenes en el hilo de cada estación.

| Mensaje | Sentido | Formato | Reacción esperada |
| :--- | :--- | :--- | :--- |
| **BLOQUEAR** | Central → Estación | `BLOQUEAR#<ID_WS>` | Si está `DISPONIBLE` o `REGANDO`, cierra el riego (si lo hay) y pasa a `PARADA`. La estación muestra naranja y la leyenda "Fuera de Servicio". |
| **ACTIVAR** | Central → Estación | `ACTIVAR#<ID_WS>` | Solo válido desde `PARADA`: pasa a `DISPONIBLE` (verde). |

### 3.4 Tramas de texto libre de CENTRAL

CENTRAL responde a algunos mensajes con tramas de texto libre, por ejemplo:

- "Estación correctamente dada de alta en el sistema"
- "Fin de riego correctamente anotado"
- "La estación no se encuentra disponible en estos momentos"
- Mensajes de error de formato.

Son **informativas**: la estación las acusa con `<ACK>` y las muestra, pero no son un comando.

### 3.5 Fuga y recuperación

1. El Monitor detecta una fuga y envía `ESTADO#<ID_WS>#FUGA`.
2. CENTRAL pasa la estación a `FUGA`. Si estaba `REGANDO` o `SOLICITADA`, envía `FIN_RIEGO#<ID_WS>` a la estación y avisa al operario afectado. Se publica un `ALERTA#ALL`.
3. **[PROPUESTO]** Si el Monitor repite `FUGA` cada segundo, CENTRAL ignora las repeticiones y procesa la fuga solo la primera vez.
4. Una vez resuelta la avería, **la estación debe darse de alta de nuevo con `REGISTRO`**. Es la única forma de volver a `DISPONIBLE`. `ESTADO#<ID_WS>#OK` no la revierte.
5. Prioridad de estados: `FUGA` prevalece sobre `PARADA`, y un nuevo `REGISTRO` borra ambos.

### 3.6 Riego iniciado desde la estación

Cuando el riego se pide desde el menú de la propia estación, **la estación ya ha comprobado por sí misma que está en condiciones**, y envía `INICIAR_RIEGO#<ID_WS>` a CENTRAL.

- CENTRAL comprueba que la estación esté `DISPONIBLE`. Si lo está, la pasa a `REGANDO` y responde con una trama informativa; si no, responde con una trama informativa indicándolo.
- No hay fase `SOLICITADA` ni mensaje `AUTORIZA_RIEGO`/`DENIEGA_RIEGO` en este caso.
- Como no hay operario asociado, no se publica nada en Kafka. Los mensajes se muestran por pantalla en CENTRAL.

---

## 4. Estados de una estación

| Estado | Color | Significado |
| :--- | :--- | :--- |
| `DESCONECTADA` | Gris | Sin conexión con CENTRAL. Es el estado de todas las estaciones al arrancar CENTRAL. |
| `DISPONIBLE` | Verde | Operativa y a la espera de una petición. |
| `SOLICITADA` | (interno) | CENTRAL ha pedido el riego y espera `AUTORIZA_RIEGO` o `DENIEGA_RIEGO`. |
| `REGANDO` | Verde parpadeante | Regando. Se muestran caudal, volumen y operario. |
| `FUGA` | Rojo | Fuga detectada. No se puede regar. |
| `PARADA` | Naranja | Fuera de servicio por orden de CENTRAL. |

| Desde | Evento | Hacia |
| :--- | :--- | :--- |
| (cualquiera) | `REGISTRO` | `DISPONIBLE` |
| (cualquiera) | Cierre de conexión, `<EOT>` o fallo de envío | `DESCONECTADA` |
| `DISPONIBLE` | Petición de riego de operario o de CENTRAL | `SOLICITADA` |
| `DISPONIBLE` | `INICIAR_RIEGO` desde la estación | `REGANDO` |
| `SOLICITADA` | `AUTORIZA_RIEGO` | `REGANDO` |
| `SOLICITADA` | `DENIEGA_RIEGO` | `DISPONIBLE` |
| `REGANDO` | `FIN_RIEGO` | `DISPONIBLE` |
| `DISPONIBLE` / `SOLICITADA` / `REGANDO` | `ESTADO#...#FUGA` | `FUGA` |
| `DISPONIBLE` / `REGANDO` | `BLOQUEAR` **[PROPUESTO]** | `PARADA` |
| `PARADA` | `ACTIVAR` **[PROPUESTO]** | `DISPONIBLE` |

---

## 5. Operarios (WM_FO) ↔ Central por Kafka

### 5.1 Peticiones del operario (topic `peticiones_operarios`)

Estructura general: `COMANDO#<ID_OP>#<PARÁMETROS>`. Un mensaje con menos de 2 campos se descarta. Con 2 campos, CENTRAL responde `ERROR#<ID_OP>#...` indicando que faltan argumentos.

| Comando | Formato | Ejemplo | Descripción |
| :--- | :--- | :--- | :--- |
| **REGISTRO_FO** | `REGISTRO_FO#<ID_OP>#<NOMBRE>` | `REGISTRO_FO#FO-12#Ana Pérez` | Da de alta al operario con su nombre. Debe hacerse antes de cualquier otra petición. |
| **PETICION_RIEGO** | `PETICION_RIEGO#<ID_OP>#<ID_WS>` | `PETICION_RIEGO#FO-12#WS-04` | Solicita regar en una estación. CENTRAL comprueba que el operario exista y que la estación esté conectada y disponible. |
| **PETICION_PARADA** | `PETICION_PARADA#<ID_OP>#<ID_WS>` | `PETICION_PARADA#FO-12#WS-04` | Solicita detener un riego en curso. **No bloquea la estación**: el bloqueo solo se hace desde el menú de CENTRAL. |

### 5.2 Respuestas de CENTRAL (topic `respuesta_central`)

Estructura: `<TIPO>#<DESTINATARIO>#<TEXTO>`. Cada FO debe filtrar los mensajes cuyo destinatario sea su `ID_OP` o `ALL`.

| Tipo | Formato | Propósito |
| :--- | :--- | :--- |
| **INFO** | `INFO#<ID_OPERARIO>#<Texto>` | Confirmaciones e información de seguimiento exclusiva para ese operario (ej. "Riego iniciado con éxito"). |
| **ERROR** | `ERROR#<ID_OPERARIO>#<Texto>` | Avisos de fallos o denegaciones para ese operario (ej. "Estación no disponible actualmente"). |
| **ALERTA**| `ALERTA#ALL#<Texto>` | Aviso global urgente que todos los operarios de la red deben ver obligatoriamente (ej. "¡FUGA detectada en WS-01!"). |



### 5.3 Qué mensajes cierran un servicio

El FO espera una conclusión, con éxito o fracaso, y 4 segundos después lanza la siguiente petición del fichero. Cierran un servicio:

- Cualquier `ERROR#<ID_OP>#...`.
- El resumen final del riego (`RESUMEN`, o su `INFO` equivalente mientras no exista ese tipo).
- **[PROPUESTO]** En caso de fuga durante el riego, el aviso dirigido al operario afectado: además del `ALERTA#ALL`, debería recibir un `ERROR#<ID_OP>#Riego abortado por fuga en <ID_WS>`.

El resto de `INFO` son de progreso y no cierran el servicio.

### 5.4 Otras notas

- Si el riego lo inicia CENTRAL desde su menú, el operario es `CENTRAL`. Esos mensajes se muestran por pantalla en CENTRAL y **no** se publican en Kafka **[PROPUESTO]**.
- El consumidor de CENTRAL no define `group_id` ni `auto_offset_reset`: las peticiones enviadas mientras CENTRAL estaba caída se pierden.

---

## 6. Puntos pendientes

1. **Formato del fichero del FO** (qué campos tiene cada línea): aún no especificado en clase.
2. **Tratamiento de `BLOQUEAR` / `ACTIVAR`** en el hilo de cada estación (sección 3.3) y su implementación en el módulo de la estación.
3. **Nuevos tipos hacia el FO** (`METRICAS`, `RESUMEN`, aviso de fuga al operario afectado) y aceptación del destinatario `ALL` por parte del FO.
4. **Riego iniciado desde la estación y rechazo de CENTRAL:** la estación debe saber qué hacer si CENTRAL responde que no estaba disponible (sección 3.6).
5. **Visualización del estado de todas las estaciones por parte de los operarios:** fuera de alcance por ahora.


## Especificación de Conexión Local: Monitor <-> Engine

La comunicación interna de la estación de riego se realiza de forma directa y asíncrona mediante Sockets, donde el Monitor actúa como un "Router" o pasarela entre la red física local y la Central remota.

| Parámetro / Característica | Especificación Técnica | Descripción del Comportamiento |
| :--- | :--- | :--- |
| **Protocolo de Transporte** | TCP / IPv4 (`SOCK_STREAM`) | Conexión persistente que garantiza la entrega íntegra y secuencial de los comandos a nivel local. |
| **Rol del Monitor (`WM_WS_M`)** | Servidor Local | Abre el puerto, hace `bind()` y escucha (`listen`) de forma **no bloqueante** para no detener la comunicación con la Central. Acepta 1 sola conexión. |
| **Rol del Engine (`WM_WS_E`)** | Cliente Local | Al arrancar, inicia la conexión hacia el Monitor (`connect()`). Si se cae, debe intentar reconectarse. |
| **Variables de Entorno** | `MONITOR_IP` (Def: `127.0.0.1`)<br>`MONITOR_PORT` (Def: `5001`) | Configuración del enchufe local. Es vital que el puerto sea diferente al de la Central (`5000`). |
| **Formato de Mensajes** | Tramas con `LRC` (`common.protocol`) | Aunque es comunicación local, se reutiliza el empaquetado oficial (`<STX>` + Comando + `<ETX>` + `<LRC>`) para estandarizar el código. |
| **Flujo de Bajada (M &rarr; E)** | Órdenes de Acción | El Monitor recibe de la red y reenvía al Engine comandos limpios como `INICIAR_RIEGO` o `FIN_RIEGO`. |
| **Flujo de Subida (E &rarr; M)** | Telemetría y Control | El Engine lee los sensores físicos y envía al Monitor comandos de respuesta como `METRICAS`, `AUTORIZA_RIEGO` o `DENIEGA_RIEGO`. |
=======
| **INFO** | `INFO#<ID_OP>#<Texto>` | Seguimiento del servicio: petición enviada, riego iniciado, métricas, alta de operario... |
| **ERROR** | `ERROR#<ID_OP>#<Texto>` | Fallo o denegación: operario no dado de alta, estación offline o no disponible, riego denegado, conexión perdida... |
| **ALERTA** | `ALERTA#ALL#<Texto>` | Aviso global urgente (fuga). Todos los FO deben procesarlo. |
| **METRICAS** **[PROPUESTO]** | `METRICAS#<ID_OP>#<ID_WS>#<Caudal>#<Volumen>` | Datos estructurados para que el FO no tenga que interpretar texto. |
| **RESUMEN** **[PROPUESTO]** | `RESUMEN#<ID_OP>#<ID_WS>#<Volumen>#<Duración>` | Resumen final del riego. La duración la calcula CENTRAL desde que recibe `AUTORIZA_RIEGO` hasta el `FIN_RIEGO`. |