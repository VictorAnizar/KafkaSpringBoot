# Proyecto para mostrar funcionamiento de apache kafka


### Ejecución de la app

1. Levantar el clúster de Kafka ```docker compose up -d```
2. Configurar la conexión en Spring Boot ```spring.kafka.bootstrap-servers=localhost:9092,localhost:9094,localhost:9096
   spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
   spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer```
3. Ejecutar app Spring: ```mvn spring-boot:run```
4. Prueba de peticion: ```curl -X POST http://localhost:8080/products \
   -H "Content-Type: application/json" \
   -d '{
   "title": "Laptop Gamer",
   "price": 1200.50,
   "quantity": 5
   }'```
5. Verificación de mensajes (Ver siguiente sección "Docker")

### Docker
#### Ver mensaje despues de enviar un evento mediante peticion HTTP
- Apuntar únicamente al puerto interno del contenedor (Recomendado si ejecutas el comando con docker exec):
    ```docker exec -it kafka-kafka-1-1 kafka-console-consumer --bootstrap-server kafka-1:9090 --topic products-created-events-topic --max-messages 1```
- Cuando le indicas --bootstrap-server localhost:9092, el consumidor se conecta inicialmente al broker 1. Sin embargo, como tu clúster tiene 3 particiones distribuidas en 3 nodos y tu tópico tiene un factor de réplica, Kafka intenta descubrir y conectarse a los demás nodos del clúster (localhost:9094 y localhost:9096).




### Notas
- El proyecto es para ilustrar el funcionamiento de kafka, debido a que es un ambiente de pruebas se dan por hecho algunas cosas como configuraciones a Bases de datos.
- Lo ideal es que el producer viva en un proyecto diferente a el/los consumer, ya que son microservicios diferentes
- La distribución de carpetas debe de seguir algun patron como MVC o Arquitectura hexagonal
- En entornos donde el producer y los consumer comparten clases, debería de existir un proyecto el cual esté exclusivamente dedicado a la consulta de determinadas clases o interfaces. Este proyecto sería "common" o "core"


# Arquitectura y Conceptos Clave de Apache Kafka

Esta guía resume los componentes fundamentales de Apache Kafka, su arquitectura de almacenamiento, el flujo de datos entre productores y consumidores, y el modelo de replicación y ordenamiento.

---

## 1. Arquitectura General y Componentes Principales

Kafka opera como una plataforma de streaming de eventos distribuida basada en un patrón de publicación-suscripción (Pub/Sub). Sus entidades centrales son:

*   **Broker:** Un servidor individual dentro de un clúster de Kafka que almacena datos y sirve a los clientes. Los brokers trabajan en conjunto y son coordinados (en arquitecturas modernas mediante KRaft o externamente mediante ZooKeeper).
*   **Producer (Productor):** La aplicación o cliente que **crea y envía (publica) eventos** hacia un tópico específico en Kafka. Los productores pueden decidir a qué partición enviar el mensaje usando una clave o un balanceador de carga.
*   **Consumer (Consumidor):** La aplicación o cliente que **lee y procesa los eventos** desde los tópicos de Kafka.
*   **Consumer Group (Grupo de Consumidores):** Un conjunto de instancias de consumidores que trabajan juntas para consumir un tópico. **Una partición de un tópico solo puede ser leída por un consumidor a la vez dentro del mismo grupo**, lo que permite escalar el procesamiento de forma paralela y tolerante a fallos.
*   **Topic (Tópico):** El canal lógico o categoría donde se organizan los mensajes (por ejemplo, `products-created-events-topic`). Los tópicos actúan como flujos de datos persistentes.
*   **Partition (Partición):** Los tópicos se dividen físicamente en particiones. Una partición es un registro ordenado e inmutable de mensajes append-only (*solo adición*). Permiten la escalabilidad horizontal del clúster al distribuirse en diferentes brokers.

---

## 2. Modelo Líder / Seguidor y Alta Disponibilidad

Para garantizar que el sistema no pierda datos si un servidor falla, Kafka utiliza un modelo de replicación basado en líderes y seguidores a nivel de partición:

*   **Líder de la Partición (Leader):** De todas las réplicas asignadas a una partición, una de ellas es elegida como *líder*. **Todas las operaciones de escritura y lectura de los clientes pasan exclusivamente por el líder**.
*   **Seguidores (Followers / Replicas):** Los demás brokers que almacenan una copia pasiva de la partición se denominan seguidores. Su trabajo es replicar de manera continua los datos del líder para mantenerse sincronizados (conjunto conocido como **ISR** - *In-Sync Replicas*).
*   **Conmutación por error (Failover):** Si el broker que aloja al líder de la partición se cae, Kafka detecta la falla y promueve automáticamente a uno de los seguidores sincronizados (ISR) para que asuma el rol de nuevo líder, garantizando la disponibilidad continua sin pérdida de datos (siempre que se configuren políticas de sincronización como `min.insync.replicas`).

---

## 3. ¿Cómo se Ordenan los Mensajes?

El orden de los eventos en Kafka está estrictamente acotado al ámbito de la **partición**:

*   **Garantía a Nivel de Partición:** Dentro de una misma partición, los mensajes se almacenan exactamente en el orden en que llegan y se les asigna un identificador secuencial incremental llamado **Offset**. Los consumidores leen los mensajes de una partición respetando rigurosamente este orden.
*   **El Rol de la Clave (Key):** Cuando un productor envía un mensaje, puede incluir una clave (por ejemplo, el `productId`). El productor aplica una función de hash sobre esa clave para determinar a qué partición exacta debe ir el mensaje. **Todos los mensajes con la misma clave van siempre a la misma partición**, garantizando así que los eventos relacionados con una misma entidad mantengan un orden cronológico estricto.
*   **Sin Orden Global:** No existe una garantía de orden global entre diferentes particiones de un mismo tópico; el procesamiento paralelo entre particiones ocurre de forma independiente.

---

## 4. Flujo de Funcionamiento Resumido

1.  El **Producer** serializa un objeto (por ejemplo, un evento de negocio) y lo envía a un **Topic**.
2.  El mensaje llega al **Broker Líder** de la partición seleccionada y se escribe en disco de forma secuencial, asignándole un **Offset**.
3.  Los **Brokers Seguidores** copian el mensaje de manera asíncrona o síncrona según la configuración de `acks`.
4.  Un **Consumer Group** lee los mensajes de las particiones asignadas. Cada consumidor procesa los datos y confirma su avance actualizando su *offset* (commit), permitiendo reanudar la lectura sin duplicados ni pérdidas en caso de reinicios.