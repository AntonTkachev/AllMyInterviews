# Apache Flink

Apache Flink — это **распределённая потоковая платформа**, разработанная для **обработки данных в реальном времени** с
высокой пропускной способностью и низкой задержкой. Он широко используется для **аналитики потоков данных, ETL,
фрод-мониторинга, IoT и рекомендательных систем**.

---

# **Архитектура Apache Flink**

### **1️⃣ JobManager (Координатор)**

- Управляет выполнением приложений Flink.
- Распределяет задачи между узлами кластера.
- Отвечает за отказоустойчивость и восстановление состояния.

### **2️⃣ TaskManager (Исполнители)**

- Выполняют вычисления, обрабатывают потоки данных.
- Содержат **слоты ресурсов** (task slots) для многозадачности.
- Связываются с **JobManager** для координации выполнения.

### **3️⃣ State Backend**

- Отвечает за сохранение состояния **stateful операций**.
- Поддерживает **RocksDB, FsStateBackend, MemoryStateBackend**.
- Используется для **checkpointing и восстановления после сбоев**.

### **4️⃣ Checkpoints & Savepoints**

- **Checkpoints** – автоматическое сохранение состояния для отказоустойчивости.
- **Savepoints** – сохраняемые вручную состояния для обновлений и откатов.

### **5️⃣ Sources & Sinks**

- **Sources** – точки входа данных (Kafka, Kinesis, S3, базы данных).
- **Sinks** – точки выхода (Snowflake, Redshift, ClickHouse, HDFS).

## **📌 Как работает Flink?**

1. **JobManager** принимает задание и разбивает его на задачи (Tasks).
2. **TaskManager** выполняют эти задачи параллельно, используя **Task Slots**.
3. **State Backend** хранит промежуточные данные для stateful-операций.
4. **Checkpoints** позволяют восстановить работу при сбоях.
5. **Результаты отправляются в Sinks (БД, Kafka, файловые системы)**.

---

### **Плюсы Apache Flink**

✅ **Настоящий Stream Processing**  
Flink изначально создан для **обработки событий в реальном времени** (в отличие от Spark Streaming,
который использует **микро-батчи**).

✅ **Поддержка Batch & Streaming**  
Можно писать **как batch, так и stream приложения**, используя один и тот же API.

✅ **Stateful Processing**  
Flink поддерживает **хранение состояния** (например, отслеживание сессий пользователей) и использует RocksDB для работы
с большими объемами данных.

✅ **Event Time Processing + Watermarks**  
Позволяет обрабатывать **запаздывающие события** и правильно агрегировать их в окнах.

✅ **Гибкость в работе с источниками данных**  
Поддерживает **Kafka, Kinesis, S3, JDBC, ElasticSearch, Cassandra и многие другие коннекторы**.

✅ **Высокая производительность и масштабируемость**  
Может **масштабироваться горизонтально**, обрабатывая **миллионы событий в секунду**.

---

### **Минусы Apache Flink**

❌ **Сложность в освоении**  
API Flink довольно сложен по сравнению с **Spark Streaming** или **Kafka Streams**.

❌ **Высокие требования к ресурсам**  
Flink активно использует память и CPU, особенно при работе с **Stateful Processing**.

❌ **Развертывание требует DevOps знаний**  
Для продакшн-окружения **нужно уметь работать с Kubernetes, Docker, Terraform**.

❌ **Ограниченная поддержка BI-инструментов**  
Flink не так хорошо интегрируется с BI-решениями, как Snowflake или Redshift.

---

### **Стоит использовать Flink, если:**

- **Требуется настоящая потоковая обработка данных** в **реальном времени** (фрод-мониторинг, обработка логов, IoT).
- **Нужен stateful processing** (сессионная агрегация или сложная обработка паттернов).
- **Работа с Event Time и Watermarks** (важно учитывать запаздывающие события).
- **Высокие требования к производительности** (миллионы событий в секунду).

### **Не стоит использовать Flink, если:**

❌ **Вам нужен классический Batch Processing** → лучше использовать **Apache Spark**.  
❌ **Задача проста и можно обойтись Kafka Streams**.  
❌ **Нет сильных требований к latency**, и можно работать с **микро-батчами** в Spark Streaming.

---

## Коннекторы Flink (Sources & Sinks)

Flink поддерживает множество источников и приемников данных:

| Категория               | Источники данных (Sources)            | Приемники данных (Sinks)              |
|-------------------------|---------------------------------------|---------------------------------------|
| **Message Queues**      | Kafka, Kinesis, Pulsar, RabbitMQ      | Kafka, Kinesis, Pulsar, RabbitMQ      |
| **Data Storage**        | S3, HDFS, GCS                         | S3, HDFS, GCS                         |
| **Databases**           | MySQL, PostgreSQL, MongoDB, Cassandra | MySQL, PostgreSQL, MongoDB, Cassandra |
| **Streaming Analytics** | Elasticsearch, ClickHouse, Redshift   | Elasticsearch, ClickHouse, Redshift   |
| **Other**               | Custom API, WebSockets, CSV files     | WebSockets, HTTP, Custom API          |

# Работа с источниками и приемниками в Apache Flink

Apache Flink поддерживает множество коннекторов для работы с различными sources и sinks данных.
Это позволяет интегрировать Flink с **Kafka, S3, Kinesis, Snowflake, Redshift, ClickHouse**.

---

### **Kafka Source (Чтение из Kafka)**

Flink позволяет читать данные из Kafka, используя `KafkaSource`.
Этот источник поддерживает **Exactly-Once Processing** - `.setCommitOffsetsOnCheckpoints(true)`
**Checkpointing** и **Watermarks**.

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
        .setBootstrapServers("localhost:9092")
        .setTopics("input-topic")
        .setGroupId("flink-consumer-group")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

DataStream<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
```

### **Kafka Sink (Запись в Kafka)**

Flink также позволяет записывать обработанные данные обратно в Kafka с помощью `KafkaSink`.

```
KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
        .setBootstrapServers("localhost:9092")
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("output-topic")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();

stream.sinkTo(kafkaSink);
```

Здесь **`DeliveryGuarantee.AT_LEAST_ONCE`** гарантирует, что данные будут записаны хотя бы один раз (но возможны дубли).

---

### **Чтение из S3**

Flink позволяет читать данные из S3 в формате **Parquet, CSV, JSON и Avro**.

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> s3Stream = env.readTextFile("s3://my-bucket/input-data.csv");
```

### **Чтение из AWS Kinesis**

Flink интегрируется с Kinesis через **Kinesis Data Streams**, что позволяет обрабатывать события в реальном времени.

```
Properties kinesisConfig = new Properties();
kinesisConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");

FlinkKinesisConsumer<String> kinesisSource = new FlinkKinesisConsumer<>(
        "my-kinesis-stream",
        new SimpleStringSchema(),
        kinesisConfig
);

DataStream<String> kinesisStream = env.addSource(kinesisSource);
```

---

## **3. Flink SQL и интеграция со Snowflake/Redshift/ClickHouse**

Flink поддерживает SQL-обработку потоковых данных с помощью **Flink Table API и Flink SQL**.
Это позволяет работать с **Snowflake, Redshift и ClickHouse**.

### **Чтение и запись в Snowflake**

Flink SQL можно использовать для чтения и записи данных в **Snowflake** через JDBC-коннектор.

```sql
CREATE TABLE snowflake_table (
    id INT,
    name STRING,
    event_time TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:snowflake://account.snowflakecomputing.com',
    'table-name' = 'events',
    'username' = 'user',
    'password' = 'password'
);

INSERT INTO snowflake_table
SELECT id, name, event_time FROM kafka_source;
```

### **Чтение и запись в Redshift**

**Пример JDBC-коннектора для Redshift:**

```sql
CREATE TABLE redshift_table (
    id INT,
    value STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:redshift://my-redshift-cluster.amazonaws.com:5439/mydb',
    'table-name' = 'my_table',
    'username' = 'awsuser',
    'password' = 'mypassword'
);
```

### **Чтение и запись в ClickHouse**

ClickHouse используется для аналитики потоков данных с высокой пропускной способностью.

**Пример Flink SQL для ClickHouse:**

```sql
CREATE TABLE clickhouse_table (
    id INT,
    metric DOUBLE,
    event_time TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:clickhouse://clickhouse-server:8123',
    'table-name' = 'metrics',
    'username' = 'default',
    'password' = ''
);
```

**Запись данных в ClickHouse:**

```sql
INSERT INTO clickhouse_table
SELECT id, metric, event_time FROM processed_stream;
```

---

# Обработка данных в Apache Flink

### Что такое Watermarking?

Watermarking — это механизм, который позволяет Flink работать с **запаздывающими событиями** и определять, когда можно
считать данные завершёнными в рамках временных окон.

**Основные концепции Watermarking:**

- **Event Time**: Время, когда событие реально произошло.
- **Processing Time**: Время, когда Flink обработал событие.
- **Watermark**: Граница времени, до которой Flink считает, что все события уже поступили.

**Пример Watermarking в Flink:**

```java
WatermarkStrategy<MyEvent> watermarkStrategy = WatermarkStrategy
        .<MyEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((event, timestamp) -> event.getTimestamp());
```

Здесь **Watermark задерживает обработку на 5 секунд**, чтобы учесть запаздывающие события.

### Обработка late data

Запаздывающие события — это данные, которые **приходят после установленного watermark'а**.
Flink позволяет настроить их обработку несколькими способами:

1. **Игнорирование (Default)**: Поздние события отбрасываются.
2. **Side Output**: Настроить Flink для отправки запаздывающих событий в альтернативный поток данных.
3. **Reprocessing**: Обновлять агрегированные данные, если запаздывающее событие пришло позже.

```java
final OutputTag<MyEvent> lateTag = new OutputTag<MyEvent>("late-data") {
};

SingleOutputStreamOperator<MyEvent> processedStream = stream
        .assignTimestampsAndWatermarks(watermarkStrategy)
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .allowedLateness(Time.seconds(5))
        .sideOutputLateData(lateTag)
        .reduce((e1, e2) -> new MyEvent(e1.getId(), e1.getValue() + e2.getValue()));
```

---

## 2. Window Functions (Tumbling, Sliding, Session)

Оконные функции (Windows) позволяют агрегировать данные **по времени или другим критериям**.
Flink поддерживает несколько типов окон:

### Tumbling Windows

- Разбивает поток данных на **фиксированные интервалы времени**.
- Каждое событие попадает **только в одно окно**.

```
stream.keyBy(event ->event.getKey()).window(TumblingEventTimeWindows.of(Time.seconds(10)))
   .reduce((e1, e2) ->newMyEvent(e1.getId(),e1.getValue() +e2.getValue()));
```

### Sliding Windows

- Разбивает поток на **окна фиксированного размера**, но с **перекрытием**.
- Каждое событие может попадать в **несколько окон одновременно**.

```
stream.keyBy(event -> event.getKey()).window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
   .reduce((e1, e2) -> new MyEvent(e1.getId(),e1.getValue() +e2.getValue()));
```

### Session Windows

- Определяются **не временем, а активностью пользователя**.
- Окно закрывается, если **нет новых событий в течение заданного интервала**.

```
stream.keyBy(event ->event.getUserId()).window(ProcessingTimeSessionWindows.withGap(Time.minutes(5)))
   .reduce((e1, e2) ->newMyEvent(e1.getId(),e1.getValue() +e2.getValue()));
```

---

## Checkpointing и Savepoints

### Checkpointing (автоматическое сохранение состояния)

Checkpointing — это механизм, который позволяет **Flink автоматически сохранять состояние приложения** через
определенные интервалы времени. Это необходимо для **обеспечения отказоустойчивости**.

**Как работает Checkpointing?**

1. Flink создает снимок состояния оператора.
2. Снимки хранятся в **S3, HDFS или RocksDB**.
3. В случае сбоя Flink **автоматически перезапускает задачу с последнего чекпоинта**.

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5000); // Чекпоинты каждые 5 секунд
```

### Savepoints (ручное сохранение состояния)

Savepoints — это **ручной снимок состояния Flink**, который можно использовать для **апгрейда или перемещения**
приложения без потери данных.

**Создание Savepoint:**

```bash
flink savepoint <jobID> hdfs:///flink-savepoints
```

**Восстановление из Savepoint:**

```bash
flink run -s hdfs:///flink-savepoints/savepoint-1234 job.jar
```

---

# Оптимизация производительности в Apache Flink

Производительность в Apache Flink зависит от множества факторов, включая **настройку параллелизма, управление памятью и
эффективную работу с Garbage Collection**. Этот документ описывает основные стратегии оптимизации.

---

## **1. Parallelism и Scaling**

### **Что такое Parallelism в Flink?**

Parallelism в Flink определяет **количество потоков обработки (Task Slots), выполняющих задачу**. Чем выше уровень
параллелизма, тем быстрее выполняется обработка.

### **Настройка Parallelism**

Flink позволяет управлять параллелизмом на **разных уровнях**:

1. **Глобальный уровень (всего кластера)**:

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(4); // Устанавливает глобальный параллелизм
```

2. **Для отдельного оператора**:

```java
DataStream<String> stream = env.fromElements("one", "two", "three")
        .map(String::toUpperCase).setParallelism(2); // Параллельная обработка с 2 потоками
```

3. **Для конкретных этапов выполнения (sink, source)**:

```java
kafkaSink.setParallelism(3);
```

### **Scaling в Flink**

Flink поддерживает **масштабирование** в зависимости от нагрузки:

- **Горизонтальное масштабирование** – увеличение числа Task Slots.
- **Динамическое масштабирование (Rescaling)** – изменение числа параллельных задач без остановки кластера.

**Пример изменения параллелизма в запущенном приложении:**

```bash
flink rescale-job --jobId <JOB_ID> --newParallelism 8
```

---

## **2. RocksDB и Memory Management**

### **Stateful Processing и RocksDB**

Если в Flink используется **Stateful Processing** (агрегированные значения), состояние данных сохраняется в
памяти или во внешнем хранилище. По умолчанию Flink хранит состояние в **JVM Heap**, но для больших данных рекомендуется
**RocksDB**.

### **Как включить RocksDB?**

```
StateBackend rocksDBBackend = new EmbeddedRocksDBStateBackend();
env.setStateBackend(rocksDBBackend);
```

### **Оптимизация памяти**

1. **Используйте RocksDB вместо Heap State**, если у вас **большие объёмы данных**.
2. **Настраивайте `taskmanager.memory` в `flink-conf.yaml`**:

```yaml
taskmanager.memory.managed.fraction: 0.5  # 50% памяти выделяется Flink для управления
```

3. **Контролируйте Checkpointing**, так как он может потреблять много ресурсов:

```java
env.enableCheckpointing(60000,CheckpointingMode.EXACTLY_ONCE);
```

---

## **3. Тюнинг Garbage Collection (GC)**

Flink активно использует **JVM Heap**, поэтому важно **настроить Garbage Collection** для предотвращения задержек.

### **Лучшие практики для GC в Flink:**

1. **Используйте G1 GC (Garbage First GC)**, если у вас большие объемы данных:

```yaml
taskmanager.env.java.opts: "-XX:+UseG1GC -XX:MaxGCPauseMillis=200"
```

2. **Ограничьте размер heap-памяти**:

```yaml
taskmanager.memory.task.off-heap.size: 4g
```

3. **Сведите к минимуму объекты в стриме**, избегая лишних преобразований `map()` и `flatMap()`.

**Проверка GC в Flink UI:**

- Открыть **Flink Web UI** → вкладка **TaskManagers** → **GC Time**.

---

# Деплой Apache Flink на AWS

Flink можно развернуть в облачной среде AWS с помощью **EKS (Kubernetes)**,
а также интегрировать с **AWS Kinesis** для потоковой обработки данных.

### **Что такое EKS и зачем использовать Kubernetes для Flink?**

Amazon Elastic Kubernetes Service (EKS) позволяет **управлять контейнеризованными приложениями**, в том числе
развертыванием Flink. Kubernetes даёт **гибкость, отказоустойчивость и масштабируемость**.

#### **1. Установка и настройка AWS EKS**

```bash
aws eks create-cluster --name flink-cluster --region us-east-1 --role-arn <IAM_ROLE>
```

#### **2. Развертывание Flink в EKS**

1. **Настройка Helm и Flink-оператора:**

```bash
helm repo add flink-operator https://downloads.apache.org/flink/flink-kubernetes-operator
helm install flink flink-operator/flink-kubernetes-operator
```

2. **Деплой Flink JobManager и TaskManager:**

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: flink-deployment
spec:
  image: flink:latest
  flinkVersion: v1.15.2
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
  jobManager:
    resource:
      memory: "1024m"
  taskManager:
    resource:
      memory: "2048m"
```

3. **Применение конфигурации:**

```bash
kubectl apply -f flink-deployment.yaml
```

Теперь Flink успешно развернут в EKS!

---

### **AWS Kinesis vs Apache Kafka**

| Характеристика         | AWS Kinesis                                 | Apache Kafka                                   |
|------------------------|---------------------------------------------|------------------------------------------------|
| **Тип сервиса**        | Fully managed (SaaS)                        | Self-managed / Cloud-managed                   |
| **Производительность** | Хорош для real-time, но выше latency        | Низкая latency, высокая пропускная способность |
| **Цена**               | Оплата за объем и retention                 | Оплата за инстансы и storage                   |
| **Exactly-Once**       | Поддерживается через Kinesis Data Analytics | Поддерживается через Kafka Transactions        |

### **Настройка Flink с Kinesis**

#### **1. Добавление зависимости в `pom.xml`**

#### **2. Чтение данных из Kinesis в Flink**

```
Properties kinesisConfig = new Properties();
kinesisConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
kinesisConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "<ACCESS_KEY>");
kinesisConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "<SECRET_KEY>");

FlinkKinesisConsumer<String> kinesisSource = new FlinkKinesisConsumer<>(
        "my-kinesis-stream",
        new SimpleStringSchema(),
        kinesisConfig
);
DataStream<String> stream = env.addSource(kinesisSource);
```

#### **3. Запись данных обратно в Kinesis**

```
FlinkKinesisProducer<String> kinesisSink = new FlinkKinesisProducer<>(
        "my-kinesis-stream",
        new SimpleStringSchema(),
        kinesisConfig
);
stream.addSink(kinesisSink);
```

Теперь Flink **читает и записывает данные в Kinesis**, используя **Exactly-Once Processing**.

---

## **3. Monitoring: Flink UI + Prometheus/Grafana**

### **Настройка Flink UI**

После деплоя Flink UI будет доступен по адресу:

```bash
kubectl port-forward svc/flink-jobmanager 8081:8081
```

Далее открываем **http://localhost:8081** в браузере и видим **метрики выполнения задач**.

### **Настройка Prometheus для Flink**

#### **1. Включение метрик в Flink**

В `flink-conf.yaml` добавляем:

```yaml
metrics.reporters: prom
metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
metrics.reporter.prom.port: 9249
```

Перезапускаем Flink, чтобы применить изменения.

#### **2. Установка Prometheus в Kubernetes**

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install prometheus prometheus-community/prometheus
```

Теперь Prometheus собирает метрики Flink.

### **Настройка Grafana для визуализации метрик Flink**

#### **1. Установка Grafana**

```bash
helm install grafana grafana/grafana
```

#### **2. Добавление Prometheus в Grafana**

1. Открыть Grafana UI:

```bash
kubectl port-forward svc/grafana 3000:3000
```

2. Войти в Grafana (**admin/admin** по умолчанию).
3. Добавить источник данных **Prometheus** (`http://prometheus-server:80`).
4. Импортировать **Flink Dashboard** из [Grafana Labs](https://grafana.com/grafana/dashboards/).

## **Оптимизация State**

Flink использует **State Backend** для хранения данных:

- **Heap State** – быстрый, но ограничен RAM.
- **RocksDB State** – подходит для больших объемов, хранит данные на диске.

### **🛠 Улучшение производительности:**

- **Incremental Checkpointing** – уменьшает нагрузку на хранилище.
- **State TTL** – автоматически удаляет устаревшие данные.
- **Оптимизация структуры хранения** – использование `MapState` вместо `ListState`.
- **Балансировка нагрузки** – равномерное распределение данных между TaskManager'ами.

---

## **Обработка ошибок и ретраи**

### **🔹 Restart Strategies** – управление перезапусками задач:

- **Fixed Delay Restart** – фиксированное число ретраев с задержкой.
- **Failure Rate Restart** – ограничение количества сбоев за интервал.
- **No Restart** – остановка при ошибке.

Пример:

```yaml
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 3
restart-strategy.fixed-delay.delay: 10s
```

### **🔹 Exception Handling и DLQ:**

- **Try/Catch в Process Functions** – предотвращает падение всего пайплайна.
- **Dead Letter Queue (DLQ)** – отправка ошибочных сообщений в Kafka/S3.
- **Мониторинг через CloudWatch/Prometheus** – настройка алертов на сбои.

---

# **Two-Phase Commit (2PC) в Apache Flink**

**Two-Phase Commit (2PC)** – это протокол, обеспечивающий **Exactly-Once** обработку при записи в внешние системы (
Kafka, JDBC, S3). Он предотвращает дубликаты и потери данных при сбоях.

## **Как работает 2PC во Flink?**

2PC выполняется в **две фазы**:

1️⃣ **Pre-Commit (подготовка)**

- Flink записывает данные во временное хранилище (Kafka, S3, БД), но **не делает их доступными**.
- Данные фиксируются в **checkpoint**.

2️⃣ **Commit (фиксация)**

- Если checkpoint завершился успешно → данные **становятся видимыми**.
- При сбое → данные **откатываются** (Abort), исключая дубли.

---

## **Как реализуется 2PC?**

Flink использует **TwoPhaseCommitSinkFunction**.

📌 **Основные шаги:**

1. `beginTransaction()` – открытие транзакции.
2. `preCommit()` – временная запись данных.
3. `commit()` – подтверждение данных после checkpoint.
4. `abort()` – откат данных при сбое.

Пример для Kafka:

```java

@Override
protected void commit(KafkaTransaction transaction) {
    transaction.commit();
}

@Override
protected void abort(KafkaTransaction transaction) {
    transaction.abort();
}
```

Где используется 2PC?

- Kafka (Transactional Producer)
- JDBC (MySQL, PostgreSQL, Oracle)
- S3, HDFS (Staged Writes)

---

Плюсы и Минусы 2PC

✅ Exactly-Once семантика без дубликатов.
✅ Гарантированная доставка данных.
✅ Работает с Kafka, БД, S3.

❌ Замедляет поток данных.
❌ Дополнительные ресурсы на обработку.
❌ Не все системы поддерживают 2PC.

---

# **Как Kubernetes (K8s) может видеть внешний сервис?**

Чтобы Kubernetes (K8s) мог подключаться к внешним сервисам (например, база данных, API, сторонний сервис), можно
использовать **ExternalName Service** или **Ingress + ExternalName**.

## **1️⃣ Использование ExternalName Service**

📌 **Что это?**

- Позволяет Kubernetes **проксировать трафик** на внешний сервис по DNS-имени.
- В **K8s не создаётся реальный Pod**, просто переадресация запросов.

🔹 **Пример**: подключаемся к **внешней базе данных (PostgreSQL)**, которая работает за пределами кластера K8s.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: external-db
spec:
  type: ExternalName
  externalName: my-database.example.com  # Внешний домен (DNS)
```

📌 Когда использовать?
✅ Если внешний сервис доступен по доменному имени (DNS).
✅ Когда не нужно балансировать трафик внутри кластера.

⚠ Ограничения

- Не поддерживает балансировку нагрузки (работает только как DNS-алиас).
- Только для DNS-имен, IP-адреса не поддерживаются.

---

2️⃣ Использование Ingress + ExternalName

📌 Что это?

- Позволяет Kubernetes проксировать запросы на внешний сервис через Ingress Controller.
- Работает на HTTP/HTTPS уровне.

🔹 Пример: внешний сервис (external-service.example.com) проксируется через Ingress.

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: external-service
spec:
  rules:
    - host: my-k8s-service.local
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: external-service
                port:
                  number: 80
---
apiVersion: v1
kind: Service
metadata:
  name: external-service
spec:
  type: ExternalName
  externalName: external-service.example.com
```

📌 Когда использовать?

- Если внешний сервис доступен по HTTP/HTTPS.
- Если нужно скрыть внешний сервис за K8s Ingress.

⚠ Ограничения

- Не подходит для TCP/UDP сервисов (только HTTP/HTTPS).
- Требует Ingress Controller (например, Nginx, Traefik).

