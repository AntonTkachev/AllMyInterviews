
--------------------------------------------

[//]: # (http://blog.madhukaraphatak.com/secondary-namenode---what-it-really-do/)

[//]: # (https://bitworks.software/2018-08-07-hadoop-cluster-setup-with-hdfs-ha.html)

[//]: # (https://www.lifewire.com/what-does-seek-time-mean-2626007)

# Data Lineage и Data Discovery

## Data Lineage

**Data Lineage** — процесс отслеживания пути данных от источника до конечного использования. Он помогает понять, откуда
пришли данные, какие трансформации они прошли и где используются.

### Основные цели:

- Обеспечение прозрачности данных
- Контроль качества и устранение ошибок
- Соответствие регуляторным требованиям (GDPR, HIPAA)
- Оптимизация ETL-процессов

### Виды Data Lineage:

- **Физический** — отслеживание файлов, таблиц, API
- **Логический** — отображение трансформаций и зависимостей
- **Бизнес-уровень** — связь данных с бизнес-правилами

### Инструменты:

Apache Atlas, OpenLineage, Collibra, Google Data Catalog, AWS Glue

## Data Discovery

**Data Discovery** — процесс обнаружения, классификации и анализа данных в организации. Позволяет понять, какие данные
существуют, где они хранятся и как могут использоваться.

### Основные цели:

- Инвентаризация и категоризация данных
- Выявление взаимосвязей между данными
- Управление метаданными
- Обнаружение чувствительных данных

### Как работает:

1. Сканирование источников (S3, PostgreSQL, Snowflake)
2. Классификация данных
3. Тегирование и разметка
4. Визуализация взаимосвязей

### Инструменты:

Collibra, Alation, Apache Atlas, Google Data Catalog, AWS Glue

## Разница между Data Lineage и Data Discovery

| Функция       | Data Lineage                  | Data Discovery                 |
|---------------|-------------------------------|--------------------------------|
| Цель          | Отслеживание движения данных  | Поиск и классификация данных   |
| Фокус         | История происхождения         | Инвентаризация                 |
| Использование | Аналитики, инженеры, аудиторы | Бизнес-аналитики, безопасность |
| Инструменты   | Apache Atlas, OpenLineage     | Alation, Google Data Catalog   |

## Итог

Data Lineage показывает, как данные изменялись, Data Discovery помогает их находить и классифицировать. Оба процесса
важны для управления данными, аналитики и безопасности.

------------


CDC (Change Data Capture)

Это способ отслеживания изменений в базе данных (например, добавление, обновление или удаление данных) в реальном
времени. Он позволяет быстро передавать эти изменения в другие системы, например, для обновления хранилища данных.

ODC (Operational Data Change)

Это изменения данных в операционных системах (например, в базах данных, которые обрабатывают текущие транзакции). Это
процесс обновления или перемещения данных между системами, но не всегда в реальном времени.

https://habr.com/ru/company/sberbank/blog/679028/

------------

- Architectural approaches, Lambda, Delta


https://habr.com/ru/post/555920/

1. Storage Layer:

- HDFS, NFS, CIFS (Windows File Share) – differences
  https://dokumen.tips/documents/-apache-hadoop-.html?page=1
- Cloud Storage types (Block, File, Object) – which type is good and for what?
  Почитать любую ссылку в гугле

3. Data Ingestion Layer
   https://www.techtarget.com/whatis/definition/data-ingestion

- What is the purpose of Ingestion Layer?
- Tools for ingestion – Apache Kafka, Spark Streaming, Apache Storm?

4. Processing Layer

- What is the purpose of this Layer?
- Types of processing jobs?
- What tools do you know?

5. Consumption Layer

- What is the purpose of this Layer?
- Types of consumptions? AI, ML, BI – explanation
- What tools do you know?

6. Orchestration

- What is the purpose of orchestration?
- What tools do you know? (Air Flow?)

7. OS

- What are basic elements or components of Linux – Kernel, Shell, applications +
- What is Kernel in Linux OS?
- What is a Linux shell?
- What are file permissions? R,W,E

8. Python

- What is a decorator in python?
- What is a context manager in python and use cases?
- Difference between Multi Processing and Multi Threading? GIL?
- Multithreading vs Async?
- How you and your team manage to contain code as clean as possible? What tools do you know for this purpose (pylint,
  black, pep8)?

9. EngX-DevOps (CI/CD, Testing, Microservices, git):

- Fixtures?
  Можно использовать Fixtures, чтобы получить набор данных для тестирования. Вы можете использовать Fixtures, чтобы
  получить систему в известном состоянии перед запуском теста. Fixtures также используются для получения данных для
  нескольких тестов.
- Dependency management tools in python? Explain why we need those?
- What is doing git rebase command?
- S.O.L.I.D principals! What stands S for? What means Single Responsibility?

10. SQL

- Give me one window function example, what is doing?
- What type of indexing do you know? +
- Explain query execution order – put it in order - select, from, group by, where, order by
  https://techrocks.ru/2021/03/05/order-of-sql-operations/

11. Public cloud

- Explain storage services in the GCP
- SAAS vs PAAS?

12. Special tools – Apache Spark

- How Spark works
- What is a resource manager? What type of resource managers you are aware?
- Spark on Kubernetes. Why?

13. Flink Architecture

Java
Kuber
Terraform
Azure

## Kubernetes

Объекты:

- Pod
- Deployment
- Service
- Nodes
- Cluster

![Kubernetes workflow](/image/KubeWorkFlow.png)
Storage - записывается информация о нашем кластере  
Master - управление Node  
Worker Node - сервера где запускаются контейнеры

### Kubernetes Cloud Сервисы

- AWS: Amazon Elastic Kubernetes Service
- GCP: Google Kubernetes Engine
- Azure: Azure Kubernetes Service
- Alibaba:
- Yandex:

Pods или поды — это абстрактный объект в кластере K8S, который состоит из одного или нескольких контейнеров с общим
хранилищем и сетевыми ресурсами, а также спецификации для запуска контейнеров.  
Это главный объект в кластере, в нем прописаны, какие контейнеры должны быть запущены, количество экземпляров или
реплик, политика перезапуска, лимиты, подключаемые ресурсы, узел кластера для размещения.

**ReplicaSet** - управляет в каком кол-ве должно запускаться приложение
**Deployments** - контроллер, который управляет состоянием развертывания подов, которое описывается в манифесте, следит
за удалением и созданием экземпляров подов. Управляет контроллерами ReplicaSet.
**ReplicaSet** - гарантирует, что определенное количество экземпляров подов всегда будет запущено в кластере.
**StatefulSets** - так же как и Deployments, управляет развертыванием и масштабированием набора подов, но сохраняет
набор идентификаторов и состояние для каждого пода.
**DaemonSet** - гарантирует, что на каждом узле кластера будет присутствовать экземпляр пода.
**Jobs** - создает определенное количество подов и смотрит, пока они успешно не завершат работу. Если под завершился с
ошибкой, повторяет создание, которое мы описали определенное количество раз. Если под успешно отработал, записывает это
в свой журнал.
**CronJob** - запускает контроллеры Jobs по определенному расписанию.

Namespace или пространство имен — это абстрактный объект, который логически разграничивает и изолирует ресурсы между
подами. Вы можете рассматривать пространство имен как внутренний виртуальный кластер, который поможет вам изолировать
проекты или пользователей между собой, применить разные политики квот на свои проекты или выдать права доступа только на
определенную область.

### Файл .yaml

**apiVersion** — используемая для создания объекта версия API Kubernetes.   
**kind** — тип создаваемого объекта  
**metadata** — данные, позволяющие идентифицировать объект (name, UID и необязательное поле namespace)  
**spec** — требуемое состояние объекта

**Service discovery and load balancing???**

Master node знает где какой контейнер запущен

Если запущеный несколько копий Docker Container, произойдет автоматическйи load balancing

**Storage orchestration**

Можно добавить локальный диск или диск из AWS, GCP к докер контейнеру

**Automated rollouts and rollbacks**

Автоматичекое обновление на новую версию Docker Image или предыдущую(green blue deployment)

**Automatic bin packing**

Вы указываете сколько CPU и RAM нужно каждой коппии Docker Container, k8s выбирает на каких Woker Node запустить
Conteiner, что бы это было максимально эфективно и оптимально

**Self-healing**

Указываете сколько копий контейнеров вам нужно и k8s всегда будет поддерживать именно это кол-во

**Secret and configuration management**

K8s позволяет хранить сереты вне приложение в контейнерах


