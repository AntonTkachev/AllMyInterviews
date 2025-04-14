Как из postgres реплицировать данные в redshift?
Как в этой архитектуре будет использоваться CDC?


----

Для репликации из PostgreSQL в Redshift используем Debezium + Kafka + S3 + COPY.
CDC реализуется через Debezium, который слушает PostgreSQL (logical replication) и отправляет изменения (INSERT/UPDATE/DELETE) в Kafka.
Kafka Connect пишет данные в S3, откуда Redshift загружает их через COPY.