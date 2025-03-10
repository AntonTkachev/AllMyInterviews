### Изучить:

- Clouds дистрибутивы
- consistency levels
- SIEM & Security Analytics - Elastic

- spark speculation
- spark accumulator
- spark structured streaming

Structured Streaming в Apache Spark — это интерфейс для обработки потоковых данных в реальном времени с использованием
концепций, схожих с пакетной обработкой. Он позволяет обрабатывать непрерывно поступающие данные, обеспечивая поддержку
состояния и окон, а также упрощает интеграцию с различными источниками и приемниками данных.

Основные особенности:
• Обработка потоковых данных как batch-процессов: Данные разбиваются на микробатчи, что позволяет использовать те же API
для обработки, что и для пакетной обработки.
• Поддержка окон (windowing): Для агрегации данных с временными окнами.
• Watermarking: Для управления задержками и пропуском данных.
• Fault tolerance: Автоматическое восстановление после сбоев с сохранением состояния.
• Интеграция с различными источниками данных: Поддержка источников, таких как Kafka, HDFS, и других.

Structured Streaming идеально подходит для использования в сценариях, где необходимо обрабатывать данные в реальном
времени, поддерживая высокую производительность и гибкость.

- https://www.youtube.com/watch?v=NKEFWyqJ5XA
- https://www.youtube.com/watch?v=P3qmqUZJ7l0&t=6684s
- https://www.youtube.com/watch?v=xcN1V5FzdZ4&list=PL7ZG6NdDdT8NRHDU5shVgGjlua297bm-H&index=15

1. Data Warehouse, Data Lake, and Data Modeling concepts:

- Data Vault
  https://habr.com/ru/post/348188/

