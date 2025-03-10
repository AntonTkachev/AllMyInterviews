NULL ISNULL???[interview.md](interview.md)

https://docs.microsoft.com/ru-ru/office/troubleshoot/access/database-normalization-description

Сложность фибоначи

Для обработки большого файла при ограниченной RAM можно использовать следующие подходы:

1. Потоковая обработка (Stream API):

- Использование BufferedReader для построчного чтения
- Files.lines() для работы со строками как потоком
- Не загружает весь файл в память

1. Разбиение файла на части (Chunking):

- Чтение файла фиксированными блоками
- Обработка каждого блока отдельно
- Объединение результатов

1. Memory-mapped файлы:

- Использование FileChannel и MappedByteBuffer
- Прямой доступ к файлу без загрузки в память
- Эффективно для очень больших файлов

1. Внешняя сортировка:

- Если требуется сортировка данных
- Разбиение на отсортированные части
- Слияние частей

Technical Skills
• Java Core
• New Java Features
• JSP, Servlets, Filters
• SQL
• ORM
• Design Patterns
• Web Services
• Messaging
• Spring
• Spring Boot
• Testing
• Version Controls
• CI /CD
• Code Reading / Writing
• Software Development Methodology:
Scrum/Kanban
• Leadership
• Mentoring
• Customer communication

```java
public class ServiceA {
    @Transactional
    public void method1() {
        method2();
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void method2() {

    }
}
```