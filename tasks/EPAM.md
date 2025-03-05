// CODE EXAMPLE VALID FOR COMPILING
class Main {
public static void main(String[] args) {
System.out.println("Hello, World!");
}
}


public class Movie {
private int id;
private String title;
private int year;
private String imdb;
private List<Director> directors;

public class Director {
private int id;
private String name;
private String imdb;
}

Надо вернуть имя режисера и кол-во фильмов, которые он снял и они начинается c J

----
// CODE EXAMPLE VALID FOR COMPILING
class Main {
public static void main(String[] args) {
List<Integer> integers = List.of(1, 2, 3, 4, 5);
Stream<Integer> integerStream = integers.stream().map(i -> i * 2);

       integerStream.forEach(System.out::println);
        List<Integer> list = integerStream.collect(Collectors.toList());
    }

}

В чем проблема кода

---


class Parent {
void methodA() throws IOException {
System.out.println("Parent: methodA");
}
}

class Child extends Parent {
void methodA() throws Exception {
System.out.println("Child: methodA");
}
}


Какие проблемы могут быть?