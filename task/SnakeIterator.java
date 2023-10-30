package com.example;

/*
Given: a sequence of iterators with string members for simplicity.
Required to implement a new Iterator class which must provide "snake"-style iteratation through all the iterators passed.
Iterators may have different amount of elements inside or be empty. The first next() call of the "snake" iterator must return the first element of the first sub-iterator.
The second next() call must return the first element of the second sub-iterator and so on until all are done.
Example output for the init sequence below: a1 b1 c1 e1 a2 b2 c2 e2 a3 c3 e3 a4 c4 a5 a6.
*/

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

class SnakeIterator<T> implements Iterator<T> {
    private Iterator<Iterator<T>> iterators;
    private Iterator<T> currentIterator;

    public SnakeIterator(Iterator<T>... iterators) {
//        this.iterators = iterators;
        this.iterators = Arrays.stream(iterators).iterator();
    }

    @Override
    public boolean hasNext() {
        return iterators.hasNext() && currentIterator.hasNext();
    }

    @Override
    public T next() {
        if (iterators.hasNext()) {
            currentIterator = iterators.next();
            if (currentIterator.hasNext()) {
                return currentIterator.next();
            }
        }
    }
    // YOUR IMPLEMENTATION HERE
}

public class SnakeIterators {
    public static void main(String[] args) {
        Iterator<String> a = List.of("a1", "a2", "a3", "a4", "a5", "a6").iterator();
        Iterator<String> b = List.of("b1", "b2").iterator();
        Iterator<String> c = List.of("c1", "c2", "c3", "c4").iterator();
        Iterator<String> d = Collections.emptyIterator();
        Iterator<String> e = List.of("e1", "e2", "e3").iterator();

        Iterator<String> x = new SnakeIterator<>(a, b, c, d, e);

        while (x.hasNext()) {
            System.out.println(x.next());
        }
    }
}
