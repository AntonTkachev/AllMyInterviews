package com.example;

import java.util.Arrays;

public class OrderedWord {

    public static void main(String[] args) {
        String s = "YEa!bB";

        // 1. Приводим строку к нижнему регистру и превращаем в массив символов
        char[] arr = s.toLowerCase().toCharArray();

        // 2. Сортируем массив символов по возрастанию
        Arrays.sort(arr);

        // 3. Находим позицию восклицательного знака и формируем новую строку без него
        int exclIndex = -1;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == '!') {
                exclIndex = i;
                continue; // пропускаем восклицательный знак
            }
            sb.append(arr[i]);
        }
        // Добавляем восклицательный знак в конец
        sb.append('!');

        // 4. Делаем первую букву заглавной
        if (sb.length() > 0) {
            sb.setCharAt(0, Character.toUpperCase(sb.charAt(0)));
        }

        System.out.println(sb.toString());
    }
}