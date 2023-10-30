package com.example;

import java.util.Arrays;

public class OrderedWord {

    public static void main(String[] args) {
        /*
         * 1. Дана строка только из латинских символов. Привести строку к нижнему регистру.
         * 2. Отсортировать символы в строке по возрастанию.
         * 3. Переместить восклицательный знак в конец строки (он в строке гарантированно один).
         * 4. Сделать первую букву строки заглавной.
         *  */
        String s = "YEa!bB";

        /* РЕШЕНИЕ */

        char[] arr = s.toLowerCase().toCharArray();
        Arrays.sort(arr);


        System.out.println(/* РЕЗУЛЬТАТ ТУТ */);
    }
}