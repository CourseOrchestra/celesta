package ru.curs.celesta.java.annotated.foo;

import ru.curs.celesta.java.annotation.CelestaProc;

public class Foo {

    @CelestaProc
    public static int annotatedFooMethod(int a, int b) {
        return a + b;
    }

    public void notAnnotatedFooMethod() {
    }

}
