package ru.curs.celesta.annotated.foo;

import ru.curs.celesta.annotation.CelestaProc;

public class Foo {

    @CelestaProc
    public static int annotatedFooMethod(int a, int b) {
        return a + b;
    }

    public void notAnnotatedFooMethod() {
    }

}
