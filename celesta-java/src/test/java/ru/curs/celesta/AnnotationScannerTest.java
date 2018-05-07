package ru.curs.celesta;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.annotated.bar.Bar;
import ru.curs.celesta.annotated.foo.Foo;
import ru.curs.celesta.annotated.returnvoid.ReturnVoid;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class AnnotationScannerTest {

    @Test
    void testNoPackages() {
        Set<Method> methods = AnnotationScanner.scan(Collections.emptySet());
        assertTrue(methods.isEmpty());
    }

    @Test
    void testScanSinglePackage() {
        Set<Method> methods = AnnotationScanner.scan(new LinkedHashSet<>(Arrays.asList("ru.curs.celesta.annotated.bar")));
        Method method = methods.stream().findFirst().orElse(null);

        assertAll(
                () -> assertEquals(1, methods.size()),
                () -> assertEquals(Bar.class, method.getDeclaringClass()),
                () -> assertEquals("annotatedBarMethod", method.getName())
        );
    }

    @Test
    void testScanMultiplePackages() {
        Set<Method> methods = AnnotationScanner.scan(
                new LinkedHashSet<>(
                        Arrays.asList("ru.curs.celesta.annotated.bar", "ru.curs.celesta.annotated.foo"))
        );

        assertEquals(2, methods.size());
        assertFooAndBar(methods);
    }

    @Test
    void testFullPackageWithNested() {
        Set<Method> methods = AnnotationScanner.scan(
                new LinkedHashSet<>(
                        Arrays.asList("ru.curs.celesta.annotated"))
        );

        assertEquals(3, methods.size());
        assertFooAndBar(methods);
        Method noReturnValueMethod = methods.stream()
                .filter(method -> method.getDeclaringClass().equals(ReturnVoid.class))
                .findFirst().orElse(null);

        assertAll(
                () -> assertEquals(ReturnVoid.class, noReturnValueMethod.getDeclaringClass()),
                () -> assertEquals("noReturnValue", noReturnValueMethod.getName())
        );
    }

    private void assertFooAndBar(Set<Method> methods) {
        Method barMethod = methods.stream()
                .filter(method -> method.getDeclaringClass().equals(Bar.class))
                .findFirst().orElse(null);
        Method fooMethod = methods.stream()
                .filter(method -> method.getDeclaringClass().equals(Foo.class))
                .findFirst().orElse(null);

        assertAll(
                () -> assertEquals(Bar.class, barMethod.getDeclaringClass()),
                () -> assertEquals("annotatedBarMethod", barMethod.getName()),
                () -> assertEquals(Foo.class, fooMethod.getDeclaringClass()),
                () -> assertEquals("annotatedFooMethod", fooMethod.getName())
        );
    }
}
