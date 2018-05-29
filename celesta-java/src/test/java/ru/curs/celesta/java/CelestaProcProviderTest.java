package ru.curs.celesta.java;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.java.annotated.bar.Bar;
import ru.curs.celesta.java.annotated.foo.Foo;
import ru.curs.celesta.java.annotated.returnvoid.ReturnVoid;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class CelestaProcProviderTest {

    @Test
    void testCelestaProcMeta() {
        Set<Method> methods = AnnotationScanner.scan(
                new LinkedHashSet<>(
                        Arrays.asList("ru.curs.celesta.java.annotated"))
        );

        CelestaProcProvider procProvider = new CelestaProcProvider(methods);

        String fooQualifier = "ru.curs.celesta.java.annotated.foo.Foo#annotatedFooMethod";
        String barQualifier = "ru.curs.celesta.java.annotated.bar.Bar#annotatedBarMethod";
        String noReturnValueQualifier = "ru.curs.celesta.java.annotated.returnvoid.ReturnVoid#noReturnValue";

        CelestaProcMeta fooProcMeta = procProvider.get(fooQualifier);
        CelestaProcMeta barProcMeta = procProvider.get(barQualifier);
        CelestaProcMeta noReturnValueProcMeta = procProvider.get(noReturnValueQualifier);

        Method fooMethod = getMethodByClass(methods, Foo.class);
        Method barMethod = getMethodByClass(methods, Bar.class);
        Method noReturnValueMethod = getMethodByClass(methods, ReturnVoid.class);

        assertAll(
                () -> assertNotNull(barProcMeta),
                () -> assertEquals(barMethod, barProcMeta.getMethod()),
                () -> assertTrue(barProcMeta.isClassInstantiationNeeded()),
                () -> assertTrue(barProcMeta.isCallContextInjectionNeeded()),
                () -> assertNotNull(fooProcMeta),
                () -> assertEquals(fooMethod, fooProcMeta.getMethod()),
                () -> assertFalse(fooProcMeta.isClassInstantiationNeeded()),
                () -> assertFalse(fooProcMeta.isCallContextInjectionNeeded()),
                () -> assertNotNull(noReturnValueProcMeta),
                () -> assertEquals(noReturnValueMethod, noReturnValueProcMeta.getMethod()),
                () -> assertTrue(noReturnValueProcMeta.isClassInstantiationNeeded()),
                () -> assertTrue(noReturnValueProcMeta.isCallContextInjectionNeeded())
        );
    }


    @Test
    void testFailWhenCallContextIsNotFirstArgument() {
        Set<Method> methods = AnnotationScanner.scan(
                new LinkedHashSet<>(
                        Arrays.asList("ru.curs.celesta.java.bad.annotated.callcontextnotfirst"))
        );

        assertThrows(CelestaException.class, () -> new CelestaProcProvider(methods));
    }

    @Test
    void testFailWhenCallContextCountIsMoreThanOne() {
        Set<Method> methods = AnnotationScanner.scan(
                new LinkedHashSet<>(
                        Arrays.asList("ru.curs.celesta.java.bad.annotated.twocallcontexts"))
        );

        assertThrows(CelestaException.class, () -> new CelestaProcProvider(methods));
    }

    private Method getMethodByClass(Set<Method> methods, Class c) {
        return methods.stream()
                .filter(method -> method.getDeclaringClass().equals(c))
                .findFirst().orElse(null);
    }
}
