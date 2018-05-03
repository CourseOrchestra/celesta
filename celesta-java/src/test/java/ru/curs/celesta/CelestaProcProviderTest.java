package ru.curs.celesta;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.annotated.bar.Bar;
import ru.curs.celesta.annotated.foo.Foo;
import ru.curs.celesta.annotated.returnvoid.ReturnVoid;

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
                        Arrays.asList("ru.curs.celesta.annotated"))
        );

        CelestaProcProvider procProvider = new CelestaProcProvider(methods);

        String fooQualifier = "ru.curs.celesta.annotated.foo.Foo#annotatedFooMethod";
        String barQualifier = "ru.curs.celesta.annotated.bar.Bar#annotatedBarMethod";
        String noReturnValueQualifier = "ru.curs.celesta.annotated.returnvoid.ReturnVoid#noReturnValue";

        CelestaProcMeta fooProcMeta = procProvider.get(fooQualifier);
        CelestaProcMeta barProcMeta = procProvider.get(barQualifier);
        CelestaProcMeta noReturnValueProcMeta = procProvider.get(noReturnValueQualifier);

        Method fooMethod = getMethodByClass(methods, Foo.class);
        Method barMethod = getMethodByClass(methods, Bar.class);
        Method noReturnValueMethod = getMethodByClass(methods, ReturnVoid.class);

        assertAll(
                () -> assertNotNull(barProcMeta),
                () -> assertEquals(barMethod, barProcMeta.getMethod()),
                () -> assertTrue(barProcMeta.isNeedClassInstantiation()),
                () -> assertTrue(barProcMeta.isNeedInjectCallContext()),
                () -> assertNotNull(fooProcMeta),
                () -> assertEquals(fooMethod, fooProcMeta.getMethod()),
                () -> assertFalse(fooProcMeta.isNeedClassInstantiation()),
                () -> assertFalse(fooProcMeta.isNeedInjectCallContext()),
                () -> assertNotNull(noReturnValueProcMeta),
                () -> assertEquals(noReturnValueMethod, noReturnValueProcMeta.getMethod()),
                () -> assertTrue(noReturnValueProcMeta.isNeedClassInstantiation()),
                () -> assertTrue(noReturnValueProcMeta.isNeedInjectCallContext())
        );
    }


    @Test
    void testFailWhenCallContextIsNotFirstArgument() {
        Set<Method> methods = AnnotationScanner.scan(
                new LinkedHashSet<>(
                        Arrays.asList("ru.curs.celesta.bad.annotated.callcontextnotfirst"))
        );

        assertThrows(CelestaException.class, () -> new CelestaProcProvider(methods));
    }

    @Test
    void testFailWhenCallContextCountIsMoreThanOne() {
        Set<Method> methods = AnnotationScanner.scan(
                new LinkedHashSet<>(
                        Arrays.asList("ru.curs.celesta.bad.annotated.twocallcontexts"))
        );

        assertThrows(CelestaException.class, () -> new CelestaProcProvider(methods));
    }

    private Method getMethodByClass(Set<Method> methods, Class c) {
        return methods.stream()
                .filter(method -> method.getDeclaringClass().equals(c))
                .findFirst().orElse(null);
    }
}
