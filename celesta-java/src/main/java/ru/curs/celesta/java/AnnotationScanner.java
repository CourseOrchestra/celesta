package ru.curs.celesta.java;

import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import ru.curs.celesta.java.annotation.CelestaProc;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.stream.Collectors;

final class AnnotationScanner {

    static Set<Method> scan(Set<String> scanningPackages) {
        return scanningPackages.stream()
                .map(AnnotationScanner::scan)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    private static Set<Method> scan(String scanningPackage) {
        Reflections reflections = new Reflections(scanningPackage, new MethodAnnotationsScanner());
        return reflections.getMethodsAnnotatedWith(CelestaProc.class);
    }
}
