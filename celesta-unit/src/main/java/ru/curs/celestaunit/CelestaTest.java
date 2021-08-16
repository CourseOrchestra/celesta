package ru.curs.celestaunit;

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for extending tests with CelestaUnitExtension.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@ExtendWith(CelestaUnitExtension.class)
public @interface CelestaTest {
    /**
     * Sets score path (maybe relative to project root).
     */
    String scorePath() default "";

    /**
     * Sets referential integrity (set to false to disable).
     */
    boolean referentialIntegrity() default true;

    /**
     * Sets tables truncation before each test.
     */
    boolean truncateTables() default true;

    /**
     * Resets sequences before each test.
     */
    boolean resetSequences() default true;
}
