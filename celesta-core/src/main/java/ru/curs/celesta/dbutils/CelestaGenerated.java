package ru.curs.celesta.dbutils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *  This annotation is used to mark source code that has been generated
 *  by Celesta. Unlike {@code javax.annotation.Generated}, this
 *  annotation has {@code CLASS} retention policy, which makes it
 *  visible by code coverage tools like JaCoCo.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.CLASS)
public @interface CelestaGenerated {
}
