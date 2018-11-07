package ru.curs.celesta.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public interface DbUpdaterTest {

    @RegisterExtension
    DbUpdaterProvider dbUpdaterProvider = new DbUpdaterProvider();

    @BeforeAll
    default void beforeAll() {
        dbUpdaterProvider.startDbs();
    }

    @AfterAll
    default void afterAll() {
        dbUpdaterProvider.stopDbs();
    }

}
