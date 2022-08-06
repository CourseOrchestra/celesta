package ru.curs.celesta.script;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public interface ScriptTest {
    @RegisterExtension
    CallContextProvider callContextProvider = new CallContextProvider();


    @BeforeAll
    default void startCelestas() {
        callContextProvider.startCelestas();
    }


    /*
    @AfterAll
    default void stopCelestas() {
        callContextProvider.stopCelestas();
    }*/

    @AfterEach
    default void closeContext() {
        callContextProvider.closeCurrentContext();
    }
}
