package ru.curs.celesta.score.discovery.fk;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.*;


import static org.junit.jupiter.api.Assertions.*;

public class FkDefaultScoreDiscoveryTest {

    private static final String GRAIN_NAME = "test";
    private static final String A_TABLE = "a";
    private static final String B_TABLE = "b";
    private static final String C_TABLE = "c";

    @Test
    @DisplayName("Score is parsed correctly when first file depends on second by order")
    void testADependOnB() throws Exception {
        AbstractScore s = ScoreUtil.createCelestaSqlTestScore(
                this.getClass(),
                "a_depends_on_b"
        );

        Grain g = s.getGrains().get(GRAIN_NAME);

        assertAll(
                () -> assertNotNull(g),
                () -> assertEquals(2, g.getElements(Table.class).size()),
                () -> assertDoesNotThrow(() -> g.getElement(A_TABLE, Table.class)),
                () -> assertDoesNotThrow(() -> g.getElement(B_TABLE, Table.class))
        );

        Table a = g.getElement(A_TABLE, Table.class);
        Table b = g.getElement(B_TABLE, Table.class);

        assertAll(
                () -> assertEquals(1, a.getForeignKeys().size()),
                () -> assertSame(b, a.getForeignKeys().stream().findFirst().get().getReferencedTable())
        );
    }

    @Test
    @DisplayName("Score is parsed correctly when second file depends on first by order")
    void testBDependOnA() throws Exception {
        AbstractScore s = ScoreUtil.createCelestaSqlTestScore(
                this.getClass(),
                "b_depends_on_a"
        );

        Grain g = s.getGrains().get(GRAIN_NAME);

        assertAll(
                () -> assertNotNull(g),
                () -> assertEquals(2, g.getElements(Table.class).size()),
                () -> assertDoesNotThrow(() -> g.getElement(A_TABLE, Table.class)),
                () -> assertDoesNotThrow(() -> g.getElement(B_TABLE, Table.class))
        );

        Table a = g.getElement(A_TABLE, Table.class);
        Table b = g.getElement(B_TABLE, Table.class);

        assertAll(
                () -> assertEquals(1, b.getForeignKeys().size()),
                () -> assertSame(a, b.getForeignKeys().stream().findFirst().get().getReferencedTable())
        );
    }


    @Test
    @DisplayName("Score is parsed correctly when two files depend on each other")
    void testDependencyOnEachOther() throws Exception {
        AbstractScore s = ScoreUtil.createCelestaSqlTestScore(
                this.getClass(),
                "dependency_on_each_other"
        );

        Grain g = s.getGrains().get(GRAIN_NAME);

        assertAll(
                () -> assertNotNull(g),
                () -> assertEquals(3, g.getElements(Table.class).size()),
                () -> assertDoesNotThrow(() -> g.getElement(A_TABLE, Table.class)),
                () -> assertDoesNotThrow(() -> g.getElement(B_TABLE, Table.class)),
                () -> assertDoesNotThrow(() -> g.getElement(C_TABLE, Table.class))
        );

        Table a = g.getElement(A_TABLE, Table.class);
        Table b = g.getElement(B_TABLE, Table.class);
        Table c = g.getElement(C_TABLE, Table.class);

        assertAll(
                () -> assertEquals(1, a.getForeignKeys().size()),
                () -> assertSame(b, a.getForeignKeys().stream().findFirst().get().getReferencedTable()),
                () -> assertEquals(1, c.getForeignKeys().size()),
                () -> assertSame(a, c.getForeignKeys().stream().findFirst().get().getReferencedTable())
        );
    }

    @Test
    @DisplayName("Score parsing fails on explicit cycle")
    void testFailsOnExplicitCycle() {
        assertThrows(
                CelestaException.class,
                () -> ScoreUtil.createCelestaSqlTestScore(
                        this.getClass(),
                        "explicit_cycle"
                ));

    }

    @Test
    @DisplayName("Score parsing fails on implicit cycle")
    void testFailsOnImplicitCycle() {
        CelestaException e = assertThrows(
                CelestaException.class,
                () -> ScoreUtil.createCelestaSqlTestScore(
                        this.getClass(),
                        "implicit_cycle"
                ));

        String expectedMessage = String.format(
                ReferenceResolver.CYCLE_REFERENCE_ERROR_MESSAGE_TEMPLATE,
                String.join(" -> ", "test.a", "test.b", "test.c", "test.a")
        );
        assertEquals(expectedMessage, e.getMessage());
    }

    @Test
    @DisplayName("Score parsing fails on implicit deep cycle")
    void testFailsOnDeepImplicitCycle() {
        CelestaException e = assertThrows(
                CelestaException.class,
                () -> ScoreUtil.createCelestaSqlTestScore(
                        this.getClass(),
                        "implicit_deep_cycle"
                ));

        String expectedMessage = String.format(
                ReferenceResolver.CYCLE_REFERENCE_ERROR_MESSAGE_TEMPLATE,
                String.join(" -> ", "schema1.b", "schema2.c", "schema1.d", "schema1.b")
        );
        assertEquals(expectedMessage, e.getMessage());
    }
}