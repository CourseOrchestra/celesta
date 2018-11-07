package ru.curs.celestaunit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import s1.HeaderCursor;
import s1.LineCursor;

import static org.junit.jupiter.api.Assertions.*;

public class CelestaUnitExtensionTest {
    public static final String SCORE_PATH = "src/test/resources/score";
    @RegisterExtension
    static CelestaUnitExtension ext =
            CelestaUnitExtension.builder().withScorePath(SCORE_PATH)
                    .withReferentialIntegrity(true).build();

    @Test
    void extensionInitializedWithCorrectValues() {
        assertEquals(SCORE_PATH, ext.getScorePath());
        assertTrue(ext.isReferentialIntegrity());
        assertTrue(ext.isTruncateAfterEach());
    }

    @Test
    void defaultValuesAreCorrect() {
        CelestaUnitExtension ext = new CelestaUnitExtension();
        assertEquals(CelestaUnitExtension.DEFAULT_SCORE, ext.getScorePath());
        assertTrue(ext.isReferentialIntegrity());
        assertTrue(ext.isTruncateAfterEach());
    }

    @Test
    void integrityCheckWorks(CallContext ctx) {
        LineCursor lc = new LineCursor(ctx);
        lc.setHeader_id(100);
        lc.setId(100);
        assertTrue(
                assertThrows(CelestaException.class,
                        () -> lc.insert()).getMessage().contains("Referential integrity"));
    }

    @Test
    @DisplayName("When truncateAfterEach is on, and you fill the tables in a test...")
    void tablesTruncated1(CallContext ctx) {
        fillTwoTables(ctx);
    }

    @Test
    @DisplayName("...their contents disappears in the following test.")
    void tablesTruncated2(CallContext ctx) {
        HeaderCursor hc = new HeaderCursor(ctx);
        LineCursor lc = new LineCursor(ctx);
        assertEquals(0, hc.count());
        assertEquals(0, lc.count());
    }

    public static void fillTwoTables(CallContext ctx) {
        HeaderCursor hc = new HeaderCursor(ctx);
        hc.deleteAll();
        hc.setId(100);
        hc.insert();

        LineCursor lc = new LineCursor(ctx);
        lc.deleteAll();
        lc.setId(10);
        lc.setHeader_id(100);
        lc.insert();

        assertEquals(1, hc.count());
        assertEquals(1, lc.count());
    }
}
