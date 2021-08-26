package ru.curs.celestaunit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import s1.HeaderCursor;
import s1.LineCursor;
import s1.LinecountCursor;
import s1.Seq1Sequence;

import static org.junit.jupiter.api.Assertions.*;

public class CelestaUnitExtensionTest {

    @RegisterExtension
    static CelestaUnitExtension ext = CelestaUnitExtension.builder()
            .withReferentialIntegrity(true)
            .build();

    @Test
    public void extensionInitializedWithCorrectValues() {
        CelestaUnitExtension.Parameters extParameters = ext.getParameters();
        assertTrue(extParameters.referentialIntegrity);
        assertTrue(extParameters.truncateTables);
        assertTrue(extParameters.resetSequences);
    }

    @Test
    public void defaultValuesAreCorrect() {
        CelestaUnitExtension ext = new CelestaUnitExtension();
        CelestaUnitExtension.Parameters extParameters = ext.getParameters();
        assertEquals(CelestaUnitExtension.DEFAULT_SCORE, extParameters.scorePath);
        assertTrue(extParameters.referentialIntegrity);
        assertTrue(extParameters.truncateTables);
        assertTrue(extParameters.resetSequences);
    }

    @Test
    public void integrityCheckWorks(CallContext ctx) {
        try(LineCursor lc = new LineCursor(ctx)) {
            lc.setHeader_id(100);
            lc.setId(100);
            assertTrue(
                    assertThrows(CelestaException.class,
                            () -> lc.insert())
                        .getMessage().contains("Referential integrity")
            );
        }
    }

    @Test
    @DisplayName("When truncateTables is on, and you fill the tables in a test...")
    public void tablesTruncated1(CallContext ctx) {
        fillTwoTables(ctx);
    }

    @Test
    @DisplayName("...their contents disappears in the following test.")
    public void tablesTruncated2(CallContext ctx) {
        try(HeaderCursor hc = new HeaderCursor(ctx);
            LineCursor lc = new LineCursor(ctx)) {

            assertEquals(0, hc.count());
            assertEquals(0, lc.count());
        }
    }

    @Test
    @DisplayName("When resetSequences is on, and you get a value from sequence...")
    public void sequencesReset1(CallContext ctx){
        Seq1Sequence sequence = new Seq1Sequence(ctx);
        assertEquals(1, sequence.nextValue());
    }


    @Test
    @DisplayName("...this value resets in the following test")
    public void sequencesReset2(CallContext ctx){
        Seq1Sequence sequence = new Seq1Sequence(ctx);
        assertEquals(1, sequence.nextValue());
    }

    public static void fillTwoTables(CallContext ctx) {
        try(HeaderCursor hc = new HeaderCursor(ctx);
            LineCursor lc = new LineCursor(ctx)) {
        
            hc.deleteAll();
            hc.setId(100);
            hc.insert();

            lc.deleteAll();
            lc.setId(10);
            lc.setHeader_id(100);
            lc.insert();

            assertEquals(1, hc.count());
            assertEquals(1, lc.count());
        }
    }

    @Test
    @DisplayName("When materialized views are involved...")
    void mvReset1(CallContext ctx) {
        HeaderCursor headerCursor = new HeaderCursor(ctx);
        headerCursor.setId(42);
        headerCursor.insert();
        LineCursor lineCursor = new LineCursor(ctx);
        lineCursor.setId(1).setHeader_id(headerCursor.getId()).insert();
        lineCursor.setId(2).setHeader_id(headerCursor.getId()).insert();
        LinecountCursor linecountCursor = new LinecountCursor(ctx);
        linecountCursor.get(42);
        //1+2
        assertEquals(3, linecountCursor.getLine_count());
    }

    @Test
    @DisplayName("...their content is cleared")
    void mvReset2(CallContext ctx) {
        HeaderCursor headerCursor = new HeaderCursor(ctx);
        headerCursor.setId(42);
        headerCursor.insert();
        LineCursor lineCursor = new LineCursor(ctx);
        lineCursor.setId(7);
        lineCursor.setHeader_id(headerCursor.getId());
        lineCursor.insert();
        LinecountCursor linecountCursor = new LinecountCursor(ctx);
        linecountCursor.get(42);
        assertEquals(7, linecountCursor.getLine_count());
    }
}
