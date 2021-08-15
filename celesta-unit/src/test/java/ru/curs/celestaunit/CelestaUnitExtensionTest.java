package ru.curs.celestaunit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import s1.HeaderCursor;
import s1.LineCursor;
import s1.Seq1Sequence;

import static org.junit.jupiter.api.Assertions.*;

public class CelestaUnitExtensionTest {

    @RegisterExtension
    static CelestaUnitExtension ext = CelestaUnitExtension.builder()
            .withReferentialIntegrity(true)
            .build();

    @Test
    public void extensionInitializedWithCorrectValues() {
        assertTrue(ext.isReferentialIntegrity());
        assertTrue(ext.isTruncateTables());
        assertTrue(ext.isResetSequences());
    }

    @Test
    public void defaultValuesAreCorrect() {
        CelestaUnitExtension ext = new CelestaUnitExtension();
        assertEquals(CelestaUnitExtension.DEFAULT_SCORE, ext.getScorePath());
        assertTrue(ext.isReferentialIntegrity());
        assertTrue(ext.isTruncateTables());
        assertTrue(ext.isResetSequences());
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

}
