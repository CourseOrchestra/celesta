package ru.curs.celestaunit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.curs.celesta.CallContext;
import s1.HeaderCursor;
import s1.LineCursor;
import s1.Seq1Sequence;

import static org.junit.jupiter.api.Assertions.assertEquals;

@CelestaTest(referentialIntegrity = false,
        truncateTables = false,
        resetSequences = false)
public class CelestaUnitAnnotatedExtensionTest {
    @Test
    void integrityCheckDoesNotWork(CallContext ctx) {
        try(LineCursor lc = new LineCursor(ctx)) {
            lc.setHeaderId(100);
            lc.setId(100);
            lc.insert();
        }
    }

    @Test
    @DisplayName("When truncateAfterEach is off, tables that are filled in a test...")
    void tablesNotTruncated1(CallContext ctx) {
        CelestaUnitExtensionTest.fillTwoTables(ctx);
    }

    @Test
    @DisplayName("...can be read in the following test.")
    void tablesNotTruncated2(CallContext ctx) {
        try(LineCursor lc = new LineCursor(ctx);
            HeaderCursor hc = new HeaderCursor(ctx)) {

            assertEquals(1, hc.count());
            assertEquals(1, lc.count());
        }
    }

    @Test
    @DisplayName("When resetSequences is on, and you get a value from sequence...")
    public void sequencesReset1(CallContext ctx){
        Seq1Sequence sequence = new Seq1Sequence(ctx);
        assertEquals(1, sequence.nextValue());
    }


    @Test
    @DisplayName("...this value persists in the following test")
    public void sequencesReset2(CallContext ctx){
        Seq1Sequence sequence = new Seq1Sequence(ctx);
        assertEquals(2, sequence.nextValue());
    }
}
