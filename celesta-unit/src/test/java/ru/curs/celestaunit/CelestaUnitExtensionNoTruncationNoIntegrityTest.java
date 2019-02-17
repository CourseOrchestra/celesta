package ru.curs.celestaunit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import ru.curs.celesta.CallContext;
import s1.HeaderCursor;
import s1.LineCursor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class CelestaUnitExtensionNoTruncationNoIntegrityTest {

    @RegisterExtension
    static CelestaUnitExtension ext = CelestaUnitExtension.builder()
                    .withReferentialIntegrity(false)
                    .withTruncateAfterEach(false)
                    .build();

    @Test
    void extensionInitializedWithCorrectValues() {
        assertFalse(ext.isReferentialIntegrity());
        assertFalse(ext.isTruncateAfterEach());
    }

    @Test
    void integrityCheckDoesNotWork(CallContext ctx) {
        try(LineCursor lc = new LineCursor(ctx)) {
            lc.setHeader_id(100);
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

}
