package ru.curs.celesta.script;

import org.junit.jupiter.api.TestTemplate;
import ru.curs.celesta.CallContext;
import substrng.IdWithValueStartLenCursor;
import substrng.SubstrngViewCursor;

import static org.junit.jupiter.api.Assertions.*;

public class TestSubstring implements ScriptTest {
    @TestTemplate
    void testSubstringInViewPgExample(CallContext ctx) {
        IdWithValueStartLenCursor cursor = new IdWithValueStartLenCursor(ctx);
        cursor.setId(1);
        cursor.setValue("Thomas");
        cursor.setStart(2);
        cursor.setLen(3);
        cursor.insert();

        SubstrngViewCursor viewCursor = new SubstrngViewCursor(ctx);
        viewCursor.first();

        assertAll(
                () -> assertEquals("hom", viewCursor.getV1()),
                () -> assertEquals("hom", viewCursor.getV2()),
                () -> assertEquals("omas", viewCursor.getV3()),
                () -> assertEquals("homfoo", viewCursor.getV4())
        );
    }

    @TestTemplate
    void testSubstringInViewZeroPos(CallContext ctx) {
        IdWithValueStartLenCursor cursor = new IdWithValueStartLenCursor(ctx);
        cursor.setId(1);
        cursor.setValue("Thomas");
        //Zero should be treated as 1 (as in Oracle and H2)
        cursor.setStart(0);
        cursor.setLen(3);
        cursor.insert();

        SubstrngViewCursor viewCursor = new SubstrngViewCursor(ctx);
        viewCursor.first();
        assertAll(
                () -> assertEquals("Tho", viewCursor.getV1()),
                () -> assertEquals("hom", viewCursor.getV2()),
                () -> assertEquals("Tho", viewCursor.getV3())
        );
    }
}
