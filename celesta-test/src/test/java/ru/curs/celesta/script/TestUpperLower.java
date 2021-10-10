package ru.curs.celesta.script;

import org.junit.jupiter.api.TestTemplate;
import ru.curs.celesta.CallContext;
import upperlower.IdWithValueCursor;
import upperlower.UpperLowerCursor;

import static org.junit.jupiter.api.Assertions.*;

public class TestUpperLower implements ScriptTest {
    @TestTemplate
    void testUpperLowerInView(CallContext ctx) {
        IdWithValueCursor t = new IdWithValueCursor(ctx);
        UpperLowerCursor v = new UpperLowerCursor(ctx);

        t.setId(1);
        t.setValue("abc#ABC");
        t.insert();
        v.first();
        assertAll(
                () -> assertEquals("ABC#ABC", v.getV1()),
                () -> assertEquals("abc#abc", v.getV2()),
                () -> assertEquals("ABC#ABCabc#abc", v.getV3()),
                () -> assertEquals("ABC#ABCABC#ABC", v.getV4())
        );
    }


    @TestTemplate
    void testUpperLowerInComplexFilter(CallContext ctx) {
        IdWithValueCursor t = new IdWithValueCursor(ctx);
        t.setId(1);
        t.setValue("abC");
        t.insert();

        t.setId(2);
        t.setValue("DeF");
        t.insert();

        t.setComplexFilter("upper(value)='ABC'");
        t.first();
        assertEquals(1, t.getId());

        t.setComplexFilter("lower(value)='def'");
        t.first();
        assertEquals(2, t.getId());
    }
}
