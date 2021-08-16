package ru.curs.celestaunit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.curs.celesta.CallContext;
import s1.HeaderCursor;

import static org.junit.jupiter.api.Assertions.assertEquals;

@CelestaTest
public class BeforeEachContextTest {
    @BeforeEach
    void fillTables(CallContext ctx) {
        System.out.println("BEFORE EACH IN TEST");
        HeaderCursor hc = new HeaderCursor(ctx);
        hc.setId(100);
        hc.insert();
    }

    @Test
    void foo(CallContext ctx){
        HeaderCursor hc = new HeaderCursor(ctx);
        assertEquals(1, hc.count());
    }
}
