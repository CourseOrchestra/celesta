package ru.curs.celesta.score;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class UnionAllViewParsingTest extends AbstractParsingTest {

    @Test
    public void testParsingNotFailsWhenUnionAllSyntaxIsCorrect() throws Exception {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "unionAll/testParsingNotFailsWhenUnionAllSyntaxIsCorrect.sql"
        );
        Grain g = parse(f);
        View testUnionAll = g.getView("testUnionAll");
        List<AbstractSelectStmt> segments = testUnionAll.getSegments();
        assertEquals(2, segments.size());
        assertEquals(2, segments.get(0).columns.size());
        assertEquals(2, segments.get(1).columns.size());
        assertEquals(ViewColumnType.INT, testUnionAll.getColumns().get("A").getColumnType());
        assertFalse(testUnionAll.getColumns().get("A").isNullable());
        assertEquals(ViewColumnType.TEXT, testUnionAll.getColumns().get("B").getColumnType());
        assertTrue(testUnionAll.getColumns().get("B").isNullable());

        ParameterizedView testUnionAllFunc = g.getParameterizedView("testUnionAllFunc");
        assertEquals(2, testUnionAllFunc.getSegments().size());
        assertEquals(3, testUnionAllFunc.getColumns().size());
        assertFalse(testUnionAllFunc.getColumns().get("f2A").isNullable());
        assertTrue(testUnionAllFunc.getColumns().get("B").isNullable());
    }

    @Test
    public void testParsingFailsWhenColumnNumberDiffers() {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "unionAll/testParsingFailsWhenColumnNumberDiffers.sql"
        );
        assertTrue(assertThrows(ParseException.class,
                () -> parse(f)).getMessage().contains("must have the same number"));
    }

    @Test
    public void testParsingFailsWhenColumnTypesDiffer() {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "unionAll/testParsingFailsWhenColumnTypesDiffer.sql"
        );
        String msg = assertThrows(ParseException.class, () -> parse(f)).getMessage();
        assertTrue(msg
                .contains("must match"), msg);
    }

    @Test
    public void testParsingFailsWhenParamsUnused() {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "unionAll/testFunction.sql"
        );
        String msg = assertThrows(ParseException.class, () -> parse(f)).getMessage();
        assertTrue(msg
                .contains("contains not used"), msg);
    }
}


