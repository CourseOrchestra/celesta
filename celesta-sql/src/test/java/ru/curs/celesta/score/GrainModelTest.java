package ru.curs.celesta.score;


import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.exception.CelestaParseException;

public class GrainModelTest {

    private AbstractScore s = new CelestaSqlTestScore();

    @Test
    void test1() throws ParseException {
        Grain g = new Grain(s, "grain1");
        assertSame(g, s.getGrain("grain1"));

        GrainPart gp = new GrainPart(g, true, null);

        Table t = new Table(gp, "table1");
        (new IntegerColumn(t, "a")).setNullableAndDefault(false, "IDENTITY");
        new IntegerColumn(t, "b").setNullableAndDefault(false, "0");
        new IntegerColumn(t, "c").setNullableAndDefault(false, "0");
        new IntegerColumn(t, "d").setNullableAndDefault(false, "0");
        new BinaryColumn(t, "e");
        t.addPK("a");
        t.finalizePK();

        assertEquals(5, t.getColumns().size());
        assertEquals(0, t.getColumnIndex("a"));
        assertEquals(2, t.getColumnIndex("c"));

        final Index ind = new Index(gp, "table1", "aa_i1");
        ind.addColumn("b");
        ind.addColumn("d");
        ind.finalizeIndex();

        assertEquals(1, g.getIndices().size());
        assertSame(ind, g.getIndices().get("aa_i1"));
        assertEquals(2, ind.getColumns().size());

        // Нельзя вставить в модель два индекса с одним и тем же именем.
        assertThrows(ParseException.class, () -> new Index(gp, "table1", "aa_i1"));

        final Index ind2 = new Index(gp, "table1", "aa_i2");

        // Нельзя индексировать IMAGE-поля.
        assertThrows(ParseException.class, () -> ind2.addColumn("e"));


        ind2.addColumn("c");
        ind2.addColumn("d");

        // Нельзя дважды вставить в индекс одно и то же поле.
        assertThrows(ParseException.class, () -> ind2.addColumn("c"));

        ind2.finalizeIndex();

        assertEquals(2, g.getIndices().size());
        assertSame(ind2, g.getIndices().get("aa_i2"));
        assertEquals(2, ind2.getColumns().size());

        // Нельзя создавать полностью дублирующиеся индексы.
        final Index ind3 = new Index(gp, "table1", "aa_i3");
        ind3.addColumn("b");
        ind3.addColumn("d");
        assertThrows(ParseException.class, ind3::finalizeIndex);


        ind3.addColumn("c");
        // Нельзя индексировать BLOB-колонки
        assertThrows(ParseException.class,
                () -> ind3.addColumn("e")
        );
        ind3.finalizeIndex();
        assertEquals(3, g.getIndices().size());
        assertSame(ind3, g.getIndices().get("aa_i3"));
        assertEquals(3, ind3.getColumns().size());
    }

    @Test
    void test2() throws ParseException {
        // Корректное и некорректное добавление таблицы
        Grain g = new Grain(s, "grain2");
        GrainPart gp = new GrainPart(g, true, null);
        new Table(gp, "aa");
        new Table(gp, "bb");
        assertEquals(2, g.getElements(Table.class).size());
        assertThrows(ParseException.class,
                () -> new Table(gp, "aa"));

        assertEquals(2, g.getElements(Table.class).size());

        assertEquals("aa", g.getElements(Table.class).get("aa").getName());
        final Table t = g.getElements(Table.class).get("bb");
        assertEquals("bb", t.getName());
        // Корректное и некорректное добавление поля
        new IntegerColumn(t, "col1");
        Column c = new DateTimeColumn(t, "col2");
        c.setNullableAndDefault(false, "GETDATE");
        assertTrue(((DateTimeColumn) c).isGetdate());
        assertFalse(c.isNullable());

        c = new StringColumn(t, "col3");
        c.setNullableAndDefault(false, "'-'");
        assertEquals(3, t.getColumns().size());

        assertThrows(ParseException.class,
                () -> new DateTimeColumn(t, "col2")
        );
        assertEquals(3, t.getColumns().size());
        c = t.getColumns().get("col2");
        assertFalse(c.isNullable());
        assertTrue(((DateTimeColumn) c).isGetdate());
        assertEquals("col2", c.getName());
        // Корректное и некорректное добавление первичного ключа
        t.addPK("col2");
        assertThrows(ParseException.class,
                () -> t.addPK("blahblah"));
        t.addPK("col3");
        Map<String, Column> key = t.getPrimaryKey();
        assertEquals(2, key.size());
        Iterator<Column> ic = key.values().iterator();
        c = ic.next();
        assertEquals("col2", c.getName());
        c = ic.next();
        assertEquals("col3", c.getName());
        t.finalizePK();
        assertThrows(ParseException.class,
                () -> t.addPK("col1"));

        t.finalizePK(); // вызывать можно более одного раза, если PK определён

        // вызывать нельзя ни разу, если PK не определён
        final Table t2 = g.getElements(Table.class).get("aa");
        assertThrows(ParseException.class, t2::finalizePK);
    }

    @Test
    void testThatUnderscoreIsNotAllowedInGrainName() {
        assertAll(
                () -> assertThrows(ParseException.class, () -> new Grain(s, "_")),
                () -> assertThrows(ParseException.class, () -> new Grain(s, "_foo")),
                () -> assertThrows(ParseException.class, () -> new Grain(s, "bar_")),
                () -> assertThrows(ParseException.class, () -> new Grain(s, "foo_bar"))
        );
    }

}
