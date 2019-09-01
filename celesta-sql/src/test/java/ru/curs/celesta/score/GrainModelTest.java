package ru.curs.celesta.score;


import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class GrainModelTest {

    private AbstractScore s = new CelestaSqlTestScore();

    @Test
    void test1() throws ParseException {
        Grain g = new Grain(s, "grain1");
        assertSame(g, s.getGrain("grain1"));

        GrainPart gp = new GrainPart(g, true, null);

        SequenceElement se = new SequenceElement(gp, "table1_a");

        BasicTable t = new Table(gp, "table1");
        (new IntegerColumn(t, "a")).setNullableAndDefault(false, "NEXTVAL(" + se.getName() + ")");
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
        assertEquals(2, g.getElements(BasicTable.class).size());
        assertThrows(ParseException.class,
                () -> new Table(gp, "aa"));

        assertEquals(2, g.getElements(BasicTable.class).size());

        assertEquals("aa", g.getElements(BasicTable.class).get("aa").getName());
        final BasicTable t = g.getElements(BasicTable.class).get("bb");
        assertEquals("bb", t.getName());
        // Корректное и некорректное добавление поля
        new IntegerColumn(t, "col1");
        Column<?> c = new DateTimeColumn(t, "col2");
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
        Map<String, Column<?>> key = t.getPrimaryKey();
        assertEquals(2, key.size());
        Iterator<Column<?>> ic = key.values().iterator();
        c = ic.next();
        assertEquals("col2", c.getName());
        c = ic.next();
        assertEquals("col3", c.getName());
        t.finalizePK();
        assertThrows(ParseException.class,
                () -> t.addPK("col1"));

        t.finalizePK(); // вызывать можно более одного раза, если PK определён

        // вызывать нельзя ни разу, если PK не определён
        final BasicTable t2 = g.getElements(BasicTable.class).get("aa");
        assertThrows(ParseException.class, t2::finalizePK);
    }

    @Test
    void test3() throws ParseException {
        Grain g = new Grain(s, "grain3");
        GrainPart gp = new GrainPart(g, true, null);
        SequenceElement st1 = new SequenceElement(gp, "t1_ida");
        BasicTable t1 = new Table(gp, "t1");
        Column<?> cc = new IntegerColumn(t1, "ida");
        cc.setNullableAndDefault(false, "NEXTVAL(" + st1.getName() + ")");

        t1.addPK("ida");
        t1.finalizePK();
        new IntegerColumn(t1, "intcol");
        new DateTimeColumn(t1, "datecol");

        SequenceElement st2 = new SequenceElement(gp, "t2_idb");
        BasicTable t2 = new Table(gp, "t2");
        cc = new IntegerColumn(t2, "idb");
        cc.setNullableAndDefault(false, "NEXTVAL(" + st2.getName() + ")");
        t2.addPK("idb");
        t2.finalizePK();

        new IntegerColumn(t2, "intcol");
        new DateTimeColumn(t2, "datecol");
        StringColumn c = new StringColumn(t2, "scol2");
        c.setLength("2");

        c = new StringColumn(t2, "scol5");
        c.setLength("5");

        assertEquals(2, g.getElements(BasicTable.class).size());
        assertSame(g, t1.getGrain());
        assertSame(g, t2.getGrain());

        // Неизвестную колонку в FK нельзя включать!
        assertThrows(ParseException.class,
                () -> new ForeignKey(t1).addColumn("abracadabra"));


        // Устанавливаем связь t1 --> t2 по простому внешнему ключу
        assertEquals(0, t1.getForeignKeys().size());
        final ForeignKey fk = new ForeignKey(t1);
        fk.addColumn("intcol");
        // Дважды одну и ту же колонку в FK нельзя включать!
        assertThrows(ParseException.class,
                () -> fk.addColumn("intcol"));
        assertEquals(0, t1.getForeignKeys().size());

        // Установка referencedtable финализирует внешний ключ
        fk.setReferencedTable("", "t2");
        assertEquals(1, t1.getForeignKeys().size());
        assertEquals(1, t1.getForeignKeys().iterator().next().getColumns().size());
        assertSame(t2, fk.getReferencedTable());

        // Несуществующее поле нельзя добавить в ссылку
        assertThrows(ParseException.class,
                () -> fk.addReferencedColumn("blahblah"));

        // Просто поле добавить можно
        fk.addReferencedColumn("intcol");

        // Но в момент финализации происходит проверка, что мы указали
        // первичный ключ.
        assertThrows(ParseException.class, fk::finalizeReference);
        // Для удобства тестирования и экономии памяти список ссылок
        // подчищается сразу за финализацией, он нигде не хранится и нигде
        // не доступен. Его единственная роль -- проверять правильность
        // текста.

        fk.addReferencedColumn("idb");
        fk.finalizeReference();

        // Проверяем невозможность вставки двух идентичных FK в одну и ту же
        // таблицу
        final ForeignKey fk2 = new ForeignKey(t1);
        fk2.addColumn("intcol");
        assertThrows(ParseException.class,
                () -> fk2.setReferencedTable("", "t2"));

        BasicTable t3 = new Table(gp, "t3");
        c = new StringColumn(t3, "idc");
        c.setLength("5");
        c.setNullableAndDefault(false, "");

        t3.addPK("idc");
        t3.finalizePK();

        // Теперь проверяем, что ключи могут указывать лишь на совпадающие по
        // типу поля
        final ForeignKey fk3 = new ForeignKey(t2);
        fk3.addColumn("datecol");
        assertThrows(ParseException.class,
                () -> fk3.setReferencedTable("", "t3"));
        assertEquals(0, t2.getForeignKeys().size());

        final ForeignKey fk4 = new ForeignKey(t2);
        fk4.addColumn("scol2");

        // Ошибка, потому что scol2 -- VARCHAR(2), а в T3 первичный ключ --
        // VARCHAR(5)
        assertThrows(ParseException.class,
                () -> fk4.setReferencedTable("", "t3"));

        assertEquals(0, t2.getForeignKeys().size());

        // Вот теперь должно быть ОК!
        final ForeignKey fk5 = new ForeignKey(t2);
        fk5.addColumn("scol5");
        fk5.setReferencedTable("", "t3");
        assertEquals(1, t2.getForeignKeys().size());
        assertSame(t3, fk5.getReferencedTable());

        fk5.addReferencedColumn("idc");
        fk5.finalizeReference();

        BasicTable t4 = new Table(gp, "t4");
        cc = new IntegerColumn(t4, "idd1");
        cc.setNullableAndDefault(false, "-1");

        cc = new IntegerColumn(t4, "idd2");
        cc.setNullableAndDefault(false, "123");

        t4.addPK("idd1");
        t4.addPK("idd2");
        t4.finalizePK();
        new IntegerColumn(t4, "dummy1");
        new IntegerColumn(t4, "dummy2");

        // А теперь проверяем выставление составного FK
        final ForeignKey fk6 = new ForeignKey(t2);
        fk6.addColumn("idb");
        // Коротковат ключик
        assertThrows(ParseException.class,
                () -> fk6.setReferencedTable("", "t4"));


        assertEquals(1, t2.getForeignKeys().size());

        final  ForeignKey fk7 = new ForeignKey(t2);
        fk7.addColumn("idb");
        fk7.addColumn("datecol");
        // Длина подходящая, но тип неподоходящий
        assertThrows(ParseException.class,
                () ->  fk7.setReferencedTable("", "t4"));


        assertEquals(1, t2.getForeignKeys().size());

        // А теперь всё должно быть ОК!
        final  ForeignKey  fk8 = new ForeignKey(t2);
        fk8.addColumn("idb");
        fk8.addColumn("intcol");
        fk8.setReferencedTable("", "t4");
        assertEquals(2, t2.getForeignKeys().size());
        assertSame(t4, fk8.getReferencedTable());

        fk8.addReferencedColumn("idd1");
        fk8.addReferencedColumn("dummy1");
        assertThrows(ParseException.class, fk8::finalizeReference);

        fk8.addReferencedColumn("idd1");
        fk8.addReferencedColumn("idd2");
        fk8.finalizeReference();

        // Проверяем невозможность вставки двух идентичных FK в одну и ту же
        // таблицу
        final ForeignKey fk9 = new ForeignKey(t2);
        fk9.addColumn("idb");
        fk9.addColumn("intcol");
        assertThrows(ParseException.class,
                ()-> fk9.setReferencedTable("", "t4"));
    }

    @Test
    void test4() throws ParseException {
        Grain gm = new Grain(s, "grain4");
        GrainPart gp = new GrainPart(gm, true, null);
        SequenceElement st1 = new SequenceElement(gp, "t1_c1");
        BasicTable t1 = new Table(gp, "t1");
        IntegerColumn c = new IntegerColumn(t1, "c1");
        c.setNullableAndDefault(false, "NEXTVAL(" + st1.getName() + ")");

        t1.addPK("c1");
        t1.finalizePK();

        SequenceElement st2 = new SequenceElement(gp, "t2_c1");
        BasicTable t2 = new Table(gp, "t2");
        c = new IntegerColumn(t2, "c1");
        c.setNullableAndDefault(false, "NEXTVAL(" + st2.getName() + ")");

        t2.addPK("c1");
        t2.finalizePK();
        c = new IntegerColumn(t2, "c2");
        c.setNullableAndDefault(false, "123");

        ForeignKey fk = new ForeignKey(t2);
        fk.addColumn("c2");
        fk.setReferencedTable("", "t1");
        assertSame(FKRule.NO_ACTION, fk.getDeleteRule());
        assertSame(FKRule.NO_ACTION, fk.getUpdateRule());

        // нельзя использовать SET NULL в Not-nullable колонках
        assertThrows(ParseException.class, ()->
            fk.setDeleteRule(FKRule.SET_NULL));

        assertSame(FKRule.NO_ACTION, fk.getDeleteRule());
        fk.setDeleteRule(FKRule.CASCADE);
        assertSame(FKRule.CASCADE, fk.getDeleteRule());

        // нельзя использовать SET NULL в Not-nullable колонках
        assertThrows(ParseException.class, ()->
            fk.setUpdateRule(FKRule.SET_NULL));

        assertSame(FKRule.NO_ACTION, fk.getUpdateRule());
        fk.setUpdateRule(FKRule.CASCADE);
        assertSame(FKRule.CASCADE, fk.getUpdateRule());
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
