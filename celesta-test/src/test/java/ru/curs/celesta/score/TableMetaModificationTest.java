package ru.curs.celesta.score;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TableMetaModificationTest {

    @Test
    void boundFieldsCannotBeDeleted() throws ParseException {
        AbstractScore score = new Score();
        Grain grain = new Grain(score, "test");
        GrainPart gp = new GrainPart(grain, true, null);
        Table tableA = new Table(gp, "a");
        Column a1 = new IntegerColumn(tableA, "a1");
        a1.setNullableAndDefault(false, "0");
        Column a2 = new IntegerColumn(tableA, "a2");
        a2.setNullableAndDefault(false, "0");
        new IntegerColumn(tableA, "a3");
        Column a4 = new IntegerColumn(tableA, "a4");
        tableA.addPK("a1");
        tableA.finalizePK();
        new Index(tableA, "IA", new String[]{"a2"});


        Table tableB = new Table(gp, "b");
        Column b1 = new IntegerColumn(tableB, "b1");
        b1.setNullableAndDefault(false, "0");
        tableB.addPK("b1");
        tableB.finalizePK();

        new ForeignKey(tableA, tableB, new String[]{"a3"});

        assertEquals(4, tableA.getColumns().size());
        a4.delete();
        assertEquals(3, tableA.getColumns().size());


        assertAll(
                //PK
                () -> assertThrows(ParseException.class,
                        () -> tableA.getColumn("a1").delete()),

                //Index
                () -> assertThrows(ParseException.class,
                        () -> tableA.getColumn("a2").delete()),

                //FK
                () -> assertThrows(ParseException.class,
                        () -> tableA.getColumn("a3").delete()));

        assertEquals(3, tableA.getColumns().size());
    }
}
