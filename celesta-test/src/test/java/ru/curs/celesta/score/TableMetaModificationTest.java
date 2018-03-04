package ru.curs.celesta.score;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.curs.celesta.score.FieldsLookupTest.generateTable;

public class TableMetaModificationTest {

    @Test
    void boundFieldsCannotBeDeleted() throws ParseException {
        AbstractScore score = new Score();
        Grain grain = new Grain(score, "test");

        Table tableA = new Table(grain, "a");
        Column a1 = new IntegerColumn(tableA, "a1");
        a1.setNullableAndDefault(false, "0");
        Column a2 = new IntegerColumn(tableA, "a2");
        a2.setNullableAndDefault(false, "0");
        Column a3 = new IntegerColumn(tableA, "a3");
        Column a4 = new IntegerColumn(tableA, "a4");
        tableA.addPK("a1");
        tableA.finalizePK();
        new Index(tableA, "IA", new String[]{"a2"});


        Table tableB = new Table(grain, "b");
        Column b1 = new IntegerColumn(tableB, "b1");
        b1.setNullableAndDefault(false, "0");
        tableB.addPK("b1");
        tableB.finalizePK();

        new ForeignKey(tableA, tableB, new String[]{"a3"});

        assertEquals(4, tableA.getColumns().size());
        a4.delete();
        assertEquals(3, tableA.getColumns().size());

        //PK
        Assertions.assertThrows(ParseException.class,
                () -> tableA.getColumn("a1").delete());

        //Index
        Assertions.assertThrows(ParseException.class,
                () -> tableA.getColumn("a2").delete());

        //FK
        Assertions.assertThrows(ParseException.class,
                () -> tableA.getColumn("a3").delete());
    }
}
