package ru.curs.celesta.score;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.filter.value.FieldsLookup;

import java.util.function.Function;

/**
 * Created by ioann on 07.06.2017.
 */
public class FieldsLookupTest {

    private static BasicTable tableA;
    private static BasicTable tableB;
    private static BasicTable tableC;
    private static View viewA;
    private static View viewB;
    private static View viewC;

    private static Runnable lookupChangeCallback = () -> {
    };
    private static Function<FieldsLookup, Void> newLookupCallback = (f) -> null;

    @BeforeAll
    public static void init() throws ParseException {
        AbstractScore score = new Score();
        Grain grain = new Grain(score, "test");
        GrainPart gp = new GrainPart(grain, true, null);

        tableA = generateTable(gp, "a");
        tableB = generateTable(gp, "b");
        tableC = generateTable(gp, "c");

        viewA = generateView(gp, "aV", tableA);
        viewB = generateView(gp, "bV", tableB);
        viewC = generateView(gp, "cV", tableC);
    }

    private static BasicTable generateTable(GrainPart gp, String name) throws ParseException {
        BasicTable table = new Table(gp, name);

        Column<?> c1 = new IntegerColumn(table, name + "1");
        Column<?> c2 = new IntegerColumn(table, name + "2");
        Column<?> c3 = new StringColumn(table, name + "3");
        Column<?> c4 = new DateTimeColumn(table, name + "4");

        String[] indexColumns = {name + "1", name + "2", name + "3"};
        Index i1 = new Index(table, "I" + name, indexColumns);

        return table;
    }

    private static View generateView(GrainPart gp, String name, BasicTable table) throws ParseException {

        class GenView extends View {
            GenView(GrainPart grainPart, String name) throws ParseException {
                super(grainPart, name);
            }
            void addColumn(String columnName, ViewColumnType columnType) {
                ViewColumnMeta<?> column = new ViewColumnMeta<>(columnType);
                column.setName(columnName);
                getColumns().put(columnName, column);
            }
        }

        GenView view = new GenView(gp, name);
        AbstractSelectStmt selectStmt = view.addSelectStatement();
        selectStmt.addFromTableRef(new TableRef(table, table.getName()));

        view.addColumn(table.getName() + "1", ViewColumnType.INT);
        view.addColumn(table.getName() + "2", ViewColumnType.INT);
        view.addColumn(table.getName() + "3", ViewColumnType.TEXT);
        view.addColumn(table.getName() + "4", ViewColumnType.DATE);

        return view;
    }

    @Test
    public void testAddWhenBothColumnsExist() throws Exception {
        FieldsLookup lookup = new FieldsLookup(tableA, tableB, lookupChangeCallback, newLookupCallback);
        lookup.add("a1", "b1");

        lookup = new FieldsLookup(viewA, viewB, lookupChangeCallback, newLookupCallback);
        lookup.add("a1", "b1");
    }

    @Test
    public void testAddWhenLeftColumnDoesNotExist() throws Exception {
        final FieldsLookup lookup = new FieldsLookup(tableA, tableB, lookupChangeCallback, newLookupCallback);
        assertThrows(ParseException.class, () -> lookup.add("notExistedField", "b1"));

        final FieldsLookup lookup2 = new FieldsLookup(viewA, viewB, lookupChangeCallback, newLookupCallback);
        assertThrows(ParseException.class, () -> lookup2.add("notExistedField", "b1"));
    }

    @Test
    public void testAddWhenRightColumnDoesNotExist() throws Exception {
        final FieldsLookup lookup = new FieldsLookup(tableA, tableB, lookupChangeCallback, newLookupCallback);
        assertThrows(ParseException.class, () -> lookup.add("a1", "notExistedField"));

        final FieldsLookup lookup2 = new FieldsLookup(viewA, viewB, lookupChangeCallback, newLookupCallback);
        assertThrows(ParseException.class, () -> lookup2.add("a1", "notExistedField"));
    }

    @Test
    public void testAddWhenBothColumnsDoNotExist() throws Exception {
        FieldsLookup lookup = new FieldsLookup(tableA, tableB, lookupChangeCallback, newLookupCallback);
        assertThrows(ParseException.class, () -> lookup.add("notExistedField", "notExistedField"));

        FieldsLookup lookup2 = new FieldsLookup(viewA, viewB, lookupChangeCallback, newLookupCallback);
        assertThrows(ParseException.class, () -> lookup2.add("notExistedField", "notExistedField"));
    }

    @Test
    public void testWithCorrectInputData() throws Exception {
        FieldsLookup lookup = new FieldsLookup(tableA, tableB, lookupChangeCallback, newLookupCallback);
        lookup.add("a1", "b1");
        lookup.add("a2", "b2");
        lookup.add("a3", "b3");

        FieldsLookup lookup2 = new FieldsLookup(viewA, viewB, lookupChangeCallback, newLookupCallback);
        lookup2.add("a1", "b1");
        lookup2.add("a2", "b2");
        lookup2.add("a3", "b3");
    }


    @Test
    public void testWithCorrectInputDataAndAdditionalLookup() throws Exception {
        FieldsLookup lookup = new FieldsLookup(tableA, tableB, lookupChangeCallback, newLookupCallback);
        lookup.add("a1", "b1");
        lookup.add("a2", "b2");
        lookup.add("a3", "b3");

        lookup = lookup.and(tableC);

        lookup.add("a1", "c1");
        lookup.add("a2", "c2");
        lookup.add("a3", "c3");

        FieldsLookup lookup2 = new FieldsLookup(viewA, viewB, lookupChangeCallback, newLookupCallback);
        lookup2.add("a1", "b1");
        lookup2.add("a2", "b2");
        lookup2.add("a3", "b3");

        lookup2 = lookup2.and(viewC);

        lookup2.add("a1", "c1");
        lookup2.add("a2", "c2");
        lookup2.add("a3", "c3");
    }

    @Test
    public void testWhenIndicesDoNotMatchForTable() throws Exception {
        FieldsLookup lookup = new FieldsLookup(tableA, tableB, lookupChangeCallback, newLookupCallback);
        lookup.add("a1", "b1");
        assertThrows(CelestaException.class, () -> lookup.add("a3", "b3"));
    }


    @Test
    public void testIndependenceFromIndicesForView() throws Exception {
        FieldsLookup lookup = new FieldsLookup(viewA, viewB, lookupChangeCallback, newLookupCallback);
        lookup.add("a1", "b1");
        lookup.add("a3", "b3");
    }

    @Test
    public void testWhenIndicesDoNotMatchInAdditionalLookupForTable() throws Exception {
        FieldsLookup lookup = new FieldsLookup(tableA, tableB, lookupChangeCallback, newLookupCallback);
        lookup.add("a1", "b1");
        lookup.add("a2", "b2");
        lookup.add("a3", "b3");

        FieldsLookup anotherLookup = lookup.and(tableC);

        anotherLookup.add("a1", "c1");
        assertThrows(CelestaException.class, () -> anotherLookup.add("a3", "c3"));
    }

    @Test
    public void testIndependenceFromIndicesInAdditionalLookupForView() throws Exception {
        FieldsLookup lookup = new FieldsLookup(viewA, viewB, lookupChangeCallback, newLookupCallback);
        lookup.add("a1", "b1");
        lookup.add("a2", "b2");
        lookup.add("a3", "b3");

        FieldsLookup anotherLookup = lookup.and(viewC);

        anotherLookup.add("a1", "c1");
        anotherLookup.add("a3", "c3");
    }

    @Test
    void testImpossibleToAddNonTargetClassLookupToExistedLookup() throws Exception {
        final FieldsLookup lookup = new FieldsLookup(tableA, tableB, lookupChangeCallback, newLookupCallback);
        assertThrows(CelestaException.class, () -> lookup.and(viewC));

        final FieldsLookup lookup2 = new FieldsLookup(viewA, viewB, lookupChangeCallback, newLookupCallback);
        assertThrows(CelestaException.class, () -> lookup2.and(tableC));
    }
}
