package ru.curs.celesta.score;

import org.junit.BeforeClass;
import org.junit.Test;
import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.filter.value.FieldsLookup;

import java.util.function.Function;

/**
 * Created by ioann on 07.06.2017.
 */
public class FieldsLookupTest {

  private static Table tableA;
  private static Table tableB;
  private static Table tableC;

  private static Runnable lookupChangeCallback = () -> {};
  private static Function<FieldsLookup, Void> newLookupCallback = (f) -> null;

  @BeforeClass
  public static void init() throws ParseException {
    Score score = new Score();
    Grain grain = new Grain(score, "test");

    tableA = generateTable(grain, "a");
    tableB = generateTable(grain, "b");
    tableC = generateTable(grain, "c");
  }

  private static Table generateTable(Grain grain, String name) throws ParseException {
    Table table = new Table(grain, name);

    Column c1 = new IntegerColumn(table, name + "1");
    Column c2 = new IntegerColumn(table, name + "2");
    Column c3 = new StringColumn(table, name + "3");
    Column c4 = new DateTimeColumn(table, name + "4");

    String[] indexColumns = {name + "1", name + "2", name + "3"};
    Index i1 = new Index(table, "I" + name, indexColumns);

    return table;
  }


  @Test
  public void testAddWhenBothColumnsExist() throws Exception {
    FieldsLookup lookup = new FieldsLookup(tableA, tableB, lookupChangeCallback, newLookupCallback);
    lookup.add("a1", "b1");
  }

  @Test(expected = ParseException.class)
  public void testAddWhenLeftColumnDoesNotExist() throws Exception {
    FieldsLookup lookup = new FieldsLookup(tableA, tableB, lookupChangeCallback, newLookupCallback);
    lookup.add("notExistedField", "b1");
  }

  @Test(expected = ParseException.class)
  public void testAddWhenRightColumnDoesNotExist() throws Exception {
    FieldsLookup lookup = new FieldsLookup(tableA, tableB, lookupChangeCallback, newLookupCallback);
    lookup.add("a1", "notExistedField");
  }

  @Test(expected = ParseException.class)
  public void testAddWhenBothColumnsDoNotExist() throws Exception {
    FieldsLookup lookup = new FieldsLookup(tableA, tableB, lookupChangeCallback, newLookupCallback);
    lookup.add("notExistedField", "notExistedField");
  }

  @Test
  public void testWithCorrectInputData() throws Exception {
    FieldsLookup lookup = new FieldsLookup(tableA, tableB, lookupChangeCallback, newLookupCallback);
    lookup.add("a1", "b1");
    lookup.add("a2", "b2");
    lookup.add("a3", "b3");
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
  }

  @Test(expected = CelestaException.class)
  public void testWhenIndicesDoNotMatchInAdditionalLookup() throws Exception {
    FieldsLookup lookup = new FieldsLookup(tableA, tableB, lookupChangeCallback, newLookupCallback);
    lookup.add("a1", "b1");
    lookup.add("a2", "b2");
    lookup.add("a3", "b3");

    lookup = lookup.and(tableC);

    lookup.add("a1", "c1");
    lookup.add("a3", "c3");
  }

  @Test(expected = CelestaException.class)
  public void testWhenIndicesDoNotMatch() throws Exception {
    FieldsLookup lookup = new FieldsLookup(tableA, tableB, lookupChangeCallback, newLookupCallback);
    lookup.add("a1", "b1");
    lookup.add("a3", "b3");
  }
}
