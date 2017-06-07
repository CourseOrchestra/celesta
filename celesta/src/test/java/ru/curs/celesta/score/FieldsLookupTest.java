package ru.curs.celesta.score;

import org.junit.BeforeClass;
import org.junit.Test;
import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.filter.value.FieldsLookup;

/**
 * Created by ioann on 07.06.2017.
 */
public class FieldsLookupTest {

  private static Table tableA;
  private static Table tableB;

  @BeforeClass
  public static void init() throws ParseException {
    Score score = new Score();
    Grain grain = new Grain(score, "test");

    tableA = generateTable(grain, "a");
    tableB = generateTable(grain, "b");
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
    FieldsLookup lookup = new FieldsLookup(tableA, tableB);
    lookup.add("a1", "b1");
  }

  @Test(expected = ParseException.class)
  public void testAddWhenLeftColumnDoesNotExist() throws Exception {
    FieldsLookup lookup = new FieldsLookup(tableA, tableB);
    lookup.add("notExistedField", "b1");
  }

  @Test(expected = ParseException.class)
  public void testAddWhenRightColumnDoesNotExist() throws Exception {
    FieldsLookup lookup = new FieldsLookup(tableA, tableB);
    lookup.add("a1", "notExistedField");
  }

  @Test(expected = ParseException.class)
  public void testAddWhenBothColumnsDoNotExist() throws Exception {
    FieldsLookup lookup = new FieldsLookup(tableA, tableB);
    lookup.add("notExistedField", "notExistedField");
  }

  @Test
  public void testValidateWithCorrectInputData() throws Exception {
    FieldsLookup lookup = new FieldsLookup(tableA, tableB);
    lookup.add("a1", "b1");
    lookup.add("a3", "b3");
    lookup.add("a2", "b2");

    lookup.validate();
  }

  @Test(expected = CelestaException.class)
  public void testValidateWhenIndicesDoNotMatch() throws Exception {
    FieldsLookup lookup = new FieldsLookup(tableA, tableB);
    lookup.add("a1", "b1");
    lookup.add("a3", "b3");

    lookup.validate();
  }
}
