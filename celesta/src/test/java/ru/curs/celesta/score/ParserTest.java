package ru.curs.celesta.score;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

import ru.curs.celesta.CelestaException;

public class ParserTest extends AbstractParsingTest {

  @Test
  public void test0() throws ParseException, CelestaException, IOException {
    InputStream input = ParserTest.class.getResourceAsStream("test.sql");
    try {
      CelestaParser cp = new CelestaParser(input, "utf-8");
      Grain g = cp.grain(s, "test1");
      g.setGrainPath(new File("testScore"));
      g.setVersion("'2.0'");
      g.save();
    } finally {
      input.close();
    }
    input = ParserTest.class.getResourceAsStream("test2.sql");
    try {
      CelestaParser cp = new CelestaParser(input, "utf-8");
      Grain g = cp.grain(s, "test2");
      g.setGrainPath(new File("testScore"));
      g.setVersion("'2.0'");
      g.save();
    } finally {
      input.close();
    }
  }

  @Test
  public void test1() throws ParseException {
    InputStream input = ParserTest.class.getResourceAsStream("test.sql");
    CelestaParser cp = new CelestaParser(input, "utf-8");
    Grain g = cp.grain(s, "test1");
    assertEquals("test1", g.getName());
    assertEquals("1.0", g.getVersion().toString());
    assertEquals("описание гранулы: * grain celestadoc", g.getCelestaDoc());

    Map<String, Table> s = g.getTables();
    assertEquals(6, s.size());

    Iterator<Table> i = s.values().iterator();
    // Первая таблица
    Table t = i.next();
    assertEquals("table1", t.getName());
    assertNull(t.getCelestaDoc());

    Iterator<Column> ic = t.getColumns().values().iterator();
    Column c = ic.next();
    assertEquals("column1", c.getName());
    assertTrue(c instanceof IntegerColumn);
    assertFalse(c.isNullable());
    assertTrue(((IntegerColumn) c).isIdentity());

    c = ic.next();
    assertEquals("column2", c.getName());
    assertTrue(c instanceof FloatingColumn);
    assertFalse(c.isNullable());
    assertEquals(-12323.2, ((FloatingColumn) c).getDefaultValue(), .00001);

    c = ic.next();
    assertEquals("c3", c.getName());
    assertTrue(c instanceof BooleanColumn);
    assertFalse(c.isNullable());

    c = ic.next();
    assertEquals("aaa", c.getName());
    assertTrue(c instanceof StringColumn);
    assertFalse(c.isNullable());
    assertEquals("testtes'ttest", ((StringColumn) c).getDefaultValue());
    assertEquals(23, ((StringColumn) c).getLength());
    assertFalse(((StringColumn) c).isMax());

    c = ic.next();
    assertEquals("bbb", c.getName());
    assertTrue(c instanceof StringColumn);
    assertTrue(c.isNullable());
    assertTrue(((StringColumn) c).isMax());

    c = ic.next();
    assertEquals("ccc", c.getName());
    assertTrue(c instanceof BinaryColumn);
    assertTrue(c.isNullable());
    assertNull(((BinaryColumn) c).getDefaultValue());

    c = ic.next();
    assertEquals("e", c.getName());
    assertTrue(c instanceof IntegerColumn);
    assertTrue(c.isNullable());
    assertEquals(-112, (int) ((IntegerColumn) c).getDefaultValue());

    c = ic.next();
    assertEquals("f", c.getName());
    assertTrue(c instanceof FloatingColumn);
    assertTrue(c.isNullable());
    assertNull(((FloatingColumn) c).getDefaultValue());

    c = ic.next();
    assertEquals("f1", c.getName());
    assertEquals(Integer.valueOf(4),
        Integer.valueOf(((IntegerColumn) c).getDefaultValue()));
    c = ic.next();
    assertEquals("f2", c.getName());
    assertEquals(5.5, ((FloatingColumn) c).getDefaultValue(), .00001);

    Map<String, Column> key = t.getPrimaryKey();
    ic = key.values().iterator();
    c = ic.next();
    assertSame(c, t.getColumns().get("column1"));
    assertEquals("column1", c.getName());
    c = ic.next();
    assertSame(c, t.getColumns().get("c3"));
    assertEquals("c3", c.getName());
    c = ic.next();
    assertSame(c, t.getColumns().get("column2"));
    assertEquals("column2", c.getName());

    // Вторая таблица
    t = i.next();
    assertEquals("table2", t.getName());
    assertEquals("table2 celestadoc", t.getCelestaDoc());
    ic = t.getColumns().values().iterator();

    c = ic.next();
    assertEquals("column1", c.getName());
    assertEquals("описание первой колонки", c.getCelestaDoc());
    assertTrue(c instanceof IntegerColumn);
    assertFalse(c.isNullable());
    assertNull(((IntegerColumn) c).getDefaultValue());
    assertTrue(((IntegerColumn) c).isIdentity());

    c = ic.next();
    assertEquals("column2", c.getName());
    assertEquals("описание второй колонки", c.getCelestaDoc());
    assertTrue(c instanceof DateTimeColumn);
    assertTrue(c.isNullable());
    Date d = ((DateTimeColumn) c).getDefaultValue();
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    assertEquals("2011-12-31", df.format(d));
    assertFalse(((DateTimeColumn) c).isGetdate());

    c = ic.next();
    assertEquals("column3", c.getName());
    assertNull(c.getCelestaDoc());
    assertTrue(c instanceof DateTimeColumn);
    assertFalse(c.isNullable());
    assertNull(((DateTimeColumn) c).getDefaultValue());
    assertTrue(((DateTimeColumn) c).isGetdate());

    c = ic.next();
    assertEquals("column4", c.getName());
    assertTrue(c instanceof BinaryColumn);
    assertEquals("0x22AB15FF", ((BinaryColumn) c).getDefaultValue());

    c = ic.next();
    assertEquals("column5", c.getName());
    assertTrue(c instanceof IntegerColumn);
    assertEquals(11, ((IntegerColumn) c).getDefaultValue().intValue());

    assertEquals(2, g.getIndices().size());

    Index idx = g.getIndices().get("idx1");
    assertEquals("table1", idx.getTable().getName());
    assertEquals("описание индекса idx1", idx.getCelestaDoc());
    assertEquals(2, idx.getColumns().size());

    assertEquals(1, idx.getTable().getIndices().size());
    assertTrue(idx.getTable().getIndices().contains(idx));

    idx = g.getIndices().get("table2_idx2");
    assertEquals("table2", idx.getTable().getName());
    assertEquals(2, idx.getColumns().size());

    assertEquals(1, idx.getTable().getIndices().size());
    assertTrue(idx.getTable().getIndices().contains(idx));
    g.removeIndex(idx);
    assertEquals(0, idx.getTable().getIndices().size());

    t = g.getTable("employees");
    assertNull(t.getCelestaDoc());
    assertTrue(t.isVersioned());
    assertFalse(t.isReadOnly());

    // Проверка дополнительных возможностей
    t = g.getTable("ttt1");
    assertTrue(t.isReadOnly());
    assertFalse(t.isVersioned());
    assertTrue(t.isAutoUpdate());

    t = g.getTable("ttt2");
    assertTrue(t.isVersioned());
    assertFalse(t.isReadOnly());
    assertTrue(t.isAutoUpdate());

    t = g.getTable("ttt3");
    assertFalse(t.isVersioned());
    assertFalse(t.isReadOnly());
    assertFalse(t.isAutoUpdate());
  }

  @Test
  public void test2() throws ParseException {
    InputStream input = ParserTest.class.getResourceAsStream("test2.sql");
    CelestaParser cp = new CelestaParser(input);
    Grain g = cp.grain(s, "test2");
    assertEquals("test2", g.getName());
    assertEquals("2.5", g.getVersion().toString());

    Table d = g.getTables().get("d");
    assertEquals(0, d.getForeignKeys().size());

    Table a = g.getTables().get("a");
    assertEquals(2, a.getForeignKeys().size());
    Iterator<ForeignKey> i = a.getForeignKeys().iterator();

    ForeignKey fk = i.next();
    assertEquals("a", fk.getParentTable().getName());
    assertEquals(1, fk.getColumns().size());
    assertEquals("kk", fk.getColumns().get("kk").getName());
    assertEquals("d", fk.getReferencedTable().getName());
    assertSame(FKRule.NO_ACTION, fk.getDeleteRule());
    assertSame(FKRule.SET_NULL, fk.getUpdateRule());

    fk = i.next();
    assertEquals("a", fk.getParentTable().getName());
    assertEquals(1, fk.getColumns().size());
    assertEquals("d", fk.getColumns().get("d").getName());
    assertEquals("c", fk.getReferencedTable().getName());
    assertSame(FKRule.NO_ACTION, fk.getDeleteRule());
    assertSame(FKRule.NO_ACTION, fk.getUpdateRule());

    Table b = g.getTables().get("b");
    assertEquals(1, b.getForeignKeys().size());
    i = b.getForeignKeys().iterator();
    fk = i.next();
    assertEquals("b", fk.getParentTable().getName());
    assertEquals(2, fk.getColumns().size());
    assertEquals("b", fk.getColumns().get("b").getName());
    assertEquals("a", fk.getColumns().get("a").getName());
    assertEquals("a", fk.getReferencedTable().getName());
    assertSame(FKRule.CASCADE, fk.getDeleteRule());
    assertSame(FKRule.CASCADE, fk.getUpdateRule());
  }

  @Test
  public void test3() throws ParseException {
    InputStream input = ParserTest.class.getResourceAsStream("test3.sql");
    CelestaParser cp = new CelestaParser(input);
    Grain g = cp.grain(s, "bc");
    Table t = g.getTable("structure_subordination");
    assertEquals(2, t.getForeignKeys().size());
  }

  @Test
  public void test4() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("test4.sql"));
    CelestaParser cp = new CelestaParser(input);
    Grain g = cp.grain(s, "skk");
    Table t = g.getTable("app_division_add_info_el");
    assertEquals("pk_app_division_add_info_el", t.getPkConstraintName());
    t = g.getTable("x_role_employees");
    assertEquals(1, t.getForeignKeys().size());
    ForeignKey fk = t.getForeignKeys().iterator().next();
    assertEquals("fk_x_rolempyees_xroles", fk.getConstraintName());

    assertEquals(20767, input.getCount());
    assertEquals(0x1754E6E7, input.getCRC32());
  }

}
