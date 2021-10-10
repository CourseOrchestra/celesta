package ru.curs.celesta.score;


import org.junit.jupiter.api.Test;
import ru.curs.celesta.score.io.FileResource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ParserTest extends AbstractParsingTest {

  @Test
  void testSchemaAndGrainKeywordsEquivalence() throws Exception {
    String createSchemaTemplate = "CREATE %s someGrain VERSION '1.0';";
    String createSchema = String.format(createSchemaTemplate, "SCHEMA");
    String createGrain = String.format(createSchemaTemplate, "GRAIN");

    File scoreDir = new File(Files.createTempDirectory("testSchemaAndGrainKeywordsEquivalence").toUri());
    scoreDir.deleteOnExit();

    File schemaDir = new File (scoreDir, "schema");
    schemaDir.mkdir();
    schemaDir.deleteOnExit();
    File grainDir = new File(scoreDir, "grain");
    grainDir.mkdir();
    grainDir.deleteOnExit();

    parseAndSaveCsqlScript(createSchema, schemaDir, "someGrain");
    parseAndSaveCsqlScript(createGrain, grainDir, "someGrain");

    File schemaScript = new File(schemaDir, "someGrain.sql");
    schemaScript.deleteOnExit();
    File grainScript = new File(grainDir, "someGrain.sql");
    grainScript.deleteOnExit();

    assertEquals(
            new String(Files.readAllBytes(schemaScript.toPath()), StandardCharsets.UTF_8.name()),
            new String(Files.readAllBytes(grainScript.toPath()), StandardCharsets.UTF_8.name())
    );

  }

  private void parseAndSaveCsqlScript(String csqlScript, File grainPath, String grainName) throws Exception {

    final GrainPart gp;

    try (InputStream is = new ByteArrayInputStream(csqlScript.getBytes(StandardCharsets.UTF_8))) {
      CelestaParser cp = new CelestaParser(is, "utf-8");
      CelestaSqlTestScore s = new CelestaSqlTestScore();
      gp = cp.extractGrainInfo(s, new FileResource(new File(grainPath, grainName + ".sql")));
    }

    try (InputStream is = new ByteArrayInputStream(csqlScript.getBytes(StandardCharsets.UTF_8))) {
      CelestaParser cp = new CelestaParser(is, "utf-8");
      Grain g = cp.parseGrainPart(gp);
      g.modify();
      new GrainSaver().save(g, new FileResource(grainPath));
    }
  }

  @Test
  void testSchemaWithNoAutoupdate() throws Exception {
    String createSchema = "CREATE SCHEMA someGrain VERSION '1.0' WITH NO AUTOUPDATE;";

    File scoreDir = new File(Files.createTempDirectory("testGrainWithNoAutoupdate").toUri());
    scoreDir.deleteOnExit();

    File grainDir = new File(scoreDir, "grain");
    grainDir.mkdir();
    grainDir.deleteOnExit();

    parseAndSaveCsqlScript(createSchema, grainDir, "someGrain");

    File grainScript = new File(grainDir, "someGrain.sql");
    grainScript.deleteOnExit();

    String actualCreateSchema = Files.lines(grainScript.toPath()).findFirst().get();

    assertEquals(createSchema, actualCreateSchema);
  }

  @Test
  public void test1() throws Exception {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "test.sql"
    );
    Grain g = parse(f);
    assertEquals("test1", g.getName());
    assertEquals("1.0", g.getVersion().toString());
    assertEquals("описание гранулы: * grain celestadoc", g.getCelestaDoc());

    assertEquals(5, g.getElements(Table.class).size());
    assertEquals(1, g.getElements(ReadOnlyTable.class).size());

    Map<String, BasicTable> s = g.getElements(BasicTable.class);
    assertEquals(6, s.size());

    Iterator<BasicTable> i = s.values().iterator();
    // First table
    BasicTable t1 = i.next();
    assertEquals("table1", t1.getName());
    assertNull(t1.getCelestaDoc());

    Iterator<Column<?>> ic = t1.getColumns().values().iterator();
    Column<?> c = ic.next();
    assertEquals("column1", c.getName());
    assertTrue(c instanceof IntegerColumn);
    assertFalse(c.isNullable());

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

    Map<String, Column<?>> key = t1.getPrimaryKey();
    ic = key.values().iterator();
    c = ic.next();
    assertSame(c, t1.getColumns().get("column1"));
    assertEquals("column1", c.getName());
    c = ic.next();
    assertSame(c, t1.getColumns().get("c3"));
    assertEquals("c3", c.getName());
    c = ic.next();
    assertSame(c, t1.getColumns().get("column2"));
    assertEquals("column2", c.getName());

    // Second table
    BasicTable t2 = i.next();
    assertEquals("table2", t2.getName());
    assertEquals("table2 celestadoc", t2.getCelestaDoc());
    ic = t2.getColumns().values().iterator();

    c = ic.next();
    assertEquals("column1", c.getName());
    assertEquals("описание первой колонки", c.getCelestaDoc());
    assertTrue(c instanceof IntegerColumn);
    assertFalse(c.isNullable());
    assertNull(((IntegerColumn) c).getDefaultValue());

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

    Table t = g.getElement("employees", Table.class);
    assertSame(g.getElement("employees", BasicTable.class), t);
    assertNull(t.getCelestaDoc());
    assertTrue(t.isVersioned());

    // Checking for additional options
    ReadOnlyTable rot = g.getElement("ttt1", ReadOnlyTable.class);
    assertSame(g.getElement("ttt1", BasicTable.class), rot);
    assertTrue(rot.isAutoUpdate());

    t = g.getElement("ttt2", Table.class);
    assertSame(g.getElement("ttt2", BasicTable.class), t);
    assertTrue(t.isVersioned());
    assertTrue(t.isAutoUpdate());

    t = g.getElement("ttt3", Table.class);
    assertSame(g.getElement("ttt3", BasicTable.class), t);
    assertFalse(t.isVersioned());
    assertFalse(t.isAutoUpdate());
  }

  @Test
  public void test2() throws Exception {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "test2.sql"
    );
    Grain g = parse(f);
    assertEquals("test2", g.getName());
    assertEquals("2.5", g.getVersion().toString());

    BasicTable d = g.getElement("d", BasicTable.class);
    assertEquals(0, d.getForeignKeys().size());

    BasicTable a = g.getElement("a", BasicTable.class);
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

    BasicTable b = g.getElement("b", BasicTable.class);
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
  public void test3() throws Exception {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "test3.sql"
    );
    Grain g = parse(f);
    BasicTable t = g.getElement("structure_subordination", BasicTable.class);
    assertEquals(2, t.getForeignKeys().size());
  }

  @Test()
  public void test4() throws Exception {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "test4.sql"
    );
    Grain g = parse(f);
    BasicTable t = g.getElement("app_division_add_info_el", BasicTable.class);
    assertEquals("pk_app_division_add_info_el", t.getPkConstraintName());
    t = g.getElement("x_role_employees", BasicTable.class);
    assertEquals(1, t.getForeignKeys().size());
    ForeignKey fk = t.getForeignKeys().iterator().next();
    assertEquals("fk_x_rolempyees_xroles", fk.getConstraintName());

    assertTrue(g.getLength() > 10000);
    assertTrue(g.getChecksum() != 0);

    //TODO: non-stable values when using Git because of line ending conversion
    //hash sum calculation should be rewritten to be line-ending independent
    //recalculation of assert expected values is needed
    if (";".equals(File.pathSeparator)){
      //Windows!
//      assertEquals(20767, g.getLength());
//      assertEquals(0x1754E6E7, g.getChecksum());
    } else {
      //Linux!
//      assertEquals(19839, g.getLength());
//      assertEquals(0x3CFB69F0, g.getChecksum());
    }
  }

}
