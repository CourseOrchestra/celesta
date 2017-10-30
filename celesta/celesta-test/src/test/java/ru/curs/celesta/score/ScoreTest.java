package ru.curs.celesta.score;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import ru.curs.celesta.CelestaException;

public class ScoreTest {
	public static final Score S = new Score();

	@Test
	public void test1() throws CelestaException, ParseException {
		Score s = new Score("score" + File.pathSeparator + "pylib");
		Grain g1 = s.getGrain("g1");
		Grain g2 = s.getGrain("g2");
		assertEquals("g2", g2.getName());
		Table b = g2.getElement("b", Table.class);
		assertEquals(1, b.getForeignKeys().size());
		Table a = b.getForeignKeys().iterator().next().getReferencedTable();
		assertEquals("a", a.getName());
		assertSame(g1, a.getGrain());

		Grain g3 = s.getGrain("g3");

		int o = g1.getDependencyOrder();
		assertEquals(o + 4, g2.getDependencyOrder());
		assertEquals(o + 5, g3.getDependencyOrder());

		assertEquals("score" + File.separator + "g1", g1.getGrainPath().toString());
		assertEquals("score" + File.separator + "g2", g2.getGrainPath().toString());
		assertEquals("score" + File.separator + "g3", g3.getGrainPath().toString());

		Grain sys = s.getGrain("celesta");
		a = sys.getElement("grains", Table.class);
		assertEquals("grains", a.getName());
		assertEquals(o - 4, sys.getDependencyOrder());
		IntegerColumn c = (IntegerColumn) a.getColumns().get("state");
		assertEquals(3, c.getDefaultValue().intValue());
	}

	@Test
	public void test2() throws CelestaException, ParseException, IOException {
		Score s = new Score("score");
		Grain g1 = s.getGrain("g1");
		View v = g1.getElement("testview", View.class);
		assertEquals("testview", v.getName());
		assertEquals("view description ", v.getCelestaDoc());
		assertTrue(v.isDistinct());

		assertEquals(4, v.getColumns().size());
		String[] ref = { "fieldAlias", "tablename", "checksum", "f1" };
		assertArrayEquals(ref, v.getColumns().keySet().toArray(new String[0]));

		String[] expected = {
				"  select distinct grainid as fieldAlias, ta.tablename as tablename, grains.checksum as checksum",
				"    , ta.tablename || grains.checksum as f1", "  from celesta.tables as ta",
				"    INNER join celesta.grains as grains on ta.grainid = grains.id",
				"  where tablename >= 'aa' AND 5 BETWEEN 0 AND 6 OR '55' > '1'" };

		assertArrayEquals(expected, v.getCelestaQueryString().split("\\r?\\n"));
	}

	@Test
	public void modificationTest1() throws CelestaException, ParseException {
		Score s = new Score("score");
		Grain g1 = s.getGrain("g1");
		Grain g2 = s.getGrain("g2");
		Grain g3 = s.getGrain("g3");

		assertFalse(g1.isModified());
		assertFalse(g2.isModified());
		assertFalse(g3.isModified());

		Table b = g2.getElement("b", Table.class);
		int oldSize = b.getColumns().size();
		new StringColumn(b, "newcolumn");
		assertEquals(oldSize + 1, b.getColumns().size());
		assertFalse(g1.isModified());
		assertTrue(g2.isModified());
		assertFalse(g3.isModified());

		new Table(g3, "newtable");
		assertFalse(g1.isModified());
		assertTrue(g2.isModified());
		assertTrue(g3.isModified());

	}

	@Test
	public void modificationTest2() throws CelestaException, ParseException {
		Score s = new Score("score");
		Grain celesta = s.getGrain("celesta");
		assertFalse(celesta.isModified());
		// Проверяем, что модифицировать элементы системной гранулы недопустимо.
		Table tables = celesta.getElement("tables", Table.class);
		boolean itWas = false;
		try {
			new StringColumn(tables, "newcolumn");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
		assertFalse(celesta.isModified());
	}

	@Test
	public void modificationTest3() throws CelestaException, ParseException {
		Score s = new Score("score");
		Grain g1 = s.getGrain("g1");
		Grain g2 = s.getGrain("g2");
		Grain g3 = s.getGrain("g3");
		Grain celesta = s.getGrain("celesta");
		Grain g4 = new Grain(s, "newgrain");

		assertFalse(g1.isModified());
		assertFalse(g2.isModified());
		assertFalse(g3.isModified());
		assertFalse(celesta.isModified());
		assertTrue(g4.isModified());

		assertEquals("1.00", g4.getVersion().toString());
		assertEquals("score" + File.separator + "newgrain", g4.getGrainPath().toString());

		g3.modify();
		assertTrue(g3.isModified());
		assertFalse(g2.isModified());

		boolean itWas = false;
		try {
			celesta.modify();
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
		assertFalse(celesta.isModified());

	}

	@Test
	public void modificationTest4() throws CelestaException, ParseException {
		Score s = new Score("score");
		Grain g2 = s.getGrain("g2");
		assertFalse(g2.isModified());

		Table b = g2.getElement("b", Table.class);
		assertEquals(1, b.getPrimaryKey().size());
		b.getColumns().get("descr").setNullableAndDefault(false, null);
		assertTrue(g2.isModified());
		String[] pk = { "idb", "descr" };
		b.setPK(pk);
		assertTrue(g2.isModified());
		assertEquals(2, b.getPrimaryKey().size());
	}

	@Test
	public void modificationTest5() throws CelestaException, ParseException {
		Score s = new Score("score");
		Grain g2 = s.getGrain("g2");
		Grain g3 = s.getGrain("g3");
		assertFalse(g2.isModified());
		assertFalse(g3.isModified());

		Table b = g2.getElement("b", Table.class);
		Table c = g3.getElement("c", Table.class);

		assertEquals(1, c.getForeignKeys().size());
		ForeignKey fk = c.getForeignKeys().iterator().next();
		assertSame(b, fk.getReferencedTable());

		assertTrue(g2.getElements(Table.class).containsKey("b"));
		b.delete();
		assertFalse(g2.getElements(Table.class).containsKey("b"));
		assertTrue(g2.isModified());
		assertEquals(0, c.getForeignKeys().size());

		assertTrue(g3.isModified());
	}

	@Test
	public void modificationTest6() throws CelestaException, ParseException {
		Score s = new Score("score");
		Grain g2 = s.getGrain("g2");
		// Нельзя создать view с именем таблицы
		boolean itWas = false;
		try {
			new View(g2, "b");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
		// Нельзя создать таблицу с именем view
		new View(g2, "newView");
		itWas = false;
		try {
			new Table(g2, "newView");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
		new Table(g2, "newView2");
	}

	@Test
	public void modificationTest7() throws CelestaException, ParseException, IOException {
		Score s = new Score("score");
		Grain g1 = s.getGrain("g1");
		assertEquals(1, g1.getElements(View.class).size());
		View v = g1.getElement("testview", View.class);
		assertFalse(g1.isModified());
		v.delete();
		assertEquals(0, g1.getElements(View.class).size());
		assertTrue(g1.isModified());
	}

	@Test
	public void modificationTest8() throws CelestaException, ParseException, IOException {
		Score s = new Score("score");
		Grain g1 = s.getGrain("g1");
		assertEquals(1, g1.getElements(View.class).size());
		assertFalse(g1.isModified());
		boolean itWas = false;
		View nv;
		try {
			nv = new View(g1, "testit", "select postalcode, city from adresses where flat = 5");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
		assertEquals(1, g1.getElements(View.class).size());
		assertTrue(g1.isModified());
		nv = new View(g1, "testit", "select postalcode, city from adresses where flat = '5'");
		assertEquals(2, nv.getColumns().size());
		assertEquals(2, g1.getElements(View.class).size());
		assertTrue(g1.isModified());

	}

	@Test
	public void setCelestaDoc() throws ParseException, CelestaException {
		Score s = new Score("testScore");
		Grain g = s.getGrain("gtest");
		Table t = g.getElement("test", Table.class);
		t.setCelestaDocLexem("/** бла бла бла бла*/");
		assertEquals(" бла бла бла бла", t.getCelestaDoc());
		// Была ошибка -- не брал многострочный комментарий
		t.setCelestaDocLexem("/** бла бла бла\r\n бла*/");
		assertEquals(" бла бла бла\r\n бла", t.getCelestaDoc());
		t.setCelestaDocLexem("/**бла\rбла\nбла\r\nбла*/");
		assertEquals("бла\rбла\nбла\r\nбла", t.getCelestaDoc());
		boolean itWas = false;
		try {
			t.setCelestaDocLexem("/*бла\rбла\nбла\r\nбла*/");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
	}

	@Test
	public void saveTest() throws ParseException, CelestaException, IOException {
		// Проверяется функциональность записи динамически изменённых объектов.
		Score s = new Score("testScore");
		Grain g = s.getGrain("gtest");
		Table t = g.getElement("test", Table.class);
		StringWriter sw = new StringWriter();

		BufferedWriter bw = new BufferedWriter(sw);
		t.save(bw);
		bw.flush();
		// System.out.println(sw);

		String[] actual = sw.toString().split("\r?\n");
		// for (String l : actual)
		// System.out.println(l);
		BufferedReader r = new BufferedReader(
				new InputStreamReader(ScoreTest.class.getResourceAsStream("expectedsave.sql"), "utf-8"));
		for (String l : actual)
			assertEquals(r.readLine(), l);

		assertEquals("VARCHAR", t.getColumn("attrVarchar").getCelestaType());
		assertEquals("INT", t.getColumn("attrInt").getCelestaType());
		assertEquals("BIT", t.getColumn("f1").getCelestaType());
		assertEquals("REAL", t.getColumn("f5").getCelestaType());
		assertEquals("TEXT", t.getColumn("f6").getCelestaType());
		assertEquals("DATETIME", t.getColumn("f8").getCelestaType());
		assertEquals("BLOB", t.getColumn("f10").getCelestaType());

	}

	@Test
	public void saveTest2() throws ParseException, IOException {
		// Проверяется функциональность записи динамически изменённых объектов с
		// опциями (Read Only, Version Check).
		Score s = new Score();
		InputStream input = ParserTest.class.getResourceAsStream("test.sql");
		CelestaParser cp = new CelestaParser(input, "utf-8");
		Grain g = cp.grain(s, "test1");
		StringWriter sw = new StringWriter();
		BufferedWriter bw = new BufferedWriter(sw);

		Table t = g.getElement("ttt1", Table.class);
		t.save(bw);
		t = g.getElement("ttt2", Table.class);
		t.save(bw);
		t = g.getElement("ttt3", Table.class);
		t.save(bw);
		t = g.getElement("table1", Table.class);
		t.save(bw);
		bw.flush();
		// System.out.println(sw);

		String[] actual = sw.toString().split("\r?\n");
		BufferedReader r = new BufferedReader(
				new InputStreamReader(ScoreTest.class.getResourceAsStream("expectedsave2.sql"), "utf-8"));
		for (String l : actual)
			assertEquals(r.readLine(), l);

	}

	@Test
	public void fknameTest() throws ParseException, CelestaException {
		Score s = new Score("testScore");
		Grain g = s.getGrain("gtest");
		Table t = g.getElement("aLongIdentityTableNaaame", Table.class);
		ForeignKey[] ff = t.getForeignKeys().toArray(new ForeignKey[0]);
		assertEquals(2, ff.length);
		assertEquals(30, ff[0].getConstraintName().length());
		assertEquals(30, ff[1].getConstraintName().length());
		assertFalse(ff[0].getConstraintName().equals(ff[1].getConstraintName()));
	}

	@Test
	public void viewTest() throws ParseException, CelestaException {
		Score s = new Score("testScore");
		Grain g = s.getGrain("gtest");

		View v = g.getElement("testview", View.class);
		String exp;
		assertFalse(v.isDistinct());
		assertEquals(4, v.getColumns().size());
		exp = String.format("  select id as id, descr as descr, descr || 'foo' as descr2, k2 as k2%n"
				+ "  from test as test%n" + "    INNER join refTo as refTo on attrVarchar = k1 AND attrInt = k2");
		assertEquals(exp, v.getCelestaQueryString());

		assertTrue(v.getColumns().get("descr").isNullable());
		assertTrue(v.getColumns().get("descr2").isNullable());
		assertFalse(v.getColumns().get("k2").isNullable());
		assertFalse(v.getColumns().get("id").isNullable());

		v = g.getElement("testview2", View.class);
		assertEquals(ViewColumnType.INT, v.getColumns().get("id").getColumnType());
		exp = String.format("  select id as id, descr as descr%n" + "  from test as t1%n"
				+ "    INNER join refTo as t2 on attrVarchar = k1 AND NOT t2.descr IS NULL AND attrInt = k2");
		assertEquals(exp, v.getCelestaQueryString());

	}

	@Test
	public void vewTest2() throws CelestaException, ParseException {
		Score s = new Score("testScore");
		Grain g = s.getGrain("gtest");
		View v = g.getElement("v3", View.class);
		String[] expected = { "  select 1 as a, 1.4 as b, 1 as c, 1 as d, 1 as e, 1 as f, 1 as g, 1 as h, 1 as j",
				"    , 1 as k", "  from test as test" };
		assertEquals(ViewColumnType.INT, v.getColumns().get("a").getColumnType());
		assertEquals(ViewColumnType.REAL, v.getColumns().get("b").getColumnType());

		assertEquals("", v.getColumns().get("a").getCelestaDoc());
		assertFalse(v.getColumns().get("a").isNullable());
		assertFalse(v.getColumns().get("b").isNullable());
		assertEquals("test celestadoc", v.getColumns().get("b").getCelestaDoc());
		assertEquals("test celestadoc2", v.getColumns().get("c").getCelestaDoc());

		assertArrayEquals(expected, v.getCelestaQueryString().split("\\r?\\n"));

		assertEquals(3, v.getColumnIndex("d"));
		assertEquals(1, v.getColumnIndex("b"));
	}

	@Test
	public void viewTest3() throws CelestaException, ParseException {
		Score s = new Score("testScore");
		Grain g = s.getGrain("gtest");
		View v = g.getElement("v4", View.class);
		String[] expected = { "  select f1 as f1, f4 as f4, f5 as f5, f4 + f5 as s, f5 * f5 + 1 as s2",
				"  from test as test", "  where f1 = true" };
		assertArrayEquals(expected, v.getCelestaQueryString().split("\\r?\\n"));

		// Checking nullability evaluation
		assertFalse(v.getColumns().get("f1").isNullable());
		assertTrue(v.getColumns().get("f4").isNullable());
		assertFalse(v.getColumns().get("f5").isNullable());
		assertTrue(v.getColumns().get("s").isNullable());
		assertFalse(v.getColumns().get("s2").isNullable());
	}

	@Test
	public void viewTest4() throws CelestaException, ParseException {
		Score s = new Score("testScore");
		Grain g = s.getGrain("gtest");
		View v = g.getElement("v5", View.class);
		Table t = g.getElement("test", Table.class);

		ViewColumnMeta vcm = v.getColumns().get("foo");
		assertTrue(vcm.isNullable());
		assertEquals(StringColumn.VARCHAR, vcm.getCelestaType());
		assertEquals(((StringColumn) t.getColumn("attrVarchar")).getLength(), vcm.getLength());
		assertEquals(2, vcm.getLength());
		
		vcm = v.getColumns().get("bar");
		assertTrue(vcm.isNullable());
		assertEquals(StringColumn.VARCHAR, vcm.getCelestaType());
		assertEquals(((StringColumn) t.getColumn("f7")).getLength(), vcm.getLength());
		assertEquals(8, vcm.getLength());
		
		vcm = v.getColumns().get("baz");
		assertTrue(vcm.isNullable());
		assertEquals(StringColumn.VARCHAR, vcm.getCelestaType());
		assertEquals(-1, vcm.getLength());
	}
}
