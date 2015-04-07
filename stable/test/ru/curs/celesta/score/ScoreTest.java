package ru.curs.celesta.score;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;

import org.junit.Test;

import ru.curs.celesta.CelestaException;

public class ScoreTest {
	public static final Score S = new Score();

	@Test
	public void test1() throws CelestaException, ParseException {
		Score s = new Score("score;test");
		Grain g1 = s.getGrain("g1");
		Grain g2 = s.getGrain("g2");
		assertEquals("g2", g2.getName());
		Table b = g2.getTables().get("b");
		assertEquals(1, b.getForeignKeys().size());
		Table a = b.getForeignKeys().iterator().next().getReferencedTable();
		assertEquals("a", a.getName());
		assertSame(g1, a.getGrain());

		Grain g3 = s.getGrain("g3");

		int o = g1.getDependencyOrder();
		assertEquals(o + 1, g2.getDependencyOrder());
		assertEquals(o + 2, g3.getDependencyOrder());

		assertEquals("score" + File.separator + "g1", g1.getGrainPath()
				.toString());
		assertEquals("score" + File.separator + "g2", g2.getGrainPath()
				.toString());
		assertEquals("score" + File.separator + "g3", g3.getGrainPath()
				.toString());

		Grain sys = s.getGrain("celesta");
		a = sys.getTable("grains");
		assertEquals("grains", a.getName());
		assertEquals(o - 1, sys.getDependencyOrder());
		IntegerColumn c = (IntegerColumn) a.getColumns().get("state");
		assertEquals(3, c.getDefaultValue().intValue());
	}

	@Test
	public void test2() throws CelestaException, ParseException, IOException {
		Score s = new Score("score");
		Grain g1 = s.getGrain("g1");
		View v = g1.getView("testview");
		assertEquals("testview", v.getName());
		assertEquals("view description ", v.getCelestaDoc());
		assertTrue(v.isDistinct());

		assertEquals(4, v.getColumns().size());
		String[] ref = { "fieldAlias", "tablename", "checksum", "f1" };
		assertArrayEquals(ref, v.getColumns().keySet().toArray(new String[0]));

		String[] expected = {
				"  select distinct grainid as fieldAlias, ta.tablename as tablename, grains.checksum as checksum, ta.tablename || grains.checksum as f1",
				"  from celesta.tables as ta",
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

		Table b = g2.getTables().get("b");
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
		Table tables = celesta.getTable("tables");
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
		assertEquals("score" + File.separator + "newgrain", g4.getGrainPath()
				.toString());

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

		Table b = g2.getTables().get("b");
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

		Table b = g2.getTable("b");
		Table c = g3.getTable("c");

		assertEquals(1, c.getForeignKeys().size());
		ForeignKey fk = c.getForeignKeys().iterator().next();
		assertSame(b, fk.getReferencedTable());

		assertTrue(g2.getTables().containsKey("b"));
		b.delete();
		assertFalse(g2.getTables().containsKey("b"));
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
	public void modificationTest7() throws CelestaException, ParseException,
			IOException {
		Score s = new Score("score");
		Grain g1 = s.getGrain("g1");
		assertEquals(1, g1.getViews().size());
		View v = g1.getView("testview");
		assertFalse(g1.isModified());
		v.delete();
		assertEquals(0, g1.getViews().size());
		assertTrue(g1.isModified());
	}

	@Test
	public void modificationTest8() throws CelestaException, ParseException,
			IOException {
		Score s = new Score("score");
		Grain g1 = s.getGrain("g1");
		assertEquals(1, g1.getViews().size());
		assertFalse(g1.isModified());
		boolean itWas = false;
		View nv;
		try {
			nv = new View(g1, "testit",
					"select postalcode, city from adresses where flat = 5");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
		assertEquals(1, g1.getViews().size());
		assertTrue(g1.isModified());
		nv = new View(g1, "testit",
				"select postalcode, city from adresses where flat = '5'");
		assertEquals(2, nv.getColumns().size());
		assertEquals(2, g1.getViews().size());
		assertTrue(g1.isModified());

	}

	@Test
	public void setCelestaDoc() throws ParseException, CelestaException {
		Score s = new Score("testScore");
		Grain g = s.getGrain("gtest");
		Table t = g.getTable("test");
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
		Table t = g.getTable("test");
		StringWriter sw = new StringWriter();

		BufferedWriter bw = new BufferedWriter(sw);
		t.save(bw);
		bw.flush();
		// System.out.println(sw);

		String[] actual = sw.toString().split("\r?\n");
		// for (String l : actual)
		// System.out.println(l);
		BufferedReader r = new BufferedReader(new InputStreamReader(
				ScoreTest.class.getResourceAsStream("expectedsave.sql"),
				"utf-8"));
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

		Table t = g.getTable("ttt1");
		t.save(bw);
		t = g.getTable("ttt2");
		t.save(bw);
		t = g.getTable("ttt3");
		t.save(bw);
		t = g.getTable("table1");
		t.save(bw);
		bw.flush();
		// System.out.println(sw);

		String[] actual = sw.toString().split("\r?\n");
		BufferedReader r = new BufferedReader(new InputStreamReader(
				ScoreTest.class.getResourceAsStream("expectedsave2.sql"),
				"utf-8"));
		for (String l : actual)
			assertEquals(r.readLine(), l);

	}

	@Test
	public void fknameTest() throws ParseException, CelestaException {
		Score s = new Score("testScore");
		Grain g = s.getGrain("gtest");
		Table t = g.getTable("aLongIdentityTableNaaame");
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

		View v = g.getView("testview");
		String exp;
		assertFalse(v.isDistinct());
		assertEquals(3, v.getColumns().size());
		exp = String
				.format("  select id as id, descr as descr, descr || 'foo' as descr2%n"
						+ "  from test as test%n"
						+ "    INNER join refTo as refTo on attrVarchar = k1 AND attrInt = k2");
		assertEquals(exp, v.getCelestaQueryString());
		v = g.getView("testview2");
		exp = String
				.format("  select id as id, descr as descr%n"
						+ "  from test as t1%n"
						+ "    INNER join refTo as t2 on attrVarchar = k1 AND NOT t2.descr IS NULL AND attrInt = k2");
		assertEquals(exp, v.getCelestaQueryString());
	}
}
