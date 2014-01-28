package ru.curs.celesta.score;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import ru.curs.celesta.CelestaException;

public class ScoreTest {
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
		assertEquals("view description", v.getCelestaDoc());
		assertTrue(v.isDistinct());

		assertEquals(4, v.getColumns().size());
		String[] ref = { "fieldAlias", "tablename", "checksum", "f1" };
		assertArrayEquals(ref, v.getColumns().keySet().toArray(new String[0]));

		// StringWriter sw = new StringWriter();
		// BufferedWriter bw = new BufferedWriter(sw);
		// v.save(bw);
		// bw.flush();
		// System.out.println(sw.toString());
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
}
