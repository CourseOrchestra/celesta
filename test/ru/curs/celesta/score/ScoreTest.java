package ru.curs.celesta.score;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.Table;

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

}
