package ru.curs.celesta.score;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.io.File;

import org.junit.Test;

import ru.curs.celesta.CelestaCritical;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.Table;

public class ScoreTest {
	@Test
	public void test1() throws CelestaCritical, ParseException {
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

}
