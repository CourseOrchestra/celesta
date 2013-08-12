package ru.curs.celesta.dbutils;

import org.junit.Test;

import ru.curs.celesta.CelestaCritical;
import ru.curs.celesta.dbutils.MSSQLAdaptor;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;

public class DBAdaptorTest {

	@Test
	public void test1() throws CelestaCritical, ParseException {
		Score s = new Score("score");
		DBAdaptor a = new MSSQLAdaptor();
		System.out
				.println(a.tableDef(s.getGrain("celesta").getTable("grains")));

		a = new PostgresAdaptor();
		System.out
				.println(a.tableDef(s.getGrain("celesta").getTable("grains")));

		a = new MySQLAdaptor();
		System.out
				.println(a.tableDef(s.getGrain("celesta").getTable("grains")));
		a = new OraAdaptor();
		System.out
				.println(a.tableDef(s.getGrain("celesta").getTable("grains")));
	}
}
