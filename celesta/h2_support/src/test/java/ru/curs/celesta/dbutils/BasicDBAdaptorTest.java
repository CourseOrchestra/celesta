package ru.curs.celesta.dbutils;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.Test;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;

public class BasicDBAdaptorTest {

	private void testCelestaScore(Score s, DBAdaptor a, String fileName)
			throws ParseException, IOException {
		String[] actual = (a.tableDef(s.getGrain("celesta").getTable("grains"))
				+ "\n" + a.tableDef(s.getGrain("celesta").getTable("tables")))
				.split("\n");
		// for (String l : actual)
		// System.out.println(l);
		BufferedReader r = new BufferedReader(new InputStreamReader(
				BasicDBAdaptorTest.class.getResourceAsStream(fileName), "utf-8"));
		for (String l : actual)
			assertEquals(r.readLine(), l);
	}

	@Test
	public void test1() throws CelestaException, ParseException, IOException {
		Score s = new Score("score");

		DBAdaptor a = new MSSQLAdaptor();
		testCelestaScore(s, a, "mssql.txt");

		a = new PostgresAdaptor();
		testCelestaScore(s, a, "postgre.txt");

		a = new OraAdaptor();
		testCelestaScore(s, a, "ora.txt");
	}
}
