package ru.curs.celesta.ormcompiler;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;

import org.junit.Test;

import ru.curs.celesta.score.CelestaParser;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.ParserTest;
import ru.curs.celesta.score.ScoreTest;

public class CompileTest {
	@Test
	public void compileTest() throws ParseException, IOException {

		InputStream input = ParserTest.class.getResourceAsStream("test.sql");
		CelestaParser cp = new CelestaParser(input, "utf-8");
		Grain g = cp.grain(ScoreTest.S, "test1");
		StringWriter sw = new StringWriter();
		BufferedWriter bw = new BufferedWriter(sw);
		ORMCompiler.compileROTable(g.getTable("ttt1"), bw);
		ORMCompiler.compileTable(g.getTable("ttt2"), bw);
		bw.flush();
		// System.out.println(sw);

		String[] actual = sw.toString().split("\r?\n");
		BufferedReader r = new BufferedReader(new InputStreamReader(
				CompileTest.class.getResourceAsStream("expectedcompile.txt"),
				"utf-8"));
		for (String l : actual)
			assertEquals(r.readLine(), l);
	}
}
