package ru.curs.celesta.ormcompiler;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URL;


import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.*;

//TODO: This class must be actualized
public class CompileTest {
	@Test
	public void compileTest() throws CelestaException, ParseException, IOException {

		URL url = CompileTest.class.getResource("test.sql");
		CelestaParser cp = new CelestaParser(url.openStream(), "utf-8");
		Score score = ScoreAccessor.createEmptyScore();
		Grain g = cp.grain(score, "test1");
		StringWriter sw = new StringWriter();
		PrintWriter bw = new PrintWriter(sw);
		ORMCompiler.compileROTable(g.getElement("ttt1", Table.class), bw);
		ORMCompiler.compileTable(g.getElement("ttt2", Table.class), bw);
		bw.flush();
		// System.out.println(sw);

		String[] actual = sw.toString().split("\r?\n");
		BufferedReader r = new BufferedReader(
				new InputStreamReader(CompileTest.class.getResourceAsStream("expectedcompile.txt"), "utf-8"));
		for (String l : actual)
			assertEquals(r.readLine(), l);
	}
}
