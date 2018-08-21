package ru.curs.celesta.ormcompiler;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import java.io.*;

import ru.curs.celesta.score.*;

//TODO: This class must be actualized
public class CompileTest {
    @Test
    public void compileTest() throws ParseException, IOException {

        AbstractScore score = ScoreAccessor.createEmptyScore();

        File f = new File(this.getClass().getResource("test.sql").getPath());

        final Grain g;

        try (
                InputStream is1 = new FileInputStream(f);
                InputStream is2 = new FileInputStream(f)
        ) {
            CelestaParser cp1 = new CelestaParser(is1, "utf-8");
            CelestaParser cp2 = new CelestaParser(is2, "utf-8");
            GrainPart gp = cp1.extractGrainInfo(score, f);
            g = cp2.parseGrainPart(gp);
        }

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
