package ru.curs.celesta.dbschemasync;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.score.Score;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchemaSyncTest {
    @Test
    void celestaToDbs() throws Exception {
        String scorePath = SchemaSyncTest.class
                .getClassLoader().getResource("score").getFile();
        Score s = new Score(scorePath);
        File tmp = File.createTempFile("sst", "tmp");
        tmp.delete();
        try {
            Celesta2DBSchema.scoreToDBS(s, tmp);
            try (
                    BufferedReader br = new BufferedReader(new InputStreamReader(
                            new FileInputStream(tmp), StandardCharsets.UTF_8))) {
                assertTrue(br.readLine().contains("xml"));
                assertTrue(br.readLine().contains("project"));
            }
        } finally {
            tmp.delete();
        }
    }

    @Test
    void dbsToCelesta() throws Exception {
        String dbs = SchemaSyncTest.class
                .getClassLoader().getResource("test.dbs").getFile();
        String scorePath = SchemaSyncTest.class
                .getClassLoader().getResource("score").getFile();
        File adoc = new File(scorePath, "../Layout_.adoc");
        adoc.delete();
        assertFalse(adoc.exists());
        Score s = new Score(scorePath);
        DBSchema2Celesta.dBSToScore(new File(dbs), s, true);
        assertTrue(adoc.exists());
    }

}
