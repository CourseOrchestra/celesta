package ru.curs.celesta.dbschemasync;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.score.AbstractScore;
import ru.curs.celesta.score.CelestaSerializer;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.GrainPart;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.discovery.ScoreByScorePathDiscovery;

import java.io.*;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchemaSyncTest {
    @Test
    void celestaToDbs() throws Exception {
        String scorePath = getScorePath();
        AbstractScore s = new AbstractScore.ScoreBuilder<>(Score.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(scorePath))
                .build();
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

    private String getScorePath() {
        return SchemaSyncTest.class
                .getClassLoader().getResource("score").getFile();
    }

    @Test
    void dbsToCelesta() throws Exception {
        String dbs = SchemaSyncTest.class
                .getClassLoader().getResource("test.dbs").getFile();
        String scorePath = getScorePath();
        File adoc = new File(scorePath, "../Layout_.adoc");
        adoc.delete();
        assertFalse(adoc.exists());
        AbstractScore s = new AbstractScore.ScoreBuilder<>(Score.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(scorePath))
                .build();
        DBSchema2Celesta.dBSToScore(new File(dbs), s, true);
        assertTrue(adoc.exists());
    }

    @Test
    void bothWays() throws Exception {
        String scorePath = getScorePath();
        Score s = new Score.ScoreBuilder<>(Score.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(scorePath))
                .build();
        Grain g = s.getGrain("logs");
        StringWriter oldval = new StringWriter();
        try (PrintWriter oldvalPrintWriter = new PrintWriter(oldval)) {
            CelestaSerializer serializer = new CelestaSerializer(oldvalPrintWriter);
            for (GrainPart gp : g.getGrainParts()) {
                serializer.save(gp);
            }
        }
        File tmp = File.createTempFile("sst", "tmp");
        tmp.delete();
        try {
            Celesta2DBSchema.scoreToDBS(s, tmp);
            DBSchema2Celesta.dBSToScore(tmp, s, false);
        } finally {
            tmp.delete();
        }
        StringWriter newval = new StringWriter();
        try (PrintWriter newvalPrintWriter = new PrintWriter(newval)) {
            CelestaSerializer serializer = new CelestaSerializer(newvalPrintWriter);
            for (GrainPart gp : g.getGrainParts()) {
                serializer.save(gp);
            }
        }
        assertEquals(oldval.toString().replaceAll("\\r\\n", "\n"),
                newval.toString().replaceAll("\\r\\n", "\n"));
    }

}
