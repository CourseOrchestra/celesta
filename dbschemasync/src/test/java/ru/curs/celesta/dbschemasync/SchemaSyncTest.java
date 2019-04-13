package ru.curs.celesta.dbschemasync;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.curs.celesta.score.AbstractScore;
import ru.curs.celesta.score.CelestaSerializer;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.discovery.ScoreByScorePathDiscovery;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchemaSyncTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaSyncTest.class);

    @Test
    void celestaToDbs() throws Exception {

        File tmpDir = Files.createTempDirectory("celestaTest").toFile();
        try {
            String scorePath = getScorePath("score");

            AbstractScore s = new AbstractScore.ScoreBuilder<>(Score.class)
                    .scoreDiscovery(new ScoreByScorePathDiscovery(scorePath))
                    .build();

            File dbs = new File(tmpDir, "test.dbs");

            Celesta2DBSchema.scoreToDBS(s, dbs);
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(dbs), StandardCharsets.UTF_8))) {
                assertTrue(br.readLine().contains("xml"));
                assertTrue(br.readLine().contains("project"));
            }
        } finally {
            removeDir(tmpDir);
        }
    }

    private String getScorePath(String scoreName) {
        return getResourcePath("scores/" + scoreName);
    }

    private String getResourcePath(String resourceName) {
        return SchemaSyncTest.class
                .getClassLoader().getResource(resourceName).getFile();
    }

    @Test
    void dbsToCelesta() throws Exception {

        File tmpDir = Files.createTempDirectory("celestaTest").toFile();
        try {
            File dbs = copyFile(new File(getResourcePath("test.dbs")), tmpDir);
            File adocCelesta = new File(tmpDir, "celesta.adoc");
            File adocLayout_ = new File(tmpDir, "Layout_.adoc");

            File score = new File(tmpDir, "score");
            copyDir(new File(getScorePath("score")), score);

            File scoreLogs = new File(score, "logs/logs.sql");

            assertTrue(scoreLogs.exists());

            AbstractScore s = new AbstractScore.ScoreBuilder<>(Score.class)
                    .scoreDiscovery(new ScoreByScorePathDiscovery(score.toString()))
                    .build();
            DBSchema2Celesta.dBSToScore(dbs, s, score, true);

            assertTrue(scoreLogs.exists());

            assertTrue(adocCelesta.exists());
            assertTrue(adocLayout_.exists());

        } finally {
            removeDir(tmpDir);
        }
    }

    @Test
    void dbsToCelestaLegacy() throws Exception {

        File tmpDir = Files.createTempDirectory("celestaTest").toFile();
        try {
            File dbs = copyFile(new File(getResourcePath("test.dbs")), tmpDir);

            File score = new File(tmpDir, "score");
            copyDir(new File(getScorePath("legacyScore")), score);

            File scoreLogsLegacy = new File(score, "logs/_logs.sql");
            File scoreLogs = new File(score, "logs/logs.sql");

            assertTrue(scoreLogsLegacy.exists());
            assertFalse(scoreLogs.exists());

            AbstractScore s = new AbstractScore.ScoreBuilder<>(Score.class)
                    .scoreDiscovery(new ScoreByScorePathDiscovery(score.toString()))
                    .build();
            DBSchema2Celesta.dBSToScore(dbs, s, score, false);

            assertFalse(scoreLogsLegacy.exists());
            assertTrue(scoreLogs.exists());

        } finally {
            removeDir(tmpDir);
        }
    }

    private static File copyFile(File fromFile, File to) throws IOException {
        if (to.isDirectory()) {
            to = new File(to, fromFile.getName());
        }

        return Files.copy(fromFile.toPath(), to.toPath(), StandardCopyOption.REPLACE_EXISTING)
                    .toFile();
    }

    private static void copyDir(File fromDir, File toDir) throws IOException {

        Path fromPath = fromDir.toPath();
        Path toPath = toDir.toPath();

        Files.walk(fromPath)
             .filter(Files::isRegularFile)
             .forEach((from) -> {
                 Path to = toPath.resolve(fromPath.relativize(from));
                 try {
                     Files.createDirectories(to.getParent());
                     Files.copy(from, to);
                 } catch (IOException ex) {
                     LOGGER.error("Error during file copying", ex);
                 }
             });
    }

    private static void removeDir(File dir) throws IOException {
        if (dir.isDirectory() && dir.exists()) {
            Files.walk(dir.toPath())
                 .sorted(Comparator.reverseOrder())
                 .map(Path::toFile)
                 .forEach(File::delete);
        }
    }

    @Test
    void dbsToCelestaNoNamespace() throws Exception {

        File tmpDir = Files.createTempDirectory("celestaTest").toFile();
        try {
            File dbs = copyFile(new File(getResourcePath("test.dbs")), tmpDir);

            File score = new File(tmpDir, "score");
            copyDir(new File(getScorePath("noNamespaceScore")), score);

            File scoreLogsNoNamespace = new File(score, "logs.sql");
            File scoreLogs = new File(score, "logs/logs.sql");

            assertTrue(scoreLogsNoNamespace.exists());
            assertFalse(scoreLogs.exists());

            AbstractScore s = new AbstractScore.ScoreBuilder<>(Score.class)
                    .scoreDiscovery(new ScoreByScorePathDiscovery(score.toString()))
                    .build();
            DBSchema2Celesta.dBSToScore(dbs, s, score, false);

            assertFalse(scoreLogsNoNamespace.exists());
            assertTrue(scoreLogs.exists());

        } finally {
            removeDir(tmpDir);
        }
    }

    @Test
    void bothWays() throws Exception {

        String scorePath = getScorePath("score");
        Score s = new Score.ScoreBuilder<>(Score.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(scorePath))
                .build();

        Grain g = s.getGrain("logs");

        StringWriter oldval = new StringWriter();
        try (PrintWriter oldvalPrintWriter = new PrintWriter(oldval)) {
            CelestaSerializer serializer = new CelestaSerializer(oldvalPrintWriter);
            serializer.save(g);
        }

        File tmpDir = Files.createTempDirectory("celestaTest").toFile();
        try {
            File dbs = new File(tmpDir, "test.dbs");
            File score = new File(tmpDir, "score");
            Celesta2DBSchema.scoreToDBS(s, dbs);
            DBSchema2Celesta.dBSToScore(dbs, s, score, false);
        } finally {
            removeDir(tmpDir);
        }

        StringWriter newval = new StringWriter();
        try (PrintWriter newvalPrintWriter = new PrintWriter(newval)) {
            CelestaSerializer serializer = new CelestaSerializer(newvalPrintWriter);
            serializer.save(g);
        }

        assertEquals(oldval.toString().replaceAll("\\r\\n", "\n"),
                     newval.toString().replaceAll("\\r\\n", "\n"));
    }

}
