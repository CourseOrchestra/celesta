package ru.curs.celesta.score.discovery;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class PyScoreDiscoveryTest {

    private static final String SCORES_PATH = "src" + File.separator + "test" + File.separator + "resources"
            + File.separator + "scores" + File.separator;

    PyScoreDiscovery pyScoreDiscovery = new PyScoreDiscovery();
    Set<File> grainPartFiles;

    @Test
    void testPyScoreWithoutScripts() {
        File scoreDir = getScoreDir("emptyScore");
        grainPartFiles = pyScoreDiscovery.discoverScore(scoreDir);
        assertTrue(grainPartFiles.isEmpty());
    }

    @Test
    void testPyScoreWithoutPyInit() {
        File scoreDir = getScoreDir("badScore");
        assertThrows(RuntimeException.class, () -> pyScoreDiscovery.discoverScore(scoreDir));
    }

    @Test
    void testGoodPyScore() {
        File scoreDir = getScoreDir("goodScore");
        grainPartFiles = pyScoreDiscovery.discoverScore(scoreDir);

        assertAll(
                () -> assertEquals(1, grainPartFiles.size()),
                () -> assertEquals("_grain.sql", grainPartFiles.stream().findFirst().get().getName())
        );
    }

    private File getScoreDir(String dirName) {
        return new File(SCORES_PATH + dirName);
    }
}
