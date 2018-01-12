package ru.curs.celesta.score.discovery;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.curs.celesta.CelestaException;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class PyScoreDiscoveryTest {

    private static final String SCORES_PATH = "src" + File.separator + "test" + File.separator + "resources"
            + File.separator + "scores" + File.separator;

    PyScoreDiscovery pyScoreDiscovery = new PyScoreDiscovery();
    Map<String, File> grainFiles = new HashMap<>();

    @BeforeEach
    void setUp() {
        grainFiles.clear();
    }

    @Test
    void testPyScoreWithoutScripts() throws CelestaException {
        File scoreDir = getScoreDir("emptyScore");
        pyScoreDiscovery.discoverScore(scoreDir, grainFiles);
        assertTrue(grainFiles.isEmpty());
    }

    @Test
    void testPyScoreWithoutPyInit() {
        File scoreDir = getScoreDir("badScore");
        assertThrows(CelestaException.class, () -> pyScoreDiscovery.discoverScore(scoreDir, grainFiles));
    }

    @Test
    void testGoodPyScore() throws CelestaException {
        File scoreDir = getScoreDir("goodScore");
        pyScoreDiscovery.discoverScore(scoreDir, grainFiles);

        assertAll(
                () -> assertEquals(1, grainFiles.size()),
                () -> assertTrue(grainFiles.containsKey("grain")),
                () -> assertEquals("_grain.sql", grainFiles.get("grain").getName())
        );
    }

    private File getScoreDir(String dirName) {
        return new File(SCORES_PATH + dirName);
    }
}
