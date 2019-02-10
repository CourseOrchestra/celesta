package ru.curs.celesta.plugin;

import ru.curs.celesta.CelestaException;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

public class GenTestScoreResourcesMojoTest extends AbstractCelestaMojoTestCase {

    private final static String CELESTA_GENERATED_TEST_RESOURCES_DIR =
            TEST_UNIT_DIR + "/target/generated-test-resources/score";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        removeDir(getTestFile(TEST_UNIT_DIR));
        getTestFile(TEST_UNIT_DIR).mkdir();
    }

    @Override
    protected void tearDown() throws Exception {
        removeDir(getTestFile(TEST_UNIT_DIR));
        super.tearDown();
    }

    public void testScoresConfig() throws Exception {
        File pom = setupPom("pom_score.xml");
        GenTestScoreResourcesMojo mojo = (GenTestScoreResourcesMojo) lookupMojo("gen-test-score-resources", pom);
        assertNotNull(mojo);
        assertEquals(2, mojo.scores.size());

        List<ScoreProperties> expectedScores = Arrays.asList(
                new ScoreProperties(
                        "src/test/resources/score/scorePart1"
                      + File.pathSeparator
                      + "src/test/resources/score/scorePart2"),
                new ScoreProperties("src/test/resources/score/emptyScore")
        );

        assertEquals(expectedScores, mojo.scores);
    }

    public void testExecuteGenScoreResources() throws Exception {
        File pom = setupPom("pom.xml");
        setupScore("scorePart1", CELESTASQL_TEST_SOURCES_DIR);

        GenTestScoreResourcesMojo mojo = (GenTestScoreResourcesMojo) lookupMojo("gen-test-score-resources", pom);
        mojo.execute();

        List<String> grainPaths = Arrays.asList(
            "data/table/table.sql",
            "seq/sequence.sql"
        );

        assertGeneratedScore("scorePart1", CELESTA_GENERATED_TEST_RESOURCES_DIR, grainPaths);

        List<String> generatedGrainPaths = Files.readAllLines(
                getTestFile(CELESTA_GENERATED_TEST_RESOURCES_DIR).toPath()
                    .resolve(GenTestScoreResourcesMojo.SCORE_FILES_FILE_NAME));
        assertEquals(grainPaths, generatedGrainPaths);
    }

    public void testFailOnGeneratingScoresWithoutPackage() throws Exception {
        File pom = setupPom("pom_badScore.xml");
        GenTestScoreResourcesMojo mojo = (GenTestScoreResourcesMojo) lookupMojo("gen-test-score-resources", pom);

        assertThrows(CelestaException.class, () ->  mojo.execute());
    }

}
