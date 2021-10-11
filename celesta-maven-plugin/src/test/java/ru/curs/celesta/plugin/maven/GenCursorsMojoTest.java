package ru.curs.celesta.plugin.maven;

import ru.curs.celesta.CelestaException;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class GenCursorsMojoTest extends AbstractCelestaMojoTestCase {

    private final static String CELESTA_GENERATED_SOURCES_DIR =
            TEST_UNIT_DIR + "/target/generated-sources/celesta";

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
        GenCursorsMojo mojo = (GenCursorsMojo) lookupMojo("gen-cursors", pom);
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

    public void testExecuteGenCursors() throws Exception {
        File pom = setupPom("pom.xml");
        setupScore("scorePart1", CELESTASQL_SOURCES_DIR);

        GenCursorsMojo mojo = (GenCursorsMojo) lookupMojo("gen-cursors", pom);
        mojo.execute();
        assertGeneratedCursors(
            CELESTA_GENERATED_SOURCES_DIR,
            Arrays.asList(
                "seq/SeqSequence.java",
                "data/table/TestTableCursor.java",
                "data/table/TestRoTableCursor.java",
                "data/table/TestSnakeTableCursor.java")
        );
    }

    /*
     * This test fails since partial grains are not resolved properly.")
     */
/*
    public void testExecuteGenCursors() throws Exception {
        File pom = setupPom("pom.xml");
        setupScore("scorePart1", CELESTA_SQL_SOURCES_DIR);
        setupScore("scorePart2", CELESTA_SQL_SOURCES_DIR);

        GenCursorsMojo mojo = (GenCursorsMojo) lookupMojo("gen-cursors", pom);
        mojo.execute();
        assertGeneratedCursors(
            CELESTA_GENERATED_SOURCES_DIR,
            Arrays.asList(
                "seq/SeqSequence.java",
                "data/table/TestTableCursor.java",
                "data/table/TestRoTableCursor.java",
                "data/view/TestTableVCursor.java",
                "data/view/TestTableMvCursor.java",
                "data/view/TestTablePvCursor.java")
        );
    }
*/
    public void testExecuteGenCursors_score() throws Exception {
        File pom = setupPom("pom_score.xml");
        GenCursorsMojo mojo = (GenCursorsMojo) lookupMojo("gen-cursors", pom);
        mojo.execute();
        assertGeneratedCursors(
            CELESTA_GENERATED_SOURCES_DIR,
            Arrays.asList(
                "seq/SeqSequence.java",
                "data/table/TestTableCursor.java",
                "data/table/TestRoTableCursor.java",
                "data/view/TestTableVCursor.java",
                "data/view/TestTableMvCursor.java",
                "data/view/TestTablePvCursor.java")
        );
    }

    public void testFailOnGeneratingClassWithoutPackage() throws Exception {
        File pom = setupPom("pom_badScore.xml");
        GenCursorsMojo mojo = (GenCursorsMojo) lookupMojo("gen-cursors", pom);

        assertThrows(CelestaException.class, () ->  mojo.execute());
    }

}
