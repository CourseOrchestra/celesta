package ru.curs.celesta.plugin;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.testing.AbstractMojoTestCase;


import java.io.File;
import java.util.Arrays;
import java.util.List;

public class GenCursorsMojoTest extends AbstractMojoTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testConfig() throws Exception {
        File pom = getTestFile("target/test-classes/unit/gen-cursors/pom.xml");
        assertNotNull(pom);
        assertTrue(pom.exists());
        GenCursorsMojo mojo = (GenCursorsMojo) lookupMojo("gen-cursors", pom);
        assertNotNull(mojo);
        assertEquals(2, mojo.scores.size());

        List<ScoreProperties> expectedScores = Arrays.asList(
                new ScoreProperties(
                        "src/test/resources/scorePart1" + File.pathSeparator + "src/test/resources/scorePart2",
                        "ru.curs.celesta.jcursor.score"
                ),
                new ScoreProperties(
                        "src/test/resources/emptyScore",
                        "ru.curs.celesta.jcursor.empty"
                )
        );

        assertEquals(expectedScores, mojo.scores);
    }

    public void testExecute() throws Exception {
        File pom = getTestFile("target/test-classes/unit/gen-cursors/pom.xml");
        GenCursorsMojo mojo = (GenCursorsMojo) lookupMojo("gen-cursors", pom);
        mojo.execute();
        assertGeneratedFiles();
    }

    private void assertGeneratedFiles() {
        String prefix = "src/test/resources/unit/gen-cursors/target/generated-sources/" +
                "celesta/ru/curs/celesta/jcursor/score/";
        String expectedPrefix = "src/test/resources/unit/gen-cursors/expectedGenerationResults/";
        List<String> paths = Arrays.asList(
                "SeqSequence.java",
                "data/TestTableCursor.java",
                "data/TestTableWithIdentityCursor.java",
                "data/TestRoTableCursor.java",
                "data/TestTableVCursor.java",
                "data/TestTableMvCursor.java",
                "data/TestTablePvCursor.java"
        );

        paths.forEach(p -> {
            File f = new File(prefix + p);
            assertTrue(f.exists());

            File expectedF = new File(expectedPrefix + p);
            try {
                assertEquals(
                        StringUtils.normalizeSpace(FileUtils.readFileToString(expectedF)),
                        StringUtils.normalizeSpace(FileUtils.readFileToString(f))
                );
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });



    }

}
