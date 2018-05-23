package ru.curs.celesta.plugin;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.model.Plugin;
import org.apache.maven.plugin.MojoExecution;
import org.apache.maven.plugin.testing.AbstractMojoTestCase;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import ru.curs.celesta.CelestaException;


import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
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
                        "src/test/resources/scorePart1" + File.pathSeparator + "src/test/resources/scorePart2"),
                new ScoreProperties("src/test/resources/emptyScore")
        );

        assertEquals(expectedScores, mojo.scores);
    }

    public void testExecute() throws Exception {
        File celestaGeneratedRoot = new File("src/test/resources/unit/gen-cursors/target/generated-sources/celesta");
        try {
            FileUtils.deleteDirectory(celestaGeneratedRoot);
            File pom = getTestFile("target/test-classes/unit/gen-cursors/pom.xml");
            GenCursorsMojo mojo = (GenCursorsMojo) lookupMojo("gen-cursors", pom);
            setExecution(mojo);
            mojo.execute();
            assertGeneratedFiles();
        } finally {
            FileUtils.deleteDirectory(celestaGeneratedRoot);
        }

    }

    public void testFailOnGeneratingClassWithoutPackage() throws Exception {
        File pom = getTestFile("target/test-classes/unit/gen-cursors/badPom.xml");
        GenCursorsMojo mojo = (GenCursorsMojo) lookupMojo("gen-cursors", pom);
        setExecution(mojo);

        boolean celestaExceptionWasThrown = false;

        try {
            mojo.execute();
        } catch (CelestaException e) {
            celestaExceptionWasThrown = true;
        }

        assertTrue(celestaExceptionWasThrown);
    }

    private void assertGeneratedFiles() {
        String prefix = "src/test/resources/unit/gen-cursors/target/generated-sources/celesta/";
        String expectedPrefix = "src/test/resources/unit/gen-cursors/expectedGenerationResults/";
        List<String> paths = Arrays.asList(
                "seq/SeqSequence.java",
                "data/table/TestTableCursor.java",
                "data/table/TestTableWithIdentityCursor.java",
                "data/table/TestRoTableCursor.java",
                "data/view/TestTableVCursor.java",
                "data/view/TestTableMvCursor.java",
                "data/view/TestTablePvCursor.java"
        );

        JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();

        paths.forEach(p -> {
            File f = new File(prefix + p);
            assertTrue(f.exists());
            assertEquals(0, javaCompiler.run(null, null, null, f.getPath()));

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


    private void setExecution(GenCursorsMojo mojo) {
        Plugin plugin = new Plugin();
        mojo.execution = new MojoExecution(plugin, "gen-cursors", "exId");
        mojo.execution.setLifecyclePhase(LifecyclePhase.GENERATE_SOURCES.id());
    }
}
