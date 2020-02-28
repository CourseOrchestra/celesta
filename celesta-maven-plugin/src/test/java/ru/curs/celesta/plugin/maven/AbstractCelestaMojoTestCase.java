package ru.curs.celesta.plugin.maven;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.testing.AbstractMojoTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractCelestaMojoTestCase extends AbstractMojoTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCelestaMojoTestCase.class);

    final static String TEST_RESOURCES_DIR = "/src/test/resources";
    final static String TEST_UNIT_DIR = CelestaMavenPluginStub.UNIT_DIR;

    final static String CELESTASQL_SOURCES_DIR = TEST_UNIT_DIR + "/src/main/celestasql";
    final static String CELESTASQL_TEST_SOURCES_DIR = TEST_UNIT_DIR + "/src/test/celestasql";

    void removeDir(File dir) throws IOException {
        if (dir.isDirectory() && dir.exists()) {
            Files.walk(dir.toPath())
                 .sorted(Comparator.reverseOrder())
                 .map(Path::toFile)
                 .forEach(File::delete);
        }
    }

    File setupPom(String pomFileName) throws IOException {

        return Files.copy(new File(getTestPath("/target/test-classes"), pomFileName).toPath(),
                          new File(getTestPath(TEST_UNIT_DIR), "pom.xml").toPath())
                .toFile();
    }


    void setupScore(String scoreName, String celestaSqlDir) throws IOException {

        Path fromPath = getTestScorePath(scoreName);
        Path toPath = getTestFile(celestaSqlDir).toPath();

        Files.walk(fromPath)
             .filter(Files::isRegularFile)
             .forEach((from) -> {
                 Path to = toPath.resolve(fromPath.relativize(from));
                 try {
                     Files.createDirectories(to.getParent());
                     Files.copy(from, to);
                 } catch (IOException ex) {
                     LOGGER.error("Error during score setup", ex);
                 }
             });
    }

    private Path getTestScorePath(String scoreName) {
        return getTestFile(TEST_RESOURCES_DIR).toPath().resolve("score").resolve(scoreName);
    }

    void assertGeneratedCursors(String generatedSourcesDir, List<String> cursorPaths) {

        File prefix = getTestFile(generatedSourcesDir);
        File expectedPrefix = getTestFile(TEST_RESOURCES_DIR + "/gen-cursors/expectedGenerationResults");

        JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();

        cursorPaths.forEach(p -> assertGeneratedCursor(p, prefix, expectedPrefix, javaCompiler));
    }

    void assertGeneratedCursor(String p, File prefix, File expectedPrefix, JavaCompiler javaCompiler) {
        File f = new File(prefix, p);
        assertTrue(f.exists());
        assertEquals(0, javaCompiler.run(null, null, null, f.getPath()));

        File expectedF = new File(expectedPrefix, p);
        System.out.printf("Comparing files:\n  %s\n  %s\n", expectedF, f);
        try {
            assertEquals(
                    replaceDateInGeneratedAnnotation(StringUtils.normalizeSpace(FileUtils.readFileToString(expectedF))),
                    replaceDateInGeneratedAnnotation(StringUtils.normalizeSpace(FileUtils.readFileToString(f)))
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    String replaceDateInGeneratedAnnotation(String s) {
        return s.replaceAll(
                "@Generated\\( value = \"ru\\.curs\\.celesta\\.plugin\\.maven\\.CursorGenerator\", date = \"[^\"]+\" \\)",
                "@Generated( value = \"ru.curs.celesta.plugin.maven.CursorGenerator\", date = \"2020-02-25T10:00:00\" )");
    }

    void assertGeneratedScore(
            String expectedScoreName, String generatedResourcesDir, List<String> grainPaths) throws IOException {

        Path expectedScorePath = getTestScorePath(expectedScoreName);
        Path generatedResourcesPath = getTestFile(generatedResourcesDir).toPath();

        for (String grainPath : grainPaths) {
            String expectedGrain = FileUtils.readFileToString(expectedScorePath.resolve(grainPath).toFile());
            String generatedGrain = FileUtils.readFileToString(generatedResourcesPath.resolve(grainPath).toFile());
            assertEquals(expectedGrain, generatedGrain);
        }
    }

}
