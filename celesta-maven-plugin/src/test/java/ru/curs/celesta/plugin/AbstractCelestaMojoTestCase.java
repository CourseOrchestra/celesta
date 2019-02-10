package ru.curs.celesta.plugin;

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

abstract class AbstractCelestaMojoTestCase extends AbstractMojoTestCase {

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
                     ex.printStackTrace();
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

        cursorPaths.forEach(p -> {
            File f = new File(prefix, p);
            assertTrue(f.exists());
            assertEquals(0, javaCompiler.run(null, null, null, f.getPath()));

            File expectedF = new File(expectedPrefix, p);
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
