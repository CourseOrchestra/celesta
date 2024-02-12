package ru.curs.celesta.plugin.maven;

import org.junit.jupiter.api.Test;

import ru.curs.celesta.score.AbstractScore;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.GrainElement;
import ru.curs.celesta.score.GrainPart;
import ru.curs.celesta.score.Namespace;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.io.FileResource;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CursorGeneratorTest {

    @Test
    public void testCalcSourcePackageForDefaultPackage() throws ParseException {

        String sourcePackage = CursorGenerator.calcSourcePackage(
                getGrainElement("/score/defaultTable.sql"), "/score");

        assertEquals(Namespace.DEFAULT.getValue(), sourcePackage);
    }

    private GrainElement getGrainElement(String sqlFilePath) throws ParseException {
        AbstractScore score = new Score();
        Grain g = new Grain(score, "testGrain");
        GrainPart gp = new GrainPart(g, true, new FileResource(new File(sqlFilePath)));

        return new GrainElement(gp, "testGrainElement") {};
    }

    @Test
    public void testCalcSourcePackageForSomePackage() throws ParseException {

        String sourcePackage = CursorGenerator.calcSourcePackage(
                getGrainElement("/score/some/package/defaultTable.sql"), "/score");

        assertEquals(new Namespace("some.package").getValue(), sourcePackage);
    }

}
