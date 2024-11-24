package ru.curs.celesta.score;

import ru.curs.celesta.score.io.FileResource;

import java.io.File;
import java.io.IOException;

/**
 * Created by ioann on 15.06.2017.
 */
abstract public class AbstractParsingTest {
    AbstractScore s = new CelestaSqlTestScore();

    Grain parse(File f) throws ParseException, IOException {
        final GrainPart gp;

        FileResource fr = new FileResource(f);
        try (ChecksumInputStream is = new ChecksumInputStream(fr.getInputStream())) {
            CelestaParser cp1 = new CelestaParser(is, "utf-8");
            gp = cp1.extractGrainInfo(s, fr);
        }

        try (ChecksumInputStream is = new ChecksumInputStream(fr.getInputStream())) {
            CelestaParser cp2 = new CelestaParser(is, "utf-8");
            Grain g = cp2.parseGrainPart(gp);

            g.setLength(g.getLength() + is.getCount());
            g.setChecksum(g.getChecksum() + is.getCRC32());
            g.finalizeParsing();

            return g;
        }
    }

}
