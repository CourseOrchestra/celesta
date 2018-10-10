package ru.curs.celesta.score;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by ioann on 15.06.2017.
 */
abstract public class AbstractParsingTest {
    AbstractScore s = new CelestaSqlTestScore();

    Grain parse(File f) throws ParseException, IOException {
        final GrainPart gp;

        try (ChecksumInputStream is = new ChecksumInputStream(new FileInputStream(f))) {
            CelestaParser cp1 = new CelestaParser(is, "utf-8");

            gp = cp1.extractGrainInfo(s, f);
        }

        try (ChecksumInputStream is = new ChecksumInputStream(new FileInputStream(f))) {
            CelestaParser cp2 = new CelestaParser(is, "utf-8");
            Grain g = cp2.parseGrainPart(gp);

            g.setLength(g.getLength() + is.getCount());
            g.setChecksum(g.getChecksum() + is.getCRC32());
            g.finalizeParsing();

            return g;
        }

    }
}
