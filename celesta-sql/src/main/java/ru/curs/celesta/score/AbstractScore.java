/*
   (с) 2013 ООО "КУРС-ИТ"

   Этот файл — часть КУРС:Celesta.

   КУРС:Celesta — свободная программа: вы можете перераспространять ее и/или изменять
   ее на условиях Стандартной общественной лицензии GNU в том виде, в каком
   она была опубликована Фондом свободного программного обеспечения; либо
   версии 3 лицензии, либо (по вашему выбору) любой более поздней версии.

   Эта программа распространяется в надежде, что она будет полезной,
   но БЕЗО ВСЯКИХ ГАРАНТИЙ; даже без неявной гарантии ТОВАРНОГО ВИДА
   или ПРИГОДНОСТИ ДЛЯ ОПРЕДЕЛЕННЫХ ЦЕЛЕЙ. Подробнее см. в Стандартной
   общественной лицензии GNU.

   Вы должны были получить копию Стандартной общественной лицензии GNU
   вместе с этой программой. Если это не так, см. http://www.gnu.org/licenses/.


   Copyright 2013, COURSE-IT Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see http://www.gnu.org/licenses/.

 */
package ru.curs.celesta.score;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.RepeatedParseException;
import ru.curs.celesta.score.discovery.ScoreByScorePathDiscovery;
import ru.curs.celesta.score.discovery.ScoreDiscovery;
import ru.curs.celesta.score.io.Resource;
import ru.curs.celesta.score.validator.IdentifierParser;

/**
 * Root class for complete data model of grains.
 */
public abstract class AbstractScore {

    static final String DEPENDENCY_SCHEMA_DOES_NOT_EXIST_ERROR_TEMPLATE
            = "Couldn't parse schema '%s'. Dependency schema '%s' does not exist.";

    static final String CYCLIC_REFERENCES_ERROR_TEMPLATE = "Error parsing grain %s "
            + "due to previous parsing errors or "
            + "cycle reference involving grains '%s' and '%s'.";

    private final Map<String, Grain> grains = new HashMap<>();

    private final Map<String, List<GrainPart>> grainNameToGrainParts = new LinkedHashMap<>();

    private int orderCounter;

    private final Set<GrainPart> currentlyParsingGrainParts = new HashSet<>();

    protected AbstractScore() {
    }

    /**
     * Core initialization by providing a set of paths to 'score' directories
     * delimited by semicolon.
     *
     * @throws CelestaException in case if non-existing path is provided or in case if
     *                          there's a double definition of a grain with the same name.
     */
    void init(ScoreDiscovery scoreDiscovery) throws ParseException {

        Set<Resource> grainResources = scoreDiscovery.discoverScore();

        initSystemGrain();

        //The first parsing step - the grouping of resources by grain names.
        fillGrainNameToGrainParts(grainResources);

        // At this moment in the table 'grainFiles' a recognized set of grain names
        // with the names of script files is contained.
        parseGrains(new StringBuilder());
    }

    private void fillGrainNameToGrainParts(Set<Resource> resources) throws ParseException {

        List<GrainPart> grainParts = new ArrayList<>();

        for (Resource r : resources) {
            GrainPart grainPart = extractGrainInfo(r, false);
            grainParts.add(grainPart);
        }

        grainParts.sort((o1, o2) -> {
            if (o1.isDefinition() && !o2.isDefinition()) {
                return -1;
            } else if (o1.isDefinition() == o2.isDefinition()) {
                return 0;
            } else {
                return 1;
            }
        });


        for (GrainPart grainPart : grainParts) {

            String grainName = grainPart.getGrain().getName().replace("\"", "");

            if (!grainNameToGrainParts.containsKey(grainName)) {
                if (!grainPart.isDefinition()) {
                    throw new ParseException(String.format("Grain %s has no definition", grainName));
                }

                grainNameToGrainParts.put(grainName, new ArrayList<>());
            }

            grainNameToGrainParts.get(grainName).add(grainPart);
        }
    }

    private void parseGrains(StringBuilder errorScript) throws ParseException {

        for (String grainName : grainNameToGrainParts.keySet()) {
            try {
                parseGrain(grainName);
            } catch (ParseException e) {
                if (errorScript.length() > 0) {
                    errorScript.append("\n\n");
                }
                errorScript.append(e.getMessage());
            }
            if (errorScript.length() > 0) {
                throw new ParseException(errorScript.toString());
            }
        }

    }

    final void parseGrain(String grainName) throws ParseException {
        Grain g = grains.get(grainName);

        if (g.isParsingComplete()) {
            return;
        }

        ChecksumInputStream cis = null;

        for (GrainPart grainPart : grainNameToGrainParts.get(grainName)) {
            if (!currentlyParsingGrainParts.add(grainPart)) {
                throw new RepeatedParseException(grainPart);
            }

            cis = parseGrainPart(grainPart, cis);
        }
        g.setChecksum(cis.getCRC32());
        g.setLength(cis.getCount());
        g.finalizeParsing();
    }

    final void addGrain(Grain grain) throws ParseException {
        if (grain.getScore() != this) {
            throw new IllegalArgumentException();
        }
        if (grains.containsKey(grain.getName())) {
            throw new ParseException(String.format("Grain '%s' is already defined.", grain.getName()));
        }
        grains.put(grain.getName(), grain);
    }

    /**
     * Returns grain by its name. In case if the grain name is unknown an exception is thrown.
     *
     * @param name Grain name.
     * @throws ParseException If grain name is unknown to the system.
     */
    public Grain getGrain(String name) throws ParseException {
        Grain result = grains.get(name);
        if (result == null) {
            throw new ParseException(String.format("Unknown grain '%s'.", name));
        }
        return result;
    }

    final Grain getGrainAsDependency(Grain currentGrain, String dependencyGrainName) throws ParseException {
        Grain g = grains.get(dependencyGrainName);

        if (g == null) {
            throw new CelestaException(
                    String.format(
                            DEPENDENCY_SCHEMA_DOES_NOT_EXIST_ERROR_TEMPLATE, currentGrain.getName(), dependencyGrainName
                    )
            );
        }

        if (currentGrain == g) {
            return currentGrain;
        }

        if (g.isModified()) {
            try {
                this.parseGrain(dependencyGrainName);
            } catch (RepeatedParseException e) {
                throwParseExceptionOnCyclicReferences(currentGrain.getName(), dependencyGrainName);
            }
        }

        if (!g.isParsingComplete()) {
            throwParseExceptionOnCyclicReferences(currentGrain.getName(), dependencyGrainName);
        }

        return g;
    }

    private static void throwParseExceptionOnCyclicReferences(String currentGrainName, String dependencyGrainName)
            throws ParseException {
        throw new ParseException(
                String.format(CYCLIC_REFERENCES_ERROR_TEMPLATE,
                        currentGrainName, currentGrainName, dependencyGrainName
                ));
    }

    static String extractLineColNo(String msg) {
        Matcher matcher = Pattern.compile("at\\s+line\\s*(\\d+),?\\s*column\\s*(\\d+)").matcher(msg);
        if (matcher.find()) {
            return String.format(":%s:%s ", matcher.group(1), matcher.group(2));
        } else {
            return "";
        }
    }

    private ChecksumInputStream parseGrainPart(GrainPart grainPart, ChecksumInputStream cis) throws ParseException {
        Resource r = grainPart.getSource();
        try (
                ChecksumInputStream is =
                        cis == null
                                ? new ChecksumInputStream(r.getInputStream())
                                : new ChecksumInputStream(r.getInputStream(), cis)
        ) {
            CelestaParser parser = new CelestaParser(is, "utf-8");
            try {
                parser.parseGrainPart(grainPart);
            } catch (ParseException | TokenMgrError e) {
                /*IntelliJ IDEA-friendly log format*/
                throw new ParseException(String.format("Error parsing %s%s: %s",
                        r.toString(),
                        extractLineColNo(e.getMessage()),
                        e.getMessage()));
            }
            return is;
        } catch (FileNotFoundException e) {
            throw new ParseException(String.format("Cannot open resource '%s'.", r.toString()));
        } catch (IOException e) {
            //TODO: Throw new CelestaException (runtime)
            // This should never happen, however.
            throw new RuntimeException(e);
        }
    }

    private GrainPart extractGrainInfo(Resource r, boolean isSystem) throws ParseException {
        try (ChecksumInputStream is = isSystem ? new ChecksumInputStream(getSysSchemaInputStream())
                : new ChecksumInputStream(r.getInputStream())) {
            CelestaParser parser = new CelestaParser(is, "utf-8");
            try {
                return parser.extractGrainInfo(this, r);
            } catch (ParseException | TokenMgrError e) {
                throw new ParseException(String.format(
                        "Error on extracting grain name '%s': %s", r.toString(), e.getMessage()));
            }
        } catch (IOException e) {
            throw new ParseException(String.format("Cannot open resource '%s'.", r.toString()));
        }
    }

    private void initSystemGrain() {
        ChecksumInputStream is = null;

        try {
            GrainPart grainPart = extractGrainInfo(null, true);
            is = new ChecksumInputStream(getSysSchemaInputStream());
            CelestaParser parser = new CelestaParser(is, "utf-8");

            Grain result;
            try {
                result = parser.parseGrainPart(grainPart);
            } catch (ParseException e) {
                throw new CelestaException(e.getMessage());
            }
            result.setChecksum(is.getCRC32());
            result.setLength(is.getCount());
            result.finalizeParsing();
        } catch (Exception e) {
            throw new CelestaException(e);
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (IOException e) {
                // This should never happen, however.
                is = null;
            }
        }

    }

    private InputStream getSysSchemaInputStream() {
        return this.getClass().getResourceAsStream(getSysSchemaName() + ".sql");
    }

    /**
     * Returns system schema name.
     *
     * @return
     */
    public abstract String getSysSchemaName();

    /**
     * Returns identifier parser.
     *
     * @return
     */
    public abstract IdentifierParser getIdentifierParser();

    /**
     * Returns an unmodifiable grain set.
     */
    public Map<String, Grain> getGrains() {
        return Collections.unmodifiableMap(grains);
    }

    final int nextOrderCounter() {
        return ++orderCounter;
    }

    /**
     * Score builder for subclasses of {@link AbstractScore}.
     *
     * @param <T>
     */
    public static final class ScoreBuilder<T extends AbstractScore> {
        private ScoreDiscovery scoreDiscovery;
        private Class<T> scoreClass;

        public ScoreBuilder(Class<T> scoreClass) {
            this.scoreClass = scoreClass;
        }

        /**
         * Sets score path.
         *
         * @param path score path
         * @return
         * @deprecated Use {@link #scoreDiscovery(ScoreDiscovery)} explicitly.
         */
        @Deprecated
        public ScoreBuilder<T> path(String path) {
            this.scoreDiscovery = new ScoreByScorePathDiscovery(path);

            return this;
        }

        /**
         * Sets score discovery.
         *
         * @param scoreDiscovery score discovery
         * @return
         */
        public ScoreBuilder<T> scoreDiscovery(ScoreDiscovery scoreDiscovery) {
            this.scoreDiscovery = scoreDiscovery;
            return this;
        }

        /**
         * Builds the score.
         *
         * @return
         * @throws ParseException when score parsing fails
         */
        public T build() throws ParseException {
            try {
                T t = scoreClass.newInstance();
                t.init(this.scoreDiscovery);

                return t;

            } catch (InstantiationException | IllegalAccessException e) {
                throw new CelestaException(e);
            }
        }
    }

    /**
     * Returns a human-readable table with all available grains,
     * their versions, checksums and lengths.
     */
    public final String describeGrains() {
        int maxGrainNameLen = "SCHEMA".length();
        int maxVersionLen = "VERSION".length();
        for (Grain g : grains.values()) {
            if (g.getName().length() > maxGrainNameLen) {
                maxGrainNameLen = g.getName().length();
            }
            if (g.getVersion().toString().length() > maxVersionLen) {
                maxVersionLen = g.getVersion().toString().length();
            }
        }
        StringBuilder builder = new StringBuilder(String.format("%n"));
        builder.append(String.format("%-" + maxGrainNameLen + "s | ", "SCHEMA"));
        builder.append(String.format("%-" + maxVersionLen + "s | CHECKSUM | %7s%n", "VERSION", "LENGTH"));
        for (Grain g : grains
                .values()
                .stream()
                .sorted(Comparator.comparing(NamedElement::getName))
                .collect(Collectors.toList())) {
            builder.append(String.format("%-" + maxGrainNameLen + "s | ", g.getName()));
            builder.append(String.format("%-" + maxVersionLen + "s | ", g.getVersion()));
            builder.append(String.format("%08X | % 7d%n", g.getChecksum(), g.getLength()));
        }
        return builder.toString();
    }

}

/**
 * Wrapper of {@link InputStream} for checksum calculation on grain reading.
 */
final class ChecksumInputStream extends InputStream {
    private final CRC32 checksum;
    private final InputStream input;
    private int counter = 0;

    ChecksumInputStream(InputStream input) {
        this.input = input;
        checksum = new CRC32();
    }

    ChecksumInputStream(InputStream input, ChecksumInputStream cis) {
        this.input = input;
        this.checksum = cis.checksum;
    }

    @Override
    public int read() throws IOException {
        int result = input.read();
        if (result >= 0) {
            counter++;
            checksum.update(result);
        }
        return result;
    }

    public int getCRC32() {
        return (int) checksum.getValue();
    }

    public int getCount() {
        return counter;
    }

    @Override
    public void close() throws IOException {
        input.close();
    }
}
