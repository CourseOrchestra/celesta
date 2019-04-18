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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.zip.CRC32;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.discovery.DefaultScoreDiscovery;
import ru.curs.celesta.score.discovery.ScoreDiscovery;
import ru.curs.celesta.score.validator.IdentifierParser;

/**
 * Корневой класс полной модели данных гранул.
 */
public abstract class AbstractScore {

    static final String GRAIN_PART_PARSING_ERROR_TEMPLATE = "Error parsing '%s': %s";

    static final String DEPENDENCY_SCHEMA_DOES_NOT_EXIST_ERROR_TEMPLATE
            = "Couldn't parse schema '%s'. Dependency schema '%s' does not exist.";

    private final Map<String, Grain> grains = new HashMap<>();

    private final Map<String, List<GrainPart>> grainNameToGrainParts = new LinkedHashMap<>();
    private final Set<File> grainFiles = new LinkedHashSet<>(); //TODO: What to do with it?

    private String path;
    private File defaultGrainPath;
    private int orderCounter;

    private final Set<GrainPart> currentlyParsingGrainParts = new HashSet<>();

    public AbstractScore() {
        //TODO!!! Used only for test and must be replaced. Must be private!!!
    }

    /**
     * @param scorePath набор путей к папкам score, разделённый точкой с запятой.
     */
    void setScorePath(String scorePath) {
        this.path = scorePath;
    }

    /**
     * Инициализация ядра путём указания набора путей к папкам score,
     * разделённого точкой с запятой.
     *
     * @throws CelestaException в случае указания несуществующего пути или в случае двойного
     *                          определения гранулы с одним и тем же именем.
     */
    void init(ScoreDiscovery scoreDiscovery) throws CelestaException, ParseException {
        for (String entry : this.path.split(File.pathSeparator)) {
            File path = new File(entry.trim());
            if (!path.exists())
                throw new CelestaException("Score path entry '%s' does not exist.", path.toString());
            if (!path.canRead())
                throw new CelestaException("Cannot read score path entry '%s'.", path.toString());
            if (!path.isDirectory())
                throw new CelestaException("Score path entry '%s' is not a directory.", path.toString());

            defaultGrainPath = path;
            grainFiles.addAll(scoreDiscovery.discoverScore(path));
        }

        initSystemGrain();

        //The first parsing step - the grouping of files by grain names.
        fillGrainNameToFilesMap(grainFiles);
        // В этот момент в таблице grainFiles содержится перечень распознанных
        // имён гранул с именами файлов-скриптов.
        parseGrains(new StringBuilder());
    }

    /**
     * Сохраняет содержимое метаданных обратно в SQL-файлы, при этом
     * перезаписывая их содержимое.
     *
     * @throws CelestaException при ошибке ввода-вывода.
     */
    public void save() throws CelestaException {
        for (Grain g : grains.values())
            if (g.isModified())
                g.save();
    }


    private void fillGrainNameToFilesMap(Set<File> files) throws ParseException {

        List<GrainPart> grainParts = new ArrayList<>();

        for (File f : files) {
            GrainPart grainPart = extractGrainInfo( f, false);
            grainParts.add(grainPart);
        }

        grainParts.sort((o1, o2) -> {
            if (o1.isDefinition() && !o2.isDefinition())
                return -1;
             else if (o1.isDefinition() == o2.isDefinition())
                return 0;
             else
                 return 1;
        });


        for (GrainPart grainPart: grainParts) {

            String grainName = grainPart.getGrain().getName().replace("\"", "");

            if (!grainNameToGrainParts.containsKey(grainName)) {
                if (!grainPart.isDefinition()) {
                    throw new ParseException(String.format("Grain %s has not definition", grainName));
                }

                grainNameToGrainParts.put(grainName, new ArrayList<>());
            }

            grainNameToGrainParts.get(grainName).add(grainPart);
        }
    }

    private void parseGrains(StringBuilder errorScript) throws ParseException {

        for (String grainName: grainNameToGrainParts.keySet()) {
            try {
                parseGrain(grainName);
            } catch (ParseException e) {
                if (errorScript.length() > 0)
                    errorScript.append("\n\n");
                errorScript.append(e.getMessage());
            }
            if (errorScript.length() > 0)
                throw new ParseException(errorScript.toString());
        }

    }

    void parseGrain(String grainName) throws ParseException {
        Grain g = grains.get(grainName);

        if (g.isParsingComplete())
            return;

        ChecksumInputStream cis = null;

        for (GrainPart grainPart : grainNameToGrainParts.get(grainName))
            cis = parseGrainPart(grainPart, cis);
        g.setChecksum(cis.getCRC32());
        g.setLength(cis.getCount());
        g.finalizeParsing();
    }

    void addGrain(Grain grain) throws ParseException {
        if (grain.getScore() != this)
            throw new IllegalArgumentException();
        if (grains.containsKey(grain.getName()))
            throw new ParseException(String.format("Grain '%s' is already defined.", grain.getName()));
        grains.put(grain.getName(), grain);
    }

    /**
     * Получение гранулы по её имени. В случае, если имя
     * гранулы неизвестно, выводится исключение.
     *
     * @param name Имя гранулы.
     * @throws ParseException Если имя гранулы неизвестно системе.
     */
    public Grain getGrain(String name) throws ParseException {
        Grain result = grains.get(name);
        if (result == null) {
            throw new ParseException(String.format("Unknown grain '%s'.", name));
        }
        return result;
    }

    Grain getGrainAsDependency(Grain currentGrain, String dependencyGrain) throws ParseException {
        Grain g = grains.get(dependencyGrain);

        if (g == null) {
            throw new ParseException(
                    String.format(
                            DEPENDENCY_SCHEMA_DOES_NOT_EXIST_ERROR_TEMPLATE, currentGrain.getName(), dependencyGrain
                    )
            );
        }

        if (currentGrain == g)
            return currentGrain;

        if (g.isModified())
            parseGrain(dependencyGrain);

        if (!g.isParsingComplete())
            throw new ParseException(
                    String.format("Error parsing grain %s "
                            + "due to previous parsing errors or "
                            + "cycle reference involving grains '%s' and '%s'.", currentGrain.getName(), dependencyGrain
                    ));

        return g;
    }

    private ChecksumInputStream parseGrainPart(GrainPart grainPart, ChecksumInputStream cis) throws ParseException {
        File f = grainPart.getSourceFile();
        try (
                ChecksumInputStream is =
                        cis == null
                                ? new ChecksumInputStream(new FileInputStream(f))
                                : new ChecksumInputStream(new FileInputStream(f), cis)
        ) {
            CelestaParser parser = new CelestaParser(is, "utf-8");
            try {
                parser.parseGrainPart(grainPart);
            } catch (ParseException | TokenMgrError e) {
                throw new ParseException(String.format(GRAIN_PART_PARSING_ERROR_TEMPLATE, f.toString(), e.getMessage()));
            }
            return is;
        } catch (FileNotFoundException e) {
            throw new ParseException(String.format("Cannot open file '%s'.", f.toString()));
        } catch (IOException e) {
            //TODO: Throw new CelestaException (runtime)
            // This should never happen, however.
            throw new RuntimeException(e);
        }
    }

    private GrainPart extractGrainInfo(File f, boolean isSystem) throws ParseException {
        try (ChecksumInputStream is = isSystem ? new ChecksumInputStream(getSysSchemaInputStream()) : new ChecksumInputStream(new FileInputStream(f))) {
            CelestaParser parser = new CelestaParser(is, "utf-8");
            try {
                return parser.extractGrainInfo(this, f);
            } catch (ParseException | TokenMgrError e) {
                throw new ParseException(String.format("Error extracting of grain name '%s': %s", f.toString(), e.getMessage()));
            }
        } catch (IOException e) {
            throw new ParseException(String.format("Cannot open file '%s'.", f.toString()));
        }
    }

    private void initSystemGrain() throws CelestaException {
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
                if (is != null)
                    is.close();
            } catch (IOException e) {
                // This should never happen, however.
                is = null;
            }
        }

    }

    private InputStream getSysSchemaInputStream() {
        return this.getClass().getResourceAsStream(getSysSchemaName() + ".sql");
    }

    public abstract String getSysSchemaName();

    public abstract IdentifierParser getIdentifierParser();

    /**
     * Возвращает неизменяемый набор гранул.
     */
    public Map<String, Grain> getGrains() {
        return Collections.unmodifiableMap(grains);
    }

    /**
     * Возвращает путь по умолчанию для создаваемых динамически гранул. Значение
     * равно последней записи в score.path.
     */
    File getDefaultGrainPath() {
        return defaultGrainPath;
    }


    public String getPath() {
        return path;
    }

    int nextOrderCounter() {
        return ++orderCounter;
    }


    public static class ScoreBuilder<T extends AbstractScore> {
        private String path;
        private ScoreDiscovery scoreDiscovery;
        private Class<T> scoreClass;

        public ScoreBuilder (Class<T> scoreClass) {
            this.scoreClass = scoreClass;
        }

        public ScoreBuilder<T> path(String path) {
            this.path = path;
            return this;
        }

        public ScoreBuilder<T> scoreDiscovery(ScoreDiscovery scoreDiscovery) {
            this.scoreDiscovery = scoreDiscovery;
            return this;
        }

        public T build() throws CelestaException, ParseException {
            if (scoreDiscovery == null)
                scoreDiscovery = new DefaultScoreDiscovery();

            try {
                T t = scoreClass.newInstance();
                t.setScorePath(this.path);
                t.init(this.scoreDiscovery);

                return t;
            } catch (InstantiationException | IllegalAccessException  e) {
                throw new CelestaException(e);
            }
        }
    }
}

/**
 * Обёртка InputStream для подсчёта контрольной суммы при чтении.
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
