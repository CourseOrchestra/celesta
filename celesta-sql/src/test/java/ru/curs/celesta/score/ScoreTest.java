package ru.curs.celesta.score;

import java.io.*;
import java.util.StringJoiner;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import ru.curs.celesta.score.discovery.ScoreByScorePathDiscovery;
import ru.curs.celesta.score.io.FileResource;
import ru.curs.celesta.score.io.Resource;

public class ScoreTest {

    private static final String SCORE_PATH_PREFIX = new StringJoiner(File.separator)
            .add("src").add("test").add("resources").add("scores").toString();
    public static final String TEST_SCORE_PATH = SCORE_PATH_PREFIX + File.separator + "testScore";

    private static final String COMPOSITE_SCORE_PATH_1 = new StringJoiner(File.separator)
            .add(SCORE_PATH_PREFIX).add("compositeScore").add("score").toString();
    private static final String COMPOSITE_SCORE_PATH_2 = new StringJoiner(File.separator)
            .add(SCORE_PATH_PREFIX).add("compositeScore").add("score2").toString();

    public static final String CANNOT_MODIFY_SYSTEM_GRAIN = "cannot modify system grain";

    @Test
    public void test1() throws ParseException {
        AbstractScore s = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(
                        COMPOSITE_SCORE_PATH_2 + File.pathSeparator + COMPOSITE_SCORE_PATH_1))
                .build();
        assertTrue(s.getGrains().size() < 20, () -> String.format("Too many grains?: %d", s.getGrains().size()));
        Grain g1 = s.getGrain("grain1");
        Grain g2 = s.getGrain("grain2");
        assertEquals("grain2", g2.getName());
        Table b = g2.getElement("b", Table.class);
        assertEquals(1, b.getForeignKeys().size());
        BasicTable a = b.getForeignKeys().iterator().next().getReferencedTable();
        assertEquals("a", a.getName());
        assertSame(g1, a.getGrain());

        Grain g3 = s.getGrain("grain3");

        int o1 = g1.getDependencyOrder();
        int o2 = g2.getDependencyOrder();
        int o3 = g3.getDependencyOrder();
        assertTrue(o1 < o2);
        assertTrue(o2 < o3);

        final Resource grain1Resource = new FileResource(new File(
                COMPOSITE_SCORE_PATH_1 + File.separator + "grain1" + File.separator + "_grain1.sql"));
        final Resource grain2Resource = new FileResource(new File(
                COMPOSITE_SCORE_PATH_1 + File.separator + "grain2" + File.separator + "_grain2.sql"));
        final Resource grain3Resource = new FileResource(new File(
                COMPOSITE_SCORE_PATH_2 + File.separator + "grain3" + File.separator + "_grain3.sql"));

        assertAll(
                () -> assertEquals(1, g1.getGrainParts().size()),
                () -> assertTrue(
                        g1.getGrainParts().stream()
                                .map(GrainPart::getSource)
                                .filter(r -> r.equals(grain1Resource))
                                .findFirst().isPresent()
                ),
                () -> assertEquals(1, g2.getGrainParts().size()),
                () -> assertTrue(
                        g2.getGrainParts().stream()
                                .map(GrainPart::getSource)
                                .filter(r -> r.equals(grain2Resource))
                                .findFirst().isPresent()
                ),
                () -> assertEquals(1, g3.getGrainParts().size()),
                () -> assertTrue(
                        g3.getGrainParts().stream()
                                .map(GrainPart::getSource)
                                .filter(r -> r.equals(grain3Resource))
                                .findFirst().isPresent()
                )
        );

        Grain sys = s.getGrain("celestaSql");
        a = sys.getElement("grains", Table.class);
        assertEquals("grains", a.getName());
        assertTrue(sys.getDependencyOrder() < o1);
        IntegerColumn c = (IntegerColumn) a.getColumns().get("state");
        assertEquals(3, c.getDefaultValue().intValue());
    }

    @Test
    public void test2() throws ParseException, IOException {
        AbstractScore s = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(COMPOSITE_SCORE_PATH_1))
                .build();

        Grain g1 = s.getGrain("grain1");
        View v = g1.getElement("testView", View.class);
        assertEquals("testView", v.getName());
        assertEquals("view description ", v.getCelestaDoc());
        assertTrue(v.isDistinct());

        assertEquals(4, v.getColumns().size());
        String[] ref = {"fieldAlias", "tablename", "checksum", "f1"};
        assertArrayEquals(ref, v.getColumns().keySet().toArray(new String[0]));

        String[] expected = {
                "  select distinct grainid as fieldAlias, ta.tablename as tablename, grains.checksum as checksum",
                "    , ta.tablename || grains.checksum as f1", "  from celestaSql.tables as ta",
                "    INNER join celestaSql.grains as grains on ta.grainid = grains.id",
                "  where tablename >= 'aa' AND 5 BETWEEN 0 AND 6 OR '55' > '1'"};

        assertArrayEquals(expected, CelestaSerializer.toQueryString(v).split("\\r?\\n"));
    }

    @Test
    public void modificationTest1() throws ParseException {
        AbstractScore s = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(
                        COMPOSITE_SCORE_PATH_1 + File.pathSeparator + COMPOSITE_SCORE_PATH_2))
                .build();
        Grain g1 = s.getGrain("grain1");
        Grain g2 = s.getGrain("grain2");
        Grain g3 = s.getGrain("grain3");

        assertFalse(g1.isModified());
        assertFalse(g2.isModified());
        assertFalse(g3.isModified());

        Table b = g2.getElement("b", Table.class);
        int oldSize = b.getColumns().size();
        new StringColumn(b, "newcolumn");
        assertEquals(oldSize + 1, b.getColumns().size());
        assertFalse(g1.isModified());
        assertTrue(g2.isModified());
        assertFalse(g3.isModified());

        GrainPart g3p = g3.getGrainParts().stream().findFirst().get();

        new Table(g3p, "newtable");
        assertFalse(g1.isModified());
        assertTrue(g2.isModified());
        assertTrue(g3.isModified());

    }

    @Test
    public void modificationTest2() throws ParseException {
        AbstractScore s = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(COMPOSITE_SCORE_PATH_1))
                .build();
        Grain celesta = s.getGrain("celestaSql");
        assertFalse(celesta.isModified());
        // Проверяем, что модифицировать элементы системной гранулы недопустимо.
        Table tables = celesta.getElement("tables", Table.class);
        ParseException e = assertThrows(ParseException.class, () -> new StringColumn(tables, "newcolumn"));
        assertTrue(e.getMessage().contains(CANNOT_MODIFY_SYSTEM_GRAIN));
        assertFalse(celesta.isModified());
    }

    @Test
    public void modificationTest3() throws ParseException {
        AbstractScore s = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(
                        COMPOSITE_SCORE_PATH_2 + File.pathSeparator + COMPOSITE_SCORE_PATH_1))
                .build();
        Grain g1 = s.getGrain("grain1");
        Grain g2 = s.getGrain("grain2");
        Grain g3 = s.getGrain("grain3");
        Grain celesta = s.getGrain("celestaSql");
        Grain g4 = new Grain(s, "newgrain");

        assertAll(
                () -> assertFalse(g1.isModified()),
                () -> assertFalse(g2.isModified()),
                () -> assertFalse(g3.isModified()),
                () -> assertFalse(celesta.isModified()),
                () -> assertTrue(g4.isModified()),
                () -> assertEquals("1.00", g4.getVersion().toString()),
                () -> assertEquals(0, g4.getGrainParts().size())
        );


        g3.modify();
        assertTrue(g3.isModified());
        assertFalse(g2.isModified());
        ParseException e = assertThrows(ParseException.class, celesta::modify);
        assertTrue(e.getMessage().contains(CANNOT_MODIFY_SYSTEM_GRAIN));
        assertFalse(celesta.isModified());

    }

    @Test
    public void modificationTest4() throws ParseException {
        AbstractScore s = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(COMPOSITE_SCORE_PATH_1))
                .build();
        Grain g2 = s.getGrain("grain2");
        assertFalse(g2.isModified());

        Table b = g2.getElement("b", Table.class);
        assertEquals(1, b.getPrimaryKey().size());
        b.getColumns().get("descr").setNullableAndDefault(false, null);
        assertTrue(g2.isModified());
        String[] pk = {"idb", "descr"};
        b.setPK(pk);
        assertTrue(g2.isModified());
        assertEquals(2, b.getPrimaryKey().size());
    }

    @Test
    public void modificationTest5() throws ParseException {
        AbstractScore s = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(
                        COMPOSITE_SCORE_PATH_1 + File.pathSeparator + COMPOSITE_SCORE_PATH_2))
                .build();
        Grain g2 = s.getGrain("grain2");
        Grain g3 = s.getGrain("grain3");
        assertFalse(g2.isModified());
        assertFalse(g3.isModified());

        Table b = g2.getElement("b", Table.class);
        Table c = g3.getElement("c", Table.class);

        assertEquals(1, c.getForeignKeys().size());
        ForeignKey fk = c.getForeignKeys().iterator().next();
        assertSame(b, fk.getReferencedTable());

        assertTrue(g2.getElements(Table.class).containsKey("b"));
        b.delete();
        assertFalse(g2.getElements(Table.class).containsKey("b"));
        assertTrue(g2.isModified());
        assertEquals(0, c.getForeignKeys().size());

        assertTrue(g3.isModified());
    }

    @Test
    public void modificationTest6() throws ParseException {
        AbstractScore s = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(COMPOSITE_SCORE_PATH_1))
                .build();
        Grain g2 = s.getGrain("grain2");
        GrainPart g2p = g2.getGrainParts().stream().findFirst().get();
        // Нельзя создать view с именем таблицы
        ParseException e = assertThrows(ParseException.class, () -> new View(g2p, "b"));
        assertTrue(e.getMessage().contains("Table with the same name already exists"));
        Grain g1 = s.getGrain("grain1");
        GrainPart g1p = g1.getGrainParts().stream().findFirst().get();
        // Нельзя создать таблицу с именем view
        e = assertThrows(ParseException.class, () -> new Table(g1p, "testView"));
        assertTrue(e.getMessage().contains("View with the same name already exists"));
        new Table(g2p, "newView2");
    }

    @Test
    public void modificationTest7() throws ParseException, IOException {
        AbstractScore s = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(COMPOSITE_SCORE_PATH_1))
                .build();
        Grain g1 = s.getGrain("grain1");
        assertEquals(1, g1.getElements(View.class).size());
        View v = g1.getElement("testView", View.class);
        assertFalse(g1.isModified());
        v.delete();
        assertEquals(0, g1.getElements(View.class).size());
        assertTrue(g1.isModified());
    }

    @Test
    public void modificationTest8() throws ParseException, IOException {
        AbstractScore s = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(COMPOSITE_SCORE_PATH_1))
                .build();
        Grain g1 = s.getGrain("grain1");
        GrainPart g1p = g1.getGrainParts().stream().findFirst().get();
        assertEquals(1, g1.getElements(View.class).size());
        assertFalse(g1.isModified());
        assertTrue(assertThrows(ParseException.class, () ->
                new View(g1p, "testit", "select postalcode, city from addresses where flat = 5"))
                .getMessage().contains("is expected to be of TEXT type"));
        assertEquals(1, g1.getElements(View.class).size());
        assertTrue(g1.isModified());
        View nv = new View(g1p, "testit", "select postalcode, city from addresses where flat = '5'");
        assertEquals(2, nv.getColumns().size());
        assertEquals(2, g1.getElements(View.class).size());
        assertTrue(g1.isModified());

    }

    @Test
    public void setCelestaDoc() throws ParseException {
        AbstractScore s = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(TEST_SCORE_PATH))
                .build();
        Grain g = s.getGrain("testGrain");
        Table t = g.getElement("testTable", Table.class);
        t.setCelestaDocLexem("/** бла бла бла бла*/");
        assertEquals(" бла бла бла бла", t.getCelestaDoc());
        // Была ошибка -- не брал многострочный комментарий
        t.setCelestaDocLexem("/** бла бла бла\r\n бла*/");
        assertEquals(" бла бла бла\r\n бла", t.getCelestaDoc());
        t.setCelestaDocLexem("/**бла\rбла\nбла\r\nбла*/");
        assertEquals("бла\rбла\nбла\r\nбла", t.getCelestaDoc());

        assertTrue(
                assertThrows(ParseException.class,
                        () -> t.setCelestaDocLexem("/*бла\rбла\nбла\r\nбла*/"))
                        .getMessage().contains("Celestadoc should match pattern")
        );

    }

    @Test
    public void saveTest() throws ParseException, IOException {
        // Проверяется функциональность записи динамически изменённых объектов.
        AbstractScore s = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(TEST_SCORE_PATH))
                .build();
        Grain g = s.getGrain("testGrain");
        Table t = g.getElement("testTable", Table.class);
        StringWriter sw = new StringWriter();

        PrintWriter bw = new PrintWriter(sw);
        new CelestaSerializer(bw).save(t);
        bw.flush();

        String[] actual = sw.toString().split("\r?\n");
        BufferedReader r = new BufferedReader(
                new InputStreamReader(ScoreTest.class.getResourceAsStream("expectedsave.sql"), "utf-8"));
        for (String l : actual)
            assertEquals(r.readLine(), l);

        assertAll(
                () -> assertEquals("VARCHAR", t.getColumn("attrVarchar").getCelestaType()),
                () -> assertEquals("INT", t.getColumn("attrInt").getCelestaType()),
                () -> assertEquals("BIT", t.getColumn("f1").getCelestaType()),
                () -> assertEquals("REAL", t.getColumn("f5").getCelestaType()),
                () -> assertEquals("TEXT", t.getColumn("f6").getCelestaType()),
                () -> assertEquals("DATETIME", t.getColumn("f8").getCelestaType()),
                () -> assertEquals("BLOB", t.getColumn("f10").getCelestaType())
        );
    }

    @Test
    public void saveTest2() throws ParseException, IOException {
        // Проверяется функциональность записи динамически изменённых объектов с
        // опциями (Read Only, Version Check).
        AbstractScore s = new CelestaSqlTestScore();

        String filePath = this.getClass().getResource("test.sql").getPath();
        FileResource fr = new FileResource(new File(filePath));
        CelestaParser cp1 = new CelestaParser(fr.getInputStream(), "utf-8");
        GrainPart gp = cp1.extractGrainInfo(s, fr);
        CelestaParser cp2 = new CelestaParser(fr.getInputStream(), "utf-8");
        Grain g = cp2.parseGrainPart(gp);
        StringWriter sw = new StringWriter();

        try (PrintWriter bw = new PrintWriter(sw)) {
            CelestaSerializer serializer = new CelestaSerializer(bw);
            ReadOnlyTable rot = g.getElement("ttt1", ReadOnlyTable.class);
            serializer.save(rot);
            Table t = g.getElement("ttt2", Table.class);
            serializer.save(t);
            t = g.getElement("ttt3", Table.class);
            serializer.save(t);
            t = g.getElement("table1", Table.class);
            serializer.save(t);
        }

        String[] actual = sw.toString().split("\r?\n");
        BufferedReader r = new BufferedReader(
                new InputStreamReader(ScoreTest.class.getResourceAsStream("expectedsave2.sql"), "utf-8"));
        for (String l : actual)
            assertEquals(r.readLine(), l);

    }

    @Test
    public void fknameTest() throws ParseException {
        AbstractScore s = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(TEST_SCORE_PATH))
                .build();
        Grain g = s.getGrain("testGrain");
        Table t = g.getElement("aLongIdentityTableNaaame", Table.class);
        ForeignKey[] ff = t.getForeignKeys().toArray(new ForeignKey[0]);
        assertEquals(2, ff.length);
        assertEquals(30, ff[0].getConstraintName().length());
        assertEquals(30, ff[1].getConstraintName().length());
        assertFalse(ff[0].getConstraintName().equals(ff[1].getConstraintName()));
    }

    @Test
    public void viewTest() throws ParseException, IOException {
        AbstractScore s = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(TEST_SCORE_PATH))
                .build();
        Grain g = s.getGrain("testGrain");

        View v = g.getElement("testView", View.class);
        String exp;
        assertFalse(v.isDistinct());
        assertEquals(4, v.getColumns().size());
        exp = String.format("  select id as id, descr as descr, descr || 'foo' as descr2, k2 as k2%n"
                + "  from testTable as testTable%n" + "    INNER join refTo as refTo on attrVarchar = k1 AND attrInt = k2");
        assertEquals(exp, CelestaSerializer.toQueryString(v));

        assertTrue(v.getColumns().get("descr").isNullable());
        assertTrue(v.getColumns().get("descr2").isNullable());
        assertFalse(v.getColumns().get("k2").isNullable());
        assertFalse(v.getColumns().get("id").isNullable());

        v = g.getElement("testView2", View.class);
        assertEquals(ViewColumnType.INT, v.getColumns().get("id").getColumnType());
        exp = String.format("  select id as id, descr as descr%n" + "  from testTable as t1%n"
                + "    INNER join refTo as t2 on attrVarchar = k1 AND NOT t2.descr IS NULL AND attrInt = k2");
        assertEquals(exp, CelestaSerializer.toQueryString(v));
    }

    @Test
    public void vewTest2() throws ParseException, IOException {
        AbstractScore s = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(TEST_SCORE_PATH))
                .build();
        Grain g = s.getGrain("testGrain");
        View v = g.getElement("testView3", View.class);
        String[] expected = {"  select 1 as a, 1.4 as b, 1 as c, 1 as d, 1 as e, 1 as f, 1 as g, 1 as h, 1 as j",
                "    , 1 as k", "  from testTable as testTable"};
        assertEquals(ViewColumnType.INT, v.getColumns().get("a").getColumnType());
        assertEquals(ViewColumnType.REAL, v.getColumns().get("b").getColumnType());

        assertEquals("", v.getColumns().get("a").getCelestaDoc());
        assertFalse(v.getColumns().get("a").isNullable());
        assertFalse(v.getColumns().get("b").isNullable());
        assertEquals("test celestadoc", v.getColumns().get("b").getCelestaDoc());
        assertEquals("test celestadoc2", v.getColumns().get("c").getCelestaDoc());

        assertArrayEquals(expected, CelestaSerializer.toQueryString(v).split("\\r?\\n"));

        assertEquals(3, v.getColumnIndex("d"));
        assertEquals(1, v.getColumnIndex("b"));
    }

    @Test
    public void viewTest3() throws ParseException {
        AbstractScore s = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(TEST_SCORE_PATH))
                .build();
        Grain g = s.getGrain("testGrain");
        View v = g.getElement("testView4", View.class);
        String[] expected = {"  select f1 as f1, f4 as f4, f5 as f5, f4 + f5 as s, f5 * f5 + 1 as s2",
                "  from testTable as testTable", "  where f1 = true"};
        assertAll(
                () -> assertArrayEquals(expected, CelestaSerializer.toQueryString(v).split("\\r?\\n")),
                // Checking nullability evaluation
                () -> assertFalse(v.getColumns().get("f1").isNullable()),
                () -> assertTrue(v.getColumns().get("f4").isNullable()),
                () -> assertFalse(v.getColumns().get("f5").isNullable()),
                () -> assertTrue(v.getColumns().get("s").isNullable()),
                () -> assertFalse(v.getColumns().get("s2").isNullable())
        );
    }

    @Test
    public void viewTest4() throws ParseException {
        AbstractScore s = new AbstractScore.ScoreBuilder<>(CelestaSqlTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(TEST_SCORE_PATH))
                .build();
        Grain g = s.getGrain("testGrain");
        View v = g.getElement("testView5", View.class);
        Table t = g.getElement("testTable", Table.class);

        ViewColumnMeta vcm = v.getColumns().get("foo");
        assertTrue(vcm.isNullable());
        assertEquals(StringColumn.VARCHAR, vcm.getCelestaType());
        assertEquals(((StringColumn) t.getColumn("attrVarchar")).getLength(), vcm.getLength());
        assertEquals(2, vcm.getLength());

        vcm = v.getColumns().get("bar");
        assertTrue(vcm.isNullable());
        assertEquals(StringColumn.VARCHAR, vcm.getCelestaType());
        assertEquals(((StringColumn) t.getColumn("f7")).getLength(), vcm.getLength());
        assertEquals(8, vcm.getLength());

        vcm = v.getColumns().get("baz");
        assertTrue(vcm.isNullable());
        assertEquals(StringColumn.VARCHAR, vcm.getCelestaType());
        assertEquals(-1, vcm.getLength());
    }

    @Test
    void testGrainWithAnsiQuotedIdentifiers() throws ParseException {
        AbstractScore s = new AbstractScore.ScoreBuilder<>(CelestaSqlAnsiQuotedTestScore.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(
                        SCORE_PATH_PREFIX + File.separator + "scoreWithAnsiQuotedIdentifiers"))
                .build();

        String grainName = "schema номер 1";

        Grain g = s.getGrains().get(grainName);

        String seq1Name = "s1";
        String seq2Name = " s2 ";
        String seq3Name = "!@#$%^&";

        assertAll(
                () -> assertNotNull(g),
                () -> assertEquals(grainName, g.getName()),
                () -> assertEquals(3, g.getElements(SequenceElement.class).size()),
                () -> assertNotNull(g.getElement(seq1Name, SequenceElement.class)),
                () -> assertNotNull(g.getElement(seq2Name, SequenceElement.class)),
                () -> assertNotNull(g.getElement(seq3Name, SequenceElement.class))
        );
    }
}
