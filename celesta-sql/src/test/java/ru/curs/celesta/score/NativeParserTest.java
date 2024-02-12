package ru.curs.celesta.score;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.DBType;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;


public class NativeParserTest {

    @Test
    void emptyBody() throws Exception {

        NativeSql nativeSql = new NativeSql("--{{--}}").isBefore(false).dbType(DBType.ORACLE);
        String nativeBlock = nativeSql(nativeSql);
        Grain g = parse(nativeBlock);

        assertAll(
                () -> assertTrue(g.getBeforeSqlList(DBType.ORACLE).isEmpty()),
                () -> assertFalse(g.getAfterSqlList(DBType.ORACLE).isEmpty()),
                () -> assertTrue(g.getBeforeSqlList(DBType.POSTGRESQL).isEmpty()),
                () -> assertTrue(g.getAfterSqlList(DBType.POSTGRESQL).isEmpty()),
                () -> assertTrue(g.getBeforeSqlList(DBType.MSSQL).isEmpty()),
                () -> assertTrue(g.getAfterSqlList(DBType.MSSQL).isEmpty()),
                () -> assertTrue(g.getBeforeSqlList(DBType.H2).isEmpty()),
                () -> assertTrue(g.getAfterSqlList(DBType.H2).isEmpty()),
                () -> assertEquals(1, g.getAfterSqlList(DBType.ORACLE).size()),
                () -> assertEquals("", g.getAfterSqlList(DBType.ORACLE).get(0).getSql())
        );
    }

    @Test
    void emptyBodyWithSpaces() throws Exception {
        NativeSql nativeSql = new NativeSql("--{{ --}}").isBefore(false).dbType(DBType.POSTGRESQL);
        String nativeBlock = nativeSql(nativeSql);
        Grain g = parse(nativeBlock);

        assertAll(
                () -> assertTrue(g.getBeforeSqlList(DBType.ORACLE).isEmpty()),
                () -> assertTrue(g.getAfterSqlList(DBType.ORACLE).isEmpty()),
                () -> assertTrue(g.getBeforeSqlList(DBType.POSTGRESQL).isEmpty()),
                () -> assertFalse(g.getAfterSqlList(DBType.POSTGRESQL).isEmpty()),
                () -> assertTrue(g.getBeforeSqlList(DBType.MSSQL).isEmpty()),
                () -> assertTrue(g.getAfterSqlList(DBType.MSSQL).isEmpty()),
                () -> assertTrue(g.getBeforeSqlList(DBType.H2).isEmpty()),
                () -> assertTrue(g.getAfterSqlList(DBType.H2).isEmpty()),
                () -> assertEquals(1, g.getAfterSqlList(DBType.POSTGRESQL).size()),
                () -> assertEquals(" ", g.getAfterSqlList(DBType.POSTGRESQL).get(0).getSql())
        );
    }

    @Test
    void testMultipleMinusEndOfExpression() throws Exception {
        NativeSql nativeSql = new NativeSql("--{{ ---}}").isBefore(false).dbType(DBType.MSSQL);
        String nativeBlock = nativeSql(nativeSql);
        Grain g = parse(nativeBlock);

        assertAll(
                () -> assertTrue(g.getBeforeSqlList(DBType.ORACLE).isEmpty()),
                () -> assertTrue(g.getAfterSqlList(DBType.ORACLE).isEmpty()),
                () -> assertTrue(g.getBeforeSqlList(DBType.POSTGRESQL).isEmpty()),
                () -> assertTrue(g.getAfterSqlList(DBType.POSTGRESQL).isEmpty()),
                () -> assertTrue(g.getBeforeSqlList(DBType.MSSQL).isEmpty()),
                () -> assertFalse(g.getAfterSqlList(DBType.MSSQL).isEmpty()),
                () -> assertTrue(g.getBeforeSqlList(DBType.H2).isEmpty()),
                () -> assertTrue(g.getAfterSqlList(DBType.H2).isEmpty()),
                () -> assertEquals(1, g.getAfterSqlList(DBType.MSSQL).size()),
                () -> assertEquals(" -", g.getAfterSqlList(DBType.MSSQL).get(0).getSql())
        );
    }

    @Test
    void testOneLine() throws Exception {
        NativeSql nativeSql = new NativeSql("--{{ asasf  --}}").isBefore(false).dbType(DBType.H2);
        String nativeBlock = nativeSql(nativeSql);
        Grain g = parse(nativeBlock);

        assertAll(
                () -> assertTrue(g.getBeforeSqlList(DBType.ORACLE).isEmpty()),
                () -> assertTrue(g.getAfterSqlList(DBType.ORACLE).isEmpty()),
                () -> assertTrue(g.getBeforeSqlList(DBType.POSTGRESQL).isEmpty()),
                () -> assertTrue(g.getAfterSqlList(DBType.POSTGRESQL).isEmpty()),
                () -> assertTrue(g.getBeforeSqlList(DBType.MSSQL).isEmpty()),
                () -> assertTrue(g.getAfterSqlList(DBType.MSSQL).isEmpty()),
                () -> assertTrue(g.getBeforeSqlList(DBType.H2).isEmpty()),
                () -> assertFalse(g.getAfterSqlList(DBType.H2).isEmpty()),
                () -> assertEquals(1, g.getAfterSqlList(DBType.H2).size()),
                () -> assertEquals(" asasf  ", g.getAfterSqlList(DBType.H2).get(0).getSql())
        );
    }

    @Test
    void testMultiLine() throws Exception {
        NativeSql nativeSql = new NativeSql("--{{ asasf  \n asda --}}").isBefore(false).dbType(DBType.ORACLE);
        String nativeBlock = nativeSql(nativeSql);
        Grain g = parse(nativeBlock);

        assertAll(
                () -> assertEquals(1, g.getAfterSqlList(DBType.ORACLE).size()),
                () -> assertEquals(" asasf  \n asda ", g.getAfterSqlList(DBType.ORACLE).get(0).getSql())
        );
    }

    @Test
    void testIncorrectStartOfExpression() {
        NativeSql nativeSql1 = new NativeSql("--{ asasf  --}}").isBefore(false).dbType(DBType.ORACLE);
        String nativeBlock1 = nativeSql(nativeSql1);

        NativeSql nativeSql2 = new NativeSql("-- asasf  --}}").isBefore(false).dbType(DBType.ORACLE);
        String nativeBlock2 = nativeSql(nativeSql2);

        NativeSql nativeSql3 = new NativeSql("- asasf  --}}").isBefore(false).dbType(DBType.ORACLE);
        String nativeBlock3 = nativeSql(nativeSql3);

        NativeSql nativeSql4 = new NativeSql(" asasf  --}}").isBefore(false).dbType(DBType.ORACLE);
        String nativeBlock4 = nativeSql(nativeSql4);

        NativeSql nativeSql5 = new NativeSql("-{ asasf  --}}").isBefore(false).dbType(DBType.ORACLE);
        String nativeBlock5 = nativeSql(nativeSql5);

        NativeSql nativeSql6 = new NativeSql("-{{ asasf  --}}").isBefore(false).dbType(DBType.ORACLE);
        String nativeBlock6 = nativeSql(nativeSql6);

        assertAll(
                () -> assertThrows(ParseException.class, () -> parse(nativeBlock1)),
                () -> assertThrows(ParseException.class, () -> parse(nativeBlock2)),
                () -> assertThrows(ParseException.class, () -> parse(nativeBlock3)),
                () -> assertThrows(ParseException.class, () -> parse(nativeBlock4)),
                () -> assertThrows(ParseException.class, () -> parse(nativeBlock5)),
                () -> assertThrows(ParseException.class, () -> parse(nativeBlock6))
        );

    }

    @Test
    void testIncorrectEndOfExpression() {
        NativeSql nativeSql1 = new NativeSql("--{{ asasf  -}}").isBefore(false).dbType(DBType.ORACLE);
        String nativeBlock1 = nativeSql(nativeSql1);

        NativeSql nativeSql2 = new NativeSql("--{{ asasf  }}").isBefore(false).dbType(DBType.ORACLE);
        String nativeBlock2 = nativeSql(nativeSql2);

        NativeSql nativeSql3 = new NativeSql("--{{ asasf  }").isBefore(false).dbType(DBType.ORACLE);
        String nativeBlock3 = nativeSql(nativeSql3);

        NativeSql nativeSql4 = new NativeSql("--{{ asasf  -}").isBefore(false).dbType(DBType.ORACLE);
        String nativeBlock4 = nativeSql(nativeSql4);

        NativeSql nativeSql5 = new NativeSql("--{{ asasf  --}").isBefore(false).dbType(DBType.ORACLE);
        String nativeBlock5 = nativeSql(nativeSql5);

        NativeSql nativeSql6 = new NativeSql("--{{ asasf  -").isBefore(false).dbType(DBType.ORACLE);
        String nativeBlock6 = nativeSql(nativeSql6);

        NativeSql nativeSql7 = new NativeSql("--{{ asasf  --").isBefore(false).dbType(DBType.ORACLE);
        String nativeBlock7 = nativeSql(nativeSql7);

        NativeSql nativeSql8 = new NativeSql("--{{ asasf").isBefore(false).dbType(DBType.ORACLE);
        String nativeBlock8 = nativeSql(nativeSql8);

        assertAll(
                () -> assertThrows(ParseException.class, () -> parse(nativeBlock1)),
                () -> assertThrows(ParseException.class, () -> parse(nativeBlock2)),
                () -> assertThrows(ParseException.class, () -> parse(nativeBlock3)),
                () -> assertThrows(ParseException.class, () -> parse(nativeBlock4)),
                () -> assertThrows(ParseException.class, () -> parse(nativeBlock5)),
                () -> assertThrows(ParseException.class, () -> parse(nativeBlock6)),
                () -> assertThrows(ParseException.class, () -> parse(nativeBlock7)),
                () -> assertThrows(ParseException.class, () -> parse(nativeBlock8))
        );
    }


    @Test
    void testPartialEndOfExpression() throws Exception {
        NativeSql nativeSql = new NativeSql("--{{ asasf --{{  --}-fasdf-asf -}}-  asf asf -- asf --} --}}")
                .isBefore(false).dbType(DBType.ORACLE);
        String nativeBlock = nativeSql(nativeSql);
        Grain g = parse(nativeBlock);

        assertAll(
                () -> assertEquals(1, g.getAfterSqlList(DBType.ORACLE).size()),
                () -> assertEquals(
                        " asasf --{{  --}-fasdf-asf -}}-  asf asf -- asf --} ",
                        g.getAfterSqlList(DBType.ORACLE).get(0).getSql()
                )
        );
    }

    @Test
    void testBefore() throws Exception {
        NativeSql nativeSql = new NativeSql("--{{a = b--}}").isBefore(true).dbType(DBType.ORACLE);
        String nativeBlock = nativeSql(nativeSql);
        Grain g = parse(nativeBlock);

        assertAll(
                () -> assertFalse(g.getBeforeSqlList(DBType.ORACLE).isEmpty()),
                () -> assertTrue(g.getAfterSqlList(DBType.ORACLE).isEmpty()),
                () -> assertEquals(1, g.getBeforeSqlList(DBType.ORACLE).size()),
                () -> assertEquals("a = b", g.getBeforeSqlList(DBType.ORACLE).get(0).getSql())
        );
    }

    @Test
    void testMultipleExpressions() throws Exception {
        NativeSql nativeSql1 = new NativeSql("--{{1--}}").isBefore(true).dbType(DBType.ORACLE);
        NativeSql nativeSql2 = new NativeSql("--{{2--}}").isBefore(true).dbType(DBType.ORACLE);
        String nativeBlock = nativeSql(nativeSql1, nativeSql2);
        Grain g = parse(nativeBlock);

        assertAll(
                () -> assertFalse(g.getBeforeSqlList(DBType.ORACLE).isEmpty()),
                () -> assertTrue(g.getAfterSqlList(DBType.ORACLE).isEmpty()),
                () -> assertEquals(2, g.getBeforeSqlList(DBType.ORACLE).size()),
                () -> assertEquals("1", g.getBeforeSqlList(DBType.ORACLE).get(0).getSql()),
                () -> assertEquals("2", g.getBeforeSqlList(DBType.ORACLE).get(1).getSql())
        );
    }

    @Test
    void testMultipleDb() throws Exception {
        NativeSql nativeSql1 = new NativeSql("--{{1--}}").isBefore(true).dbType(DBType.ORACLE);
        NativeSql nativeSql2 = new NativeSql("--{{2--}}").isBefore(false).dbType(DBType.ORACLE);
        NativeSql nativeSql3 = new NativeSql("--{{3--}}").isBefore(false).dbType(DBType.POSTGRESQL);
        NativeSql nativeSql4 = new NativeSql("--{{4--}}").isBefore(true).dbType(DBType.MSSQL);
        NativeSql nativeSql5 = new NativeSql("--{{5--}}").isBefore(true).dbType(DBType.H2);

        String nativeBlock = nativeSql(nativeSql1, nativeSql2, nativeSql3, nativeSql4, nativeSql5);
        Grain g = parse(nativeBlock);

        assertAll(
                () -> assertFalse(g.getBeforeSqlList(DBType.ORACLE).isEmpty()),
                () -> assertFalse(g.getAfterSqlList(DBType.ORACLE).isEmpty()),
                () -> assertTrue(g.getBeforeSqlList(DBType.POSTGRESQL).isEmpty()),
                () -> assertFalse(g.getAfterSqlList(DBType.POSTGRESQL).isEmpty()),
                () -> assertFalse(g.getBeforeSqlList(DBType.MSSQL).isEmpty()),
                () -> assertTrue(g.getAfterSqlList(DBType.MSSQL).isEmpty()),
                () -> assertFalse(g.getBeforeSqlList(DBType.H2).isEmpty()),
                () -> assertTrue(g.getAfterSqlList(DBType.H2).isEmpty()),
                () -> assertEquals(1, g.getBeforeSqlList(DBType.ORACLE).size()),
                () -> assertEquals("1", g.getBeforeSqlList(DBType.ORACLE).get(0).getSql()),
                () -> assertEquals(1, g.getAfterSqlList(DBType.ORACLE).size()),
                () -> assertEquals("2", g.getAfterSqlList(DBType.ORACLE).get(0).getSql()),
                () -> assertEquals(1, g.getAfterSqlList(DBType.POSTGRESQL).size()),
                () -> assertEquals("3", g.getAfterSqlList(DBType.POSTGRESQL).get(0).getSql()),
                () -> assertEquals(1, g.getBeforeSqlList(DBType.MSSQL).size()),
                () -> assertEquals("4", g.getBeforeSqlList(DBType.MSSQL).get(0).getSql()),
                () -> assertEquals(1, g.getBeforeSqlList(DBType.H2).size()),
                () -> assertEquals("5", g.getBeforeSqlList(DBType.H2).get(0).getSql())
        );
    }


    private String nativeSql(NativeSql... nativeSqlArray) {
        StringBuilder sb = new StringBuilder("CREATE Schema nativeSql VERSION '1.0';\n");

        Arrays.stream(nativeSqlArray).forEach(
                (sql) -> sb.append(sql.toString())
        );

        return sb.toString();
    }

    private Grain parse(String text) throws Exception {
        final GrainPart gp;
        try (InputStream is = new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8))) {
            CelestaParser cp = new CelestaParser(is, "utf-8");
            gp = cp.extractGrainInfo(new CelestaSqlTestScore(), null);
        }

        try (InputStream is = new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8))) {
            CelestaParser cp = new CelestaParser(is, "utf-8");
            return cp.parseGrainPart(gp);
        }
    }

    private static class NativeSql {
        private final String sql;
        private boolean isBefore;
        private DBType dbType;

        private NativeSql(String sql) {
            this.sql = sql;
        }

        private NativeSql isBefore(boolean isBefore) {
            this.isBefore = isBefore;
            return this;
        }

        private NativeSql dbType(DBType dbType) {
            this.dbType = dbType;
            return this;
        }

        @Override
        public String toString() {
            return "EXECUTE NATIVE " + dbType.name()
                    + (isBefore ? " BEFORE " : " AFTER ")
                    + sql + ";\n";
        }
    }
}
