package ru.curs.celesta.dbutils.adaptors;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.Date;
import java.util.function.BiConsumer;


import ru.curs.celesta.CelestaException;
import ru.curs.celesta.CurrentScore;
import ru.curs.celesta.dbutils.*;
import ru.curs.celesta.dbutils.meta.*;
import ru.curs.celesta.dbutils.query.FromClause;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.dbutils.term.WhereTerm;
import ru.curs.celesta.dbutils.term.WhereTermsMaker;
import ru.curs.celesta.mock.CelestaImpl;
import ru.curs.celesta.score.*;
import ru.curs.celesta.syscursors.GrainsCursor;
import ru.curs.lyra.grid.VarcharFieldEnumerator;

import static ru.curs.celesta.score.ExprUtils.*;

public abstract class AbstractAdaptorTest {
    final static String GRAIN_NAME = "gtest";
    final static String SCORE_NAME = "testScore";

    final static String RESOURCES_SCORES_PATH = new StringJoiner(File.separator)
            .add("src").add("test").add("resources").add("ru").add("curs").add("celesta")
            .add("dbutils").add("adaptors").add("scores").toString();

    final static String ADD_NOT_NULL_COLUMN_WITH_DEFAULT_VALUE_1_SCORE_NAME = "addNotNullColumnWithDefaultValue1";
    final static String ADD_NOT_NULL_COLUMN_WITH_DEFAULT_VALUE_2_SCORE_NAME = "addNotNullColumnWithDefaultValue2";

    final static String ADD_NOT_NULL_COLUMN_WITH_DEFAULT_VALUE_1_SCORE_PATH = RESOURCES_SCORES_PATH
            + File.separator + ADD_NOT_NULL_COLUMN_WITH_DEFAULT_VALUE_1_SCORE_NAME;
    final static String ADD_NOT_NULL_COLUMN_WITH_DEFAULT_VALUE_2_SCORE_PATH = RESOURCES_SCORES_PATH
            + File.separator + ADD_NOT_NULL_COLUMN_WITH_DEFAULT_VALUE_2_SCORE_NAME;

    private DBAdaptor dba;
    private AbstractScore score;

    private Connection conn;
    private Table t;


    abstract Connection getConnection();

    private int insertRow(Connection conn, Table t, int val) throws IOException, SQLException {
        int count = t.getColumns().size();
        assertEquals(16, count);
        boolean[] nullsMask = {
                true, false, false, false, true, false, true, true,
                false, true, true, true, false, false, true, true
        };
        BLOB b = new BLOB();
        b.getOutStream().write(new byte[]{1, 2, 3});
        Object[] rowData = {
                null, "ab", val, false, null, 1.1, null, null,
                "eee", null, null, null, b, BigDecimal.ONE, null, null
        };
        List<ParameterSetter> program = new ArrayList<>();
        PreparedStatement pstmt = dba.getInsertRecordStatement(conn, t, nullsMask, program);
        assertNotNull(pstmt);

        int i = 1;
        for (ParameterSetter ps : program) {
            ps.execute(pstmt, i++, rowData, 0);
        }
        // int i = 1;
        // for (Object fieldVal : rowData)
        // DBAdaptor.setParam(pstmt, i++, fieldVal);
        try {
            int rowCount = pstmt.execute() ? 1 : pstmt.getUpdateCount(); // pstmt.executeUpdate();
            return rowCount;
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }

    private int getCount(Connection conn, DataGrainElement ge) throws Exception {
        FromClause from = new FromClause();
        from.setGe(ge);
        from.setExpression(dba.tableString(ge.getGrain().getName(), ge.getName()));
        PreparedStatement stmt = dba.getSetCountStatement(
                conn,
                from,
                "");
        try {
            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } finally {
            stmt.close();
        }
    }

    protected void setDba(DBAdaptor dba) {
        this.dba = dba;
    }

    protected void setScore(AbstractScore score) {
        this.score = score;
    }

    @BeforeEach
    public void setup() throws Exception {
        Grain g = score.getGrain(GRAIN_NAME);


        conn = getConnection();
        dba.createSchemaIfNotExists(conn, GRAIN_NAME);

        final boolean hasRecordInGrainsTable;
        String sql = "SELECT * FROM " + dba.tableString("celesta", GrainsCursor.TABLE_NAME) + " WHERE \"id\"=?";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, GRAIN_NAME);
            ResultSet rs = statement.executeQuery();

            hasRecordInGrainsTable = rs.next();
        }

        if (!hasRecordInGrainsTable) {
            sql = "INSERT INTO " + dba.tableString("celesta", GrainsCursor.TABLE_NAME)
                    + " (\"id\", \"version\", \"length\", \"checksum\",\"state\",\"lastmodified\",\"message\") "
                    + " VALUES(?,?,?,?,?,?,?)";

            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, g.getName());
                statement.setString(2, g.getVersion().toString());
                statement.setInt(3, g.getLength());
                statement.setString(4, String.format("%08X", g.getChecksum()));
                statement.setInt(5, GrainsCursor.READY);
                statement.setDate(6, new java.sql.Date(new Date().getTime()));
                statement.setString(7, "");

                statement.executeUpdate();
            }

        }

        conn.commit();

        t = g.getElement("test", Table.class);
        try {
            // Могла остаться от незавершившегося теста
            dba.dropTable(conn, t);
        } catch (CelestaException e) {
            conn.rollback();
        }
        dba.createTable(conn, t);
        conn.commit();
    }

    @AfterEach
    public void tearDown() throws Exception {
        conn.rollback();
        try {
            dba.dropTable(conn, t);
        } catch (Exception e) {
            // e.printStackTrace();
        }
        conn.close();
    }

    @Test
    public void isValidConnection() throws Exception {
        assertTrue(dba.isValidConnection(conn, 0));
    }

    @Test
    public void pidIsReturned() throws Exception {
        assertTrue(dba.getDBPid(conn) != 0);
    }

    @Test
    public void createAndDropAndExists() throws Exception {
        boolean result = dba.tableExists(conn, t.getGrain().getName(), t.getName());
        assertTrue(result);
        // В этот момент userTablesExist точно должен возвращать true, но на
        // false протестировать не можем, ибо также другие таблицы есть в базе
        // данных
        result = dba.userTablesExist(conn);
        assertTrue(result);
        dba.dropTable(conn, t);
        result = dba.tableExists(conn, t.getGrain().getName(), t.getName());
        assertFalse(result);
    }

    @Test
    public void getInsertRecordStatement() throws Exception {
        int rowCount = insertRow(conn, t, 1);
        assertEquals(1, rowCount);
        assertEquals(1, getCount(conn, t));
    }

    @Test
    public void getSetCountStatement() throws Exception {
        int count = getCount(conn, t);
        assertEquals(0, count);
        insertRow(conn, t, 1);
        assertEquals(1, dba.getCurrentIdent(conn, t));

        insertRow(conn, t, 2);
        assertEquals(2, dba.getCurrentIdent(conn, t));

        insertRow(conn, t, 3);
        assertEquals(3, dba.getCurrentIdent(conn, t));
        count = getCount(conn, t);
        assertEquals(3, count);

        insertRow(conn, t, 10);
        assertEquals(4, dba.getCurrentIdent(conn, t));
        assertEquals(4, getCount(conn, t));
    }

    @Test
    public void getUpdateRecordStatement() throws Exception {
        insertRow(conn, t, 1);
        assertEquals(1, getCount(conn, t));

        assertEquals(16, t.getColumns().size());
        boolean[] mask = {
                true, true, false, true, true, true, true, true,
                true, true, true, true, true, true, true, true
        };
        boolean[] nullsMask = {
                false, false, false, false, false, false, false, false,
                false, false, false, false, false, false, false, false
        };
        Integer[] rec = {1, null, 2, null, null, null, null, null, null, null, null, null, null, null, null, null};
        List<ParameterSetter> program = new ArrayList<>();
        WhereTerm w = WhereTermsMaker.getPKWhereTerm(t);
        PreparedStatement pstmt = dba.getUpdateRecordStatement(conn, t, mask, nullsMask, program, w.getWhere());
        w.programParams(program, dba);
        int i = 1;
        for (ParameterSetter ps : program) {
            ps.execute(pstmt, i++, rec, 1);
        }
        assertNotNull(pstmt);
        int rowCount = pstmt.executeUpdate();
        assertEquals(1, rowCount);
        assertEquals(1, getCount(conn, t));
    }

    @Test
    public void lostUpdatesTest() throws Exception {
        dba.createSysObjects(conn, score.getSysSchemaName());
        insertRow(conn, t, 1);
        WhereTerm w = WhereTermsMaker.getPKWhereTerm(t);
        PreparedStatement pstmt = dba.getOneRecordStatement(conn, t, w.getWhere(), Collections.emptySet());

        List<ParameterSetter> program = new ArrayList<>();
        w.programParams(program, dba);
        int i = 1;
        for (ParameterSetter ps : program) {
            ps.execute(pstmt, i++, new Integer[]{1}, 1);
        }
        ResultSet rs = pstmt.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt("recversion"));
        rs.close();

        boolean[] mask = {true, true, false, true, true, true, true, true, true, true, true, true, true, false, true, true};
        boolean[] nullsMask = {false, false, false, false, false, false, false, false, false, false, false, false,
                false, false, false, false};
        Integer[] rec = {1, null, 22, null, null, null, null, null, null, null, null, null, null, null, null};
        program = new ArrayList<>();
        w = WhereTermsMaker.getPKWhereTerm(t);

        PreparedStatement pstmt2 = dba.getUpdateRecordStatement(conn, t, mask, nullsMask, program, w.getWhere());

        // System.out.println(pstmt2.toString());
        assertNotNull(pstmt2);

        w.programParams(program, dba);
        i = 1;
        for (ParameterSetter ps : program) {
            ps.execute(pstmt2, i++, rec, 1);
        }

        pstmt2.executeUpdate();

        rs = pstmt.executeQuery();
        rs.next();
        assertEquals(2, rs.getInt("recversion"));
        assertEquals(22, rs.getInt("attrInt"));
        rs.close();

        rec = new Integer[]{1, null, 23, null, null, null, null, null, null, null, null, null, null, null, null};
        i = 1;
        for (ParameterSetter ps : program) {
            ps.execute(pstmt2, i++, rec, 2);
        }
        pstmt2.executeUpdate();

        rs = pstmt.executeQuery();
        rs.next();
        assertEquals(3, rs.getInt("recversion"));
        assertEquals(23, rs.getInt("attrInt"));
        rs.close();

        // we do not increment recversion value here, so we expect version check
        // failure
        SQLException e = assertThrows(SQLException.class, () -> pstmt2.executeUpdate());
        assertTrue(e.getMessage().contains("record version check failure"));
    }

    @Test
    public void getDeleteRecordStatement() throws Exception {
        insertRow(conn, t, 1);
        insertRow(conn, t, 10);
        assertEquals(2, getCount(conn, t));
        WhereTerm w = WhereTermsMaker.getPKWhereTerm(t);
        List<ParameterSetter> program = new ArrayList<>();

        PreparedStatement pstmt = dba.getDeleteRecordStatement(conn, t, w.getWhere());
        w.programParams(program, dba);
        int i = 1;
        for (ParameterSetter ps : program) {
            ps.execute(pstmt, i++, new Integer[]{1}, 1);
        }

        assertNotNull(pstmt);

        int rowCount = pstmt.executeUpdate();
        assertEquals(1, rowCount);
        assertEquals(1, getCount(conn, t));
    }

    @Test
    public void getColumns() throws Exception {
        Set<String> columnSet = dba.getColumns(conn, t);
        assertNotNull(columnSet);
        assertEquals(17, columnSet.size());
        assertTrue(columnSet.contains("f4"));
        assertFalse(columnSet.contains("nonExistentColumn"));
        // String[] columnNames = { "id", "attrVarchar", "attrInt", "f1",
        // "f2", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12", "f13" };
        // System.out
        // .println(Arrays.toString(columnSet.toArray(new String[0])));
    }

    @Test
    public void getIndices() throws Exception {
        Grain grain = score.getGrain(GRAIN_NAME);
        Index i = grain.getIndices().get("idxTest");

        dba.createIndex(conn, i);
        Map<String, DbIndexInfo> indicesSet = dba.getIndices(conn, t.getGrain());
        assertNotNull(indicesSet);
        assertEquals(1, indicesSet.size());
        DbIndexInfo inf = indicesSet.get("idxTest");
        assertTrue(inf.reflects(i));

        dba.dropIndex(grain, new DbIndexInfo(t.getName(), i.getName()));

        indicesSet = dba.getIndices(conn, t.getGrain());
        assertNotNull(indicesSet);
        assertEquals(0, indicesSet.size());
        String[] cols = {"f5", "f1", "f9", "id"};
        i = new Index(t, "testName", cols);
        dba.createIndex(conn, i);
        indicesSet = dba.getIndices(conn, t.getGrain());
        inf = indicesSet.get("testName");
        assertTrue(inf.reflects(i));
        dba.dropIndex(grain, inf);
    }

    @Test
    public void getOneFieldStatement() throws Exception {
        insertRow(conn, t, 121215);
        Column c = t.getColumns().get("attrInt");

        WhereTerm w = WhereTermsMaker.getPKWhereTerm(t);
        List<ParameterSetter> program = new ArrayList<>();

        PreparedStatement pstmt = dba.getOneFieldStatement(conn, c, w.getWhere());

        w.programParams(program, dba);
        int i = 1;
        for (ParameterSetter ps : program) {
            ps.execute(pstmt, i++, new Integer[]{1}, 1);
        }

        assertNotNull(pstmt);

        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(121215, rs.getInt("attrInt"));
    }

    @Test
    public void getOneRecordStatement() throws Exception {
        insertRow(conn, t, 17);

        WhereTerm w = WhereTermsMaker.getPKWhereTerm(t);
        List<ParameterSetter> program = new ArrayList<>();

        PreparedStatement pstmt = dba.getOneRecordStatement(conn, t, w.getWhere(), Collections.emptySet());

        w.programParams(program, dba);
        int i = 1;
        for (ParameterSetter ps : program) {
            ps.execute(pstmt, i++, new Integer[]{1}, 1);
        }

        assertNotNull(pstmt);
        // DBAdaptor.setParam(pstmt, 1, 1);// key value
        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(17, rs.getInt("attrInt"));
        assertEquals("ab", rs.getString("attrVarchar"));
    }

    @Test
    public void deleteRecordSetStatement() throws Exception {
        insertRow(conn, t, 1);
        insertRow(conn, t, 1);
        assertEquals(2, getCount(conn, t));
        PreparedStatement pstmt = dba.deleteRecordSetStatement(conn, t, "");
        assertNotNull(pstmt);
        int rowCount = pstmt.executeUpdate();
        assertEquals(2, rowCount);
        assertEquals(0, getCount(conn, t));
    }

    @Test
    public void getColumnInfo1() throws ParseException {
        DbColumnInfo c;
        // Проверяем реакцию на столбец, которого нет в базе данных
        Column newCol = new IntegerColumn(t, "nonExistentColumn");
        assertSame(newCol, t.getColumn("nonExistentColumn"));
        c = dba.getColumnInfo(conn, newCol);
        assertNull(c);

        // Этот тест проверяет типы колонок и выражения not null
        // attrVarchar varchar(2),
        c = dba.getColumnInfo(conn, t.getColumn("attrVarchar"));
        assertEquals("attrVarchar", c.getName());
        assertSame(StringColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(2, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());
        assertEquals(false, c.isIdentity());

        // f1 bit not null,
        c = dba.getColumnInfo(conn, t.getColumn("f1"));
        assertEquals("f1", c.getName());
        assertSame(BooleanColumn.class, c.getType());
        assertEquals(false, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());
        assertEquals(false, c.isIdentity());

        // f4 real,
        c = dba.getColumnInfo(conn, t.getColumn("f4"));
        assertEquals("f4", c.getName());
        assertSame(FloatingColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());
        assertEquals(false, c.isIdentity());

        // f7 varchar(8),
        c = dba.getColumnInfo(conn, t.getColumn("f7"));
        assertEquals("f7", c.getName());
        assertSame(StringColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(8, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());
        assertEquals(false, c.isIdentity());

        // f11 image not null
        c = dba.getColumnInfo(conn, t.getColumn("f11"));
        assertEquals("f11", c.getName());
        assertSame(BinaryColumn.class, c.getType());
        assertEquals(false, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());
        assertEquals(false, c.isIdentity());

        // f12 decimal(11, 7),
        c = dba.getColumnInfo(conn, t.getColumn("f12"));
        assertEquals("f12", c.getName());
        assertSame(DecimalColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(11, c.getLength());
        assertEquals(7, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());
        assertEquals(false, c.isIdentity());

        //f14 datetime with time zone
        c = dba.getColumnInfo(conn, t.getColumn("f14"));
        assertEquals("f14", c.getName());
        assertSame(ZonedDateTimeColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());
        assertEquals(false, c.isIdentity());
    }

    @Test
    public void getColumnInfo2() throws ParseException {
        DbColumnInfo c;
        // Этот тест проверяет выражения default и дополнительные атрибуты
        // id int identity not null primary key,
        c = dba.getColumnInfo(conn, t.getColumn("id"));
        assertEquals("id", c.getName());
        assertSame(IntegerColumn.class, c.getType());
        assertEquals(false, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());
        assertEquals(true, c.isIdentity());

        // attrInt int default 3,
        c = dba.getColumnInfo(conn, t.getColumn("attrInt"));
        assertEquals("attrInt", c.getName());
        assertSame(IntegerColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("3", c.getDefaultValue());
        assertEquals(false, c.isMax());
        assertEquals(false, c.isIdentity());

        // f2 bit default 'true',
        c = dba.getColumnInfo(conn, t.getColumn("f2"));
        assertEquals("f2", c.getName());
        assertSame(BooleanColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("'TRUE'", c.getDefaultValue());
        assertEquals(false, c.isMax());
        assertEquals(false, c.isIdentity());

        // f5 real not null default 5.5,
        c = dba.getColumnInfo(conn, t.getColumn("f5"));
        assertEquals("f5", c.getName());
        assertSame(FloatingColumn.class, c.getType());
        assertEquals(false, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("5.5", c.getDefaultValue());
        assertEquals(false, c.isMax());
        assertEquals(false, c.isIdentity());

        // f6 varchar(MAX) not null default 'abc',
        c = dba.getColumnInfo(conn, t.getColumn("f6"));
        assertEquals("f6", c.getName());
        assertSame(StringColumn.class, c.getType());
        assertEquals(false, c.isNullable());
        // assertEquals(0, c.getLength()); -- database-dependent value
        assertEquals(0, c.getScale());
        assertEquals("'abc'", c.getDefaultValue());
        assertEquals(true, c.isMax());
        assertEquals(false, c.isIdentity());

        // f8 datetime default '20130401',
        c = dba.getColumnInfo(conn, t.getColumn("f8"));
        assertEquals("f8", c.getName());
        assertSame(DateTimeColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("'20130401'", c.getDefaultValue());
        assertEquals(false, c.isMax());
        assertEquals(false, c.isIdentity());

        // f9 datetime not null default getdate(),
        c = dba.getColumnInfo(conn, t.getColumn("f9"));
        assertEquals("f9", c.getName());
        assertSame(DateTimeColumn.class, c.getType());
        assertEquals(false, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("GETDATE()", c.getDefaultValue());
        assertEquals(false, c.isMax());
        assertEquals(false, c.isIdentity());

        // f10 image default 0xFFAAFFAAFF,
        c = dba.getColumnInfo(conn, t.getColumn("f10"));
        assertEquals("f10", c.getName());
        assertSame(BinaryColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("0xFFAAFFAAFF", c.getDefaultValue());
        assertEquals(false, c.isMax());
        assertEquals(false, c.isIdentity());

        // f13 decimal(5, 3) not null default 46.123
        c = dba.getColumnInfo(conn, t.getColumn("f13"));
        assertEquals("f13", c.getName());
        assertSame(DecimalColumn.class, c.getType());
        assertEquals(false, c.isNullable());
        assertEquals(5, c.getLength());
        assertEquals(3, c.getScale());
        assertEquals("46.123", c.getDefaultValue());
        assertEquals(false, c.isMax());
        assertEquals(false, c.isIdentity());
    }


    @Test
    public void testColumnInfoForMaterializedView() {

        Grain g = score.getGrain(GRAIN_NAME);
        Table tableForMatView = g.getElement("tableForMatView", Table.class);
        MaterializedView mView1gTest = g.getElement("mView1gTest", MaterializedView.class);

        boolean tablesAreCreated = false;

        try {
            // Могли остаться от незавершившегося теста
            try {
                dba.dropTable(conn, tableForMatView);
            } catch (CelestaException e) {
                conn.rollback();
            }
            try {
                dba.dropTable(conn, mView1gTest);
            } catch (CelestaException e) {
                conn.rollback();
            }

            dba.createTable(conn, tableForMatView);
            dba.createTable(conn, mView1gTest);
            tablesAreCreated = true;

            DbColumnInfo c;
            Column col;

            col = mView1gTest.getColumn("idsum");
            c = dba.getColumnInfo(conn, col);
            assertEquals("idsum", c.getName());
            assertSame(IntegerColumn.class, c.getType());
            assertEquals(true, c.isNullable());
            assertEquals("", c.getDefaultValue());
            assertEquals(false, c.isIdentity());
            assertEquals(0, c.getLength());
            assertEquals(0, c.getScale());

            col = mView1gTest.getColumn("f1");
            c = dba.getColumnInfo(conn, col);
            assertEquals("f1", c.getName());
            assertSame(StringColumn.class, c.getType());
            assertEquals(false, c.isNullable());
            assertEquals("", c.getDefaultValue());
            assertEquals(false, c.isIdentity());
            assertEquals(2, c.getLength());
            assertEquals(0, c.getScale());

            col = mView1gTest.getColumn("f2");
            c = dba.getColumnInfo(conn, col);
            assertEquals("f2", c.getName());
            assertSame(IntegerColumn.class, c.getType());
            assertEquals(false, c.isNullable());
            assertEquals("", c.getDefaultValue());
            assertEquals(false, c.isIdentity());
            assertEquals(0, c.getLength());
            assertEquals(0, c.getScale());

            col = mView1gTest.getColumn("f3");
            c = dba.getColumnInfo(conn, col);
            assertEquals("f3", c.getName());
            assertSame(BooleanColumn.class, c.getType());
            assertEquals(false, c.isNullable());
            assertEquals("", c.getDefaultValue());
            assertEquals(false, c.isIdentity());
            assertEquals(0, c.getLength());
            assertEquals(0, c.getScale());

            col = mView1gTest.getColumn("f4");
            c = dba.getColumnInfo(conn, col);
            assertEquals("f4", c.getName());
            assertSame(FloatingColumn.class, c.getType());
            assertEquals(false, c.isNullable());
            assertEquals("", c.getDefaultValue());
            assertEquals(false, c.isIdentity());
            assertEquals(0, c.getLength());
            assertEquals(0, c.getScale());

            col = mView1gTest.getColumn("f5");
            c = dba.getColumnInfo(conn, col);
            assertEquals("f5", c.getName());
            assertSame(DateTimeColumn.class, c.getType());
            assertEquals(false, c.isNullable());
            assertEquals("", c.getDefaultValue());
            assertEquals(false, c.isIdentity());
            assertEquals(0, c.getLength());
            assertEquals(0, c.getScale());

            col = mView1gTest.getColumn("f6");
            c = dba.getColumnInfo(conn, col);
            assertEquals("f6", c.getName());
            assertSame(DecimalColumn.class, c.getType());
            assertEquals(false, c.isNullable());
            assertEquals("", c.getDefaultValue());
            assertEquals(false, c.isIdentity());
            assertEquals(2, c.getLength());
            assertEquals(1, c.getScale());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (tablesAreCreated) {
                dba.dropTable(conn, tableForMatView);
                dba.dropTable(conn, mView1gTest);
            }
        }
    }


    @Test
    public void updateColumn() throws ParseException, IOException, SQLException {
        // NULL/NOT NULL и DEFAULT (простые)
        DbColumnInfo c;
        Column col;
        // To test transforms on non-empty table
        insertRow(conn, t, 17);

        col = t.getColumn("attrInt");
        c = dba.getColumnInfo(conn, col);
        assertEquals("attrInt", c.getName());
        assertSame(IntegerColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals("3", c.getDefaultValue());
        assertEquals(false, c.isIdentity());
        col.setNullableAndDefault(false, "55");
        dba.updateColumn(conn, col, c);
        c = dba.getColumnInfo(conn, col);
        assertEquals("attrInt", c.getName());
        assertSame(IntegerColumn.class, c.getType());
        assertEquals(false, c.isNullable());
        assertEquals("55", c.getDefaultValue());
        assertEquals(false, c.isIdentity());
        col.setNullableAndDefault(false, null);
        dba.updateColumn(conn, col, c);
        c = dba.getColumnInfo(conn, col);
        assertEquals("", c.getDefaultValue());

        // f6 varchar(MAX) not null default 'abc',
        col = t.getColumn("f6");
        c = dba.getColumnInfo(conn, col);
        assertEquals("f6", c.getName());
        assertSame(StringColumn.class, c.getType());
        assertEquals(false, c.isNullable());
        assertEquals("'abc'", c.getDefaultValue());
        assertEquals(true, c.isMax());
        StringColumn scol = (StringColumn) col;
        scol.setLength("234");
        scol.setNullableAndDefault(true, "'eee'");
        dba.updateColumn(conn, col, c);
        c = dba.getColumnInfo(conn, col);
        assertEquals("f6", c.getName());
        assertSame(StringColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals("'eee'", c.getDefaultValue());
        assertEquals(false, c.isMax());
        assertEquals(234, c.getLength());

        // f8 datetime default '20130401',
        col = t.getColumn("f8");
        DateTimeColumn dcol = (DateTimeColumn) col;
        assertFalse(dcol.isGetdate());
        c = dba.getColumnInfo(conn, col);
        assertEquals("f8", c.getName());
        assertSame(DateTimeColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals("'20130401'", c.getDefaultValue());

        col.setNullableAndDefault(true, "getdate");
        assertTrue(dcol.isGetdate());
        dba.updateColumn(conn, col, c);
        c = dba.getColumnInfo(conn, col);
        assertEquals("f8", c.getName());
        assertSame(DateTimeColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals("GETDATE()", c.getDefaultValue());

        col.setNullableAndDefault(true, null);
        assertFalse(dcol.isGetdate());
        dba.updateColumn(conn, col, c);
        c = dba.getColumnInfo(conn, col);
        assertEquals("f8", c.getName());
        assertSame(DateTimeColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals("", c.getDefaultValue());

        col.setNullableAndDefault(false, "getdate");
        assertTrue(dcol.isGetdate());
        dba.updateColumn(conn, col, c);
        c = dba.getColumnInfo(conn, col);
        assertEquals("f8", c.getName());
        assertSame(DateTimeColumn.class, c.getType());
        assertEquals(false, c.isNullable());
        assertEquals("GETDATE()", c.getDefaultValue());

        // f10 image default 0xFFAAFFAAFF
        col = t.getColumn("f10");
        c = dba.getColumnInfo(conn, col);
        assertTrue(col.isNullable());
        assertTrue(c.isNullable());
        assertEquals("0xFFAAFFAAFF", c.getDefaultValue());
        col.setNullableAndDefault(false, "0xBBCC");
        dba.updateColumn(conn, col, c);
        dba.manageAutoIncrement(conn, t);
        c = dba.getColumnInfo(conn, col);
        assertEquals("0xBBCC", c.getDefaultValue());
        assertFalse(c.isNullable());

        //f12 decimal(11, 7)
        col = t.getColumn("f12");
        c = dba.getColumnInfo(conn, col);
        assertTrue(col.isNullable());
        assertTrue(c.isNullable());
        assertEquals("", c.getDefaultValue());
        assertEquals(11, c.getLength());
        assertEquals(7, c.getScale());
        col.setNullableAndDefault(false, "155.0216");
        DecimalColumn dc = (DecimalColumn) col;
        dc.setPrecision(7);
        dc.setScale(4);
        dba.updateColumn(conn, col, c);
        dba.manageAutoIncrement(conn, t);
        c = dba.getColumnInfo(conn, col);
        assertEquals("155.0216", c.getDefaultValue());
        assertEquals(7, c.getLength());
        assertEquals(4, c.getScale());
        assertFalse(c.isNullable());
    }

    @Test
    public void updateColumn2test() throws ParseException, IOException, SQLException {
        // IDENTITY
        DbColumnInfo c;
        IntegerColumn col;

        // To test transforms on non-empty table
        insertRow(conn, t, 15);

        col = (IntegerColumn) t.getColumn("id");
        assertTrue(col.isIdentity());
        c = dba.getColumnInfo(conn, col);
        assertTrue(c.isIdentity());
        col.setNullableAndDefault(false, null);
        assertFalse(col.isIdentity());
        dba.updateColumn(conn, col, c);
        dba.manageAutoIncrement(conn, t);
        c = dba.getColumnInfo(conn, col);
        assertFalse(c.isIdentity());
        assertFalse(c.isNullable());

        col = (IntegerColumn) t.getColumn("attrInt");
        c = dba.getColumnInfo(conn, col);
        assertFalse(c.isIdentity());
        col.setNullableAndDefault(true, "identity");
        dba.updateColumn(conn, col, c);
        dba.manageAutoIncrement(conn, t);
        c = dba.getColumnInfo(conn, col);
        assertTrue(c.isIdentity());
        assertTrue(c.isNullable());

        col.setNullableAndDefault(true, null);
        dba.updateColumn(conn, col, c);
        dba.manageAutoIncrement(conn, t);
        c = dba.getColumnInfo(conn, col);
        assertFalse(c.isIdentity());
        assertTrue(c.isNullable());

        col = (IntegerColumn) t.getColumn("id");
        assertFalse(col.isIdentity());
        c = dba.getColumnInfo(conn, col);
        assertFalse(c.isIdentity());
        assertFalse(c.isNullable());

        col.setNullableAndDefault(false, "identity");
        dba.updateColumn(conn, col, c);
        dba.manageAutoIncrement(conn, t);
        c = dba.getColumnInfo(conn, col);
        assertTrue(c.isIdentity());
        assertFalse(c.isNullable());

    }

    @Test
    public void updateColumn3test() throws ParseException, IOException, SQLException {
        // String length
        DbColumnInfo c;
        StringColumn col;
        // To test transforms on non-empty table
        insertRow(conn, t, 15);

        col = (StringColumn) t.getColumn("attrVarchar");
        assertEquals(2, col.getLength());
        c = dba.getColumnInfo(conn, col);
        assertEquals(2, c.getLength());
        assertFalse(c.isMax());
        col.setLength("19");
        assertEquals(19, col.getLength());
        assertFalse(col.isMax());

        dba.updateColumn(conn, col, c);
        c = dba.getColumnInfo(conn, col);
        assertEquals(19, c.getLength());
        assertFalse(c.isMax());

        col.setLength("MAX");
        assertTrue(col.isMax());
        dba.updateColumn(conn, col, c);
        c = dba.getColumnInfo(conn, col);
        assertTrue(c.isMax());

        col.setNullableAndDefault(false, "'www'");
        col.setLength("3");
        assertFalse(col.isMax());
        dba.updateColumn(conn, col, c);
        c = dba.getColumnInfo(conn, col);
        assertFalse(c.isMax());
        assertEquals(3, c.getLength());
        assertFalse(c.isNullable());
        assertEquals("'www'", c.getDefaultValue());

    }

    @Test
    public void updateColumn4test() throws ParseException, IOException, SQLException {
        // BLOB Default
        DbColumnInfo c;
        BinaryColumn col;
        // To test transforms on non-empty table
        insertRow(conn, t, 11);

        col = (BinaryColumn) t.getColumn("f10");
        c = dba.getColumnInfo(conn, col);
        assertEquals("0xFFAAFFAAFF", c.getDefaultValue());
        assertTrue(c.isNullable());

        col.setNullableAndDefault(false, "0xABABAB");
        dba.updateColumn(conn, col, c);
        dba.manageAutoIncrement(conn, t);
        c = dba.getColumnInfo(conn, col);
        assertFalse(c.isNullable());
        assertEquals("0xABABAB", c.getDefaultValue());

        col.setNullableAndDefault(false, null);
        dba.updateColumn(conn, col, c);
        dba.manageAutoIncrement(conn, t);
        c = dba.getColumnInfo(conn, col);
        assertFalse(c.isNullable());
        assertEquals("", c.getDefaultValue());
    }

    @Test
    public void updateColumn5test() throws ParseException {
        // Change data type
        DbColumnInfo c;
        IntegerColumn col;
        StringColumn scol;
        BooleanColumn bcol;
        // Table should be empty in Oracle to change data type...
        // insertRow(conn, t, 11);


        for (ForeignKey fk : t.getForeignKeys()) {
            //to make column attrInt deletable
            fk.delete();
        }

        col = (IntegerColumn) t.getColumn("attrInt");
        c = dba.getColumnInfo(conn, col);
        assertEquals("attrInt", c.getName());
        assertSame(IntegerColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals("3", c.getDefaultValue());
        assertEquals(false, c.isIdentity());

        t.getGrain().getIndices().get("idxTest").delete();
        // int --> varchar
        col.delete();
        scol = new StringColumn(t, "attrInt");
        scol.setLength("40");
        scol.setNullableAndDefault(true, "'русские буквы'");
        dba.updateColumn(conn, scol, c);
        c = dba.getColumnInfo(conn, scol);
        assertEquals("attrInt", c.getName());
        assertSame(StringColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals("'русские буквы'", c.getDefaultValue());
        assertEquals(40, c.getLength());

        // varchar --> int
        scol.delete();
        col = new IntegerColumn(t, "attrInt");
        col.setNullableAndDefault(true, "5");
        dba.updateColumn(conn, col, c);
        c = dba.getColumnInfo(conn, col);
        assertEquals("attrInt", c.getName());
        assertSame(IntegerColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals("5", c.getDefaultValue());
        assertEquals(false, c.isIdentity());

        // int --> boolean (test specially for Oracle!)
        col.delete();
        bcol = new BooleanColumn(t, "attrInt");
        bcol.setNullableAndDefault(true, null);
        dba.updateColumn(conn, bcol, c);
        c = dba.getColumnInfo(conn, col);
        assertEquals("attrInt", c.getName());
        assertSame(BooleanColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals("", c.getDefaultValue());

        // boolean --> back to int
        bcol.delete();
        col = new IntegerColumn(t, "attrInt");
        col.setNullableAndDefault(true, null);
        dba.updateColumn(conn, col, c);
        c = dba.getColumnInfo(conn, col);
        assertEquals("attrInt", c.getName());
        assertSame(IntegerColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isIdentity());

    }


    @Test
    public void testReflects() throws ParseException {
        DbColumnInfo c;

        c = dba.getColumnInfo(conn, t.getColumn("f8"));
        assertTrue(c.reflects(t.getColumn("f8")));
        assertFalse(c.reflects(t.getColumn("f1")));
        assertFalse(c.reflects(t.getColumn("f9")));

        c = dba.getColumnInfo(conn, t.getColumn("attrVarchar"));
        assertTrue(c.reflects(t.getColumn("attrVarchar")));
        assertFalse(c.reflects(t.getColumn("f7")));

        c = dba.getColumnInfo(conn, t.getColumn("f10"));
        assertTrue(c.reflects(t.getColumn("f10")));
        assertFalse(c.reflects(t.getColumn("f11")));

    }

    @Test
    public void getPKInfo() throws ParseException, IOException, SQLException {
        DbPkInfo c;
        insertRow(conn, t, 15);

        c = dba.getPKInfo(conn, t);
        assertNotNull(c);
        assertEquals("pk_test", c.getName());
        assertEquals(1, c.getColumnNames().size());
        String[] expected = {"id"};
        assertTrue(Arrays.equals(expected, c.getColumnNames().toArray(new String[0])));
        assertTrue(c.reflects(t));
        assertFalse(c.isEmpty());

        dba.dropPk(conn, t, "pk_test");
        c = dba.getPKInfo(conn, t);
        assertNull(c.getName());
        assertEquals(0, c.getColumnNames().size());
        assertFalse(c.reflects(t));
        assertTrue(c.isEmpty());

        t.setPK("id", "f1", "f9");
        dba.createPK(conn, t);
        c = dba.getPKInfo(conn, t);
        assertNotNull(c);
        assertEquals("pk_test", c.getName());
        assertEquals(3, c.getColumnNames().size());
        String[] expected2 = {"id", "f1", "f9"};
        assertTrue(Arrays.equals(expected2, c.getColumnNames().toArray(new String[0])));
        assertTrue(c.reflects(t));
        assertFalse(c.isEmpty());

    }

    @Test
    public void getFKInfo() throws ParseException, SQLException {
        dba.dropTable(conn, t);
        assertFalse(dba.tableExists(conn, "gtest", "test"));
        assertFalse(dba.tableExists(conn, "gtest", "refTo"));

        Grain g = score.getGrain(GRAIN_NAME);
        Table t2 = g.getElement("refTo", Table.class);
        ForeignKey fk = t.getForeignKeys().iterator().next();
        assertEquals("fk_testNameVeryVeryLongLonName", fk.getConstraintName());
        try {
            dba.createTable(conn, t);
            try {
                dba.dropTable(conn, t2);
            } catch (CelestaException e) {
                conn.rollback();
            }
            dba.createTable(conn, t2);
            assertTrue(dba.tableExists(conn, "gtest", "test"));
            assertTrue(dba.tableExists(conn, "gtest", "refTo"));

            List<DbFkInfo> l = dba.getFKInfo(conn, g);
            assertNotNull(l);
            assertEquals(0, l.size());

            dba.createFK(conn, fk);
            l = dba.getFKInfo(conn, g);
            assertEquals(1, l.size());
            DbFkInfo info = l.get(0);
            assertEquals("fk_testNameVeryVeryLongLonName", info.getName());
            String[] expected = {"attrVarchar", "attrInt"};
            String[] actual = info.getColumnNames().toArray(new String[0]);

            // System.out.println(Arrays.toString(actual));
            assertTrue(Arrays.equals(expected, actual));
            assertEquals(FKRule.CASCADE, info.getUpdateRule());
            assertEquals(FKRule.SET_NULL, info.getDeleteRule());

            assertEquals("gtest", info.getRefGrainName());
            assertEquals("refTo", info.getRefTableName());

            assertTrue(info.reflects(fk));

            // Проверяем, что не произошло "запутки" первичных и внешних ключей
            // (существовавший когда-то баг)
            assertTrue(dba.getPKInfo(conn, t).reflects(t));
            assertTrue(dba.getPKInfo(conn, t2).reflects(t2));

            dba.dropFK(conn, t.getGrain().getName(), t.getName(), fk.getConstraintName());
            l = dba.getFKInfo(conn, g);
            assertEquals(0, l.size());
        } catch (CelestaException e) {
            e.printStackTrace();
            throw e;
        } finally {
            conn.rollback();
            dba.dropTable(conn, t);
            dba.dropTable(conn, t2);
        }
    }

    @Test
    public void additionalCreateTableTest() throws ParseException {
        Grain g = score.getGrain(GRAIN_NAME);
        Table t3 = g.getElement("aLongIdentityTableNaaame", Table.class);
        try {
            dba.createTable(conn, t3);
            DbColumnInfo c = dba.getColumnInfo(conn, t3.getColumn("f1"));
            assertTrue(c.isIdentity());
            c = dba.getColumnInfo(conn, t3.getColumn("field2"));
            assertSame(BooleanColumn.class, c.getType());
            assertFalse(c.isIdentity());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dba.dropTable(conn, t3);
        }
    }

    @Test
    public void viewTest() throws ParseException, SQLException {
        Grain g = score.getGrain(GRAIN_NAME);
        Table t2 = g.getElement("refTo", Table.class);
        try {
            ForeignKey fk = t.getForeignKeys().iterator().next();
            View v = g.getElement("testview", View.class);
            assertEquals("testview", v.getName());
            dba.createTable(conn, t2);
            dba.createFK(conn, fk);
            // пустой перечень view
            List<String> viewList = dba.getViewList(conn, g);
            assertNotNull(viewList);
            assertTrue(viewList.isEmpty());
            // создать view
            dba.createView(conn, v);
            // увидеть его в перечне
            viewList = dba.getViewList(conn, g);
            assertEquals(1, viewList.size());
            assertEquals(v.getName(), viewList.get(0));
            // удалить view
            dba.dropView(conn, v.getGrain().getName(), v.getName());
            // пустой перечень view
            viewList = dba.getViewList(conn, g);
            assertTrue(viewList.isEmpty());
        } finally {
            conn.rollback();
            dba.dropTable(conn, t);
            dba.dropTable(conn, t2);
        }
    }

    @Test
    public void resetIdentityTest() throws IOException, SQLException {
        insertRow(conn, t, 110);
        WhereTerm w = WhereTermsMaker.getPKWhereTerm(t);
        PreparedStatement pstmt = dba.getOneRecordStatement(conn, t, w.getWhere(), Collections.emptySet());

        assertNotNull(pstmt);

        List<ParameterSetter> program = new ArrayList<>();
        w.programParams(program, dba);
        int i = 1;
        for (ParameterSetter ps : program) {
            ps.execute(pstmt, i++, new Integer[]{555 /* key value */}, 1);
        }

        ResultSet rs = pstmt.executeQuery();
        assertFalse(rs.next());
        rs.close();
        dba.resetIdentity(conn, t, 555);
        insertRow(conn, t, 110);
        assertEquals(555, dba.getCurrentIdent(conn, t));
        rs = pstmt.executeQuery();
        assertTrue(rs.next());
        insertRow(conn, t, 110);
        assertEquals(556, dba.getCurrentIdent(conn, t));
    }

    @Test
    public void createComplexIndex() throws ParseException {
        Grain g = score.getGrain(GRAIN_NAME);
        Index idx = g.getIndices().get("idxTest2");
        dba.createIndex(conn, idx);
        Map<String, DbIndexInfo> indicesSet = dba.getIndices(conn, t.getGrain());
        DbIndexInfo inf = indicesSet.get("idxTest2");
        assertEquals(2, inf.getColumnNames().size());
        assertTrue(inf.reflects(idx));
        dba.dropIndex(g, inf);
    }

    @Test
    public void selectWithLimitAndOffset() throws ParseException, IOException, SQLException {
        insertRow(conn, t, 1);
        insertRow(conn, t, 2);

        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            String orderBy = t.getColumn("id").getQuotedName();
            int count = 0;

            FromClause from = new FromClause();
            from.setGe(t);
            from.setExpression(dba.tableString(t.getGrain().getName(), t.getName()));

            stmt = dba.getRecordSetStatement(
                    conn, from, "", orderBy, 0, 0, Collections.emptySet()
            );
            rs = stmt.executeQuery();
            while (rs.next()) {
                ++count;
            }
            assertEquals(2, count);

            count = 0;
            stmt = dba.getRecordSetStatement(
                    conn, from, "", orderBy, 1, 0, Collections.emptySet()
            );
            rs = stmt.executeQuery();
            while (rs.next()) {
                ++count;
            }
            assertEquals(1, count);

            count = 0;
            stmt = dba.getRecordSetStatement(
                    conn, from, "", orderBy, 0, 1, Collections.emptySet()
            );
            rs = stmt.executeQuery();
            while (rs.next()) {
                ++count;
            }
            assertEquals(1, count);
        } catch (Exception e) {
            throw e;
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (rs != null) {
                rs.close();
            }
        }

    }

    @Test
    public void testInitDataForMaterializedView() throws Exception {
        Grain g = score.getGrain(GRAIN_NAME);
        Table t = g.getElement("tableForInitMvData", Table.class);
        MaterializedView mv = g.getElement("mViewForInit", MaterializedView.class);

        PreparedStatement pstmt = null;
        try {

            // Могли остаться от незавершившегося теста
            try {
                dba.dropTable(conn, t);
            } catch (CelestaException e) {
                conn.rollback();
            }
            try {
                dba.dropTable(conn, mv);
            } catch (CelestaException e) {
                conn.rollback();
            }

            dba.createTable(conn, t);
            dba.createTable(conn, mv);

            boolean[] nullsMask = {true, false, false, false};
            LocalDateTime d = LocalDateTime.now();
            Object[] rowData = {null, "A", 5, Date.from(
                    d.toInstant(ZoneOffset.UTC))};
            List<ParameterSetter> program = new ArrayList<>();
            pstmt = dba.getInsertRecordStatement(conn, t, nullsMask, program);
            int i = 1;
            for (ParameterSetter ps : program) {
                ps.execute(pstmt, i++, rowData, 0);
            }
            pstmt.execute();

            d = d.plusSeconds(1);
            Object[] rowData2 = {null, "A", 5, Date.from(
                    d.toInstant(ZoneOffset.UTC))};
            i = 1;
            for (ParameterSetter ps : program) {
                ps.execute(pstmt, i++, rowData2, 0);
            }
            pstmt.execute();

            assertEquals(0, getCount(conn, mv));

            Object[] secondRowData = {null, "B", 3, d};
            i = 1;
            for (ParameterSetter ps : program) {
                ps.execute(pstmt, i++, secondRowData, 0);
            }
            pstmt.execute();

            assertEquals(0, getCount(conn, mv));

            dba.initDataForMaterializedView(conn, mv);
            assertEquals(2, getCount(conn, mv));

            dba.initDataForMaterializedView(conn, mv);
            assertEquals(2, getCount(conn, mv));

            FromClause from = new FromClause();
            from.setGe(mv);
            from.setExpression(dba.tableString(mv.getGrain().getName(), mv.getName()));

            pstmt = dba.getRecordSetStatement(
                    conn, from, "", "\"var\"", 0, 0, Collections.emptySet()
            );
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertEquals("A", rs.getString("var"));
            assertEquals(10, rs.getInt("s"));
            assertEquals(2, rs.getInt(MaterializedView.SURROGATE_COUNT));

            rs.next();
            assertEquals("B", rs.getString("var"));
            assertEquals(3, rs.getInt("s"));
            assertEquals(1, rs.getInt(MaterializedView.SURROGATE_COUNT));

        } catch (Exception e) {
            throw e;
        } finally {
            if (pstmt != null) {
                pstmt.close();
            }

            dba.dropTable(conn, t);
            dba.dropTable(conn, mv);
        }
    }


    @Test
    public void testCreateParameterizedView() throws Exception {
        Grain g = score.getGrain(GRAIN_NAME);
        String pViewName = "pView";

        try {
            if (dba.getParameterizedViewList(conn, g).contains(pViewName))
                dba.dropParameterizedView(conn, g.getName(), pViewName);

            List<String> list = dba.getParameterizedViewList(conn, g);
            assertEquals(0, list.size());

            ParameterizedView pView = g.getElement(pViewName, ParameterizedView.class);
            dba.createParameterizedView(conn, pView);


            list = dba.getParameterizedViewList(conn, g);
            assertEquals(1, list.size());
            assertEquals(pViewName, list.get(0));
        } catch (Exception e) {
            throw e;
        } finally {
            if (dba.getParameterizedViewList(conn, g).contains(pViewName))
                dba.dropParameterizedView(conn, g.getName(), pViewName);
        }

    }


    @Test
    public void testDropParameterizedView() throws Exception {
        Grain g = score.getGrain(GRAIN_NAME);
        String pViewName = "pView";

        if (dba.getParameterizedViewList(conn, g).contains(pViewName))
            dba.dropParameterizedView(conn, g.getName(), pViewName);

        List<String> list = dba.getParameterizedViewList(conn, g);
        assertEquals(0, list.size());

        ParameterizedView pView = g.getElement(pViewName, ParameterizedView.class);
        dba.createParameterizedView(conn, pView);

        dba.dropParameterizedView(conn, g.getName(), pViewName);
        assertEquals(0, list.size());
    }


    @Test
    public void testGetInFilterClause() throws Exception {
        Grain g = score.getGrain(GRAIN_NAME);
        Table t2 = g.getElement("testInFilterClause", Table.class);

        boolean tableIsCreated = false;
        Statement stmt = conn.createStatement();
        PreparedStatement pstmt = null;
        try {
            // Могла остаться от незавершившегося теста
            try {
                dba.dropTable(conn, t2);
            } catch (CelestaException e) {
                conn.rollback();
            }

            dba.createTable(conn, t2);
            tableIsCreated = true;


            insertRow(conn, t, 1);
            insertRow(conn, t, 2);

            List<String> tFields = Arrays.asList("attrInt");
            List<String> t2Fields = Arrays.asList("atInt");
            String otherWhere = "(1 = 1)";

            String where = dba.getInFilterClause(t, t2, tFields, t2Fields, otherWhere);

            FromClause from = new FromClause();
            from.setGe(t);
            from.setExpression(dba.tableString(g.getName(), t.getName()));

            pstmt = dba.getSetCountStatement(conn, from, where);
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            assertEquals(0, rs.getInt(1));

            boolean[] nullsMask = {true, false, false};
            Object[] rowData = {null, "A", 1};
            List<ParameterSetter> program = new ArrayList<>();
            pstmt = dba.getInsertRecordStatement(conn, t2, nullsMask, program);
            int i = 1;
            for (ParameterSetter ps : program) {
                ps.execute(pstmt, i++, rowData, 0);
            }
            pstmt.execute();

            pstmt = dba.getSetCountStatement(conn, from, where);
            rs = pstmt.executeQuery();
            rs.next();
            assertEquals(1, rs.getInt(1));

            boolean[] nullsMask2 = {true, false, false};
            Object[] rowData2 = {null, "A", 2};
            program = new ArrayList<>();
            pstmt = dba.getInsertRecordStatement(conn, t2, nullsMask2, program);
            i = 1;
            for (ParameterSetter ps : program) {
                ps.execute(pstmt, i++, rowData2, 0);
            }
            pstmt.execute();

            pstmt = dba.getSetCountStatement(conn, from, where);
            rs = pstmt.executeQuery();
            rs.next();
            assertEquals(2, rs.getInt(1));

            otherWhere = ("(\"atInt\" = 2)");
            where = dba.getInFilterClause(t, t2, tFields, t2Fields, otherWhere);

            pstmt = dba.getSetCountStatement(conn, from, where);
            rs = pstmt.executeQuery();
            rs.next();
            assertEquals(1, rs.getInt(1));
        } catch (Exception e) {
            throw e;
        } finally {
            stmt.close();

            if (pstmt != null) {
                pstmt.close();
            }

            if (tableIsCreated)
                dba.dropTable(conn, t2);
        }
    }


    @Test
    void testCreateAndAlterSequence() throws Exception {
        Grain g = score.getGrain(GRAIN_NAME);
        SequenceElement sequence = g.getElement("testSequence", SequenceElement.class);

        if (dba.sequenceExists(conn, g.getName(), sequence.getName()))
            dba.dropSequence(conn, sequence);

        //Sequence not exists
        assertFalse(dba.sequenceExists(conn, g.getName(), sequence.getName()));
        dba.createSequence(conn, sequence);
        assertTrue(dba.sequenceExists(conn, g.getName(), sequence.getName()));

        DbSequenceInfo sequenceInfo = dba.getSequenceInfo(conn, sequence);
        assertFalse(sequenceInfo.reflects(sequence));

        assertAll(
                () -> assertEquals(1L, sequenceInfo.getIncrementBy()),
                () -> assertEquals(5L, sequenceInfo.getMinValue()),
                () -> assertEquals(Long.MAX_VALUE, sequenceInfo.getMaxValue()),
                () -> assertEquals(false, sequenceInfo.isCycle())
        );

        assertEquals(5, dba.nextSequenceValue(conn, sequence));
        assertEquals(6, dba.nextSequenceValue(conn, sequence));

        //Modifying of increment by
        sequence.getArguments().put(SequenceElement.Argument.INCREMENT_BY, 2L);
        assertTrue(sequenceInfo.reflects(sequence));

        dba.alterSequence(conn, sequence);

        DbSequenceInfo sequenceInfo2 = dba.getSequenceInfo(conn, sequence);
        assertFalse(sequenceInfo2.reflects(sequence));

        assertAll(
                () -> assertEquals(2L, sequenceInfo2.getIncrementBy()),
                () -> assertEquals(5L, sequenceInfo2.getMinValue()),
                () -> assertEquals(Long.MAX_VALUE, sequenceInfo2.getMaxValue()),
                () -> assertEquals(false, sequenceInfo2.isCycle())
        );

        //Altering to short cycle
        sequence.getArguments().put(SequenceElement.Argument.INCREMENT_BY, 1L);
        sequence.getArguments().put(SequenceElement.Argument.MINVALUE, 5L);
        sequence.getArguments().put(SequenceElement.Argument.MAXVALUE, 7L);
        sequence.getArguments().put(SequenceElement.Argument.CYCLE, true);
        assertTrue(sequenceInfo.reflects(sequence));

        dba.alterSequence(conn, sequence);
        DbSequenceInfo sequenceInfo3 = dba.getSequenceInfo(conn, sequence);
        assertFalse(sequenceInfo3.reflects(sequence));

        assertAll(
                () -> assertEquals(1L, sequenceInfo3.getIncrementBy()),
                () -> assertEquals(5L, sequenceInfo3.getMinValue()),
                () -> assertEquals(7L, sequenceInfo3.getMaxValue()),
                () -> assertEquals(true, sequenceInfo3.isCycle())
        );

        dba.dropSequence(conn, sequence);
    }

    @Test
    void testColumnUpdateWithDefaultSequence() throws Exception {
        Grain g = score.getGrain(GRAIN_NAME);
        SequenceElement sequence = g.getElement("testSequence", SequenceElement.class);
        SequenceElement sequence2 = g.getElement("testSequence2", SequenceElement.class);
        Table table = g.getElement("tableForTestSequence", Table.class);

        if (dba.sequenceExists(conn, g.getName(), sequence.getName()))
            dba.dropSequence(conn, sequence);
        if (dba.sequenceExists(conn, g.getName(), sequence2.getName()))
            dba.dropSequence(conn, sequence2);
        if (dba.tableExists(conn, g.getName(), table.getName()))
            dba.dropTable(conn, table);

        dba.createSequence(conn, sequence);
        dba.createSequence(conn, sequence2);
        dba.createTable(conn, table);

        IntegerColumn id = (IntegerColumn) table.getColumn("id");

        final DbColumnInfo idInfo1 = dba.getColumnInfo(conn, table.getColumn("id"));
        assertAll(
                () -> assertTrue(idInfo1.reflects(id)),
                () -> assertEquals(IntegerColumn.class, idInfo1.getType()),
                () -> assertEquals("NEXTVAL(testSequence)".toLowerCase(), idInfo1.getDefaultValue().toLowerCase())
        );

        id.setNullableAndDefault(false, "NEXTVAL(" + sequence2.getName() + ")");
        assertFalse(idInfo1.reflects(id));
        dba.updateColumn(conn, id, idInfo1);

        final DbColumnInfo idInfo2 = dba.getColumnInfo(conn, table.getColumn("id"));
        assertAll(
                () -> assertTrue(idInfo2.reflects(id)),
                () -> assertEquals(IntegerColumn.class, idInfo2.getType()),
                () -> assertEquals("NEXTVAL(testSequence2)".toLowerCase(), idInfo2.getDefaultValue().toLowerCase())
        );

        id.setNullableAndDefault(false, "5");
        assertFalse(idInfo2.reflects(id));
        dba.updateColumn(conn, id, idInfo2);

        final DbColumnInfo idInfo3 = dba.getColumnInfo(conn, table.getColumn("id"));
        assertAll(
                () -> assertTrue(idInfo3.reflects(id)),
                () -> assertEquals(IntegerColumn.class, idInfo3.getType()),
                () -> assertEquals("5".toLowerCase(), idInfo3.getDefaultValue().toLowerCase())
        );

        id.setNullableAndDefault(false, "NEXTVAL(" + sequence.getName() + ")");
        assertFalse(idInfo3.reflects(id));
        dba.updateColumn(conn, id, idInfo3);

        final DbColumnInfo idInfo4 = dba.getColumnInfo(conn, table.getColumn("id"));
        assertAll(
                () -> assertTrue(idInfo4.reflects(id)),
                () -> assertEquals(IntegerColumn.class, idInfo4.getType()),
                () -> assertEquals("NEXTVAL(testSequence)".toLowerCase(), idInfo4.getDefaultValue().toLowerCase())
        );


        IntegerColumn numb = (IntegerColumn) table.getColumn("numb");
        final DbColumnInfo numbInfo1 = dba.getColumnInfo(conn, numb);
        assertAll(
                () -> assertTrue(numbInfo1.reflects(numb)),
                () -> assertEquals(IntegerColumn.class, numbInfo1.getType()),
                () -> assertTrue(numbInfo1.getDefaultValue().isEmpty())
        );

        numb.setNullableAndDefault(false, "NEXTVAL(" + sequence2.getName() + ")");
        assertFalse(numbInfo1.reflects(numb));
        dba.updateColumn(conn, numb, numbInfo1);
        final DbColumnInfo numbInfo2 = dba.getColumnInfo(conn, numb);
        assertAll(
                () -> assertTrue(numbInfo2.reflects(numb)),
                () -> assertEquals(IntegerColumn.class, numbInfo2.getType()),
                () -> assertEquals("NEXTVAL(testSequence2)".toLowerCase(), numbInfo2.getDefaultValue().toLowerCase())
        );

        dba.dropTable(conn, table);
        dba.dropSequence(conn, sequence);
        dba.dropSequence(conn, sequence2);
    }

    @Test
    void testSelectStaticStrings() throws Exception {
        List<String> data = Arrays.asList("A", "B");

        List<String> result = dba.selectStaticStrings(data, "id", "");
        assertEquals(data, result);

        result = dba.selectStaticStrings(data, "id", "id desc");
        Collections.reverse(data);
        assertEquals(data, result);
    }

    @Test
    public void testCompareStrings() throws Exception {
        //less
        int comparisonResult = dba.compareStrings("a", "ab");
        assertEquals(-1, comparisonResult);

        //equals
        comparisonResult = dba.compareStrings("a", "a");
        assertEquals(0, comparisonResult);

        //greater
        comparisonResult = dba.compareStrings("ab", "a");
        assertEquals(1, comparisonResult);
    }

    @Test
    void testVarcharFieldEnumeratorCollation() throws Exception {
        List<String> data = dba.selectStaticStrings(VarcharFieldEnumerator.CHARS, "\"id\"", "\"id\" ASC");
        VarcharFieldEnumerator varcharFieldEnumerator = new VarcharFieldEnumerator(dba, 1);

        BigInteger prevOrder = BigInteger.valueOf(-1);

        for (int i = 0; i < data.size(); ++i) {
            String ch = data.get(i);
            varcharFieldEnumerator.setValue(ch);

            BigInteger currentOrder = varcharFieldEnumerator.getOrderValue();
            assertTrue(prevOrder.longValue() < currentOrder.longValue());
            prevOrder = currentOrder;
        }
    }


    @Test
    void testAddNotNullColumnWithDefaultValue() throws Exception {
        Score score = new Score.ScoreBuilder<>(Score.class)
                .path(ADD_NOT_NULL_COLUMN_WITH_DEFAULT_VALUE_1_SCORE_PATH)
                .build();
        DbUpdater dbUpdater = createDbUpdater(score, this.dba);
        dbUpdater.updateDb();

        Table t = score.getGrain("test").getTable("t");

        assertThrows(ParseException.class, () -> t.getColumn("description"));

        boolean[] nullsMask = {false};

        List<ParameterSetter> program = new ArrayList<>();
        PreparedStatement pstmt = dba.getInsertRecordStatement(this.conn, t, nullsMask, program);
        Object[] rowData = {"A"};
        program.get(0).execute(pstmt, 1, rowData, 0);
        pstmt.execute();
        assertEquals(1, this.getCount(this.conn, t));
        this.conn.commit();

        score = new Score.ScoreBuilder<>(Score.class)
                .path(ADD_NOT_NULL_COLUMN_WITH_DEFAULT_VALUE_2_SCORE_PATH)
                .build();
        Table newT = score.getGrain("test").getTable("t");
        dbUpdater = createDbUpdater(score, this.dba);
        dbUpdater.updateDb();

        WhereTerm w = WhereTermsMaker.getPKWhereTerm(newT);
        pstmt = this.dba.getOneRecordStatement(this.conn, newT, w.getWhere(), Collections.emptySet());

        program = new ArrayList<>();
        w.programParams(program, this.dba);
        program.get(0).execute(pstmt, 1, new String[]{"A"}, 0);

        try (ResultSet rs = pstmt.executeQuery()) {

            assertAll(
                    () -> assertTrue(rs.next()),
                    () -> assertEquals("DESC", rs.getString("description")),
                    () -> assertFalse(rs.next())
            );
        }

    }

    @Test
    @DisplayName("Adding of additional materialized view on same table works")
    void testAddSecondMaterializedViewOnSameTable() throws Exception {

        CurrentScore.set(this.score);
        Grain g = this.score.getGrain(GRAIN_NAME);
        SequenceElement seq = g.getElement("tForAddSecondMvNum", SequenceElement.class);
        Table tForAddSecondMv = g.getElement("tForAddSecondMv", Table.class);
        MaterializedView mv1 = g.getElement("tForAddSecondMv1", MaterializedView.class);

        MaterializedView mv2 = new MaterializedView(mv1.getGrainPart(), "tForAddSecondMv2");

        AbstractViewUtils.addColumn(mv2, "s", sum(fieldRef(tForAddSecondMv.getName(), "num2")));
        AbstractViewUtils.addColumn(mv2,"num1", fieldRef(tForAddSecondMv.getName(), "num1"));

        TableRef ref = new TableRef(tForAddSecondMv, tForAddSecondMv.getName());
        AbstractViewUtils.addFromTableRef(mv2, ref);
        AbstractViewUtils.addGroupByColumn(mv2, fieldRef(tForAddSecondMv.getName(), "num1"));
        AbstractViewUtils.finalizeParsing(mv2);

        try {
            this.dba.createSequence(this.conn, seq);
            this.dba.createTable(this.conn, tForAddSecondMv);

            this.dba.createTable(this.conn, mv1);
            this.dba.initDataForMaterializedView(this.conn, mv1);
            this.dba.dropTableTriggerForMaterializedView(this.conn, mv1);
            this.dba.createTableTriggerForMaterializedView(this.conn, mv1);

            this.dba.createTable(this.conn, mv2);
            this.dba.initDataForMaterializedView(this.conn, mv2);
            this.dba.dropTableTriggerForMaterializedView(this.conn, mv2);
            this.dba.createTableTriggerForMaterializedView(this.conn, mv2);

            class Pair {
                int num1;
                int num2;

                Pair (int num1, int num2) {
                    this.num1 = num1;
                    this.num2 = num2;
                }
            }

            List<Pair> pairs = new ArrayList<>();
            pairs.add(new Pair(1, 2));
            pairs.add(new Pair(2, 2));
            pairs.add(new Pair(1, 4));
            pairs.add(new Pair(2, 5));

            for (Pair pair : pairs) {
                boolean[] nullsMask = {true, false, false, true};
                Object[] rowData = {null, pair.num1, pair.num2};
                List<ParameterSetter> program = new ArrayList<>();
                PreparedStatement pstmt = this.dba.getInsertRecordStatement(
                        this.conn, tForAddSecondMv, nullsMask, program
                );

                int i = 1;
                for (ParameterSetter ps : program) {
                    ps.execute(pstmt, i++, rowData, 0);
                }
                pstmt.execute();
            }

            FromClause from1 = new FromClause();
            from1.setGe(mv1);
            from1.setExpression(this.dba.tableString(g.getName(), mv1.getName()));


            ResultSet rs11 = this.dba.getRecordSetStatement(
                    this.conn, from1, "", "\"s\"", 0, 0, new HashSet<>()
            ).executeQuery();

            // check insert triggers
            assertAll(
                    () -> assertTrue(rs11.next()),
                    () -> assertEquals(1, rs11.getInt("s")),
                    () -> assertEquals(4, rs11.getInt("num2")),
                    () -> assertTrue(rs11.next()),
                    () -> assertEquals(2, rs11.getInt("s")),
                    () -> assertEquals(5, rs11.getInt("num2")),
                    () -> assertTrue(rs11.next()),
                    () -> assertEquals(3, rs11.getInt("s")),
                    () -> assertEquals(2, rs11.getInt("num2")),
                    () -> assertFalse(rs11.next())
            );

            FromClause from2 = new FromClause();
            from2.setGe(mv2);
            from2.setExpression(this.dba.tableString(g.getName(), mv2.getName()));


            ResultSet rs21 = this.dba.getRecordSetStatement(
                    this.conn, from2, "", "\"s\"", 0, 0, new HashSet<>()
            ).executeQuery();

            assertAll(
                    () -> assertTrue(rs21.next()),
                    () -> assertEquals(6, rs21.getInt("s")),
                    () -> assertEquals(1, rs21.getInt("num1")),
                    () -> assertTrue(rs21.next()),
                    () -> assertEquals(7, rs21.getInt("s")),
                    () -> assertEquals(2, rs21.getInt("num1")),
                    () -> assertFalse(rs21.next())
            );

            // check delete triggers
            this.dba.deleteRecordSetStatement(conn, tForAddSecondMv, "\"num1\" = 1").execute();

            ResultSet rs12 = this.dba.getRecordSetStatement(
                    this.conn, from1, "", "\"num2\"", 0, 0, new HashSet<>()
            ).executeQuery();

            assertAll(
                    () -> assertTrue(rs12.next()),
                    () -> assertEquals(2, rs12.getInt("s")),
                    () -> assertEquals(2, rs12.getInt("num2")),
                    () -> assertTrue(rs12.next()),
                    () -> assertEquals(2, rs12.getInt("s")),
                    () -> assertEquals(5, rs12.getInt("num2")),
                    () -> assertFalse(rs12.next())
            );

            ResultSet rs22 = this.dba.getRecordSetStatement(
                    this.conn, from2, "", "\"num1\"", 0, 0, new HashSet<>()
            ).executeQuery();

            assertAll(
                    () -> assertTrue(rs22.next()),
                    () -> assertEquals(7, rs22.getInt("s")),
                    () -> assertEquals(2, rs22.getInt("num1")),
                    () -> assertFalse(rs22.next())
            );

            // check update triggers
            boolean[] equalsMask = {true, true, false};
            boolean[] nullsMask = {true, true, false};
            Object[] rowData = {null, null, 6};
            List<ParameterSetter> program = new ArrayList<>();
            int i = 1;

            PreparedStatement pstmt = this.dba.getUpdateRecordStatement(
                    this.conn, tForAddSecondMv, equalsMask, nullsMask, program, "\"num2\" = 5");
            for (ParameterSetter ps : program) {
                ps.execute(pstmt, i++, rowData, 1);
            }
            pstmt.execute();

            // RecVersion trigger works
            FromClause fromT = new FromClause();
            fromT.setGe(tForAddSecondMv);
            fromT.setExpression(this.dba.tableString(g.getName(), tForAddSecondMv.getName()));

            ResultSet rsT = this.dba.getOneRecordStatement(
                    this.conn, tForAddSecondMv, "\"num2\" = 6", Collections.emptySet()
            ).executeQuery();

            assertAll(
                    () -> assertTrue(rsT.next()),
                    () -> assertEquals(2, rsT.getInt("recversion"))
            );

            ResultSet rs13 = this.dba.getRecordSetStatement(
                    this.conn, from1, "", "\"num2\"", 0, 0, new HashSet<>()
            ).executeQuery();

            assertAll(
                    () -> assertTrue(rs13.next()),
                    () -> assertEquals(2, rs13.getInt("s")),
                    () -> assertEquals(2, rs13.getInt("num2")),
                    () -> assertTrue(rs13.next()),
                    () -> assertEquals(2, rs13.getInt("s")),
                    () -> assertEquals(6, rs13.getInt("num2")),
                    () -> assertFalse(rs13.next())
            );

            ResultSet rs23 = this.dba.getRecordSetStatement(
                    this.conn, from2, "", "\"num1\"", 0, 0, new HashSet<>()
            ).executeQuery();

            assertAll(
                    () -> assertTrue(rs23.next()),
                    () -> assertEquals(8, rs23.getInt("s")),
                    () -> assertEquals(2, rs23.getInt("num1")),
                    () -> assertFalse(rs23.next())
            );
        } finally {
            if (dba.tableExists(conn, g.getName(), mv1.getName())) {
                dba.dropTable(conn, mv1);
            }
            if (dba.tableExists(conn, g.getName(), mv2.getName())) {
                dba.dropTable(conn, mv2);
            }
            if (dba.tableExists(conn, g.getName(), tForAddSecondMv.getName())) {
                dba.dropTable(conn, tForAddSecondMv);
            }
            if (this.dba.sequenceExists(conn, g.getName(), seq.getName())) {
                this.dba.dropSequence(conn, seq);
            }

        }
    }

    static DbUpdaterImpl createDbUpdater(Score score, DBAdaptor dba) {
        CelestaImpl celesta = new CelestaImpl(dba, dba.connectionPool, score);
        PermissionManager permissionManager = celesta.getPermissionManager();
        LoggingManager loggingManager = celesta.getLoggingManager();

        return new DbUpdaterBuilder()
                .dbAdaptor(dba)
                .connectionPool(dba.connectionPool)
                .score(score)
                .setCelesta(celesta)
                .setPermissionManager(permissionManager)
                .setLoggingManager(loggingManager)
                .build();
    }
}
