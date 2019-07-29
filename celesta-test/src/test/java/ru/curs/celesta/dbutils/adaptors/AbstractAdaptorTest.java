package ru.curs.celesta.dbutils.adaptors;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.Date;


import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.*;
import ru.curs.celesta.dbutils.meta.*;
import ru.curs.celesta.dbutils.query.FromClause;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.dbutils.term.WhereTerm;
import ru.curs.celesta.dbutils.term.WhereTermsMaker;
import ru.curs.celesta.test.mock.CelestaImpl;
import ru.curs.celesta.score.*;
import ru.curs.celesta.score.discovery.ScoreByScorePathDiscovery;
import ru.curs.celesta.syscursors.GrainsCursor;

public abstract class AbstractAdaptorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAdaptorTest.class);

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
    Table t;

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
        } catch (SQLException ex) {
            LOGGER.error("Error on counting rows", ex);
            throw ex;
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

        SequenceElement ts = g.getElement("test_id", SequenceElement.class);
        try {
            dba.dropSequence(conn, ts);
        } catch (CelestaException e) {
            conn.rollback();
        }
        
        dba.createSequence(conn, ts);
        dba.createTable(conn, t);

        conn.commit();
    }

    @AfterEach
    public void tearDown() throws Exception {
        conn.rollback();
        try {
            dba.dropTable(conn, t);
        } catch (Exception ex) {
            // LOGGER.error("Error on table droping table {}", t.getName(), ex);
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
        // LOGGER.info(Arrays.toString(columnSet.toArray(new String[0])));
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

        // f1 bit not null,
        c = dba.getColumnInfo(conn, t.getColumn("f1"));
        assertEquals("f1", c.getName());
        assertSame(BooleanColumn.class, c.getType());
        assertEquals(false, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());

        // f4 real,
        c = dba.getColumnInfo(conn, t.getColumn("f4"));
        assertEquals("f4", c.getName());
        assertSame(FloatingColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());

        // f7 varchar(8),
        c = dba.getColumnInfo(conn, t.getColumn("f7"));
        assertEquals("f7", c.getName());
        assertSame(StringColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(8, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());

        // f11 image not null
        c = dba.getColumnInfo(conn, t.getColumn("f11"));
        assertEquals("f11", c.getName());
        assertSame(BinaryColumn.class, c.getType());
        assertEquals(false, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());

        // f12 decimal(11, 7),
        c = dba.getColumnInfo(conn, t.getColumn("f12"));
        assertEquals("f12", c.getName());
        assertSame(DecimalColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(11, c.getLength());
        assertEquals(7, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());

        //f14 datetime with time zone
        c = dba.getColumnInfo(conn, t.getColumn("f14"));
        assertEquals("f14", c.getName());
        assertSame(ZonedDateTimeColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());
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
        assertFalse(c.getDefaultValue().isEmpty());
        assertEquals(false, c.isMax());

        // attrInt int default 3,
        c = dba.getColumnInfo(conn, t.getColumn("attrInt"));
        assertEquals("attrInt", c.getName());
        assertSame(IntegerColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("3", c.getDefaultValue());
        assertEquals(false, c.isMax());

        // f2 bit default 'true',
        c = dba.getColumnInfo(conn, t.getColumn("f2"));
        assertEquals("f2", c.getName());
        assertSame(BooleanColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("'TRUE'", c.getDefaultValue());
        assertEquals(false, c.isMax());

        // f5 real not null default 5.5,
        c = dba.getColumnInfo(conn, t.getColumn("f5"));
        assertEquals("f5", c.getName());
        assertSame(FloatingColumn.class, c.getType());
        assertEquals(false, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("5.5", c.getDefaultValue());
        assertEquals(false, c.isMax());

        // f6 varchar(MAX) not null default 'abc',
        c = dba.getColumnInfo(conn, t.getColumn("f6"));
        assertEquals("f6", c.getName());
        assertSame(StringColumn.class, c.getType());
        assertEquals(false, c.isNullable());
        // assertEquals(0, c.getLength()); -- database-dependent value
        assertEquals(0, c.getScale());
        assertEquals("'abc'", c.getDefaultValue());
        assertEquals(true, c.isMax());

        // f8 datetime default '20130401',
        c = dba.getColumnInfo(conn, t.getColumn("f8"));
        assertEquals("f8", c.getName());
        assertSame(DateTimeColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("'20130401'", c.getDefaultValue());
        assertEquals(false, c.isMax());

        // f9 datetime not null default getdate(),
        c = dba.getColumnInfo(conn, t.getColumn("f9"));
        assertEquals("f9", c.getName());
        assertSame(DateTimeColumn.class, c.getType());
        assertEquals(false, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("GETDATE()", c.getDefaultValue());
        assertEquals(false, c.isMax());

        // f10 image default 0xFFAAFFAAFF,
        c = dba.getColumnInfo(conn, t.getColumn("f10"));
        assertEquals("f10", c.getName());
        assertSame(BinaryColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("0xFFAAFFAAFF", c.getDefaultValue());
        assertEquals(false, c.isMax());

        // f13 decimal(5, 3) not null default 46.123
        c = dba.getColumnInfo(conn, t.getColumn("f13"));
        assertEquals("f13", c.getName());
        assertSame(DecimalColumn.class, c.getType());
        assertEquals(false, c.isNullable());
        assertEquals(5, c.getLength());
        assertEquals(3, c.getScale());
        assertEquals("46.123", c.getDefaultValue());
        assertEquals(false, c.isMax());
    }


    @Test
    public void testColumnInfoForMaterializedView() throws ParseException {

        Grain g = score.getGrain(GRAIN_NAME);
        Table tableForMatView = g.getElement("tableForMatView", Table.class);
        SequenceElement seqTableForMatView = g.getElement("tableForMatView_id", SequenceElement.class);
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
                dba.dropSequence(conn, seqTableForMatView);
            } catch (CelestaException e) {
                conn.rollback();
            }
            
            try {
                dba.dropTable(conn, mView1gTest);
            } catch (CelestaException e) {
                conn.rollback();
            }

            dba.createSequence(conn, seqTableForMatView);
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
            assertEquals(0, c.getLength());
            assertEquals(0, c.getScale());

            col = mView1gTest.getColumn("f1");
            c = dba.getColumnInfo(conn, col);
            assertEquals("f1", c.getName());
            assertSame(StringColumn.class, c.getType());
            assertEquals(false, c.isNullable());
            assertEquals("", c.getDefaultValue());
            assertEquals(2, c.getLength());
            assertEquals(0, c.getScale());

            col = mView1gTest.getColumn("f2");
            c = dba.getColumnInfo(conn, col);
            assertEquals("f2", c.getName());
            assertSame(IntegerColumn.class, c.getType());
            assertEquals(false, c.isNullable());
            assertEquals("", c.getDefaultValue());
            assertEquals(0, c.getLength());
            assertEquals(0, c.getScale());

            col = mView1gTest.getColumn("f3");
            c = dba.getColumnInfo(conn, col);
            assertEquals("f3", c.getName());
            assertSame(BooleanColumn.class, c.getType());
            assertEquals(false, c.isNullable());
            assertEquals("", c.getDefaultValue());
            assertEquals(0, c.getLength());
            assertEquals(0, c.getScale());

            col = mView1gTest.getColumn("f4");
            c = dba.getColumnInfo(conn, col);
            assertEquals("f4", c.getName());
            assertSame(FloatingColumn.class, c.getType());
            assertEquals(false, c.isNullable());
            assertEquals("", c.getDefaultValue());
            assertEquals(0, c.getLength());
            assertEquals(0, c.getScale());

            col = mView1gTest.getColumn("f5");
            c = dba.getColumnInfo(conn, col);
            assertEquals("f5", c.getName());
            assertSame(DateTimeColumn.class, c.getType());
            assertEquals(false, c.isNullable());
            assertEquals("", c.getDefaultValue());
            assertEquals(0, c.getLength());
            assertEquals(0, c.getScale());

            col = mView1gTest.getColumn("f6");
            c = dba.getColumnInfo(conn, col);
            assertEquals("f6", c.getName());
            assertSame(DecimalColumn.class, c.getType());
            assertEquals(false, c.isNullable());
            assertEquals("", c.getDefaultValue());
            assertEquals(2, c.getLength());
            assertEquals(1, c.getScale());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (tablesAreCreated) {
                dba.dropTable(conn, tableForMatView);
                dba.dropSequence(conn, seqTableForMatView);
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
        col.setNullableAndDefault(false, "55");
        dba.updateColumn(conn, col, c);
        c = dba.getColumnInfo(conn, col);
        assertEquals("attrInt", c.getName());
        assertSame(IntegerColumn.class, c.getType());
        assertEquals(false, c.isNullable());
        assertEquals("55", c.getDefaultValue());
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
        c = dba.getColumnInfo(conn, col);
        col.setNullableAndDefault(false, null);
        dba.updateColumn(conn, col, c);
        c = dba.getColumnInfo(conn, col);
        assertFalse(c.isNullable());

        col = (IntegerColumn) t.getColumn("attrInt");
        col.setNullableAndDefault(true, "1");
        c = dba.getColumnInfo(conn, col);
        dba.updateColumn(conn, col, c);
        c = dba.getColumnInfo(conn, col);
        assertTrue(c.isNullable());
        assertEquals("1", c.getDefaultValue());

        col.setNullableAndDefault(true, null);
        dba.updateColumn(conn, col, c);
        c = dba.getColumnInfo(conn, col);
        assertTrue(c.isNullable());

        col = (IntegerColumn) t.getColumn("id");
        c = dba.getColumnInfo(conn, col);
        assertFalse(c.isNullable());

        col.setNullableAndDefault(false, "1");
        dba.updateColumn(conn, col, c);
        c = dba.getColumnInfo(conn, col);
        assertFalse(c.isNullable());
        assertEquals("1", c.getDefaultValue());
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
        c = dba.getColumnInfo(conn, col);
        assertFalse(c.isNullable());
        assertEquals("0xABABAB", c.getDefaultValue());

        col.setNullableAndDefault(false, null);
        dba.updateColumn(conn, col, c);
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
    public void pkConstraintString() {
        final String pkName = dba.pkConstraintString(t);
        assertEquals("pk_test", pkName);
    }

    @Test
    public void getPKInfo() throws ParseException, IOException, SQLException {
        DbPkInfo c;
        insertRow(conn, t, 15);

        final String pkName = dba.pkConstraintString(t);

        c = dba.getPKInfo(conn, t);
        assertNotNull(c);
        assertEquals(pkName, c.getName());
        assertEquals(1, c.getColumnNames().size());
        String[] expected = {"id"};
        assertTrue(Arrays.equals(expected, c.getColumnNames().toArray(new String[0])));
        assertTrue(c.reflects(t));
        assertFalse(c.isEmpty());

        dba.dropPk(conn, t, pkName);
        c = dba.getPKInfo(conn, t);
        assertNull(c.getName());
        assertEquals(0, c.getColumnNames().size());
        assertFalse(c.reflects(t));
        assertTrue(c.isEmpty());

        t.setPK("id", "f1", "f9");
        dba.createPK(conn, t);
        c = dba.getPKInfo(conn, t);
        assertNotNull(c);
        assertEquals(pkName, c.getName());
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

            assertTrue(Arrays.equals(expected, actual));
            assertEquals(FKRule.CASCADE, info.getUpdateRule());
            assertEquals(FKRule.SET_NULL, info.getDeleteRule());

            assertEquals("gtest", info.getRefGrainName());
            assertEquals("refTo", info.getRefTableName());

            assertTrue(info.reflects(fk));

            // Check that there was no entanglement of primary and foreign keys.
            // (a bug that existed at some point)
            assertTrue(dba.getPKInfo(conn, t).reflects(t));
            assertTrue(dba.getPKInfo(conn, t2).reflects(t2));

            dba.dropFK(conn, t.getGrain().getName(), t.getName(), fk.getConstraintName());
            l = dba.getFKInfo(conn, g);
            assertEquals(0, l.size());
        } catch (CelestaException ex) {
            LOGGER.error("Celesta error", ex);
            throw ex;
        } finally {
            conn.rollback();
            dba.dropTable(conn, t);
            dba.dropTable(conn, t2);
        }
    }

    @Test
    public void additionalCreateTableTest() throws ParseException {
        Grain g = score.getGrain(GRAIN_NAME);
        SequenceElement t3s = g.getElement("aLongIdentityTableNxx_f1", SequenceElement.class);
        Table t3 = g.getElement("aLongIdentityTableNaaame", Table.class);
        try {
            dba.createSequence(conn, t3s);
            dba.createTable(conn, t3);
            DbColumnInfo c = dba.getColumnInfo(conn, t3.getColumn("f1"));
            assertEquals("NEXTVAL(aLongIdentityTableNxx_f1)", c.getDefaultValue());
            c = dba.getColumnInfo(conn, t3.getColumn("field2"));
            assertSame(BooleanColumn.class, c.getType());
        } catch (Exception ex) {
            // TODO: When can it happen?
            LOGGER.error("Error", ex);
        } finally {
            dba.dropTable(conn, t3);
            dba.dropSequence(conn, t3s);
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
        
        SequenceElement ts = g.getElement("tableForInitMvData_id", SequenceElement.class);
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
                dba.dropSequence(conn, ts);
            } catch (CelestaException e) {
                conn.rollback();
            }
            try {
                dba.dropTable(conn, mv);
            } catch (CelestaException e) {
                conn.rollback();
            }

            dba.createSequence(conn, ts);
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
            dba.dropSequence(conn, ts);
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
        SequenceElement t2s = g.getElement("testInFilterClause_id", SequenceElement.class); 
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
            try {
                dba.dropSequence(conn, t2s);
            } catch (CelestaException e) {
                conn.rollback();
            }
            
            dba.createSequence(conn, t2s);
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

            if (tableIsCreated) {
                dba.dropTable(conn, t2);
                dba.dropSequence(conn, t2s);
            }
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
        List<String> test = Arrays.asList(
                "'", "-", "–", "—", " ", "!", "\"", "#", "$", "%", "&", "(", ")",
                "*", ",", ".", "/", ":", ";",
                "?", "@", "[", "\\", "]", "^", "_", "`", "{", "|", "}",
                "~", "¦", "‘", "’", "‚", "“", "”", "„", "‹", "›", "+",
                "<", "=", ">", "«", "»", "§", "©", "¬", "®", "°", "†", "‡", "•", "‰",
                "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
                "a", "A", "b", "B", "c", "C", "d", "D", "e", "E", "f", "F", "g", "G",
                "h", "H", "i", "I", "j", "J", "k", "K", "l", "L", "m", "M", "n", "N", "№", "o", "O", "p", "P",
                "q", "Q", "r", "R", "s", "S", "t", "T", "™", "u", "U", "v", "V", "w", "W",
                "x", "X", "y", "Y", "z", "Z",
                "а", "А", "б", "Б", "в", "В", "г", "Г", "д", "Д", "е", "Е", "ё", "Ё",
                "ж", "Ж", "з", "З", "и", "И", "й", "Й", "к", "К", "л", "Л", "м", "М", "н", "Н",
                "о", "О", "п", "П", "р", "Р", "с", "С", "т", "Т",
                "у", "У", "ф", "Ф", "х", "Х", "ц", "Ц", "ч", "Ч", "ш", "Ш", "щ", "Щ", "ъ", "Ъ", "ы", "Ы", "ь", "Ь",
                "э", "Э", "ю", "Ю", "я", "Я"
        );
        List<String> data = dba.selectStaticStrings(test, "\"id\"", "\"id\" ASC");
        Collections.sort(test);
        Collections.sort(data);
        assertLinesMatch(test, data);
    }

    @Test
    void testAddNotNullColumnWithDefaultValue() throws Exception {
        Score score = new Score.ScoreBuilder<>(Score.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(ADD_NOT_NULL_COLUMN_WITH_DEFAULT_VALUE_1_SCORE_PATH))
                .build();
        DbUpdater<?> dbUpdater = createDbUpdater(score, this.dba);
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
                .scoreDiscovery(new ScoreByScorePathDiscovery(ADD_NOT_NULL_COLUMN_WITH_DEFAULT_VALUE_2_SCORE_PATH))
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


    static DbUpdaterImpl createDbUpdater(Score score, DBAdaptor dba) {
        CelestaImpl celesta = new CelestaImpl(dba, dba.connectionPool, score);
        return new DbUpdaterBuilder()
                .dbAdaptor(dba)
                .connectionPool(dba.connectionPool)
                .score(score)
                .setCelesta(celesta)
                .build();
    }

}
