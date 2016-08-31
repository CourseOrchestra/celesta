package ru.curs.celesta.dbutils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.FKRule;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.ForeignKey;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.Table;
import ru.curs.celesta.score.View;

public abstract class AbstractAdaptorTest {
	final static String GRAIN_NAME = "gtest";
	final static String SCORE_NAME = "testScore";
	private DBAdaptor dba;
	private Score score;

	private Connection conn;
	private Table t;

	private int insertRow(Connection conn, Table t, int val) throws IOException, CelestaException, SQLException {
		int count = t.getColumns().size();
		assertEquals(13, count);
		boolean[] nullsMask = { true, false, false, false, true, false, true, true, false, true, true, true, false };
		BLOB b = new BLOB();
		b.getOutStream().write(new byte[] { 1, 2, 3 });
		Object[] rowData = { null, "ab", val, false, null, 1.1, null, null, "eee", null, null, null, b };
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

	private int getCount(Connection conn, Table t) throws Exception {
		PreparedStatement stmt = dba.getSetCountStatement(conn, t, "");
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

	protected void setScore(Score score) {
		this.score = score;
	}

	@Before
	public void setup() throws Exception {
		conn = ConnectionPool.get();
		dba.createSchemaIfNotExists(conn, GRAIN_NAME);
		conn.commit();

		t = score.getGrain(GRAIN_NAME).getTable("test");
		try {
			// Могла остаться от незавершившегося теста
			dba.dropTable(conn, t);
		} catch (CelestaException e) {
			conn.rollback();
		}
		dba.createTable(conn, t);
	}

	@After
	public void tearDown() throws Exception {
		conn.rollback();
		try {
			dba.dropTable(conn, t);
		} catch (Exception e) {
			// e.printStackTrace();
		}
		ConnectionPool.putBack(conn);
	}

	@Test
	public void isValidConnection() throws Exception {
		assertTrue(dba.isValidConnection(conn, 0));
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

		assertEquals(13, t.getColumns().size());
		boolean[] mask = { true, true, false, true, true, true, true, true, true, true, true, true, true };
		Integer[] rec = { 1, null, 2, null, null, null, null, null, null, null, null, null, null };
		List<ParameterSetter> program = new ArrayList<>();
		WhereTerm w = WhereTermsMaker.getPKWhereTerm(t);
		PreparedStatement pstmt = dba.getUpdateRecordStatement(conn, t, mask, program, w.getWhere());
		w.programParams(program);
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
		dba.createSysObjects(conn);
		insertRow(conn, t, 1);
		WhereTerm w = WhereTermsMaker.getPKWhereTerm(t);
		PreparedStatement pstmt = dba.getOneRecordStatement(conn, t, w.getWhere());

		List<ParameterSetter> program = new ArrayList<>();
		w.programParams(program);
		int i = 1;
		for (ParameterSetter ps : program) {
			ps.execute(pstmt, i++, new Integer[] { 1 }, 1);
		}
		ResultSet rs = pstmt.executeQuery();
		rs.next();
		assertEquals(1, rs.getInt("recversion"));
		rs.close();

		boolean[] mask = { true, true, false, true, true, true, true, true, true, true, true, true, true, false };
		Integer[] rec = { 1, null, 22, null, null, null, null, null, null, null, null, null, null };
		program = new ArrayList<>();
		w = WhereTermsMaker.getPKWhereTerm(t);

		PreparedStatement pstmt2 = dba.getUpdateRecordStatement(conn, t, mask, program, w.getWhere());

		// System.out.println(pstmt2.toString());
		assertNotNull(pstmt2);

		w.programParams(program);
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

		rec = new Integer[] { 1, null, 23, null, null, null, null, null, null, null, null, null, null };
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
		boolean itWas = false;
		try {
			pstmt2.executeUpdate();
		} catch (SQLException e) {
			// System.out.println(e.getMessage());
			itWas = true;
			assertTrue(e.getMessage().contains("record version check failure"));
		}
		assertTrue(itWas);

	}

	@Test
	public void getDeleteRecordStatement() throws Exception {
		insertRow(conn, t, 1);
		insertRow(conn, t, 10);
		assertEquals(2, getCount(conn, t));
		WhereTerm w = WhereTermsMaker.getPKWhereTerm(t);
		List<ParameterSetter> program = new ArrayList<>();

		PreparedStatement pstmt = dba.getDeleteRecordStatement(conn, t, w.getWhere());
		w.programParams(program);
		int i = 1;
		for (ParameterSetter ps : program) {
			ps.execute(pstmt, i++, new Integer[] { 1 }, 1);
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
		assertEquals(14, columnSet.size());
		assertTrue(columnSet.contains("f4"));
		assertFalse(columnSet.contains("nonExistentColumn"));
		// String[] columnNames = { "id", "attrVarchar", "attrInt", "f1",
		// "f2", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11" };
		// System.out
		// .println(Arrays.toString(columnSet.toArray(new String[0])));
	}

	@Test
	public void getIndices() throws Exception {
		Grain grain = score.getGrain(GRAIN_NAME);
		Index i = grain.getIndices().get("idxTest");

		dba.createIndex(conn, i);
		Map<String, DBIndexInfo> indicesSet = dba.getIndices(conn, t.getGrain());
		assertNotNull(indicesSet);
		assertEquals(1, indicesSet.size());
		DBIndexInfo inf = indicesSet.get("idxTest");
		assertTrue(inf.reflects(i));

		dba.dropIndex(grain, new DBIndexInfo(t.getName(), i.getName()), false);

		indicesSet = dba.getIndices(conn, t.getGrain());
		assertNotNull(indicesSet);
		assertEquals(0, indicesSet.size());
		String[] cols = { "f5", "f1", "f9", "id" };
		i = new Index(t, "testName", cols);
		dba.createIndex(conn, i);
		indicesSet = dba.getIndices(conn, t.getGrain());
		inf = indicesSet.get("testName");
		assertTrue(inf.reflects(i));
		dba.dropIndex(grain, inf, false);
	}

	@Test
	public void getOneFieldStatement() throws Exception {
		insertRow(conn, t, 121215);
		Column c = t.getColumns().get("attrInt");

		WhereTerm w = WhereTermsMaker.getPKWhereTerm(t);
		List<ParameterSetter> program = new ArrayList<>();

		PreparedStatement pstmt = dba.getOneFieldStatement(conn, c, w.getWhere());

		w.programParams(program);
		int i = 1;
		for (ParameterSetter ps : program) {
			ps.execute(pstmt, i++, new Integer[] { 1 }, 1);
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

		PreparedStatement pstmt = dba.getOneRecordStatement(conn, t, w.getWhere());

		w.programParams(program);
		int i = 1;
		for (ParameterSetter ps : program) {
			ps.execute(pstmt, i++, new Integer[] { 1 }, 1);
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
	public void getColumnInfo1() throws CelestaException, ParseException {
		DBColumnInfo c;
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
		assertEquals("", c.getDefaultValue());
		assertEquals(false, c.isMax());
		assertEquals(false, c.isIdentity());

		// f1 bit not null,
		c = dba.getColumnInfo(conn, t.getColumn("f1"));
		assertEquals("f1", c.getName());
		assertSame(BooleanColumn.class, c.getType());
		assertEquals(false, c.isNullable());
		assertEquals(0, c.getLength());
		assertEquals("", c.getDefaultValue());
		assertEquals(false, c.isMax());
		assertEquals(false, c.isIdentity());

		// f4 real,
		c = dba.getColumnInfo(conn, t.getColumn("f4"));
		assertEquals("f4", c.getName());
		assertSame(FloatingColumn.class, c.getType());
		assertEquals(true, c.isNullable());
		assertEquals(0, c.getLength());
		assertEquals("", c.getDefaultValue());
		assertEquals(false, c.isMax());
		assertEquals(false, c.isIdentity());

		// f7 varchar(8),
		c = dba.getColumnInfo(conn, t.getColumn("f7"));
		assertEquals("f7", c.getName());
		assertSame(StringColumn.class, c.getType());
		assertEquals(true, c.isNullable());
		assertEquals(8, c.getLength());
		assertEquals("", c.getDefaultValue());
		assertEquals(false, c.isMax());
		assertEquals(false, c.isIdentity());

		// f11 image not null
		c = dba.getColumnInfo(conn, t.getColumn("f11"));
		assertEquals("f11", c.getName());
		assertSame(BinaryColumn.class, c.getType());
		assertEquals(false, c.isNullable());
		assertEquals(0, c.getLength());
		assertEquals("", c.getDefaultValue());
		assertEquals(false, c.isMax());
		assertEquals(false, c.isIdentity());

	}

	@Test
	public void getColumnInfo2() throws CelestaException, ParseException {
		DBColumnInfo c;
		// Этот тест проверяет выражения default и дополнительные атрибуты
		// id int identity not null primary key,
		c = dba.getColumnInfo(conn, t.getColumn("id"));
		assertEquals("id", c.getName());
		assertSame(IntegerColumn.class, c.getType());
		assertEquals(false, c.isNullable());
		assertEquals(0, c.getLength());
		assertEquals("", c.getDefaultValue());
		assertEquals(false, c.isMax());
		assertEquals(true, c.isIdentity());

		// attrInt int default 3,
		c = dba.getColumnInfo(conn, t.getColumn("attrInt"));
		assertEquals("attrInt", c.getName());
		assertSame(IntegerColumn.class, c.getType());
		assertEquals(true, c.isNullable());
		assertEquals(0, c.getLength());
		assertEquals("3", c.getDefaultValue());
		assertEquals(false, c.isMax());
		assertEquals(false, c.isIdentity());

		// f2 bit default 'true',
		c = dba.getColumnInfo(conn, t.getColumn("f2"));
		assertEquals("f2", c.getName());
		assertSame(BooleanColumn.class, c.getType());
		assertEquals(true, c.isNullable());
		assertEquals(0, c.getLength());
		assertEquals("'TRUE'", c.getDefaultValue());
		assertEquals(false, c.isMax());
		assertEquals(false, c.isIdentity());

		// f5 real not null default 5.5,
		c = dba.getColumnInfo(conn, t.getColumn("f5"));
		assertEquals("f5", c.getName());
		assertSame(FloatingColumn.class, c.getType());
		assertEquals(false, c.isNullable());
		assertEquals(0, c.getLength());
		assertEquals("5.5", c.getDefaultValue());
		assertEquals(false, c.isMax());
		assertEquals(false, c.isIdentity());

		// f6 varchar(max) not null default 'abc',
		c = dba.getColumnInfo(conn, t.getColumn("f6"));
		assertEquals("f6", c.getName());
		assertSame(StringColumn.class, c.getType());
		assertEquals(false, c.isNullable());
		// assertEquals(0, c.getLength()); -- database-dependent value
		assertEquals("'abc'", c.getDefaultValue());
		assertEquals(true, c.isMax());
		assertEquals(false, c.isIdentity());

		// f8 datetime default '20130401',
		c = dba.getColumnInfo(conn, t.getColumn("f8"));
		assertEquals("f8", c.getName());
		assertSame(DateTimeColumn.class, c.getType());
		assertEquals(true, c.isNullable());
		assertEquals(0, c.getLength());
		assertEquals("'20130401'", c.getDefaultValue());
		assertEquals(false, c.isMax());
		assertEquals(false, c.isIdentity());

		// f9 datetime not null default getdate(),
		c = dba.getColumnInfo(conn, t.getColumn("f9"));
		assertEquals("f9", c.getName());
		assertSame(DateTimeColumn.class, c.getType());
		assertEquals(false, c.isNullable());
		assertEquals(0, c.getLength());
		assertEquals("GETDATE()", c.getDefaultValue());
		assertEquals(false, c.isMax());
		assertEquals(false, c.isIdentity());

		// f10 image default 0xFFAAFFAAFF,
		c = dba.getColumnInfo(conn, t.getColumn("f10"));
		assertEquals("f10", c.getName());
		assertSame(BinaryColumn.class, c.getType());
		assertEquals(true, c.isNullable());
		assertEquals(0, c.getLength());
		assertEquals("0xFFAAFFAAFF", c.getDefaultValue());
		assertEquals(false, c.isMax());
		assertEquals(false, c.isIdentity());
	}

	@Test
	public void updateColumn() throws CelestaException, ParseException, IOException, SQLException {
		// NULL/NOT NULL и DEFAULT (простые)
		DBColumnInfo c;
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

		// f6 varchar(max) not null default 'abc',
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

	}

	@Test
	public void updateColumn2test() throws CelestaException, ParseException, IOException, SQLException {
		// IDENTITY
		DBColumnInfo c;
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
	public void updateColumn3test() throws CelestaException, ParseException, IOException, SQLException {
		// String length
		DBColumnInfo c;
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

		col.setLength("max");
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
	public void updateColumn4test() throws CelestaException, ParseException, IOException, SQLException {
		// BLOB Default
		DBColumnInfo c;
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
	public void updateColumn5test() throws CelestaException, ParseException, IOException, SQLException {
		// Change data type
		DBColumnInfo c;
		IntegerColumn col;
		StringColumn scol;
		BooleanColumn bcol;
		// Table should be empty in Oracle to change data type...
		// insertRow(conn, t, 11);
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
	public void testReflects() throws CelestaException, ParseException {
		DBColumnInfo c;

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
	public void getPKInfo() throws CelestaException, ParseException, IOException, SQLException {
		DBPKInfo c;
		insertRow(conn, t, 15);

		c = dba.getPKInfo(conn, t);
		assertNotNull(c);
		assertEquals("pk_test", c.getName());
		assertEquals(1, c.getColumnNames().size());
		String[] expected = { "id" };
		assertTrue(Arrays.equals(expected, c.getColumnNames().toArray(new String[0])));
		assertTrue(c.reflects(t));
		assertFalse(c.isEmpty());

		dba.dropPK(conn, t, "pk_test");
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
		String[] expected2 = { "id", "f1", "f9" };
		assertTrue(Arrays.equals(expected2, c.getColumnNames().toArray(new String[0])));
		assertTrue(c.reflects(t));
		assertFalse(c.isEmpty());

	}

	@Test
	public void getFKInfo() throws ParseException, CelestaException, SQLException {
		dba.dropTable(conn, t);
		assertFalse(dba.tableExists(conn, "gtest", "test"));
		assertFalse(dba.tableExists(conn, "gtest", "refTo"));

		Grain g = score.getGrain(GRAIN_NAME);
		Table t2 = g.getTable("refTo");
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

			List<DBFKInfo> l = dba.getFKInfo(conn, g);
			assertNotNull(l);
			assertEquals(0, l.size());

			dba.createFK(conn, fk);
			l = dba.getFKInfo(conn, g);
			assertEquals(1, l.size());
			DBFKInfo info = l.get(0);
			assertEquals("fk_testNameVeryVeryLongLonName", info.getName());
			String[] expected = { "attrVarchar", "attrInt" };
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
			// Проверяем, что не отображаются "лишние" индексы (актуально для
			// MySQL)
			Map<String, DBIndexInfo> indicesSet = dba.getIndices(conn, t.getGrain());
			assertNotNull(indicesSet);
			assertEquals(0, indicesSet.size());

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
	public void additionalCreateTableTest() throws ParseException, CelestaException {
		Grain g = score.getGrain(GRAIN_NAME);
		Table t3 = g.getTable("aLongIdentityTableNaaame");
		try {
			dba.createTable(conn, t3);
			DBColumnInfo c = dba.getColumnInfo(conn, t3.getColumn("f1"));
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
	public void viewTest() throws ParseException, CelestaException, SQLException {
		Grain g = score.getGrain(GRAIN_NAME);
		Table t2 = g.getTable("refTo");
		try {
			ForeignKey fk = t.getForeignKeys().iterator().next();
			View v = g.getView("testview");
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
	public void resetIdentityTest() throws IOException, CelestaException, SQLException {
		insertRow(conn, t, 110);
		WhereTerm w = WhereTermsMaker.getPKWhereTerm(t);
		PreparedStatement pstmt = dba.getOneRecordStatement(conn, t, w.getWhere());

		assertNotNull(pstmt);

		List<ParameterSetter> program = new ArrayList<>();
		w.programParams(program);
		int i = 1;
		for (ParameterSetter ps : program) {
			ps.execute(pstmt, i++, new Integer[] { 555 /* key value */ }, 1);
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

}
