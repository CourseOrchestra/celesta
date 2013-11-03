package ru.curs.celesta.dbutils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.dbutils.DBAdaptor.IndexInfo;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.Table;

public abstract class AbstractAdaptorTest {
	final static String GRAIN_NAME = "gtest";
	final static String SCORE_NAME = "testScore";
	private DBAdaptor dba;
	private Score score;

	@Before
	public void setup() throws Exception {
		Connection conn = ConnectionPool.get();
		try {
			dba.createSchemaIfNotExists(conn, GRAIN_NAME);
		} finally {
			ConnectionPool.putBack(conn);
		}
		Table t = score.getGrain(GRAIN_NAME).getTable("test");
		try {
			dba.dropTable(t);
		} catch (Exception e) {
		}
	}

	@Test
	public void tableExists() throws Exception {
		Table t = score.getGrain(GRAIN_NAME).getTable("test");
		dba.createTable(t);
		try {
			boolean result = dba.tableExists(t.getGrain().getName(),
					t.getName());
			assertTrue(result);
		} finally {
			dba.dropTable(t);
		}
	}

	@Test
	public void userTablesExist() throws Exception {
		Table t = score.getGrain(GRAIN_NAME).getTable("test");
		dba.createTable(t);
		try {
			boolean result = dba.userTablesExist();
			assertTrue(result);
		} finally {
			dba.dropTable(t);
		}
	}

	@Test
	public void createTable() throws CelestaException {
		try {
			Table t = score.getGrain(GRAIN_NAME).getTable("test");
			dba.createTable(t);
			dba.dropTable(t);
		} catch (Exception ex) {
			fail("Threw Exception:" + ex.getMessage());
		}
	}

	private int insertRow(Connection conn, Table t, int val) throws Exception {
		int count = t.getColumns().size();
		assertEquals(13, count);
		boolean[] nullsMask = { true, false, false, false, true, false, true,
				true, false, true, true, true, false };
		BLOB b = new BLOB();
		b.getOutStream().write(new byte[] { 1, 2, 3 });
		Object[] rowData = { "ab", val, false, 1.1, "eee", b };
		PreparedStatement pstmt = dba.getInsertRecordStatement(conn, t,
				nullsMask);
		assertNotNull(pstmt);
		int i = 1;
		for (Object fieldVal : rowData)
			DBAdaptor.setParam(pstmt, i++, fieldVal);
		try {
			int rowCount = pstmt.executeUpdate();
			return rowCount;
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	@Test
	public void getInsertRecordStatement() throws Exception {
		Table t = score.getGrain(GRAIN_NAME).getTable("test");
		dba.createTable(t);
		Connection conn = ConnectionPool.get();
		try {
			int rowCount = insertRow(conn, t, 1);
			assertTrue(rowCount == 1);
		} finally {
			ConnectionPool.putBack(conn);
			dba.dropTable(t);
		}
	}

	@Test
	public void getSetCountStatement() throws Exception {
		Table t = score.getGrain(GRAIN_NAME).getTable("test");
		dba.createTable(t);
		Connection conn = ConnectionPool.get();
		try {
			PreparedStatement stmt = dba.getSetCountStatement(conn, t,
					Collections.<String, AbstractFilter> emptyMap());
			ResultSet rs = stmt.executeQuery();
			rs.next();
			assertEquals(0, rs.getInt(1));

			insertRow(conn, t, 1);
			insertRow(conn, t, 2);
			insertRow(conn, t, 3);

			rs = stmt.executeQuery();
			rs.next();
			assertEquals(3, rs.getInt(1));

		} finally {
			ConnectionPool.putBack(conn);
			dba.dropTable(t);
		}
	}

	@Test
	public void getUpdateRecordStatement() throws Exception {
		Table t = score.getGrain(GRAIN_NAME).getTable("test");
		dba.createTable(t);
		Connection conn = ConnectionPool.get();
		try {
			insertRow(conn, t, 1);
			int count = t.getColumns().size();
			assertEquals(13, count);
			boolean[] mask = { true, true, false, true, true, true, true, true,
					true, true, true, true, true };

			PreparedStatement pstmt = dba.getUpdateRecordStatement(conn, t,
					mask);
			assertNotNull(pstmt);
			DBAdaptor.setParam(pstmt, 1, 2); // field value
			DBAdaptor.setParam(pstmt, 2, 1); // key value
			int rowCount = pstmt.executeUpdate();
			assertTrue(rowCount == 1);
		} finally {
			ConnectionPool.putBack(conn);
			dba.dropTable(t);
		}
	}

	@Test
	public void getDeleteRecordStatement() throws Exception {
		Table t = score.getGrain(GRAIN_NAME).getTable("test");
		dba.createTable(t);
		Connection conn = ConnectionPool.get();
		try {
			insertRow(conn, t, 1);
			PreparedStatement pstmt = dba.getDeleteRecordStatement(conn, t);
			assertNotNull(pstmt);
			DBAdaptor.setParam(pstmt, 1, 1);// key value
			int rowCount = pstmt.executeUpdate();
			assertTrue(rowCount == 1);
		} finally {
			ConnectionPool.putBack(conn);
			dba.dropTable(t);
		}
	}

	@Test
	public void getColumns() throws Exception {
		Table t = score.getGrain(GRAIN_NAME).getTable("test");
		dba.createTable(t);
		Connection conn = ConnectionPool.get();
		try {
			Set<String> columnSet = dba.getColumns(conn, t);
			assertNotNull(columnSet);
			assertTrue(columnSet.size() != 0);
		} finally {
			ConnectionPool.putBack(conn);
			dba.dropTable(t);
		}
	}

	@Test
	public void getIndices() throws Exception {
		Grain grain = score.getGrain(GRAIN_NAME);
		Table t = grain.getTable("test");
		dba.createTable(t);
		Index i = grain.getIndices().get("idxtest");
		dba.createIndex(i);
		Connection conn = ConnectionPool.get();
		try {
			Map<IndexInfo, TreeMap<Short, String>> indicesSet = dba.getIndices(
					conn, t.getGrain());
			assertNotNull(indicesSet);
			assertTrue(indicesSet.size() == 1);
			dba.dropIndex(grain,
					new DBAdaptor.IndexInfo(t.getName(), i.getName()));

			indicesSet = dba.getIndices(conn, t.getGrain());
			assertNotNull(indicesSet);
			assertTrue(indicesSet.size() == 0);
		} finally {
			ConnectionPool.putBack(conn);
			dba.dropTable(t);
		}
	}

	@Test
	public void getOneFieldStatement() throws Exception {
		Table t = score.getGrain(GRAIN_NAME).getTable("test");
		dba.createTable(t);
		Connection conn = ConnectionPool.get();
		try {
			insertRow(conn, t, 1);
			Column c = t.getColumns().get("attrInt");
			PreparedStatement pstmt = dba.getOneFieldStatement(conn, c);
			assertNotNull(pstmt);
			DBAdaptor.setParam(pstmt, 1, 1);// key value
			ResultSet rs = pstmt.executeQuery();
			assertTrue(rs.next());
			rs.getInt("attrInt");
		} finally {
			ConnectionPool.putBack(conn);
			dba.dropTable(t);
		}
	}

	@Test
	public void getOneRecordStatement() throws Exception {
		Table t = score.getGrain(GRAIN_NAME).getTable("test");
		dba.createTable(t);
		Connection conn = ConnectionPool.get();
		try {
			insertRow(conn, t, 1);
			PreparedStatement pstmt = dba.getOneRecordStatement(conn, t);
			assertNotNull(pstmt);
			DBAdaptor.setParam(pstmt, 1, 1);// key value
			ResultSet rs = pstmt.executeQuery();
			assertTrue(rs.next());
		} finally {
			ConnectionPool.putBack(conn);
			dba.dropTable(t);
		}
	}

	@Test
	public void deleteRecordSetStatement() throws Exception {
		Table t = score.getGrain(GRAIN_NAME).getTable("test");
		dba.createTable(t);
		Connection conn = ConnectionPool.get();
		try {
			insertRow(conn, t, 1);
			PreparedStatement pstmt = dba.deleteRecordSetStatement(conn, t,
					Collections.<String, AbstractFilter> emptyMap());
			assertNotNull(pstmt);
			int rowCount = pstmt.executeUpdate();
			assertTrue(rowCount == 1);
		} finally {
			ConnectionPool.putBack(conn);
			dba.dropTable(t);
		}
	}

	protected void setDba(DBAdaptor dba) {
		this.dba = dba;
	}

	protected void setScore(Score score) {
		this.score = score;
	}
}
