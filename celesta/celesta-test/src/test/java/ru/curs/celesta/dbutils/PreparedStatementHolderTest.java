package ru.curs.celesta.dbutils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import org.junit.Test;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.filter.Range;
import ru.curs.celesta.dbutils.filter.SingleValue;
import ru.curs.celesta.dbutils.stmt.MaskedStatementHolder;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.dbutils.stmt.PreparedStmtHolder;

public class PreparedStatementHolderTest {

	@Test
	public void test1() throws CelestaException {
		A a = new A();
		Integer[] rec = { 11, 12, 15 };

		DummyPreparedStatement s = (DummyPreparedStatement) a.getStatement(rec, 0);
		assertEquals("[1->5][2->15][3->bar][4->foo]", s.params);
		a.filter.setValue(111);
		a.filter2.setValues("vvv", "xxx");
		rec[2] = 16;
		s.params = "";

		DummyPreparedStatement s2 = (DummyPreparedStatement) a.getStatement(rec, 0);
		assertSame(s, s2);
		assertEquals("[1->111][2->16][3->vvv][4->xxx]", s.params);

		a.close();
		rec[2] = 17;
		s = (DummyPreparedStatement) a.getStatement(rec, 0);
		assertNotSame(s, s2);
		assertEquals("[1->111][2->17][3->vvv][4->xxx]", s.params);
	}

	@Test
	public void test2() throws CelestaException {
		B b = new B();
		Integer[] rec = { 11, 12, 13, 14, 15 };

		DummyPreparedStatement s = (DummyPreparedStatement) b.getStatement(rec, 0);
		assertTrue(Arrays.equals(new boolean[] { false, false }, b.getNullsMask()));
		assertEquals("[1->12][2->14][3->13]", s.params);

		rec = new Integer[] { 11, 22, 23, 24, 15 };
		s.params = "";
		DummyPreparedStatement s2 = (DummyPreparedStatement) b.getStatement(rec, 0);
		assertSame(s, s2);
		assertTrue(Arrays.equals(new boolean[] { false, false }, b.getNullsMask()));
		assertEquals("[1->22][2->24][3->23]", s.params);

		rec = new Integer[] { 11, 22, 23, null, 15 };
		s = (DummyPreparedStatement) b.getStatement(rec, 0);
		assertNotSame(s, s2);
		assertTrue(Arrays.equals(new boolean[] { false, true }, b.getNullsMask()));
		assertEquals("[1->22][2->NULL][3->23]", s.params);

		rec = new Integer[] { 11, 22, null, null, 15 };
		s.params = "";
		s2 = (DummyPreparedStatement) b.getStatement(rec, 0);
		assertTrue(Arrays.equals(new boolean[] { false, true }, b.getNullsMask()));
		assertSame(s, s2);
		assertEquals("[1->22][2->NULL][3->NULL]", s.params);

		rec = new Integer[] { 11, null, null, null, 15 };
		s.params = "";
		s = (DummyPreparedStatement) b.getStatement(rec, 0);
		assertNotSame(s, s2);
		assertTrue(Arrays.equals(new boolean[] { true, true }, b.getNullsMask()));
		assertEquals("[1->NULL][2->NULL][3->NULL]", s.params);

	}

	@Test
	public void test3() throws CelestaException {
		C c = new C();
		Double[] rec = { 23.4, 2.83, 29.3, 37.8, 6.8 };
		DummyPreparedStatement s = (DummyPreparedStatement) c.getStatement(rec, 15);
		assertEquals("[1->37.8][2->15]", s.params);
		s.params = "";
		DummyPreparedStatement s2 = (DummyPreparedStatement) c.getStatement(rec, 16);
		assertSame(s, s2);
		assertEquals("[1->37.8][2->16]", s2.params);
	}

	class A extends PreparedStmtHolder {

		SingleValue filter = new SingleValue(5);
		Range filter2 = new Range("bar", "foo");

		@Override
		protected PreparedStatement initStatement(List<ParameterSetter> program) throws CelestaException {
			program.add(ParameterSetter.create(filter));
			program.add(ParameterSetter.create(2));
			program.add(ParameterSetter.createForValueFrom(filter2));
			program.add(ParameterSetter.createForValueTo(filter2));
			return new DummyPreparedStatement();
		}
	}

	class B extends MaskedStatementHolder {

		@Override
		protected int[] getNullsMaskIndices() {
			return new int[] { 1, 3 };
		}

		@Override
		protected PreparedStatement initStatement(List<ParameterSetter> program) throws CelestaException {
			program.add(ParameterSetter.create(1));
			program.add(ParameterSetter.create(3));
			program.add(ParameterSetter.create(2));
			return new DummyPreparedStatement();
		}

	}

	class C extends PreparedStmtHolder {

		@Override
		protected PreparedStatement initStatement(List<ParameterSetter> program) throws CelestaException {
			program.add(ParameterSetter.create(3));
			program.add(ParameterSetter.createForRecversion());
			return new DummyPreparedStatement();
		}
	}

	class DummyPreparedStatement implements PreparedStatement {

		private String params = "";
		private boolean closed = false;

		@Override
		public ResultSet executeQuery(String sql) throws SQLException {

			return null;
		}

		@Override
		public int executeUpdate(String sql) throws SQLException {

			return 0;
		}

		@Override
		public void close() throws SQLException {
			closed = true;

		}

		@Override
		public int getMaxFieldSize() throws SQLException {

			return 0;
		}

		@Override
		public void setMaxFieldSize(int max) throws SQLException {

		}

		@Override
		public int getMaxRows() throws SQLException {

			return 0;
		}

		@Override
		public void setMaxRows(int max) throws SQLException {

		}

		@Override
		public void setEscapeProcessing(boolean enable) throws SQLException {

		}

		@Override
		public int getQueryTimeout() throws SQLException {

			return 0;
		}

		@Override
		public void setQueryTimeout(int seconds) throws SQLException {

		}

		@Override
		public void cancel() throws SQLException {

		}

		@Override
		public SQLWarning getWarnings() throws SQLException {

			return null;
		}

		@Override
		public void clearWarnings() throws SQLException {

		}

		@Override
		public void setCursorName(String name) throws SQLException {

		}

		@Override
		public boolean execute(String sql) throws SQLException {

			return false;
		}

		@Override
		public ResultSet getResultSet() throws SQLException {

			return null;
		}

		@Override
		public int getUpdateCount() throws SQLException {

			return 0;
		}

		@Override
		public boolean getMoreResults() throws SQLException {

			return false;
		}

		@Override
		public void setFetchDirection(int direction) throws SQLException {

		}

		@Override
		public int getFetchDirection() throws SQLException {

			return 0;
		}

		@Override
		public void setFetchSize(int rows) throws SQLException {

		}

		@Override
		public int getFetchSize() throws SQLException {

			return 0;
		}

		@Override
		public int getResultSetConcurrency() throws SQLException {

			return 0;
		}

		@Override
		public int getResultSetType() throws SQLException {

			return 0;
		}

		@Override
		public void addBatch(String sql) throws SQLException {

		}

		@Override
		public void clearBatch() throws SQLException {

		}

		@Override
		public int[] executeBatch() throws SQLException {

			return null;
		}

		@Override
		public Connection getConnection() throws SQLException {

			return null;
		}

		@Override
		public boolean getMoreResults(int current) throws SQLException {

			return false;
		}

		@Override
		public ResultSet getGeneratedKeys() throws SQLException {

			return null;
		}

		@Override
		public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {

			return 0;
		}

		@Override
		public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {

			return 0;
		}

		@Override
		public int executeUpdate(String sql, String[] columnNames) throws SQLException {

			return 0;
		}

		@Override
		public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {

			return false;
		}

		@Override
		public boolean execute(String sql, int[] columnIndexes) throws SQLException {

			return false;
		}

		@Override
		public boolean execute(String sql, String[] columnNames) throws SQLException {

			return false;
		}

		@Override
		public int getResultSetHoldability() throws SQLException {

			return 0;
		}

		@Override
		public boolean isClosed() throws SQLException {
			return closed;
		}

		@Override
		public void setPoolable(boolean poolable) throws SQLException {

		}

		@Override
		public boolean isPoolable() throws SQLException {

			return false;
		}

		@Override
		public void closeOnCompletion() throws SQLException {

		}

		@Override
		public boolean isCloseOnCompletion() throws SQLException {

			return false;
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {

			return null;
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) throws SQLException {

			return false;
		}

		@Override
		public ResultSet executeQuery() throws SQLException {

			return null;
		}

		@Override
		public int executeUpdate() throws SQLException {

			return 0;
		}

		@Override
		public void setNull(int parameterIndex, int sqlType) throws SQLException {
			params = params + String.format("[%d->NULL]", parameterIndex);
		}

		@Override
		public void setBoolean(int parameterIndex, boolean x) throws SQLException {

		}

		@Override
		public void setByte(int parameterIndex, byte x) throws SQLException {

		}

		@Override
		public void setShort(int parameterIndex, short x) throws SQLException {

		}

		@Override
		public void setInt(int parameterIndex, int x) throws SQLException {
			params = params + String.format("[%d->%d]", parameterIndex, x);

		}

		@Override
		public void setLong(int parameterIndex, long x) throws SQLException {

		}

		@Override
		public void setFloat(int parameterIndex, float x) throws SQLException {

		}

		@Override
		public void setDouble(int parameterIndex, double x) throws SQLException {
			params = params + String.format("[%d->%s]", parameterIndex, x);
		}

		@Override
		public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {

		}

		@Override
		public void setString(int parameterIndex, String x) throws SQLException {
			params = params + String.format("[%d->%s]", parameterIndex, x);
		}

		@Override
		public void setBytes(int parameterIndex, byte[] x) throws SQLException {

		}

		@Override
		public void setDate(int parameterIndex, Date x) throws SQLException {

		}

		@Override
		public void setTime(int parameterIndex, Time x) throws SQLException {

		}

		@Override
		public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {

		}

		@Override
		public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {

		}

		@Override
		public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {

		}

		@Override
		public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {

		}

		@Override
		public void clearParameters() throws SQLException {

		}

		@Override
		public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {

		}

		@Override
		public void setObject(int parameterIndex, Object x) throws SQLException {

		}

		@Override
		public boolean execute() throws SQLException {

			return false;
		}

		@Override
		public void addBatch() throws SQLException {

		}

		@Override
		public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {

		}

		@Override
		public void setRef(int parameterIndex, Ref x) throws SQLException {

		}

		@Override
		public void setBlob(int parameterIndex, Blob x) throws SQLException {

		}

		@Override
		public void setClob(int parameterIndex, Clob x) throws SQLException {

		}

		@Override
		public void setArray(int parameterIndex, Array x) throws SQLException {

		}

		@Override
		public ResultSetMetaData getMetaData() throws SQLException {

			return null;
		}

		@Override
		public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {

		}

		@Override
		public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {

		}

		@Override
		public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {

		}

		@Override
		public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
			params = params + String.format("[%d->NULL]", parameterIndex);
		}

		@Override
		public void setURL(int parameterIndex, URL x) throws SQLException {

		}

		@Override
		public ParameterMetaData getParameterMetaData() throws SQLException {

			return null;
		}

		@Override
		public void setRowId(int parameterIndex, RowId x) throws SQLException {

		}

		@Override
		public void setNString(int parameterIndex, String value) throws SQLException {

		}

		@Override
		public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {

		}

		@Override
		public void setNClob(int parameterIndex, NClob value) throws SQLException {

		}

		@Override
		public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {

		}

		@Override
		public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {

		}

		@Override
		public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {

		}

		@Override
		public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {

		}

		@Override
		public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {

		}

		@Override
		public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {

		}

		@Override
		public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {

		}

		@Override
		public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {

		}

		@Override
		public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {

		}

		@Override
		public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

		}

		@Override
		public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

		}

		@Override
		public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {

		}

		@Override
		public void setClob(int parameterIndex, Reader reader) throws SQLException {

		}

		@Override
		public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

		}

		@Override
		public void setNClob(int parameterIndex, Reader reader) throws SQLException {

		}

	}
}
