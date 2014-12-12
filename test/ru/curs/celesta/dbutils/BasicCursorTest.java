package ru.curs.celesta.dbutils;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.junit.Test;

import ru.curs.celesta.AppSettings;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.SessionContext;
import ru.curs.celesta.syscursors.LogSetupCursor;

public class BasicCursorTest {
	// @Test
	// public void test1() {
	// assertEquals("(d > ?)", BasicCursor.getNavigationWhereClause("d"));
	//
	// assertEquals(
	// "((a > ?) or ((a = ?) and ((b > ?) or ((b = ?) and (c > ?)))",
	// BasicCursor.getNavigationWhereClause("a", "b", "c"));
	// }

	@Test
	public void testWhere() throws CelestaException, NoSuchMethodException,
			SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {

		SessionContext sc = new SessionContext("foo", "foo");
		try {
			AppSettings.getDBType();
		} catch (NullPointerException e) {
			Properties p = new Properties();
			p.setProperty("score.path", "c:/score");
			p.setProperty("rdbms.connection.url",
					"jdbc:sqlserver://localhost;databaseName=celesta;user=sa;password=123");
			Method method = AppSettings.class.getDeclaredMethod("init",
					Properties.class);
			method.setAccessible(true);
			method.invoke(null, p);
		}

		Cursor c = new LogSetupCursor(new CallContext(conn, sc));
		assertEquals(
				"((\"grainid\" < ?) or ((\"grainid\" = ?) and (\"tablename\" < ?))",
				c.getNavigationWhereClause('<'));
		assertEquals(
				"((\"grainid\" > ?) or ((\"grainid\" = ?) and (\"tablename\" > ?))",
				c.getNavigationWhereClause('>'));
		assertEquals("((\"grainid\" = ?) and (\"tablename\" = ?))",
				c.getNavigationWhereClause('='));
		assertEquals("\"grainid\", \"tablename\"", c.getOrderBy());
		assertEquals("\"grainid\" desc, \"tablename\" desc",
				c.getReversedOrderBy());

		c.orderBy("d", "i ASC", "m DESC");
		assertEquals("\"d\", \"i\", \"m\" desc, \"grainid\", \"tablename\"",
				c.getOrderBy());
		assertEquals(
				"\"d\" desc, \"i\" desc, \"m\", \"grainid\" desc, \"tablename\" desc",
				c.getReversedOrderBy());

		assertEquals(
				"((\"d\" > ?) or ((\"d\" = ?) and ((\"i\" > ?) or ((\"i\" = ?) and ((\"m\" < ?) or ((\"m\" = ?) and ((\"grainid\" > ?) or ((\"grainid\" = ?) and (\"tablename\" > ?)))))",
				c.getNavigationWhereClause('>'));
		assertEquals(
				"((\"d\" < ?) or ((\"d\" = ?) and ((\"i\" < ?) or ((\"i\" = ?) and ((\"m\" > ?) or ((\"m\" = ?) and ((\"grainid\" < ?) or ((\"grainid\" = ?) and (\"tablename\" < ?)))))",
				c.getNavigationWhereClause('<'));
		assertEquals("((\"grainid\" = ?) and (\"tablename\" = ?))",
				c.getNavigationWhereClause('='));

		c.orderBy("grainid", "m DESC");
		assertEquals("\"grainid\", \"m\" desc, \"tablename\"", c.getOrderBy());
		assertEquals("\"grainid\" desc, \"m\", \"tablename\" desc", c.getReversedOrderBy());
		assertEquals(
				"((\"grainid\" > ?) or ((\"grainid\" = ?) and ((\"m\" < ?) or ((\"m\" = ?) and (\"tablename\" > ?)))",
				c.getNavigationWhereClause('>'));
		assertEquals(
				"((\"grainid\" < ?) or ((\"grainid\" = ?) and ((\"m\" > ?) or ((\"m\" = ?) and (\"tablename\" < ?)))",
				c.getNavigationWhereClause('<'));
		assertEquals("((\"grainid\" = ?) and (\"tablename\" = ?))",
				c.getNavigationWhereClause('='));
	}

	final static Connection conn = new Connection() {

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			return null;
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			return false;
		}

		@Override
		public Statement createStatement() throws SQLException {
			return null;
		}

		@Override
		public PreparedStatement prepareStatement(String sql)
				throws SQLException {
			return null;
		}

		@Override
		public CallableStatement prepareCall(String sql) throws SQLException {
			return null;
		}

		@Override
		public String nativeSQL(String sql) throws SQLException {
			return null;
		}

		@Override
		public void setAutoCommit(boolean autoCommit) throws SQLException {
		}

		@Override
		public boolean getAutoCommit() throws SQLException {
			return false;
		}

		@Override
		public void commit() throws SQLException {
		}

		@Override
		public void rollback() throws SQLException {
		}

		@Override
		public void close() throws SQLException {

		}

		@Override
		public boolean isClosed() throws SQLException {
			return false;
		}

		@Override
		public DatabaseMetaData getMetaData() throws SQLException {
			return null;
		}

		@Override
		public void setReadOnly(boolean readOnly) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public boolean isReadOnly() throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void setCatalog(String catalog) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public String getCatalog() throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void setTransactionIsolation(int level) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public int getTransactionIsolation() throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public SQLWarning getWarnings() throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void clearWarnings() throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public Statement createStatement(int resultSetType,
				int resultSetConcurrency) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public PreparedStatement prepareStatement(String sql,
				int resultSetType, int resultSetConcurrency)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public CallableStatement prepareCall(String sql, int resultSetType,
				int resultSetConcurrency) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Map<String, Class<?>> getTypeMap() throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void setHoldability(int holdability) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public int getHoldability() throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Savepoint setSavepoint() throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Savepoint setSavepoint(String name) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void rollback(Savepoint savepoint) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void releaseSavepoint(Savepoint savepoint) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public Statement createStatement(int resultSetType,
				int resultSetConcurrency, int resultSetHoldability)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public PreparedStatement prepareStatement(String sql,
				int resultSetType, int resultSetConcurrency,
				int resultSetHoldability) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public CallableStatement prepareCall(String sql, int resultSetType,
				int resultSetConcurrency, int resultSetHoldability)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public PreparedStatement prepareStatement(String sql,
				int autoGeneratedKeys) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public PreparedStatement prepareStatement(String sql,
				int[] columnIndexes) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public PreparedStatement prepareStatement(String sql,
				String[] columnNames) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Clob createClob() throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Blob createBlob() throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public NClob createNClob() throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public SQLXML createSQLXML() throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean isValid(int timeout) throws SQLException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void setClientInfo(String name, String value)
				throws SQLClientInfoException {
			// TODO Auto-generated method stub

		}

		@Override
		public void setClientInfo(Properties properties)
				throws SQLClientInfoException {
			// TODO Auto-generated method stub

		}

		@Override
		public String getClientInfo(String name) throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Properties getClientInfo() throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Array createArrayOf(String typeName, Object[] elements)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Struct createStruct(String typeName, Object[] attributes)
				throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void setSchema(String schema) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public String getSchema() throws SQLException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void abort(Executor executor) throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public void setNetworkTimeout(Executor executor, int milliseconds)
				throws SQLException {
			// TODO Auto-generated method stub

		}

		@Override
		public int getNetworkTimeout() throws SQLException {
			// TODO Auto-generated method stub
			return 0;
		}

	};
}
