package ru.curs.celesta.dbutils;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.dbutils.filter.Range;
import ru.curs.celesta.dbutils.filter.SingleValue;
import ru.curs.celesta.dbutils.stmt.MaskedStatementHolder;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.dbutils.stmt.PreparedStmtHolder;

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

import static org.junit.jupiter.api.Assertions.*;

public class PreparedStatementHolderTest {

    @Test
    public void test1() {
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
    public void test2() {
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
    public void test3() {
        C c = new C();
        Double[] rec = { 23.4, 2.83, 29.3, 37.8, 6.8 };
        DummyPreparedStatement s = (DummyPreparedStatement) c.getStatement(rec, 15);
        assertEquals("[1->37.8][2->15]", s.params);
        s.params = "";
        DummyPreparedStatement s2 = (DummyPreparedStatement) c.getStatement(rec, 16);
        assertSame(s, s2);
        assertEquals("[1->37.8][2->16]", s2.params);
    }

    static class A extends PreparedStmtHolder {

        SingleValue filter = new SingleValue(5);
        Range filter2 = new Range("bar", "foo");

        @Override
        protected PreparedStatement initStatement(List<ParameterSetter> program) {
            program.add(ParameterSetter.create(filter, null));
            program.add(ParameterSetter.create(2, null));
            program.add(ParameterSetter.createForValueFrom(filter2, null));
            program.add(ParameterSetter.createForValueTo(filter2, null));
            return new DummyPreparedStatement();
        }
    }

    static class B extends MaskedStatementHolder {

        @Override
        protected int[] getNullsMaskIndices() {
            return new int[] { 1, 3 };
        }

        @Override
        protected PreparedStatement initStatement(List<ParameterSetter> program) {
            program.add(ParameterSetter.create(1, null));
            program.add(ParameterSetter.create(3, null));
            program.add(ParameterSetter.create(2, null));
            return new DummyPreparedStatement();
        }

    }

    static class C extends PreparedStmtHolder {

        @Override
        protected PreparedStatement initStatement(List<ParameterSetter> program) {
            program.add(ParameterSetter.create(3, null));
            program.add(ParameterSetter.createForRecversion(null));
            return new DummyPreparedStatement();
        }
    }

    static class DummyPreparedStatement implements PreparedStatement {

        private String params = "";
        private boolean closed = false;

        @Override
        public ResultSet executeQuery(String sql) {

            return null;
        }

        @Override
        public int executeUpdate(String sql) {

            return 0;
        }

        @Override
        public void close() {
            closed = true;

        }

        @Override
        public int getMaxFieldSize() {

            return 0;
        }

        @Override
        public void setMaxFieldSize(int max) {

        }

        @Override
        public int getMaxRows() {

            return 0;
        }

        @Override
        public void setMaxRows(int max) {

        }

        @Override
        public void setEscapeProcessing(boolean enable) {

        }

        @Override
        public int getQueryTimeout() {

            return 0;
        }

        @Override
        public void setQueryTimeout(int seconds) {

        }

        @Override
        public void cancel() {

        }

        @Override
        public SQLWarning getWarnings() {

            return null;
        }

        @Override
        public void clearWarnings() {

        }

        @Override
        public void setCursorName(String name) {

        }

        @Override
        public boolean execute(String sql) {

            return false;
        }

        @Override
        public ResultSet getResultSet() {

            return null;
        }

        @Override
        public int getUpdateCount() {

            return 0;
        }

        @Override
        public boolean getMoreResults() {

            return false;
        }

        @Override
        public void setFetchDirection(int direction) {

        }

        @Override
        public int getFetchDirection() {

            return 0;
        }

        @Override
        public void setFetchSize(int rows) {

        }

        @Override
        public int getFetchSize() {

            return 0;
        }

        @Override
        public int getResultSetConcurrency() {

            return 0;
        }

        @Override
        public int getResultSetType() {

            return 0;
        }

        @Override
        public void addBatch(String sql) {

        }

        @Override
        public void clearBatch() {

        }

        @Override
        public int[] executeBatch() {

            return null;
        }

        @Override
        public Connection getConnection() {

            return null;
        }

        @Override
        public boolean getMoreResults(int current) {

            return false;
        }

        @Override
        public ResultSet getGeneratedKeys() {

            return null;
        }

        @Override
        public int executeUpdate(String sql, int autoGeneratedKeys) {

            return 0;
        }

        @Override
        public int executeUpdate(String sql, int[] columnIndexes) {

            return 0;
        }

        @Override
        public int executeUpdate(String sql, String[] columnNames) {

            return 0;
        }

        @Override
        public boolean execute(String sql, int autoGeneratedKeys) {

            return false;
        }

        @Override
        public boolean execute(String sql, int[] columnIndexes) {

            return false;
        }

        @Override
        public boolean execute(String sql, String[] columnNames) {

            return false;
        }

        @Override
        public int getResultSetHoldability() {

            return 0;
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public void setPoolable(boolean poolable) {

        }

        @Override
        public boolean isPoolable() {

            return false;
        }

        @Override
        public void closeOnCompletion() {

        }

        @Override
        public boolean isCloseOnCompletion() {

            return false;
        }

        @Override
        public <T> T unwrap(Class<T> iface) {

            return null;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {

            return false;
        }

        @Override
        public ResultSet executeQuery() {

            return null;
        }

        @Override
        public int executeUpdate() {

            return 0;
        }

        @Override
        public void setNull(int parameterIndex, int sqlType) {
            params = params + String.format("[%d->NULL]", parameterIndex);
        }

        @Override
        public void setBoolean(int parameterIndex, boolean x) {

        }

        @Override
        public void setByte(int parameterIndex, byte x) {

        }

        @Override
        public void setShort(int parameterIndex, short x) {

        }

        @Override
        public void setInt(int parameterIndex, int x) {
            params = params + String.format("[%d->%d]", parameterIndex, x);

        }

        @Override
        public void setLong(int parameterIndex, long x) {

        }

        @Override
        public void setFloat(int parameterIndex, float x) {

        }

        @Override
        public void setDouble(int parameterIndex, double x) {
            params = params + String.format("[%d->%s]", parameterIndex, x);
        }

        @Override
        public void setBigDecimal(int parameterIndex, BigDecimal x) {

        }

        @Override
        public void setString(int parameterIndex, String x) {
            params = params + String.format("[%d->%s]", parameterIndex, x);
        }

        @Override
        public void setBytes(int parameterIndex, byte[] x) {

        }

        @Override
        public void setDate(int parameterIndex, Date x) {

        }

        @Override
        public void setTime(int parameterIndex, Time x) {

        }

        @Override
        public void setTimestamp(int parameterIndex, Timestamp x) {

        }

        @Override
        public void setAsciiStream(int parameterIndex, InputStream x, int length) {

        }

        @Override
        @Deprecated
        public void setUnicodeStream(int parameterIndex, InputStream x, int length) {

        }

        @Override
        public void setBinaryStream(int parameterIndex, InputStream x, int length) {

        }

        @Override
        public void clearParameters() {

        }

        @Override
        public void setObject(int parameterIndex, Object x, int targetSqlType) {

        }

        @Override
        public void setObject(int parameterIndex, Object x) {

        }

        @Override
        public boolean execute() {

            return false;
        }

        @Override
        public void addBatch() {

        }

        @Override
        public void setCharacterStream(int parameterIndex, Reader reader, int length) {

        }

        @Override
        public void setRef(int parameterIndex, Ref x) {

        }

        @Override
        public void setBlob(int parameterIndex, Blob x) {

        }

        @Override
        public void setClob(int parameterIndex, Clob x) {

        }

        @Override
        public void setArray(int parameterIndex, Array x) {

        }

        @Override
        public ResultSetMetaData getMetaData() {

            return null;
        }

        @Override
        public void setDate(int parameterIndex, Date x, Calendar cal) {

        }

        @Override
        public void setTime(int parameterIndex, Time x, Calendar cal) {

        }

        @Override
        public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) {

        }

        @Override
        public void setNull(int parameterIndex, int sqlType, String typeName) {
            params = params + String.format("[%d->NULL]", parameterIndex);
        }

        @Override
        public void setURL(int parameterIndex, URL x) {

        }

        @Override
        public ParameterMetaData getParameterMetaData() {

            return null;
        }

        @Override
        public void setRowId(int parameterIndex, RowId x) {

        }

        @Override
        public void setNString(int parameterIndex, String value) {

        }

        @Override
        public void setNCharacterStream(int parameterIndex, Reader value, long length) {

        }

        @Override
        public void setNClob(int parameterIndex, NClob value) {

        }

        @Override
        public void setClob(int parameterIndex, Reader reader, long length) {

        }

        @Override
        public void setBlob(int parameterIndex, InputStream inputStream, long length) {

        }

        @Override
        public void setNClob(int parameterIndex, Reader reader, long length) {

        }

        @Override
        public void setSQLXML(int parameterIndex, SQLXML xmlObject) {

        }

        @Override
        public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) {

        }

        @Override
        public void setAsciiStream(int parameterIndex, InputStream x, long length) {

        }

        @Override
        public void setBinaryStream(int parameterIndex, InputStream x, long length) {

        }

        @Override
        public void setCharacterStream(int parameterIndex, Reader reader, long length) {

        }

        @Override
        public void setAsciiStream(int parameterIndex, InputStream x) {

        }

        @Override
        public void setBinaryStream(int parameterIndex, InputStream x) {

        }

        @Override
        public void setCharacterStream(int parameterIndex, Reader reader) {

        }

        @Override
        public void setNCharacterStream(int parameterIndex, Reader value) {

        }

        @Override
        public void setClob(int parameterIndex, Reader reader) {

        }

        @Override
        public void setBlob(int parameterIndex, InputStream inputStream) {

        }

        @Override
        public void setNClob(int parameterIndex, Reader reader) {

        }

    }
}
