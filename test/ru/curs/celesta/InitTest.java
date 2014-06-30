package ru.curs.celesta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import ru.curs.celesta.syscursors.GrainsCursor;
import ru.curs.celesta.syscursors.LogCursor;

public class InitTest {

	@BeforeClass
	public static void init() throws IOException, CelestaException {
		Properties params = new Properties();
		params.load(InitTest.class.getResourceAsStream("test.properties"));// celesta.oracle.properties
		ConnectionPool.clear();
		Celesta.initialize(params);
	}

	@Test
	public void test1() throws CelestaException {
		Celesta.getInstance();
	}

	@Test
	public void testSysCursors() throws CelestaException {
		Connection conn = ConnectionPool.get();
		CallContext ctxt = new CallContext(conn, "user");
		try {
			GrainsCursor g = new GrainsCursor(ctxt);
			assertEquals("grains", g.meta().getName());
			assertEquals(8, g.getMaxStrLen("checksum"));
			assertEquals(16, g.getMaxStrLen("id"));
			assertEquals(-1, g.getMaxStrLen("message"));
			g.reset();
			g.init();
			g.clear();
		} finally {
			ConnectionPool.putBack(conn);
		}
	}

	@Test
	public void testSysCursors2() throws CelestaException {
		Connection conn = ConnectionPool.get();
		CallContext ctxt = new CallContext(conn, "user");
		try {
			LogCursor l = new LogCursor(ctxt);
			assertEquals("log", l.meta().getName());
			l.orderBy("userid ASC", "pkvalue3 DESC", "pkvalue2");
			boolean itWas = false;
			// Неизвестная колонка
			try {
				l.orderBy("userid", "psekvalue3 ASC", "pkvsealue3");
			} catch (CelestaException e) {
				itWas = true;
			}
			assertTrue(itWas);

			// Повтор колонок
			itWas = false;
			try {
				l.orderBy("userid ASC", "pkvalue3 ASC", "pkvalue3 DESC");
			} catch (CelestaException e) {
				itWas = true;
			}
			assertTrue(itWas);

			// Пустой orderBy
			l.orderBy();

		} finally {
			ConnectionPool.putBack(conn);
		}
	}

}
