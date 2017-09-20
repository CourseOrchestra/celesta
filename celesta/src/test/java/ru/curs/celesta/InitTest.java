package ru.curs.celesta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.syscursors.CallLogCursor;
import ru.curs.celesta.syscursors.GrainsCursor;
import ru.curs.celesta.syscursors.LogCursor;
import ru.curs.celesta.syscursors.PermissionsCursor;
import ru.curs.celesta.syscursors.RolesCursor;

public class InitTest {

	@BeforeClass
	public static void init() throws IOException, CelestaException {
		Properties params = new Properties();
		params.setProperty("score.path", "score");
		params.setProperty("h2.in-memory", "true");

		AppSettings appSettings = new AppSettings(params);

		ConnectionPoolConfiguration cpc = new ConnectionPoolConfiguration();
		cpc.setJdbcConnectionUrl(appSettings.getDatabaseConnection());
		cpc.setDriverClassName(appSettings.getDbClassName());
		cpc.setLogin(appSettings.getDBLogin());
		cpc.setPassword(appSettings.getDBPassword());

		ConnectionPool.init(cpc);

		ConnectionPool.clear();
		try {
			Celesta.initialize(params);
		} catch (CelestaException e) {
			// do nothing
		}
	}

	@Test
	public void testGetInstance() throws CelestaException {
		assertNotNull(Celesta.getInstance());
	}

	@Test
	public void grainCusrorIsCallable() throws CelestaException {
		Connection conn = ConnectionPool.get();
		SessionContext sc = new SessionContext("user", "S");
		CallContext ctxt = new CallContext(conn, sc);
		try {
			GrainsCursor g = new GrainsCursor(ctxt);
			assertEquals("grains", g.meta().getName());
			assertEquals(8, g.getMaxStrLen("checksum"));
			assertEquals(30, g.getMaxStrLen("id"));
			assertEquals(-1, g.getMaxStrLen("message"));
			g.reset();
			g.init();
			g.clear();

			g.close();
		} finally {
			ConnectionPool.putBack(conn);
		}
	}

	@Test
	public void logCursorIsCallable() throws CelestaException {
		Connection conn = ConnectionPool.get();
		SessionContext sc = new SessionContext("user", "S");
		CallContext ctxt = new CallContext(conn, sc);
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

	@Test
	public void cursorsAreClosingOnContext() throws CelestaException {
		Connection conn = ConnectionPool.get();
		SessionContext sc = new SessionContext("user", "S");
		CallContext ctxt = new CallContext(conn, sc);
		try {
			BasicCursor a = new LogCursor(ctxt);
			BasicCursor b = new PermissionsCursor(ctxt);
			BasicCursor c = new RolesCursor(ctxt);
			BasicCursor d = new CallLogCursor(ctxt);
			assertFalse(a.isClosed());
			assertFalse(b.isClosed());
			assertFalse(c.isClosed());
			assertFalse(d.isClosed());

			ctxt.closeCursors();

			assertTrue(a.isClosed());
			assertTrue(b.isClosed());
			assertTrue(c.isClosed());
			assertTrue(d.isClosed());
		} finally {
			ConnectionPool.putBack(conn);
		}
	}
}
