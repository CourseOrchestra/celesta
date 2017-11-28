package ru.curs.celesta;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;


import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.syscursors.CallLogCursor;
import ru.curs.celesta.syscursors.GrainsCursor;
import ru.curs.celesta.syscursors.LogCursor;
import ru.curs.celesta.syscursors.PermissionsCursor;
import ru.curs.celesta.syscursors.RolesCursor;

import static org.junit.jupiter.api.Assertions.*;

public class InitTest {

	private static Celesta celesta;

	@BeforeAll
	public static void init() throws IOException, CelestaException {
		Properties params = new Properties();
		params.setProperty("score.path", "score");
		params.setProperty("h2.in-memory", "true");

		try {
			celesta = Celesta.createInstance(params);
		} catch (CelestaException e) {
			// do nothing
		}
	}

	@AfterAll
	public static void destroy() throws CelestaException, SQLException {
		celesta.connectionPool.get().createStatement().execute("SHUTDOWN");
		celesta.close();
	}

	@Test
	public void testGetInstance() throws CelestaException {
		assertNotNull(celesta);
	}

	@Test
	public void grainCusrorIsCallable() throws CelestaException {
		SessionContext sc = new SessionContext("user", "S");
		try (CallContext ctxt = celesta.callContext(sc)) {
			GrainsCursor g = new GrainsCursor(ctxt);
			assertEquals("grains", g.meta().getName());
			assertEquals(8, g.getMaxStrLen("checksum"));
			assertEquals(30, g.getMaxStrLen("id"));
			assertEquals(-1, g.getMaxStrLen("message"));
			g.reset();
			g.init();
			g.clear();

			g.close();
		}
	}

	@Test
	public void logCursorIsCallable() throws CelestaException {
		SessionContext sc = new SessionContext("user", "S");
		try (CallContext ctxt = celesta.callContext(sc)) {
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

		}
	}

	@Test
	public void cursorsAreClosingOnContext() throws CelestaException {
		SessionContext sc = new SessionContext("user", "S");
		try (CallContext ctxt = celesta.callContext(sc)) {
			BasicCursor a = new LogCursor(ctxt);
			BasicCursor b = new PermissionsCursor(ctxt);
			BasicCursor c = new RolesCursor(ctxt);
			BasicCursor d = new CallLogCursor(ctxt);
			assertFalse(a.isClosed());
			assertFalse(b.isClosed());
			assertFalse(c.isClosed());
			assertFalse(d.isClosed());

			ctxt.close();

			assertTrue(a.isClosed());
			assertTrue(b.isClosed());
			assertTrue(c.isClosed());
			assertTrue(d.isClosed());
		}
	}
}
