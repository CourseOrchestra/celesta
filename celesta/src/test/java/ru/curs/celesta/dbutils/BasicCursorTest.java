package ru.curs.celesta.dbutils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.curs.celesta.AppSettings;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.Celesta;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.SessionContext;
import ru.curs.celesta.syscursors.LogSetupCursor;

public class BasicCursorTest {
	private SessionContext sc = new SessionContext("super", "foo");
	private BasicCursor c;
	private Connection conn;

	@BeforeClass
	public static void init() throws IOException, CelestaException {
		Properties params = new Properties();
		params.setProperty("score.path", "score");
		params.setProperty("h2.in-memory", "true");
		AppSettings.init(params);
		ConnectionPool.clear();
		try {
			Celesta.initialize(params);
		} catch (CelestaException e) {
			// do nothing, Celesta is initialized!
		}
	}

	@Before
	public void before() throws CelestaException {
		conn = ConnectionPool.get();
		c = new LogSetupCursor(new CallContext(conn, sc));
	}

	@After
	public void after() throws CelestaException {
		ConnectionPool.putBack(conn);
	}

	@Test
	public void cursorIsNavigable() throws CelestaException {
		c.setFilter("grainid", "'b'%");
		LogSetupCursor c2 = (LogSetupCursor) c;
		c2.setGrainid("grainval");
		c2.setTablename("tablenameval");
		c2.setI(true);
		c2.setM(false);
		c2.setD(true);
		boolean itWas = false;
		try {
			c.navigate("=s><+-");
		} catch (CelestaException e) {
			// System.out.println(e.getMessage());
			itWas = true;
		}
		assertTrue(itWas);
		c.navigate("=><+-");
	}

	@Test
	public void fieldsAreAssignable() throws CelestaException {
		LogSetupCursor lsc = (LogSetupCursor) c;
		assertNull(lsc.getGrainid());
		lsc.setValue("grainid", "asdFsaf");

		assertEquals("asdFsaf", lsc.getGrainid());

		boolean itWas = false;
		try {
			lsc.setValue("asdfasdf", "sswe");
		} catch (CelestaException e) {
			itWas = true;
		}
		assertTrue(itWas);

	}
}
