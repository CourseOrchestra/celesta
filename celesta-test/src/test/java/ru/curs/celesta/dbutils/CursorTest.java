package ru.curs.celesta.dbutils;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;


import ru.curs.celesta.*;
import ru.curs.celesta.syscursors.LogSetupCursor;

public class CursorTest {

	private static Celesta celesta;

	private SessionContext sc = new SessionContext("super", "foo");
	private Cursor c;

	@BeforeAll
	public static void init() throws IOException, CelestaException {
		Properties properties = new Properties();
		properties.setProperty("score.path", "score");
		properties.setProperty("h2.in-memory", "true");

		celesta = Celesta.createInstance(properties);
	}

	@AfterAll
	public static void destroy() throws CelestaException, SQLException {
		celesta.callContext(new SessionContext("super", "foo")).getConn().createStatement().execute("SHUTDOWN");
		celesta.close();
	}

	@BeforeEach
	public void before() throws CelestaException {
		c = new LogSetupCursor(celesta.callContext(sc));
	}

	@AfterEach
	public void after() throws CelestaException {
		c.close();
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

	@Test
	public void testClose() throws Exception {
		BasicCursor xRec = c.getXRec();

		Object[] rec = {"f1", "f2", "f3", "f4", "f5"};

		c.getHelper.getHolder().getStatement(rec, 0);
		c.insert.getStatement(rec, 0);

		boolean[] updateMask = {true, false, false, true, true};
		c.updateMask = updateMask;
		boolean[] nullUpdateMask = {false, true, true, false, false};
		c.nullUpdateMask = nullUpdateMask;

		c.update.getStatement(rec, 0);
		c.delete.getStatement(rec, 0);

		c.set.getStatement(rec, 0);
		c.forwards.getStatement(rec, 0);
		c.backwards.getStatement(rec, 0);
		c.here.getStatement(rec, 0);
		c.first.getStatement(rec, 0);
		c.last.getStatement(rec, 0);
		c.count.getStatement(rec, 0);
		c.position.getStatement(rec, 0);


        assertAll(
				() -> assertFalse(c.isClosed()),
				() -> assertFalse(xRec.isClosed()),
                () -> assertTrue(c.getHelper.getHolder().isStmtValid()),
                () -> assertTrue(c.insert.isStmtValid()),
                () -> assertTrue(c.update.isStmtValid()),
                () -> assertTrue(c.delete.isStmtValid()),

                () -> assertTrue(c.set.isStmtValid()),
                () -> assertTrue(c.forwards.isStmtValid()),
                () -> assertTrue(c.backwards.isStmtValid()),
                () -> assertTrue(c.here.isStmtValid()),
                () -> assertTrue(c.first.isStmtValid()),
                () -> assertTrue(c.last.isStmtValid()),
                () -> assertTrue(c.count.isStmtValid()),
                () -> assertTrue(c.position.isStmtValid())
        );

		c.close();

		assertAll(
				() -> assertTrue(xRec.isClosed()),
				() -> assertFalse(c.getHelper.getHolder().isStmtValid()),
				() -> assertFalse(c.insert.isStmtValid()),
				() -> assertFalse(c.update.isStmtValid()),
				() -> assertFalse(c.delete.isStmtValid()),

				() -> assertFalse(c.set.isStmtValid()),
				() -> assertFalse(c.forwards.isStmtValid()),
				() -> assertFalse(c.backwards.isStmtValid()),
				() -> assertFalse(c.here.isStmtValid()),
				() -> assertFalse(c.first.isStmtValid()),
				() -> assertFalse(c.last.isStmtValid()),
				() -> assertFalse(c.count.isStmtValid()),
				() -> assertFalse(c.position.isStmtValid())
		);

	}
}
