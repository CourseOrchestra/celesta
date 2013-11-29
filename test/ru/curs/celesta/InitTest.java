package ru.curs.celesta;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import ru.curs.celesta.syscursors.GrainsCursor;

public class InitTest {

	@BeforeClass
	public static void init() throws IOException, CelestaException {
		Properties params = new Properties();
		params.load(InitTest.class.getResourceAsStream("test.properties"));// celesta.oracle.properties
		Celesta.initialize(params);
	}

	@Test
	public void test1() throws CelestaException {
		Celesta.getInstance();
	}

	@Test
	public void testSysCursors() throws CelestaException {
		Connection conn = ConnectionPool.get();
		CallContext ctxt = new CallContext(conn, "user", null);
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

}
