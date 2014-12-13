package ru.curs.celesta.dbutils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.Celesta;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.InitTest;
import ru.curs.celesta.SessionContext;
import ru.curs.celesta.syscursors.LogSetupCursor;

public class BasicCursorTest {
	private SessionContext sc = new SessionContext("super", "foo");
	private Cursor c;
	private Connection conn;

	@BeforeClass
	public static void init() throws IOException, CelestaException {
		Properties params = new Properties();
		params.load(InitTest.class.getResourceAsStream("test.properties"));// celesta.oracle.properties
		ConnectionPool.clear();
		try {
			Celesta.initialize(params);
		} catch (CelestaException e) {
			// Do nothing, celesta is initialized!
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
	public void testWhere() throws CelestaException {

		assertEquals(
				"((\"grainid\" < ?) or ((\"grainid\" = ?) and (\"tablename\" < ?)))",
				c.getNavigationWhereClause('<'));
		assertEquals(
				"((\"grainid\" > ?) or ((\"grainid\" = ?) and (\"tablename\" > ?)))",
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
				"((\"d\" > ?) or ((\"d\" = ?) and ((\"i\" > ?) or ((\"i\" = ?) and ((\"m\" < ?) or ((\"m\" = ?) and ((\"grainid\" > ?) or ((\"grainid\" = ?) and (\"tablename\" > ?)))))))))",
				c.getNavigationWhereClause('>'));
		assertEquals(
				"((\"d\" < ?) or ((\"d\" = ?) and ((\"i\" < ?) or ((\"i\" = ?) and ((\"m\" > ?) or ((\"m\" = ?) and ((\"grainid\" < ?) or ((\"grainid\" = ?) and (\"tablename\" < ?)))))))))",
				c.getNavigationWhereClause('<'));
		assertEquals("((\"grainid\" = ?) and (\"tablename\" = ?))",
				c.getNavigationWhereClause('='));

		c.orderBy("grainid", "m DESC");
		assertEquals("\"grainid\", \"m\" desc, \"tablename\"", c.getOrderBy());
		assertEquals("\"grainid\" desc, \"m\", \"tablename\" desc",
				c.getReversedOrderBy());
		assertEquals(
				"((\"grainid\" > ?) or ((\"grainid\" = ?) and ((\"m\" < ?) or ((\"m\" = ?) and (\"tablename\" > ?)))))",
				c.getNavigationWhereClause('>'));
		assertEquals(
				"((\"grainid\" < ?) or ((\"grainid\" = ?) and ((\"m\" > ?) or ((\"m\" = ?) and (\"tablename\" < ?)))))",
				c.getNavigationWhereClause('<'));
		assertEquals("((\"grainid\" = ?) and (\"tablename\" = ?))",
				c.getNavigationWhereClause('='));

	}

	@Test
	public void test2() throws CelestaException {
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

}
