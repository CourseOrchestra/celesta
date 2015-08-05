package ru.curs.celesta.dbutils;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;
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
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.syscursors.GrainsCursor;

public class SerializerTest {

	private SessionContext sc = new SessionContext("super", "foo");
	private GrainsCursor c;
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
		c = new GrainsCursor(new CallContext(conn, sc));
	}

	@After
	public void after() throws CelestaException {
		ConnectionPool.putBack(conn);
	}

	@Test
	public void test() throws ParseException, CelestaException,
			UnsupportedEncodingException {
		LyraFormData fd = new LyraFormData();
		Date d = new Date();

		fd.addValue("z", 123);
		fd.addValue("aa", "русский текст");
		fd.addValue("fe", d);
		fd.addValue("bs", true);
		fd.addNullValue(LyraFieldType.BIT, "we");

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		fd.serialize(bos);

		String expected = String
				.format("<?xml version=\"1.0\" ?><schema recversion=\"0\">"
						+ "<z type=\"INT\" null=\"false\">123</z><aa type=\"VARCHAR\" null=\"false\">русский текст</aa>"
						+ "<fe type=\"DATETIME\" null=\"false\">%s</fe><bs type=\"BIT\" null=\"false\">true</bs>"
						+ "<we type=\"BIT\" null=\"true\"></we></schema>",
						new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(d));

		String actual = bos.toString("utf-8");
		assertEquals(expected, actual);
	}

	@Test
	public void test2() throws CelestaException, ParseException,
			UnsupportedEncodingException {
		c.get("celesta");
		LyraFormData fd = new LyraFormData(c, "sdasdf");
		fd.addValue("aa", "русский текст");
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		fd.serialize(bos);
		String buf = bos.toString("utf-8");
		assertEquals(8, c.getChecksum().length());
		GrainsCursor c2 = new GrainsCursor(new CallContext(conn, sc));
		fd.populateFields(c2);
		assertEquals(c.getChecksum(), c2.getChecksum());
		assertEquals(c.getLastmodified(), c2.getLastmodified());

		ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());

		bos.reset();
		LyraFormData fd2 = new LyraFormData(bis);
		fd2.serialize(bos);
		assertEquals(buf, bos.toString("utf-8"));
		System.out.println(buf);

		c2.clear();
		fd2.populateFields(c2);
		assertEquals(c.getChecksum(), c2.getChecksum());
		assertEquals(c.getLastmodified().getTime(), c2.getLastmodified()
				.getTime(), 1000);

	}
}
