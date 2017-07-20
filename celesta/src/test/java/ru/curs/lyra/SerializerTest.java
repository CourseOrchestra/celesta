package ru.curs.lyra;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
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
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.syscursors.GrainsCursor;
import ru.curs.celesta.syscursors.TablesCursor;

public class SerializerTest {

	private SessionContext sc = new SessionContext("super", "foo");
	private GrainsCursor c;
	private TablesCursor tt;
	private Connection conn;

	@BeforeClass
	public static void init() throws IOException, CelestaException {
		Properties params = new Properties();
		params.setProperty("score.path", "score");
		params.setProperty("h2.in-memory", "true");
		ConnectionPool.clear();
		AppSettings.init(params);
		try {
			Celesta.initialize(params);
		} catch (CelestaException e) {
			// do nothing, Celesta is initialized
		}
	}

	@Before
	public void before() throws CelestaException {
		conn = ConnectionPool.get();
		CallContext context = new CallContext(conn, sc);
		c = new GrainsCursor(context);
		tt = new TablesCursor(context);
	}

	@After
	public void after() throws CelestaException {
		ConnectionPool.putBack(conn);
	}

	@Test
	public void test() throws CelestaException, UnsupportedEncodingException {

		Map<String, LyraFormField> metaform = new LinkedHashMap<>();
		LyraFormField lff;
		lff = new LyraFormField("z", new FieldAccessor() {
			@Override
			public Object getValue(Object[] c) {
				return 123;
			}

			@Override
			public void setValue(BasicCursor c, Object newValue) throws CelestaException {

			}
		});
		lff.setType(LyraFieldType.INT);
		lff.setRequired(true);
		metaform.put(lff.getName(), lff);

		lff = new LyraFormField("aa", new FieldAccessor() {
			@Override
			public Object getValue(Object[] c) {
				return "русский текст";
			}

			@Override
			public void setValue(BasicCursor c, Object newValue) throws CelestaException {

			}
		});
		lff.setType(LyraFieldType.VARCHAR);
		metaform.put(lff.getName(), lff);

		final Date d = new Date();
		lff = new LyraFormField("fe", new FieldAccessor() {
			@Override
			public Object getValue(Object[] c) {
				return d;
			}

			@Override
			public void setValue(BasicCursor c, Object newValue) throws CelestaException {

			}
		});
		lff.setType(LyraFieldType.DATETIME);
		metaform.put(lff.getName(), lff);

		lff = new LyraFormField("bs", new FieldAccessor() {
			@Override
			public Object getValue(Object[] c) {
				return true;
			}

			@Override
			public void setValue(BasicCursor c, Object newValue) throws CelestaException {

			}
		});
		lff.setType(LyraFieldType.BIT);
		metaform.put(lff.getName(), lff);

		lff = new LyraFormField("we", new FieldAccessor() {
			@Override
			public Object getValue(Object[] c) {
				return null;
			}

			@Override
			public void setValue(BasicCursor c, Object newValue) throws CelestaException {

			}
		});
		lff.setType(LyraFieldType.BIT);
		metaform.put(lff.getName(), lff);

		LyraFormData fd = new LyraFormData(tt, metaform, "myform");

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		fd.serialize(bos);

		String expected = String.format(
				"<?xml version=\"1.0\" ?><schema recversion=\"0\" formId=\"myform\">"
						+ "<z type=\"INT\" required=\"true\">123</z><aa type=\"VARCHAR\">русский текст</aa>"
						+ "<fe type=\"DATETIME\">%s</fe><bs type=\"BIT\">true</bs>"
						+ "<we type=\"BIT\" null=\"true\"></we></schema>",
				new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(d));

		String actual = bos.toString("utf-8");

		// System.out.println(actual);

		assertEquals(expected, actual);
	}

	@Test
	public void test2() throws CelestaException, UnsupportedEncodingException {
		BasicCardForm bcf = new BasicCardForm(new CallContext(conn, sc)) {
			{
				createAllBoundFields();
				createField("aab");
			}

			@Override
			public BasicCursor _getCursor(CallContext context) {
				try {
					return new GrainsCursor(context);
				} catch (CelestaException e) {
					return null;
				}
			}

			@Override
			public String _getId() {
				return "sdasdf";
			}

			@Override
			public LyraFormField _createUnboundField(LyraNamedElementHolder<LyraFormField> meta, String name) {
				try {
					LyraFormField lff = new LyraFormField(name, new FieldAccessor() {
						@Override
						public Object getValue(Object[] c) {
							return "русский текст";
						}

						@Override
						public void setValue(BasicCursor c, Object newValue) throws CelestaException {
							assertEquals("русский текст", newValue);
						}
					});
					lff.setType(LyraFieldType.VARCHAR);

					meta.addElement(lff);
					return lff;
				} catch (CelestaException e) {
					e.printStackTrace();
					return null;
				}

			}

			@Override
			public void _beforeSending(BasicCursor c) {

			}

			@Override
			public void _afterReceiving(BasicCursor c) {

			}

			@Override
			protected void _createAllUnboundFields(LyraNamedElementHolder<LyraFormField> fieldsMeta) {
				// do nothing for this test
			}

			@Override
			public LyraFormProperties getFormProperties() {
				// do nothing for this test
				return null;
			}

		};

		c.get("celesta");
		c.setRecversion(11);

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		bcf.serialize(c, bos);
		String buf = bos.toString("utf-8");
		// System.out.println(buf);
		assertEquals(8, c.getChecksum().length());

		ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		GrainsCursor c2 = new GrainsCursor(new CallContext(conn, sc));
		bcf.deserialize(c2, bis);

		assertEquals(c.getRecversion(), c2.getRecversion());
		assertEquals(c.getChecksum(), c2.getChecksum());
		assertEquals(c.getLastmodified().getTime(), c2.getLastmodified().getTime(), 1000);

		bos.reset();
		bcf.serialize(c2, bos);

		assertEquals(buf, bos.toString("utf-8"));
		// System.out.println(buf);

		c2.clear();
		assertEquals("", c2.getChecksum());
		bis = new ByteArrayInputStream(bos.toByteArray());
		bcf.deserialize(c2, bis);

		assertEquals(c.getChecksum(), c2.getChecksum());
		assertEquals(c.getLastmodified().getTime(), c2.getLastmodified().getTime(), 1000);

	}
}
