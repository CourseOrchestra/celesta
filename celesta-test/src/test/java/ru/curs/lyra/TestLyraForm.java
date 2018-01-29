package ru.curs.lyra;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map.Entry;


import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.score.*;

public class TestLyraForm {

	static class DummyMeta implements ColumnMeta {
		private final String cDoc;

		DummyMeta(String cDoc) {
			this.cDoc = cDoc;
		}

		@Override
		public String jdbcGetterName() {
			return null;
		}

		@Override
		public String getCelestaType() {
			return null;
		}

		@Override
		public boolean isNullable() {
			return false;
		}

		@Override
		public String getCelestaDoc() {
			return cDoc;
		}

	}
	
	private static String testJSON(String test) throws CelestaException{
		return (new DummyMeta(test)).getCelestaDocJSON();
	}

	@Test
	public void test1() throws CelestaException {
		assertEquals("{}", testJSON(""));
		assertEquals("{}", testJSON(null));
		assertEquals("{}", testJSON("   sfdwer фывафа "));
		boolean exception = false;
		try {
			assertEquals("{}", testJSON("   sfdwer фы{вафа "));
		} catch (Exception e) {
			exception = true;
		}
		assertTrue(exception);
		assertEquals("{ва}", testJSON("   sfdwer фы{ва}фа "));
		assertEquals("{\"}\"}", testJSON("   sfdwer фы{\"}\"}фа "));
		assertEquals("{\"\\\"}\"aa}", testJSON("   sfdwer фы{\"\\\"}\"aa}фа "));
		String jsonTest = "{\"object_or_array\": \"ob{j}}}e\\\"ct\",\n" + "\"empty\": false,\n"
				+ "\"parse_time_nanoseconds\": 19608,\n" + "\"validate\": true,\n" + "\"size\": 1\n" + "}";
		assertEquals(jsonTest, testJSON(jsonTest));
		assertEquals(jsonTest, testJSON("foo" + jsonTest));
	}

	@Test
	public void test2() throws Exception {
		AbstractScore s = ScoreAccessor.createEmptyScore();

		final Grain g;

		File f = new File(this.getClass().getResource("test.sql").getPath());
		try (
				InputStream is1 = new FileInputStream(f);
				InputStream is2 = new FileInputStream(f)
		) {
			CelestaParser cp1 = new CelestaParser(is1, "utf-8");
			CelestaParser cp2 = new CelestaParser(is2, "utf-8");
			GrainPart gp = cp1.extractGrainInfo(s, f);
			g = cp2.parseGrainPart(gp);
		}

		Table t = g.getElement("table1", Table.class);
		BasicLyraForm blf = new BasicLyraForm(t) {

			{
				createAllBoundFields();
			}

			@Override
			public BasicCursor _getCursor(CallContext context) {
				return null;
			}

			@Override
			public String _getId() {
				return null;
			}

			@Override
			public LyraFormField _createUnboundField(LyraNamedElementHolder<LyraFormField> meta, String name) {
				return null;
			}

			@Override
			protected void _createAllUnboundFields(LyraNamedElementHolder<LyraFormField> fieldsMeta) {
				// do nothing
			}

			@Override
			public LyraFormProperties getFormProperties() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void _beforeSending(BasicCursor c) {
				// TODO Auto-generated method stub

			}
		};

		String[] names = { "column1", "column2", "column3" };
		String[] captions = { "длинный кириллический текст", "текст с \"кавычками\"", "column3" };
		boolean[] visibles = { false, true, true };
		boolean[] editables = { true, false, true };
		int i = 0;
		for (Entry<String, LyraFormField> e : blf.getFieldsMeta().entrySet()) {
			assertEquals(names[i], e.getKey());
			assertEquals(names[i], e.getValue().getName());
			assertEquals(captions[i], e.getValue().getCaption());
			assertEquals(visibles[i], e.getValue().isVisible());
			assertEquals(editables[i], e.getValue().isEditable());
			i++;
		}
		assertEquals(names.length, i);

	}
}
