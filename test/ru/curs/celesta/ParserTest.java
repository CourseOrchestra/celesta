package ru.curs.celesta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;

import org.junit.Test;

public class ParserTest {

	@Test
	public void test1() throws ParseException {
		InputStream input = ParserTest.class.getResourceAsStream("test.txt");
		CelestaParser cp = new CelestaParser(input);
		GrainModel m = cp.model();

		Set<Table> s = m.getTables();
		assertEquals(2, s.size());

		Iterator<Table> i = s.iterator();
		// Первая таблица
		Table t = i.next();
		assertEquals("table1", t.getName());

		Iterator<Column> ic = t.getColumns().iterator();
		Column c = ic.next();
		assertEquals("column1", c.getName());
		assertTrue(c instanceof IntegerColumn);
		assertTrue(c.isNullable());

		c = ic.next();
		assertEquals("column2", c.getName());
		assertTrue(c instanceof FloatingColumn);
		assertFalse(c.isNullable());
		assertEquals(-12323.2, ((FloatingColumn) c).getDefaultvalue(), .00001);

		c = ic.next();
		assertEquals("c3", c.getName());
		assertTrue(c instanceof BooleanColumn);
		assertFalse(c.isNullable());

		c = ic.next();
		assertEquals("aaa", c.getName());
		assertTrue(c instanceof StringColumn);
		assertFalse(c.isNullable());
		assertEquals("testtes'ttest", ((StringColumn) c).getDefaultValue());
		assertEquals(23, ((StringColumn) c).getLength());
		assertFalse(((StringColumn) c).isMax());

		c = ic.next();
		assertEquals("bbb", c.getName());
		assertTrue(c instanceof StringColumn);
		assertTrue(c.isNullable());
		assertTrue(((StringColumn) c).isMax());

		c = ic.next();
		assertEquals("ccc", c.getName());
		assertTrue(c instanceof BinaryColumn);
		assertTrue(c.isNullable());

		c = ic.next();
		assertEquals("e", c.getName());
		assertTrue(c instanceof IntegerColumn);
		assertTrue(c.isNullable());
		assertEquals(-112, (int) ((IntegerColumn) c).getDefaultvalue());

		c = ic.next();
		assertEquals("f", c.getName());
		assertTrue(c instanceof FloatingColumn);
		assertTrue(c.isNullable());
		assertEquals(null, ((FloatingColumn) c).getDefaultvalue());

		// Пустая таблица
		t = i.next();
		assertEquals("table2", t.getName());
		ic = t.getColumns().iterator();

		c = ic.next();

		c = ic.next();
		assertEquals("column2", c.getName());
		assertTrue(c instanceof DateTimeColumn);
		assertTrue(c.isNullable());
		Date d = ((DateTimeColumn) c).getDefaultValue();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		assertEquals("2011-12-31", df.format(d));

	}

}
