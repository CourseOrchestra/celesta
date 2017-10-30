package ru.curs.celesta.score;


import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import ru.curs.celesta.CelestaException;

public class GrainModelTest {

	private Score s = new Score();

	@BeforeAll
	public static void Setup() throws CelestaException {
		Properties p = new Properties();
		p.setProperty("score.path", ".");
		p.setProperty("rdbms.connection.url", "jdbc:oracle:123");
	}

	@Test
	public void test1() throws ParseException {
		Grain g = new Grain(s, "grain1");
		assertSame(g, s.getGrain("grain1"));

		Table t = new Table(g, "table1");
		(new IntegerColumn(t, "a")).setNullableAndDefault(false, "IDENTITY");
		new IntegerColumn(t, "b").setNullableAndDefault(false, "0");
		new IntegerColumn(t, "c").setNullableAndDefault(false, "0");
		new IntegerColumn(t, "d").setNullableAndDefault(false, "0");
		new BinaryColumn(t, "e");
		t.addPK("a");
		t.finalizePK();

		assertEquals(5, t.getColumns().size());
		assertEquals(0, t.getColumnIndex("a"));
		assertEquals(2, t.getColumnIndex("c"));
		
		Index ind = new Index(g, "table1", "aa_i1");
		ind.addColumn("b");
		ind.addColumn("d");
		ind.finalizeIndex();

		assertEquals(1, g.getIndices().size());
		assertSame(ind, g.getIndices().get("aa_i1"));
		assertEquals(2, ind.getColumns().size());

		boolean itWas = false;
		try {
			// Нельзя вставить в модель два индекса с одним и тем же именем.
			ind = new Index(g, "table1", "aa_i1");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);

		ind = new Index(g, "table1", "aa_i2");

		itWas = false;
		try {
			// Нельзя индексировать IMAGE-поля.
			ind.addColumn("e");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);

		ind.addColumn("c");
		ind.addColumn("d");

		itWas = false;
		try {
			// Нельзя дважды вставить в индекс одно и то же поле.
			ind.addColumn("c");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);

		ind.finalizeIndex();

		assertEquals(2, g.getIndices().size());
		assertSame(ind, g.getIndices().get("aa_i2"));
		assertEquals(2, ind.getColumns().size());

		// Нелзя создавать полностью дублирующиеся индексы.
		ind = new Index(g, "table1", "aa_i3");
		ind.addColumn("b");
		ind.addColumn("d");
		itWas = false;
		try {
			ind.finalizeIndex();
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
		ind.addColumn("c");
		itWas = false;
		try {
			// Нельзя индексировать nullable-колонки
			ind.addColumn("e");
		} catch (ParseException e) {
			itWas = true;
		}

		ind.finalizeIndex();
		assertEquals(3, g.getIndices().size());
		assertSame(ind, g.getIndices().get("aa_i3"));
		assertEquals(3, ind.getColumns().size());
	}

	@Test
	public void test2() throws ParseException {
		// Корректное и некорректное добавление таблицы
		Grain g = new Grain(s, "grain2");
		Table t = new Table(g, "aa");
		t = new Table(g, "bb");
		assertEquals(2, g.getElements(Table.class).size());
		boolean itWas = false;
		try {
			t = new Table(g, "aa");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
		assertEquals(2, g.getElements(Table.class).size());

		t = g.getElements(Table.class).get("aa");
		assertEquals("aa", t.getName());
		t = g.getElements(Table.class).get("bb");
		assertEquals("bb", t.getName());
		// Корректное и некорректное добавление поля
		Column c = new IntegerColumn(t, "col1");

		c = new DateTimeColumn(t, "col2");
		c.setNullableAndDefault(false, "GETDATE");
		assertTrue(((DateTimeColumn) c).isGetdate());
		assertFalse(c.isNullable());

		c = new StringColumn(t, "col3");

		c.setNullableAndDefault(false, "'-'");
		assertEquals(3, t.getColumns().size());

		itWas = false;
		try {
			c = new DateTimeColumn(t, "col2");

		} catch (ParseException e) {
			itWas = true;
		}

		assertTrue(itWas);
		assertEquals(3, t.getColumns().size());
		c = t.getColumns().get("col2");
		assertFalse(c.isNullable());
		assertTrue(((DateTimeColumn) c).isGetdate());
		assertEquals("col2", c.getName());
		// Корректное и некорректное добавление первичного ключа
		t.addPK("col2");
		itWas = false;
		try {
			t.addPK("blahblah");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
		t.addPK("col3");
		Map<String, Column> key = t.getPrimaryKey();
		assertEquals(2, key.size());
		Iterator<Column> ic = key.values().iterator();
		c = ic.next();
		assertEquals("col2", c.getName());
		c = ic.next();
		assertEquals("col3", c.getName());
		t.finalizePK();
		itWas = false;
		try {
			t.addPK("col1");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
		t.finalizePK(); // вызывать можно более одного раза, если PK определён

		// вызывать нельзя ни разу, если PK не определён
		t = g.getElements(Table.class).get("aa");
		itWas = false;
		try {
			t.finalizePK();
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);

	}

	@Test
	public void test3() throws ParseException {
		Grain g = new Grain(s, "grain3");
		Table t1 = new Table(g, "t1");
		Column cc = new IntegerColumn(t1, "ida");
		cc.setNullableAndDefault(false, "IDENTITY");

		t1.addPK("ida");
		t1.finalizePK();
		new IntegerColumn(t1, "intcol");
		new DateTimeColumn(t1, "datecol");

		Table t2 = new Table(g, "t2");
		cc = new IntegerColumn(t2, "idb");
		cc.setNullableAndDefault(false, "IDENTITY");
		t2.addPK("idb");
		t2.finalizePK();

		new IntegerColumn(t2, "intcol");
		new DateTimeColumn(t2, "datecol");
		StringColumn c = new StringColumn(t2, "scol2");
		c.setLength("2");

		c = new StringColumn(t2, "scol5");
		c.setLength("5");

		assertEquals(2, g.getElements(Table.class).size());
		assertSame(g, t1.getGrain());
		assertSame(g, t2.getGrain());

		// Неизвестную колонку в FK нельзя включать!
		ForeignKey fk = new ForeignKey(t1);
		boolean itWas = false;
		try {
			fk.addColumn("abracadabra");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);

		// Устанавливаем связь t1 --> t2 по простому внешнему ключу
		assertEquals(0, t1.getForeignKeys().size());
		fk = new ForeignKey(t1);
		fk.addColumn("intcol");
		// Дважды одну и ту же колонку в FK нельзя включать!
		itWas = false;
		try {
			fk.addColumn("intcol");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
		assertEquals(0, t1.getForeignKeys().size());

		// Установка referencedtable финализирует внешний ключ
		fk.setReferencedTable("", "t2");
		assertEquals(1, t1.getForeignKeys().size());
		assertEquals(1, t1.getForeignKeys().iterator().next().getColumns().size());
		assertSame(t2, fk.getReferencedTable());

		itWas = false;
		try {
			// Несуществующее поле нельзя добавить в ссылку
			fk.addReferencedColumn("blahblah");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
		// Просто поле добавить можно
		fk.addReferencedColumn("intcol");
		try {
			// Но в момент финализации происходит проверка, что мы указали
			// первичный ключ.
			fk.finalizeReference();
			// Для удобства тестирования и экономии памяти список ссылок
			// подчищается сразу за финализацией, он нигде не хранится и нигде
			// не доступен. Его единственная роль -- проверять правильность
			// текста.
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
		fk.addReferencedColumn("idb");
		fk.finalizeReference();

		// Проверяем невозможность вставки двух идентичных FK в одну и ту же
		// таблицу
		fk = new ForeignKey(t1);
		fk.addColumn("intcol");
		itWas = false;
		try {
			fk.setReferencedTable("", "t2");
		} catch (ParseException e) {
			itWas = true;
		}

		Table t3 = new Table(g, "t3");
		c = new StringColumn(t3, "idc");
		c.setLength("5");
		c.setNullableAndDefault(false, "");

		t3.addPK("idc");
		t3.finalizePK();

		// Теперь проверяем, что ключи могут указывать лишь на совпадающие по
		// типу поля
		fk = new ForeignKey(t2);
		fk.addColumn("datecol");
		itWas = false;
		try {
			// Ошибка, потому что datecol -- Datetime, а в T3 первичный ключ --
			// VARCHAR(5)
			fk.setReferencedTable("", "t3");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
		assertEquals(0, t2.getForeignKeys().size());

		fk = new ForeignKey(t2);
		fk.addColumn("scol2");
		itWas = false;
		try {
			// Ошибка, потому что scol2 -- VARCHAR(2), а в T3 первичный ключ --
			// VARCHAR(5)
			fk.setReferencedTable("", "t3");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
		assertEquals(0, t2.getForeignKeys().size());

		// Вот теперь должно быть ОК!
		fk = new ForeignKey(t2);
		fk.addColumn("scol5");
		fk.setReferencedTable("", "t3");
		assertEquals(1, t2.getForeignKeys().size());
		assertSame(t3, fk.getReferencedTable());

		fk.addReferencedColumn("idc");
		fk.finalizeReference();

		Table t4 = new Table(g, "t4");
		cc = new IntegerColumn(t4, "idd1");
		cc.setNullableAndDefault(false, "-1");

		cc = new IntegerColumn(t4, "idd2");
		cc.setNullableAndDefault(false, "123");

		t4.addPK("idd1");
		t4.addPK("idd2");
		t4.finalizePK();
		new IntegerColumn(t4, "dummy1");
		new IntegerColumn(t4, "dummy2");
		// А теперь проверяем выставление составного FK
		fk = new ForeignKey(t2);
		fk.addColumn("idb");
		itWas = false;
		try {
			// Коротковат ключик
			fk.setReferencedTable("", "t4");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
		assertEquals(1, t2.getForeignKeys().size());

		fk = new ForeignKey(t2);
		fk.addColumn("idb");
		fk.addColumn("datecol");
		itWas = false;
		try {
			// Длина подходящая, но тип неподоходящий
			fk.setReferencedTable("", "t4");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
		assertEquals(1, t2.getForeignKeys().size());

		// А теперь всё должно быть ОК!
		fk = new ForeignKey(t2);
		fk.addColumn("idb");
		fk.addColumn("intcol");
		fk.setReferencedTable("", "t4");
		assertEquals(2, t2.getForeignKeys().size());
		assertSame(t4, fk.getReferencedTable());

		fk.addReferencedColumn("idd1");
		fk.addReferencedColumn("dummy1");
		itWas = false;
		try {
			fk.finalizeReference();
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
		fk.addReferencedColumn("idd1");
		fk.addReferencedColumn("idd2");
		fk.finalizeReference();

		// Проверяем невозможность вставки двух идентичных FK в одну и ту же
		// таблицу
		fk = new ForeignKey(t2);
		fk.addColumn("idb");
		fk.addColumn("intcol");
		itWas = false;
		try {
			fk.setReferencedTable("", "t4");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
	}

	@Test
	public void test4() throws ParseException {
		Grain gm = new Grain(s, "grain4");

		Table t1 = new Table(gm, "t1");
		IntegerColumn c = new IntegerColumn(t1, "c1");
		c.setNullableAndDefault(false, "IDENTITY");

		t1.addPK("c1");
		t1.finalizePK();

		Table t2 = new Table(gm, "t2");
		c = new IntegerColumn(t2, "c1");
		c.setNullableAndDefault(false, "IDENTITY");

		t2.addPK("c1");
		t2.finalizePK();
		c = new IntegerColumn(t2, "c2");
		c.setNullableAndDefault(false, "123");

		ForeignKey fk = new ForeignKey(t2);
		fk.addColumn("c2");
		fk.setReferencedTable("", "t1");
		assertSame(FKRule.NO_ACTION, fk.getDeleteRule());
		assertSame(FKRule.NO_ACTION, fk.getUpdateRule());

		boolean itWas = false;
		try {
			fk.setDeleteRule(FKRule.SET_NULL);
		} catch (ParseException e) {
			// нельзя использовать SET NULL в Not-nullable колонках
			itWas = true;
		}
		assertTrue(itWas);
		assertSame(FKRule.NO_ACTION, fk.getDeleteRule());
		fk.setDeleteRule(FKRule.CASCADE);
		assertSame(FKRule.CASCADE, fk.getDeleteRule());

		itWas = false;
		try {
			fk.setUpdateRule(FKRule.SET_NULL);
		} catch (ParseException e) {
			// нельзя использовать SET NULL в Not-nullable колонках
			itWas = true;
		}
		assertTrue(itWas);
		assertSame(FKRule.NO_ACTION, fk.getUpdateRule());
		fk.setUpdateRule(FKRule.CASCADE);
		assertSame(FKRule.CASCADE, fk.getUpdateRule());

	}

}
