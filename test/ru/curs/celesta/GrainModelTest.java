package ru.curs.celesta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

public class GrainModelTest {

	@Test
	public void test2() throws ParseException {
		// Корректное и некорректное добавление таблицы
		GrainModel gm = new GrainModel();
		Table t = new Table(gm, "aa");
		t = new Table(gm, "bb");
		assertEquals(2, gm.getTables().size());
		boolean itWas = false;
		try {
			t = new Table(gm, "aa");
		} catch (ParseException e) {
			itWas = true;
		}
		assertTrue(itWas);
		assertEquals(2, gm.getTables().size());

		t = gm.getTables().get("aa");
		assertEquals("aa", t.getName());
		t = gm.getTables().get("bb");
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
		t = gm.getTables().get("aa");
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
		GrainModel gm = new GrainModel();
		Table t1 = new Table(gm, "t1");
		Column cc = new IntegerColumn(t1, "ida");
		cc.setNullableAndDefault(false, "IDENTITY");

		t1.addPK("ida");
		t1.finalizePK();
		new IntegerColumn(t1, "intcol");
		new DateTimeColumn(t1, "datecol");

		Table t2 = new Table(gm, "t2");
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

		assertEquals(2, gm.getTables().size());
		assertSame(gm, t1.getGrainModel());
		assertSame(gm, t2.getGrainModel());

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
		assertEquals(1, t1.getForeignKeys().iterator().next().getColumns()
				.size());
		assertSame(t2, fk.getReferencedTable());

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

		Table t3 = new Table(gm, "t3");
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
			// NVARCHAR(5)
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
			// Ошибка, потому что scol2 -- NVARCHAR(2), а в T3 первичный ключ --
			// NVARCHAR(5)
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

		Table t4 = new Table(gm, "t4");
		cc = new IntegerColumn(t4, "idd1");
		cc.setNullableAndDefault(false, "-1");

		cc = new IntegerColumn(t4, "idd2");
		cc.setNullableAndDefault(false, "123");

		t4.addPK("idd1");
		t4.addPK("idd2");
		t4.finalizePK();
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
		GrainModel gm = new GrainModel();

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
		assertSame(FKBehaviour.NO_ACTION, fk.getDeleteBehaviour());
		assertSame(FKBehaviour.NO_ACTION, fk.getUpdateBehaviour());

		boolean itWas = false;
		try {
			fk.setDeleteBehaviour(FKBehaviour.SET_NULL);
		} catch (ParseException e) {
			// нельзя использовать SET NULL в Not-nullable колонках
			itWas = true;
		}
		assertTrue(itWas);
		assertSame(FKBehaviour.NO_ACTION, fk.getDeleteBehaviour());
		fk.setDeleteBehaviour(FKBehaviour.CASCADE);
		assertSame(FKBehaviour.CASCADE, fk.getDeleteBehaviour());

		itWas = false;
		try {
			fk.setUpdateBehaviour(FKBehaviour.SET_NULL);
		} catch (ParseException e) {
			// нельзя использовать SET NULL в Not-nullable колонках
			itWas = true;
		}
		assertTrue(itWas);
		assertSame(FKBehaviour.NO_ACTION, fk.getUpdateBehaviour());
		fk.setUpdateBehaviour(FKBehaviour.CASCADE);
		assertSame(FKBehaviour.CASCADE, fk.getUpdateBehaviour());

	}

}
