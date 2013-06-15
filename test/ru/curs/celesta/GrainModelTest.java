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
		gm.addTable(t);
		t = new Table(gm, "bb");
		gm.addTable(t);
		assertEquals(2, gm.getTables().size());
		t = new Table(gm, "aa");
		boolean itWas = false;
		try {
			gm.addTable(t);
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
		Column c = new IntegerColumn("col1");
		t.addColumn(c);
		c = new DateTimeColumn("col2");
		c.setNullableAndDefault(false, "GETDATE");
		assertTrue(((DateTimeColumn) c).isGetdate());
		assertFalse(c.isNullable());
		t.addColumn(c);
		c = new StringColumn("col3");
		t.addColumn(c);
		c.setNullableAndDefault(false, "'-'");
		assertEquals(3, t.getColumns().size());
		c = new DateTimeColumn("col2");
		itWas = false;
		try {
			t.addColumn(c);
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
		Column cc = new IntegerColumn("ida");
		cc.setNullableAndDefault(false, "IDENTITY");
		t1.addColumn(cc);
		t1.addPK("ida");
		t1.finalizePK();
		t1.addColumn(new IntegerColumn("intcol"));
		t1.addColumn(new DateTimeColumn("datecol"));
		gm.addTable(t1);

		Table t2 = new Table(gm, "t2");
		cc = new IntegerColumn("idb");
		cc.setNullableAndDefault(false, "IDENTITY");
		t2.addColumn(cc);
		t2.addPK("idb");
		t2.finalizePK();

		t2.addColumn(new IntegerColumn("intcol"));
		t2.addColumn(new DateTimeColumn("datecol"));
		StringColumn c = new StringColumn("scol2");
		c.setLength("2");
		t2.addColumn(c);
		c = new StringColumn("scol5");
		c.setLength("5");
		t2.addColumn(c);

		gm.addTable(t2);
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
		c = new StringColumn("idc");
		c.setLength("5");
		c.setNullableAndDefault(false, "");
		t3.addColumn(c);
		t3.addPK("idc");
		t3.finalizePK();
		gm.addTable(t3);

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
		cc = new IntegerColumn("idd1");
		cc.setNullableAndDefault(false, "-1");
		t4.addColumn(cc);
		cc = new IntegerColumn("idd2");
		cc.setNullableAndDefault(false, "123");
		t4.addColumn(cc);
		t4.addPK("idd1");
		t4.addPK("idd2");
		t4.finalizePK();
		gm.addTable(t4);

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
		IntegerColumn c = new IntegerColumn("c1");
		c.setNullableAndDefault(false, "IDENTITY");
		t1.addColumn(c);
		t1.addPK("c1");
		t1.finalizePK();
		gm.addTable(t1);

		Table t2 = new Table(gm, "t2");
		c = new IntegerColumn("c1");
		c.setNullableAndDefault(false, "IDENTITY");
		t2.addColumn(c);
		t2.addPK("c1");
		t2.finalizePK();
		c = new IntegerColumn("c2");
		c.setNullableAndDefault(false, "123");
		t2.addColumn(c);

		ForeignKey fk = new ForeignKey(t2);
		fk.addColumn("c2");
		fk.setReferencedTable("", "t1");
		assertSame(FKBehaviour.NO_ACTION, fk.getDeleteBehaviour());
		assertSame(FKBehaviour.NO_ACTION, fk.getUpdateBehaviour());

		gm.addTable(t2);

		boolean itWas = false;
		try {
			fk.setDeleteBehaviour(FKBehaviour.SETNULL);
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
			fk.setUpdateBehaviour(FKBehaviour.SETNULL);
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
