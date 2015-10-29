package ru.curs.lyra.grid;

import static org.junit.Assert.*;

import java.math.BigInteger;

import org.junit.Test;

import ru.curs.celesta.CelestaException;

class DummyKeyMgr extends KeyEnumerator {

	private int card;
	private int val;

	public int getVal() {
		return val;
	}

	public void setVal(int val) {
		this.val = val;
	}

	DummyKeyMgr(int cardinality) {
		this.card = cardinality;
	}

	@Override
	public BigInteger cardinality() {
		return BigInteger.valueOf(card);
	}

	@Override
	public BigInteger getOrderValue() {
		return BigInteger.valueOf(val);
	}

	@Override
	public void setOrderValue(BigInteger value) {
		val = value.intValue();
	}

	@Override
	public void setValue(Object value) {

	}

	@Override
	public Object getValue() {
		return null;
	}

}

public class CompositeKeyMgrTest {

	@Test
	public void test1() throws CelestaException {
		DummyKeyMgr km = new DummyKeyMgr(100);
		CompositeKeyEnumerator ckm = new CompositeKeyEnumerator(km);
		assertEquals(BigInteger.valueOf(100), ckm.cardinality());
		assertEquals(BigInteger.ZERO, km.getOrderValue());
		assertEquals(BigInteger.ZERO, ckm.getOrderValue());
		ckm.setOrderValue(BigInteger.valueOf(5));
		assertEquals(5, km.getVal());
		assertEquals(BigInteger.valueOf(5), km.getOrderValue());
		assertEquals(BigInteger.valueOf(5), ckm.getOrderValue());

		ckm.setOrderValue(BigInteger.valueOf(11));
		assertEquals(11, km.getVal());
		assertEquals(BigInteger.valueOf(11), km.getOrderValue());
		assertEquals(BigInteger.valueOf(11), ckm.getOrderValue());
	}

	@Test
	public void test2() throws CelestaException {
		DummyKeyMgr km1 = new DummyKeyMgr(7);
		DummyKeyMgr km2 = new DummyKeyMgr(10);
		DummyKeyMgr km3 = new DummyKeyMgr(17);

		CompositeKeyEnumerator ckm = new CompositeKeyEnumerator(km1, km2, km3);
		assertEquals(BigInteger.valueOf(1190), ckm.cardinality());
		assertEquals(BigInteger.ZERO, ckm.getOrderValue());
		km1.setVal(3);
		km2.setVal(9);
		km3.setVal(11);
		assertEquals(BigInteger.valueOf(674), ckm.getOrderValue());
		ckm.setOrderValue(BigInteger.ZERO);
		assertEquals(0, km1.getVal());
		assertEquals(0, km2.getVal());
		assertEquals(0, km3.getVal());

		ckm.setOrderValue(BigInteger.ONE);
		assertEquals(0, km1.getVal());
		assertEquals(0, km2.getVal());
		assertEquals(1, km3.getVal());

		ckm.setOrderValue(BigInteger.valueOf(674));
		assertEquals(3, km1.getVal());
		assertEquals(9, km2.getVal());
		assertEquals(11, km3.getVal());

		ckm.setOrderValue(BigInteger.valueOf(1189));
		assertEquals(6, km1.getVal());
		assertEquals(9, km2.getVal());
		assertEquals(16, km3.getVal());
	}

}
