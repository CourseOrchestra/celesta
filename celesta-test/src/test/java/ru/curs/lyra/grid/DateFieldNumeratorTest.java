package ru.curs.lyra.grid;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Date;

public class DateFieldNumeratorTest {

	@Test
	public void testConversion() throws InterruptedException {
		Date d = new Date();
		DateFieldEnumerator dfe = new DateFieldEnumerator();
		dfe.setValue(d);
		BigInteger bi = dfe.getOrderValue();
		assertNotNull(bi);
		Thread.sleep(1);
		dfe.setValue(new Date());
		assertTrue(bi.compareTo(dfe.getOrderValue()) < 0);
		dfe.setOrderValue(bi);
		assertEquals(d, dfe.getValue());
	}

	@Test
	public void testCompositeUsage() {
		DateFieldEnumerator dfe = new DateFieldEnumerator();
		VarcharFieldEnumerator vfe = new VarcharFieldEnumerator(VarcharFieldEnumeratorTest.DBA, 36);
		NullsLast nle = new NullsLast(dfe);

		CompositeKeyEnumerator cke = new CompositeKeyEnumerator(nle, vfe);
		Date d = new Date();
		nle.setValue(d);
		vfe.setValue("E8E2ED91-88A3-4E67-B0F2-663DEDC0260F");
		assertNotNull(cke.getValue());
		BigInteger bi = cke.getOrderValue();

		nle.setValue(null);
		vfe.setValue("");
		assertNotNull(cke.getValue());
		cke.setOrderValue(bi);
		assertEquals("E8E2ED91-88A3-4E67-B0F2-663DEDC0260F", vfe.getValue());
		assertEquals(d, dfe.getValue());
		nle.setValue(null);
		vfe.setValue("C62C02E7-8941-465A-9F2B-68C890CDC995");

		BigInteger bi2 = cke.getOrderValue();
		cke.setOrderValue(bi2);
		assertEquals("C62C02E7-8941-465A-9F2B-68C890CDC995", vfe.getValue());
		assertNull(nle.getValue());
		assertNotNull(cke.getValue());

		BigInteger bi3 = bi.add(bi2).shiftRight(1);
		cke.setOrderValue(bi3);

	}

}
