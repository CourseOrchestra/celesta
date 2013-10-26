package ru.curs.celesta.dbutils;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import org.junit.Test;

public class BLOBTest {

	private final static byte[] shortData = { 10, 15, -1, 111, 2, 55, 6, 7, 15,
			4, 65, -117, 8, 55, -2 };

	@Test
	public void test1() {
		// 1. Нулевой BLOB
		BLOB a = new BLOB(null);
		assertTrue(a.isNull());
		assertNull(a.getInStream());
		assertFalse(a.isModified());
		assertEquals(0, a.size());
	}

	@Test
	public void test2() throws IOException {
		subTest(shortData);
	}

	@Test
	public void test3() throws IOException {
		byte[] longData = new byte[100000];
		Random rnd = new Random();
		rnd.nextBytes(longData);
		subTest(longData);
	}

	@Test
	public void test4() throws IOException {
		subTest2(shortData);
	}

	@Test
	public void test5() throws IOException {
		byte[] longData = new byte[100000];
		Random rnd = new Random();
		rnd.nextBytes(longData);
		subTest2(longData);
	}

	@Test
	public void test6() throws IOException {
		DataPage d = DataPage.load(new InputStream() {
			private int i = 0;

			@Override
			public int read() {
				return i < shortData.length ? (int) shortData[i++] & 0xFF : -1;
			}
		});

		BLOB a = new BLOB(d);

		byte[] longData = new byte[90000];
		Random rnd = new Random();
		rnd.nextBytes(longData);

		assertFalse(a.isModified());
		OutputStream os = a.getOutStream();
		assertTrue(a.isModified());
		os.write(longData);
		assertTrue(a.isModified());
		InputStream is = a.getInStream();
		assertNotNull(is);
		int b = is.read();
		int i = 0;
		while (b >= 0) {
			assertEquals(longData[i++], (byte) b);
			b = is.read();
		}
		assertEquals(longData.length, a.size());
	}

	private void subTest2(final byte[] data) throws IOException {
		BLOB a = new BLOB(null);
		assertFalse(a.isModified());
		OutputStream os = a.getOutStream();
		os.write(data);
		assertTrue(a.isModified());
		assertFalse(a.isNull());
		InputStream is = a.getInStream();
		assertNotNull(is);
		int b = is.read();
		int i = 0;
		while (b >= 0) {
			assertEquals(data[i++], (byte) b);
			b = is.read();
		}
		assertEquals(data.length, i);

		assertEquals(data.length, a.size());
	}

	private void subTest(final byte[] data) throws IOException {
		DataPage d = DataPage.load(new InputStream() {
			private int i = 0;

			@Override
			public int read() {
				return i < data.length ? (int) data[i++] & 0xFF : -1;
			}
		});

		BLOB a = new BLOB(d);

		assertFalse(a.isNull());
		InputStream is = a.getInStream();
		assertNotNull(is);
		int b = is.read();
		int i = 0;
		while (b >= 0) {
			assertEquals(data[i++], (byte) b);
			b = is.read();
		}
		assertEquals(data.length, i);
		assertFalse(a.isModified());

		assertEquals(data.length, a.size());

		// Тестируем также установку null-а
		a.setNull();
		assertTrue(a.isModified());
		assertTrue(a.isNull());
		assertNull(a.getInStream());

		assertEquals(0, a.size());

		a.setNull();
		assertTrue(a.isModified());
		assertTrue(a.isNull());
		assertNull(a.getInStream());

	}
}
