package ru.curs.celesta.dbutils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Класс для работы с BLOB-полями.
 */
public class BLOB {
	private DataPage data;
	private boolean isModified = false;

	BLOB(DataPage data) {
		this.data = data;
	}

	/**
	 * Были ли данные BLOB-а изменены.
	 */
	public boolean isModified() {
		return isModified;
	}

	/**
	 * Возвращает поток для чтения данных.
	 */
	public InputStream getInStream() {
		return data == null ? null : data.getInStream();
	}

	/**
	 * Возвращает поток для записи данных, сбросив при этом текущие данные
	 * BLOB-а.
	 */
	public OutputStream getOutStream() {
		isModified = true;
		data = new DataPage();
		return new OutputStream() {
			private DataPage tail = data;

			@Override
			public void write(int b) {
				tail = tail.write(b);
			}
		};
	}

	/**
	 * Принимает ли данное поле в таблице значение NULL.
	 */
	public boolean isNull() {
		return data == null;
	}

	/**
	 * Сбрасывает BLOB в значение NULL.
	 */
	public void setNull() {
		isModified = isModified || (data != null);
		data = null;
	}

	/**
	 * Вычисляет размер данных. Внимание: операция может быть дорогостоящей для
	 * данных, превосходящих 64К.
	 */
	public int size() {
		return data == null ? 0 : data.size();
	}
}

/**
 * Данные BLOB-поля.
 */
final class DataPage {
	private static final int DEFAULT_PAGE_SIZE = 0xFFFF;
	private static final int BYTE_MASK = 0xFF;

	private final byte[] data;
	private DataPage nextPage;
	private int pos;

	DataPage() {
		this(DEFAULT_PAGE_SIZE);
	}

	private DataPage(int size) {
		data = new byte[size];
	}

	private DataPage(int firstByte, InputStream source) throws IOException {
		this();
		int buf = firstByte;
		while (pos < data.length && buf >= 0) {
			data[pos++] = (byte) buf;
			buf = source.read();
		}
		nextPage = buf < 0 ? null : new DataPage(buf, source);
	}

	static DataPage load(InputStream source) throws IOException {
		int buf = source.read();
		return buf < 0 ? new DataPage(0) : new DataPage(buf, source);
	}

	DataPage write(int b) {
		if (pos < data.length) {
			data[pos++] = (byte) (b & BYTE_MASK);
			return this;
		} else {
			DataPage result = new DataPage();
			nextPage = result;
			return result.write(b);
		}
	}

	InputStream getInStream() {
		return new InputStream() {
			private int i = 0;
			private DataPage currentPage = DataPage.this;

			@Override
			public int read() {
				if (i < currentPage.pos)
					return (int) currentPage.data[i++] & BYTE_MASK;
				else if (currentPage.nextPage != null) {
					i = 0;
					currentPage = currentPage.nextPage;
					return read();
				} else {
					return -1;
				}
			}
		};
	}

	int size() {
		int result = 0;
		DataPage currPage = this;
		while (currPage.pos == currPage.data.length) {
			result += currPage.pos;
			currPage = currPage.nextPage;
		}
		return result + currPage.pos;
	}
}