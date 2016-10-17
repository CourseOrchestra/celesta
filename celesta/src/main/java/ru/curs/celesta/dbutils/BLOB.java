package ru.curs.celesta.dbutils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

/**
 * Класс для работы с BLOB-полями.
 */
public final class BLOB {
	private DataPage data;
	private boolean isModified;
	private int size;

	/**
	 * Пустой (NULL) BLOB.
	 */
	public BLOB() {
	}

	/**
	 * BLOB на основе данных потока.
	 * 
	 * @param source
	 *            Поток, из которого данные прочитываются в BLOB.
	 * @throws IOException
	 *             При ошибке чтения.
	 */
	BLOB(final InputStream source) throws IOException {
		InputStream counter = new InputStream() {
			@Override
			public int read() throws IOException {
				int result = source.read();
				if (result >= 0)
					size++;
				return result;
			}
		};
		int buf = counter.read();
		data = buf < 0 ? new DataPage(0) : new DataPage(buf, counter);
	}

	/**
	 * Клон-BLOB, указывающий на ту же самую страницу данных.
	 */
	public BLOB clone() {
		BLOB result = new BLOB();
		result.data = data;
		result.size = size;
		return result;
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
		size = 0;
		return new OutputStream() {
			private DataPage tail = data;

			@Override
			public void write(int b) {
				tail = tail.write(b);
				size++;
			}
		};
	}

	void saveToJDBCBlob(Blob b) throws SQLException {
		DataPage currPage = data;
		int i = 1;
		while (currPage != null && currPage.pos == currPage.data.length) {
			i += b.setBytes(i, currPage.data);
			currPage = currPage.nextPage;
		}
		if (currPage != null)
			b.setBytes(i, currPage.data, 0, currPage.pos);
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
		size = 0;
		data = null;
	}

	/**
	 * Возвращает размер данных.
	 */
	public int size() {
		return size;
	}

	/**
	 * Данные BLOB-поля.
	 */
	private static final class DataPage {
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
	}
}