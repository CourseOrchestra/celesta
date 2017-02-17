package ru.curs.lyra;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.Cursor;

/**
 * Base Java class for Lyra card form.
 */
public abstract class BasicCardForm extends BasicLyraForm {

	private static final String UTF_8 = "utf-8";
	private LyraFormData lfd;

	public BasicCardForm(CallContext context) throws CelestaException {
		super(context);
	}

	/**
	 * Отыскивает первую запись в наборе записей.
	 * 
	 * @throws CelestaException
	 *             Ошибка извлечения данных из базы.
	 */
	public String findRec() throws CelestaException {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		serialize(rec(), result);
		try {
			return result.toString(UTF_8);
		} catch (UnsupportedEncodingException e) {
			return "";
		}
	}

	/**
	 * Отменяет текущие изменения в курсоре и возвращает актуальную информацию
	 * из базы данных.
	 * 
	 * @param data
	 *            сериализованный курсор
	 * 
	 * @throws CelestaException
	 *             Ошибка извлечения данных из базы.
	 */
	public synchronized String revert(String data) throws CelestaException {

		Cursor c = getCursor();

		ByteArrayInputStream dataIS;
		try {
			dataIS = new ByteArrayInputStream(data.getBytes(UTF_8));
			deserialize(c, dataIS);
			c.navigate("=<>");
			ByteArrayOutputStream result = new ByteArrayOutputStream();
			serialize(c, result);
			return result.toString(UTF_8);
		} catch (UnsupportedEncodingException e) {
			return "";
		}
	}

	/**
	 * Перемещает курсор.
	 * 
	 * @param cmd
	 *            Команда перемещения (комбинация знаков &lt;, &gt;, =, +, -, см.
	 *            документацию по методу курсора navigate)
	 * 
	 * @param data
	 *            сериализованный курсор.
	 * 
	 * @throws CelestaException
	 *             Ошибка извлечения данных из базы.
	 */
	public synchronized String move(String cmd, String data) throws CelestaException {
		try {
			BasicCursor rec = rec();
			if (rec instanceof Cursor) {
				Cursor c = (Cursor) rec;
				ByteArrayInputStream dataIS = new ByteArrayInputStream(data.getBytes(UTF_8));
				deserialize(c, dataIS);
				if (!c.tryUpdate())
					c.insert();
			}
			rec.navigate(cmd);
			ByteArrayOutputStream result = new ByteArrayOutputStream();
			serialize(rec(), result);
			return result.toString(UTF_8);
		} catch (UnsupportedEncodingException e) {
			return "";
		}
	}

	/**
	 * Инициирует новую запись для вставки в базу данных.
	 * 
	 * @throws CelestaException
	 *             Ошибка извлечения данных из базы.
	 */
	public synchronized String newRec() throws CelestaException {
		Cursor c = getCursor();
		c.clear();
		c.setRecversion(0);
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		serialize(c, result);
		try {
			return result.toString(UTF_8);
		} catch (UnsupportedEncodingException e) {
			return "";
		}
	}

	/**
	 * Удаляет текущую запись.
	 * 
	 * @param data
	 *            сериализованный курсор.
	 * 
	 * @throws CelestaException
	 *             Ошибка извлечения данных из базы.
	 */
	public synchronized String deleteRec(String data) throws CelestaException {
		Cursor c = getCursor();

		ByteArrayInputStream dataIS;
		try {
			dataIS = new ByteArrayInputStream(data.getBytes(UTF_8));

			deserialize(c, dataIS);

			c.delete();
			if (!c.navigate(">+")) {
				c.clear();
				c.setRecversion(0);
			}
			ByteArrayOutputStream result = new ByteArrayOutputStream();
			serialize(c, result);
			return result.toString(UTF_8);
		} catch (UnsupportedEncodingException e) {
			return "";
		}
	}

	void serialize(BasicCursor c, OutputStream result) throws CelestaException {
		_beforeSending(c);
		lfd = new LyraFormData(c, getFieldsMeta(), _getId());
		lfd.serialize(result);
	}

	void deserialize(Cursor c, InputStream dataIS) throws CelestaException {
		lfd = new LyraFormData(dataIS);
		lfd.populateFields(c, getFieldsMeta());
		_afterReceiving(c);
	}

	// CHECKSTYLE:OFF
	/*
	 * This methods are named in Python style, not Java style. This is why
	 * methods meant to be protected are called starting from underscore.
	 */

	public abstract void _afterReceiving(BasicCursor c);
	// CHECKSTYLE:ON
}
