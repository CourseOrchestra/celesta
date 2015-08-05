package ru.curs.lyra;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.dbutils.LyraFormData;
import ru.curs.celesta.score.ParseException;

/**
 * Базовый класс формы Lyra.
 */
public abstract class BasicLyraForm {

	private static final String UTF_8 = "utf-8";

	/**
	 * Отыскивает первую запись в наборе записей.
	 * 
	 * @throws CelestaException
	 *             Ошибка извлечения данных из базы.
	 * @throws ParseException
	 *             Ошибка сериализации.
	 */
	public String findRec() throws CelestaException, ParseException {
		BasicCursor c = _getCursor();
		c.navigate("-");
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		serialize(c, result);
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
	 * @throws ParseException
	 *             Ошибка сериализации.
	 */
	public String revert(String data) throws CelestaException, ParseException {
		Cursor c = (Cursor) _getCursor();

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
	 *            Команда перемещения (комбинация знаков <, >, =, +, -, см.
	 *            документацию по методу курсора navigate)
	 * 
	 * @param data
	 *            сериализованный курсор.
	 * 
	 * @throws CelestaException
	 *             Ошибка извлечения данных из базы.
	 * @throws ParseException
	 *             Ошибка сериализации.
	 */
	public String move(String cmd, String data) throws CelestaException,
			ParseException {
		Cursor c = (Cursor) _getCursor();
		ByteArrayInputStream dataIS;
		try {
			dataIS = new ByteArrayInputStream(data.getBytes(UTF_8));

			deserialize(c, dataIS);

			// print c._currentValues()

			Cursor c2 = (Cursor) _getCursor();
			c2.copyFieldsFrom(c);
			if (c2.tryGetCurrent()) {
				c2.copyFieldsFrom(c);
				c2.update();
			} else {
				c.insert();
			}
			c.navigate(cmd);

			ByteArrayOutputStream result = new ByteArrayOutputStream();
			serialize(c, result);
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
	 * @throws ParseException
	 *             Ошибка сериализации.
	 */
	public String newRec() throws CelestaException, ParseException {
		Cursor c = (Cursor) _getCursor();
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
	 * @throws ParseException
	 *             Ошибка сериализации.
	 */
	public String deleteRec(String data) throws CelestaException,
			ParseException {
		Cursor c = (Cursor) _getCursor();

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

	private void serialize(BasicCursor c, OutputStream result)
			throws CelestaException, ParseException {
		_beforeSending(c);
		LyraFormData lfd = new LyraFormData(c, _getId());
		lfd.serialize(result);
	}

	private void deserialize(Cursor c, InputStream dataIS)
			throws CelestaException {
		LyraFormData lfd = new LyraFormData(dataIS);
		lfd.populateFields(c);
		_afterReceiving(c);
	}

	// CHECKSTYLE:OFF
	/*
	 * Эта группа методов именуется по правилам Python, а не Java. В Python
	 * имена protected-методов начинаются с underscore.
	 */
	public abstract BasicCursor _getCursor();

	public abstract String _getId();

	public abstract void _beforeSending(BasicCursor c);

	public abstract void _afterReceiving(Cursor c);
	// CHECKSTYLE:ON
}
