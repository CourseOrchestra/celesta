package ru.curs.lyra;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.stream.*;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.*;

/**
 * Значение поля, передаваемого в форму и обратно.
 */
public final class LyraFieldValue extends LyraNamedElement {
	static final String XML_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
	private static final long serialVersionUID = 1L;
	private final LyraFieldType lyraFieldType;
	private final Object val;
	private final boolean local;

	LyraFieldValue(LyraFieldType lyraFieldType, String fieldName, Object val, boolean local) throws CelestaException {
		super(fieldName);
		this.lyraFieldType = lyraFieldType;
		this.val = val;
		this.local = local;
	}

	/**
	 * Возвращает объект, соответствующий полю в таблице.
	 * 
	 * @param c
	 *            Колонка.
	 * @param val
	 *            Значение.
	 * @throws ParseException
	 *             неверное имя.
	 * @throws CelestaException
	 *             Неудачное присвоение.
	 */
	public static LyraFieldValue getValue(Column c, Object val) throws CelestaException {
		return new LyraFieldValue(LyraFieldType.lookupFieldType(c), c.getName(), val, false);
	}

	/**
	 * Возвращает объект, соответствующий полю в представлении.
	 * 
	 * @param c
	 *            Тип колонки.
	 * @param columnName
	 *            Имя поля.
	 * @param val
	 *            Значение.
	 * @throws CelestaException
	 *             неверное имя
	 */
	public static LyraFieldValue getValue(ViewColumnType c, String columnName, Object val) throws CelestaException {
		return new LyraFieldValue(LyraFieldType.lookupFieldType(c), columnName, val, false);
	}

	/**
	 * Сериализация.
	 * 
	 * @param xmlWriter
	 *            Объект, в который записывается XML-поток.
	 * @throws XMLStreamException
	 *             Ошибка записи в поток.
	 */
	public void serialize(XMLStreamWriter xmlWriter) throws XMLStreamException {
		xmlWriter.writeStartElement(getName());
		xmlWriter.writeAttribute("type", lyraFieldType.toString());
		xmlWriter.writeAttribute("null", Boolean.toString(val == null));
		xmlWriter.writeAttribute("local", Boolean.toString(local));

		if (val instanceof Date) {
			SimpleDateFormat sdf = new SimpleDateFormat(XML_DATE_FORMAT);
			xmlWriter.writeCharacters(val == null ? "" : sdf.format(val));
		} else {
			xmlWriter.writeCharacters(val == null ? "" : val.toString());
		}
		xmlWriter.writeEndElement();
	}

	/**
	 * Значение поля.
	 */
	public Object getValue() {
		return val;
	}

	/**
	 * Тип поля.
	 */
	public LyraFieldType getFieldType() {
		return lyraFieldType;
	}

	/**
	 * Является ли значение локальным (не взятым из курсора).
	 */
	public boolean isLocal() {
		return local;
	}
}
