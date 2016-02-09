package ru.curs.lyra;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import ru.curs.celesta.CelestaException;

/**
 * Значение поля, передаваемого в форму и обратно.
 */
public final class LyraFieldValue extends LyraNamedElement {
	static final String XML_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
	private final LyraFieldType lyraFieldType;
	private final Object val;
	private final int scale;

	LyraFieldValue(LyraFormField lff, Object val) throws CelestaException {
		super(lff.getName());
		this.lyraFieldType = lff.getType();
		this.val = val;
		this.scale = lff.getScale();
	}

	LyraFieldValue(LyraFieldType lyraFieldType, String fieldName, Object val, int scale) throws CelestaException {
		super(fieldName);
		this.lyraFieldType = lyraFieldType;
		this.val = val;
		this.scale = scale;
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
		if (val == null)
			xmlWriter.writeAttribute("null", Boolean.toString(true));
		if (scale != LyraFormField.DEFAULT_SCALE)
			xmlWriter.writeAttribute("scale", Integer.toString(scale));

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
	 * Число знаков после запятой.
	 */
	public int getScale() {
		return scale;
	}

}
