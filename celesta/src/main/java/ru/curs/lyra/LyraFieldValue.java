package ru.curs.lyra;

import static ru.curs.lyra.LyraFormField.DEFAULT_SCALE;
import static ru.curs.lyra.LyraFormField.REQUIRED;
import static ru.curs.lyra.LyraFormField.SCALE;
import static ru.curs.lyra.LyraFormField.SUBTYPE;

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
	private final boolean required;
	private final String subtype;

	LyraFieldValue(LyraFormField lff, Object val) throws CelestaException {
		super(lff.getName());
		this.lyraFieldType = lff.getType();
		this.subtype = lff.getSubtype();
		this.scale = lff.getScale();
		this.required = lff.isRequired();
		this.val = val;
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

		if (subtype != null)
			xmlWriter.writeAttribute(SUBTYPE, subtype);

		if (val == null)
			xmlWriter.writeAttribute("null", Boolean.toString(true));
		if (scale != DEFAULT_SCALE)
			xmlWriter.writeAttribute(SCALE, Integer.toString(scale));
		if (required)
			xmlWriter.writeAttribute(REQUIRED, Boolean.toString(true));

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

	/**
	 * Является ли поле обязательным.
	 */
	public boolean isRequired() {
		return required;
	}

	/**
	 * Field's subtype.
	 */
	public String getSubtype() {
		return subtype;
	}

}
