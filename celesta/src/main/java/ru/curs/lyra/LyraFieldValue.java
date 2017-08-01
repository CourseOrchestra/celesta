package ru.curs.lyra;

import static ru.curs.lyra.LyraFormField.*;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.stream.*;

import ru.curs.celesta.CelestaException;

/**
 * Значение поля, передаваемого в форму и обратно.
 */
public final class LyraFieldValue extends LyraNamedElement {
	static final String XML_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";

	private final Object val;
	private final LyraFormField lff;

	LyraFieldValue(LyraFormField lff, Object val) throws CelestaException {
		super(lff.getName());
		this.lff = lff;
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
		xmlWriter.writeAttribute("type", lff.getType().toString());

		if (lff.getSubtype() != null)
			xmlWriter.writeAttribute(SUBTYPE, lff.getSubtype());

		if (val == null)
			xmlWriter.writeAttribute("null", Boolean.toString(true));
		if (lff.getScale() != DEFAULT_SCALE)
			xmlWriter.writeAttribute(SCALE, Integer.toString(lff.getScale()));
		if (lff.isRequired())
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
	 * Метаданные поля.
	 */
	public LyraFormField meta() {
		return lff;
	}

}
