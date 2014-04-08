package ru.curs.celesta.showcase.utils;

import java.io.*;

import javax.xml.parsers.*;
import javax.xml.transform.TransformerException;

import org.json.JSONException;
import org.xml.sax.SAXException;

import ru.curs.celesta.showcase.utils.JSONToXMLParser;

import com.google.gson.JsonElement;

/**
 * Класс преобразования xml в JSON и обратно - JSON в xml.
 * 
 * @author bogatov
 * 
 */
public final class XMLJSONConverter {

	private XMLJSONConverter() {

	}

	/**
	 * Преобразования XML в JSON. Все атрибуты тега переносятся в атрибуты json,
	 * имена которох начинаются с префикса @. В случа если встречен тег sorted,
	 * то все подобные теги становятся элементами json массива с именем #sorted
	 * с сохранением порядка следования в xml. Если тег содержащий атрибуты,
	 * содержит также значение, то оно переносится в json в атрибут с именем
	 * #text
	 * 
	 * @param xml
	 *            - XML строка.
	 * @return строка в формате json.
	 * @throws SAXException
	 * @throws IOException
	 */
	public static String xmlToJson(final String xml) throws SAXException, IOException {
		SAXParser parser = createSAXParser();
		XMLToJSONConverterSaxHandler handler = new XMLToJSONConverterSaxHandler();
		InputStream in = stringToStream(xml);
		parser.parse(in, handler);
		JsonElement result = handler.getResult();
		return result.toString();
	}

	/**
	 * Преобразование JSON в XML. Все атрибуты имена которох начинаются с
	 * префикса @ переносятся в xml в виде соответствующего атрибута тега. В
	 * случа если встречен атрибут с именем #sorted, то все дочернии элементы
	 * переносятся в xml с сохранением порядка элементов в json
	 * массиве(#sorted). В случа если встречен атрибут с именем #text, то его
	 * значение переносится как значение соответствующего (с именем
	 * родительского атрибута) тега.
	 * 
	 * @param json
	 *            - JSON строка
	 * @return xml строка.
	 */
	public static String jsonToXml(final String json) throws JSONException, TransformerException,
			ParserConfigurationException {
		// return null;
		// throw new NotImplementedYetException();
		JSONToXMLParser jtxParser = new JSONToXMLParser(json);
		String result = jtxParser.outPrint();
		return result;
	}

	/**
	 * Стандартная функция для создания SAX XML Parser.
	 * 
	 * @return парсер.
	 */
	public static SAXParser createSAXParser() {
		SAXParserFactory factory = SAXParserFactory.newInstance();
		factory.setNamespaceAware(true);
		factory.setValidating(false);
		SAXParser parser = null;
		try {
			factory.setFeature("http://xml.org/sax/features/namespace-prefixes", true);
			parser = factory.newSAXParser();
		} catch (SAXException | ParserConfigurationException e) {
			// throw new Exception(e);
			System.out.println(e.getMessage());
		}
		return parser;
	}

	public static InputStream stringToStream(final String str) {
		try {
			return new ByteArrayInputStream(str.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			System.out.println(e.getMessage());
		}
		return null;

	}
}
