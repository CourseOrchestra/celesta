package ru.curs.celesta.showcase.utils;

import java.io.*;
import java.util.*;

import javax.xml.parsers.*;
import javax.xml.transform.TransformerException;

import org.json.*;
import org.xml.sax.SAXException;

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
	 * Преобразование XML в JSON. Все атрибуты тега переносятся в атрибуты json,
	 * имена которых начинаются с префикса @. В случае если встречен тег sorted,
	 * то все подобные теги становятся элементами json массива с именем #sorted
	 * с сохранением порядка следования в xml. Если тег содержащий атрибуты,
	 * содержит также значение, то оно переносится в json в атрибут с именем
	 * #text
	 * 
	 * @param xml
	 *            - XML строка.
	 * @return строка в формате json.
	 * @throws SAXException
	 *             метод SAXParser.parse() может вызвать это исключение в случае
	 *             возникновениея SAX-ошибок.
	 * @throws IOException
	 *             метод SAXParser.parse() может вызвать это исключение в случае
	 *             ошибок ввода-вывода.
	 */
	public static String xmlToJson(final String xml) throws SAXException, IOException {
		return xmlToJson(xml, true);
	}

	/**
	 * Преобразования XML в JSON. Все атрибуты тега переносятся в атрибуты json,
	 * имена которых начинаются или не начинаются с префикса @, в зависимости от
	 * значения входной переменной isAttributesPrefixPresented. В случа если
	 * встречен тег sorted, то все подобные теги становятся элементами json
	 * массива с именем #sorted с сохранением порядка следования в xml. Если тег
	 * содержащий атрибуты, содержит также значение, то оно переносится в json в
	 * атрибут с именем #text
	 * 
	 * @param xml
	 *            - XML строка.
	 * @param isAttributesPrefixPresented
	 *            - булевская переменная, отвечающая за то, будут ли атрибуты
	 *            json начинаться с префикса @.
	 * @return строка в формате json.
	 * @throws SAXException
	 * @throws IOException
	 */
	public static String xmlToJson(final String xml, final boolean isAttributesPrefixPresented)
			throws SAXException, IOException {
		if (xml == null || xml.isEmpty()) {
			return "";
		}

		SAXParser parser = createSAXParser();
		XMLToJSONConverterSaxHandler handler =
			new XMLToJSONConverterSaxHandler(isAttributesPrefixPresented);
		// Данный код вставлен для разрешения случая, когда в xml-файле имеется
		// несколько корневых эелементов, тогда как на вход SaxParser
		// должен подаваться xml-файл только с одним корневым элементом.
		String newXml = xml;
		if (newXml.startsWith("<?xml")) {
			newXml = newXml.replaceFirst("<[?]xml(.)*[?]>", "");
		}

		while (newXml.contains("&gt;")) {
			newXml = newXml.replace("&gt;", "&amp;gt;");
		}

		while (newXml.contains("&lt;")) {
			newXml = newXml.replace("&lt;", "&amp;lt;");
		}

		while (newXml.contains("&nbsp;")) {
			newXml = newXml.replace("&nbsp;", "&amp;nbsp;");
		}

		List<String> innerOfCurColIdList = new ArrayList<String>();
		if (newXml.toUpperCase().contains("<currentColumnId>".toUpperCase())) {
			String[] arr = newXml.toUpperCase().split("<currentColumnId>".toUpperCase());
			String string = "";
			int begin = 1;
			if (newXml.toUpperCase().startsWith("<currentColumnId>".toUpperCase())) {
				begin = 0;
			}
			if (arr.length > 0) {
				for (int i = begin; i < arr.length; i++) {
					int innerIndex = arr[i].indexOf("</currentColumnId>".toUpperCase());
					string = arr[i].substring(0, innerIndex);
					int outerIndex = newXml.toUpperCase().indexOf(string);
					innerOfCurColIdList.add(newXml.substring(outerIndex,
							outerIndex + string.length()));
				}
			}
		}

		int j = 0;
		for (String content : innerOfCurColIdList) {
			newXml = newXml.replace(content, "innerOfCurColIdList" + j);
			j++;
		}

		List<String> innerOfSortedColIdList = new ArrayList<String>();
		if (newXml.contains("<sortedColumn")) {
			String[] arr = newXml.split("<sortedColumn");
			String string = "";
			int begin = 1;
			if (newXml.startsWith("<sortedColumn")) {
				begin = 0;
			}
			if (arr.length > 0) {
				for (int i = begin; i < arr.length; i++) {
					int innerIndex = arr[i].indexOf("/>");
					string = arr[i].substring(0, innerIndex);
					if (string.contains("id=\"")) {
						String[] innerArr = string.split("id=\"");
						int outerIndex = innerArr[1].indexOf("\"");
						innerOfSortedColIdList.add(innerArr[1].substring(0, outerIndex));
					}
				}
			}
		}

		int k = 0;
		for (String content : innerOfSortedColIdList) {
			newXml = newXml.replace(content, "innerOfSortedColIdList" + k);
			k++;
		}

		final String tempRootForResolvingProblem = "tempRootForResolvingProblem";
		newXml =
			"<" + tempRootForResolvingProblem + ">" + newXml + "</" + tempRootForResolvingProblem
					+ ">";
		InputStream in = stringToStream(newXml);
		parser.parse(in, handler);
		JsonElement result = handler.getResult();
		String str = result.toString();
		int ind = str.indexOf(':');
		str = str.substring(ind + 1);
		ind = str.lastIndexOf("}");
		str = str.substring(0, ind);

		j = 0;
		for (String content : innerOfCurColIdList) {
			str = str.replace("innerOfCurColIdList" + j, content);
			j++;
		}

		k = 0;
		for (String content : innerOfSortedColIdList) {
			str = str.replace("innerOfSortedColIdList" + k, content);
			k++;
		}

		// while(str.contains("\\\"")) {
		// str = str.replace("\\\"","\"");}
		List<String> quoteList = handler.getQuoteList();
		for (String s : quoteList) {
			if (!str.contains(s)) {
				// while(s.contains("\"")) {
				s = s.replace("\"", "\\\"");
				// }
				str = str.replace("\"" + s + "\"", s);
			} else {
				str = str.replace("\"" + s + "\"", s);
			}
		}

		if (str.contains("&quot;")) {
			str = str.replace("&quot;", "\\\"");
		}
		if (str.contains("&lt;")) {
			str = str.replace("&lt;", "<");
		}
		if (str.contains("&gt;")) {
			str = str.replace("&gt;", ">");
		}

		return str;
	}

	/**
	 * Преобразование JSON в XML. Все атрибуты, имена которох начинаются с
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
	 * @throws JSONException
	 *             методы JSONToXMLParser(json) и JSONToXMLParser.outPrint()
	 *             могут вызывать данное исключение в случае ошибки парсинга
	 *             json-объекта.
	 * @throws TransformerException
	 *             метод JSONToXMLParser.outPrint() может вызывать данное
	 *             исключение в случае ошибки построениея DOM-модели документа,
	 *             используемой в данном методе.
	 * @throws ParserConfigurationException
	 *             метод JSONToXMLParser.outPrint() может вызывать данное
	 *             исключение в случае ошибки парсинга.
	 */
	public static String jsonToXml(final String json) throws JSONException, TransformerException,
			ParserConfigurationException {
		// return null;
		// throw new NotImplementedYetException();
		String newJson = "{\"tempRootForResolvingProblem\":" + json + "}";
		JSONToXMLParser jtxParser = new JSONToXMLParser(newJson);
		String result = jtxParser.outPrint();
		String fstr = "<tempRootForResolvingProblem>";
		// int ind1 = result.indexOf(fstr);
		int ind2 = result.indexOf("</tempRootForResolvingProblem>");
		result = result.substring(fstr.length(), ind2 - 1);
		result = result.trim();
		result = result.replaceAll(">\\n[ ]+", ">");
		if (result.contains("\\&quot;")) {
			while (result.contains("\\&quot;")) {
				result = result.replace("\\&quot;", "");
			}
		}
		return result;
	}

	/**
	 * Преобразование XML в JSONObject. Все атрибуты тега переносятся в атрибуты
	 * json, имена которых начинаются с префикса @. В случае если встречен тег
	 * sorted, то все подобные теги становятся элементами json массива с именем
	 * #sorted с сохранением порядка следования в xml. Если тег содержащий
	 * атрибуты, содержит также значение, то оно переносится в json в атрибут с
	 * именем #text
	 * 
	 * @param xml
	 *            - XML строка.
	 * @return объект JSONObject
	 * @throws SAXException
	 *             вызывается в случае ошибки конвертации из xml в json.
	 * @throws IOException
	 *             вызывается в случае ошибки ввода-вывода.
	 * @throws JSONException
	 *             вызывается методом JSONObject(str) в случае ошибки парсинга
	 *             json-объекта
	 */
	public static JSONObject xmlToJsonObject(final String xml) throws JSONException, SAXException,
			IOException {
		String str = XMLJSONConverter.xmlToJson(xml);
		JSONObject jsonObj = new JSONObject(str);
		return jsonObj;
	}

	/**
	 * Преобразования XML в JSONObject. Все атрибуты тега переносятся в атрибуты
	 * json, имена которых начинаются или не начинаются с префикса @, в
	 * зависимости от значения входной переменной isAttributesPrefixPresented. В
	 * случае если встречен тег sorted, то все подобные теги становятся
	 * элементами json массива с именем #sorted с сохранением порядка следования
	 * в xml. Если тег содержащий атрибуты, содержит также значение, то оно
	 * переносится в json в атрибут с именем #text
	 * 
	 * @param xml
	 *            - XML строка.
	 * @param isAttributesPrefixPresented
	 *            - булевская переменная, отвечающая за то, будут ли атрибуты
	 *            json начинаться с префикса @.
	 * @return объект JSONObject.
	 * @throws SAXException
	 * @throws IOException
	 * @throws JSONException
	 */
	public static JSONObject xmlToJsonObject(final String xml,
			final boolean isAttributesPrefixPresented) throws JSONException, SAXException,
			IOException {
		String str = XMLJSONConverter.xmlToJson(xml, isAttributesPrefixPresented);
		JSONObject jsonObj = new JSONObject(str);
		return jsonObj;
	}

	/**
	 * Стандартная функция для создания SAX XML Parser.
	 * 
	 * @return парсер.
	 */
	public static SAXParser createSAXParser() {
		SAXParserFactory factory = SAXParserFactory.newInstance();
		factory.setNamespaceAware(false);
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

	/**
	 * Стандартная функция для конвертаци строки в выходной поток.
	 * 
	 * @param str
	 *            - входная строка
	 * @return байтовый выхоной поток
	 */

	public static InputStream stringToStream(final String str) {
		try {
			if (str != null) {
				return new ByteArrayInputStream(str.getBytes("UTF-8"));
			}
		} catch (UnsupportedEncodingException e) {
			System.out.println(e.getMessage());
		}
		return null;

	}
}
