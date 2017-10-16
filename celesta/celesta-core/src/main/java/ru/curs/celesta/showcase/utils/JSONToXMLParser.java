package ru.curs.celesta.showcase.utils;

import java.io.*;
import java.util.*;

import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.json.*;
import org.w3c.dom.*;

/**
 * Преобразование из json в xml.
 * 
 * @author borodanev
 * 
 */
public class JSONToXMLParser {
	private static final String UNSUCCESSFUL =
		"Не удалось добавить элемент \"%s\" в DOM-модель xml%n";
	private DocumentBuilder builder;
	private Transformer t;
	private final JSONTokener jt;
	private JSONObject jo = null;
	private JSONObject templateJOInitial = new JSONObject();
	private StringBuffer sbuf;
	private Boolean vBool = false;
	private boolean bLeft = false;
	private boolean bRight = false;
	private boolean bCenter = false;

	public JSONToXMLParser(String json) {
		String newJson = json;

		if (newJson.contains("&lt;")) {
			bLeft = true;
		}

		if (newJson.contains("&gt;")) {
			bRight = true;
		}

		if (newJson.contains("&nbsp;")) {
			bCenter = true;
		}

		if (newJson.contains("'{}'")) {
			newJson = newJson.replaceAll("[{][}]", "{\"myTagForResolvingProblem\"=\"2\"}");
		}

		if (newJson.contains("{}")) {
			newJson = newJson.replaceAll("[{][}]", "{'myTagForResolvingProblem'='2'}");
		}

		// Обработка тега <template> для объекта Chart
		if (newJson.contains("chartsettings") && newJson.contains("template")
				&& newJson.contains("plot")) {
			try {
				JSONObject jsonSerg = new JSONObject(newJson);
				List<String> arList = new ArrayList<>();
				List<JSONObject> jsonList = new ArrayList<>();
				String[] ar = JSONObject.getNames(jsonSerg);
				arList.add(ar[0]);
				jsonSerg = jsonSerg.getJSONObject(ar[0]);
				ar = JSONObject.getNames(jsonSerg);
				jsonSerg = jsonSerg.getJSONObject(ar[0]);
				arList.add(ar[0]);
				ar = JSONObject.getNames(jsonSerg);
				for (String ttt : ar) {
					arList.add(ttt);
					jsonList.add(jsonSerg.getJSONObject(ttt));
				}

				JSONObject templateJO = new JSONObject();
				templateJO.put("myChartTemplateTag", "sergio");
				JSONObject newJO = new JSONObject();
				Map<String, JSONObject> mapSerg = new HashMap<>();
				for (int kkk = arList.size() - 1; kkk >= 2; kkk--) {
					mapSerg.put(arList.get(kkk), jsonList.get(kkk - 2));
					if ("template".equals(arList.get(kkk)))
						templateJOInitial = jsonList.get(kkk - 2);
				}
				mapSerg.put("template", templateJO);
				newJO.put(arList.get(1), mapSerg);
				JSONObject newJO1 = new JSONObject();
				newJO = newJO1.put(arList.get(0), newJO);
				newJson = newJO.toString();
			} catch (JSONException e1) {
				// TODO Auto-generated catch block
				System.out.println(e1);
			}
		}

		jt = new JSONTokener(newJson);

		try {
			jo = new JSONObject(jt);
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	/**
	 * Функция, выдающая результирующую xml-строку.
	 * 
	 * @return выходная xml-строка
	 * @throws TransformerException
	 *             вызывется в случае ошибки построениея DOM-модели документа,
	 *             используемой в данном методе.
	 * @throws ParserConfigurationException
	 *             вызываtется в случае ошибки парсинга.
	 * @throws JSONException
	 *             вызывается в случае ошибки парсинга json-объекта.
	 */

	public String outPrint() throws TransformerException, JSONException,
			ParserConfigurationException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		builder = factory.newDocumentBuilder();
		Document doc = builder.newDocument();

		doc = gettingDocRoot(doc, jo);
		doc.normalizeDocument();

		Writer strWriter = new StringWriter();
		StreamResult streamRes = new StreamResult(strWriter);

		t = TransformerFactory.newInstance().newTransformer();
		t.setOutputProperty(OutputKeys.INDENT, "yes");
		t.transform(new DOMSource(doc), streamRes);
		String outString = strWriter.toString();
		// outString = outString.replaceFirst("<[?]xml(.)*[?]>", "");
		outString = outString.replace(" standalone=\"no\"", "");
		// outString = outString.replaceFirst("[?]>", "?>\r\n");
		outString = outString.trim();

		if (sbuf != null) {
			String sBufStr = sbuf.toString();
			sBufStr = sBufStr.replaceAll("<[?]xml(.)*[?]>", "");
			return sBufStr;
		}

		if (outString.contains("xmlns2") && vBool) {
			outString = outString.replaceFirst("xmlns2", "xmlns");
		}

		while (outString.contains("{\"myTagForResolvingProblem\"=\"2\"}")) {
			int ind = outString.indexOf("{\"myTagForResolvingProblem\"=\"2\"}");
			String str1 = outString.substring(0, ind + 1).trim();
			String str2 =
				outString.substring(ind + "{\"myTagForResolvingProblem\"=\"2\"}".length() - 1,
						outString.length()).trim();
			outString = str1 + str2;
		}

		while (outString.contains("{'myTagForResolvingProblem'='2'}")) {
			int ind = outString.indexOf("{'myTagForResolvingProblem'='2'}");
			String str1 = outString.substring(0, ind + 1).trim();
			String str2 =
				outString.substring(ind + "{'myTagForResolvingProblem'='2'}".length() - 1,
						outString.length()).trim();
			outString = str1 + str2;
		}

		while (outString.contains("myTagForResolvingProblem")) {
			String str = "</myTagForResolvingProblem>";
			int ind = outString.indexOf("<myTagForResolvingProblem>");
			int ind1 = outString.indexOf("</myTagForResolvingProblem>");
			String str1 = outString.substring(0, ind).trim();
			String str2 = outString.substring(ind1 + str.length(), outString.length()).trim();
			outString = str1 + str2;
		}

		while (outString.contains("myChartTemplateTag")) {
			String strstr = "<myChartTemplateTag>";
			String str = "</myChartTemplateTag>";
			int ind = outString.indexOf("<myChartTemplateTag>");
			int ind1 = outString.indexOf("</myChartTemplateTag>");
			String str100 = outString.substring(0, ind + strstr.length()).trim();
			String str200 = outString.substring(ind1, outString.length()).trim();
			outString = str100 + str200;
			outString =
				outString.replace("<myChartTemplateTag></myChartTemplateTag>",
						templateJOInitial.toString());
		}

		outString = outString.replaceFirst("<[?]xml(.)*[?]>", "");
		outString = outString.trim();

		if (bLeft) {
			while (outString.contains("&amp;lt;")) {
				outString = outString.replace("&amp;lt;", "&lt;");
			}
		}
		if (bRight) {
			while (outString.contains("&amp;gt;")) {
				outString = outString.replace("&amp;gt;", "&gt;");
			}
		}
		if (bCenter) {
			while (outString.contains("&amp;nbsp;")) {
				outString = outString.replace("&amp;nbsp;", "&nbsp;");
			}
		}

		return outString;
	}

	private Document buildDoc(final Document doc, final Element root, final JSONObject jsonObj)
			throws JSONException {
		String[] ar = JSONObject.getNames(jsonObj);
		String[] newAr = new String[ar.length];
		newAr[0] = ar[ar.length - 1];
		for (int i = 1; i < newAr.length; i++) {
			newAr[i] = ar[i - 1];
		}

		Object value;

		for (String key : newAr) {
			value = jsonObj.get(key);

			if (key.startsWith("@")) {
				key = key.substring(1);
				settingAttribute(key, root, value);
			} else if ("#text".equalsIgnoreCase(key)) {
				comparison3(doc, root, value);
			} else if ("#sorted".equalsIgnoreCase(key)) {
				JSONArray jArSorted = (JSONArray) value;
				for (int j = 0; j < jArSorted.length(); j++) {
					JSONObject jObjCell = (JSONObject) jArSorted.get(j);
					String[] arCell = JSONObject.getNames(jObjCell);
					Element elemCell = null;
					try {
						elemCell = doc.createElement(arCell[0]);
						root.appendChild(elemCell);
					} catch (Exception e) {
						System.out.printf(UNSUCCESSFUL, arCell[0]);
					}
					// elemCell.setAttribute("sorted", "True");
					Object valueCell = jObjCell.get(arCell[0]);

					comparison(doc, elemCell, valueCell);

					if (valueCell.getClass() == JSONArray.class) {
						buildArSortedDoc(doc, root, (JSONArray) valueCell, elemCell);
					}
				}
			} else {
				comparison2(doc, root, value, key);

				if (value.getClass() == JSONArray.class) {
					JSONArray jArray = (JSONArray) value;
					buildArDoc(doc, root, jArray, key);
				}
			}
		}

		return doc;
	}

	private Document buildArDoc(final Document doc, final Element root, final JSONArray jsonArray,
			final String key) throws JSONException {
		Object cell;

		for (int j = 0; j < jsonArray.length(); j++) {
			String childKey = key;
			Element childElem = null;
			try {
				childElem = doc.createElement(childKey);
				root.appendChild(childElem);
			} catch (Exception e) {
				System.out.printf(UNSUCCESSFUL, childKey);
			}
			cell = jsonArray.get(j);

			comparison(doc, childElem, cell);
		}

		return doc;
	}

	private Document buildArSortedDoc(final Document doc, final Element root,
			final JSONArray jsonArray, final Element elem) throws JSONException {
		Object cell;
		Element childElem;
		Element elemClone = (Element) elem.cloneNode(true);

		for (int j = 0; j < jsonArray.length(); j++) {
			if (j == 0) {
				childElem = elem;
			} else {
				childElem = (Element) elemClone.cloneNode(true);
			}

			root.appendChild(childElem);

			cell = jsonArray.get(j);

			comparison(doc, childElem, cell);
		}

		return doc;
	}

	private void buildArMultiDoc(JSONObject jsonObj) throws TransformerException, JSONException {
		String[] ar = JSONObject.getNames(jsonObj);
		sbuf = new StringBuffer();
		JSONArray jsonArray = (JSONArray) jsonObj.get(ar[0]);

		JSONObject cell;
		for (int j = 0; j < jsonArray.length(); j++) {

			Document doc1 = builder.newDocument();
			Element root = null;
			try {
				root = doc1.createElement(ar[0]);
				doc1.appendChild(root);
			} catch (Exception e) {
				System.out.printf(UNSUCCESSFUL, ar[0]);
			}
			cell = (JSONObject) jsonArray.get(j);

			doc1 = buildDoc(doc1, root, cell);

			t = TransformerFactory.newInstance().newTransformer();

			Writer strWriter = new StringWriter();
			StreamResult streamRes = new StreamResult(strWriter);

			t.setOutputProperty(OutputKeys.INDENT, "yes");
			t.transform(new DOMSource(doc1), streamRes);

			String outString = strWriter.toString();
			sbuf.append(outString);
		}

	}

	private Document gettingDocRoot(final Document doc, final JSONObject jsonObj)
			throws JSONException, TransformerException {

		String[] ar = JSONObject.getNames(jsonObj);
		Element root = null;
		try {
			root = doc.createElement(ar[0]);
			doc.appendChild(root);
		} catch (Exception e) {
			System.out.printf(UNSUCCESSFUL, ar[0]);
		}
		Object value = jsonObj.get(ar[0]);

		comparison(doc, root, value);

		if (value.getClass() == JSONArray.class) {
			buildArMultiDoc(jsonObj);
		}

		return doc;
	}

	private Text settingTextNode(final Document doc, final Object value) {
		Text text;
		if (value.getClass() == Boolean.class) {
			String change = value.toString();
			if (change.startsWith("t")) {
				change = "T" + change.substring(1);
			} else {
				change = "F" + change.substring(1);
			}

			text = doc.createTextNode(change);
		} else if ("None".equalsIgnoreCase(value.toString())
				|| "null".equalsIgnoreCase(value.toString())) {
			text = null;
		} else {
			text = doc.createTextNode(value.toString());
		}

		return text;
	}

	private void settingAttribute(final String key, final Element root, final Object value) {
		Object twer = null;

		if (value.getClass() == Boolean.class) {
			String change = value.toString();
			if (change.startsWith("t")) {
				change = "T" + change.substring(1);
			} else {
				change = "F" + change.substring(1);
			}

			root.setAttribute(key, change);
		} else if (value.equals(twer)) {
			// else if ("None".equalsIgnoreCase(value.toString())
			// || "null".equalsIgnoreCase(value.toString())) {
			root.setAttribute(key, "");
		} else if ("xmlns".equals(key)) {
			String key1 = key;
			key1 = key1 + "2";
			vBool = true;
			root.setAttribute(key1, value.toString());
		} else {
			root.setAttribute(key, value.toString());
		}
	}

	private void comparison(final Document doc, final Element elem, final Object value)
			throws JSONException {
		if (value.getClass() == String.class || value.getClass() == Integer.class
				|| value.getClass() == Double.class || value.getClass() == Boolean.class) {

			Text text = settingTextNode(doc, value);
			if (text != null) {
				elem.appendChild(text);
			}
		}

		if (value.getClass() == JSONObject.class) {
			buildDoc(doc, elem, (JSONObject) value);
		}
	}

	private void comparison2(final Document doc, final Element root, final Object value,
			final String key) throws JSONException {
		Object twer = null;

		if (value.equals(twer)) {
			Element elem = null;
			try {
				elem = doc.createElement(key);
				root.appendChild(elem);
			} catch (Exception e) {
				System.out.printf(UNSUCCESSFUL, key);
			}
			Text text = doc.createTextNode("");
			elem.appendChild(text);
		}

		if (value.getClass() == String.class || value.getClass() == Integer.class
				|| value.getClass() == Double.class || value.getClass() == Boolean.class) {

			Element elem = null;
			try {
				elem = doc.createElement(key);
				root.appendChild(elem);
			} catch (Exception e) {
				System.out.printf(UNSUCCESSFUL, key);
			}
			Text text = settingTextNode(doc, value);
			if (text != null) {
				elem.appendChild(text);
			}
		}

		if (value.getClass() == JSONObject.class) {
			Element elem = null;
			try {
				elem = doc.createElement(key);
				root.appendChild(elem);
			} catch (Exception e) {
				System.out.printf(UNSUCCESSFUL, key);
			}
			buildDoc(doc, elem, (JSONObject) value);
		}
	}

	private void comparison3(final Document doc, final Element root, final Object value) {
		Text text = null;
		if ("None".equalsIgnoreCase(value.toString()) || "null".equalsIgnoreCase(value.toString())) {
			text = doc.createTextNode("");
		} else {
			text = doc.createTextNode(value.toString());
		}
		if (root.hasChildNodes()) {
			root.insertBefore(text, root.getFirstChild());
		} else {
			root.appendChild(text);
		}
	}
}
