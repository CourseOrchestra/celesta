package ru.curs.celesta.showcase.utils;

import java.util.*;
import java.util.Map.Entry;

import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;

import com.google.gson.*;

/**
 * Sax handler преобразования xml в json.
 * 
 * @author bogatov
 * 
 */
public class XMLToJSONConverterSaxHandler extends DefaultHandler {

	/**
	 * Данные xml тега.
	 */
	private class Item {
		private final String name;
		private String value;
		private Map<String, List<Item>> children;
		private Boolean bool = false;

		public Item(final String sName) {
			this.name = sName;
		}

		public Item(final String sName, final String sValue) {
			this.name = sName;
			this.value = sValue;
		}

		private Item add(final String sName, final String sValue, final Boolean aBool) {
			Item item = new Item(sName, sValue);
			add(item, aBool);
			return item;
		}

		private void add(final Item item, final Boolean aBool) {
			if (children == null) {
				children = new HashMap<String, List<Item>>();
			}
			List<Item> list = children.get(item.name);
			if (list == null) {
				list = new LinkedList<Item>();
				children.put(item.name, list);
			}
			item.bool = aBool;
			list.add(item);
		}

		public boolean isLeaf() {
			return children == null;
		}

	}

	private final Stack<Item> stack;
	private final Item result;
	private StringBuilder tagValue;

	public XMLToJSONConverterSaxHandler() {
		this.stack = new Stack<Item>();
		result = new Item("head");
		stack.push(result);
	}

	@Override
	public void startElement(final String uri, final String localName, final String qName,
			final Attributes attrs) throws SAXException {
		boolean isSorted = false;
		Item item = new Item(qName);
		int attrsLength = attrs.getLength();
		if (attrsLength != 0) {
			for (int i = 0; i < attrsLength; i++) {
				String attrName = attrs.getQName(i);
				if ("sorted".equalsIgnoreCase(attrName)) {
					isSorted = true;
				} else {
					if ("urlparam".equals(qName) && "value".equals(attrName)
							&& attrs.getValue(i).startsWith("[")
							&& attrs.getValue(i).endsWith("]")) {
						item.add("@" + attrName, attrs.getValue(i), true);
					} else {
						item.add("@" + attrName, attrs.getValue(i), item.bool);
					}
				}
			}
		}

		Item head = stack.peek();
		if (isSorted) {
			Item itemSorted = new Item("#sorted");
			itemSorted.add(item, item.bool);
			head.add(itemSorted, item.bool);
		} else {
			head.add(item, item.bool);
		}
		stack.push(item);
	}

	@Override
	public void characters(final char[] ch, final int start, final int length) {
		if (tagValue == null) {
			tagValue = new StringBuilder();
		}
		tagValue.append(ch, start, length);
	}

	@Override
	public void endElement(final String uri, final String localName, final String qName)
			throws SAXException {
		Item item = stack.pop();
		if (tagValue != null && tagValue.length() != 0) {
			item.value = tagValue.toString().replaceAll("\\t|\\r", "").trim();
		}
		tagValue = null;
	}

	JsonArray parseStringToJsonArray(final String aStr) {
		String str = aStr;
		if (str.trim().startsWith("[") && str.trim().endsWith("]")) {
			JsonArray jAr = new JsonArray();
			str = str.trim().replace("[", "").replace("]", "");
			String[] strAr = str.split(",");
			for (String s : strAr) {
				jAr.add(new JsonPrimitive(s));
			}
			return jAr;
		} else {
			return null;
		}
	}

	private JsonElement getJsonElement(final Item parentItem) {
		JsonObject parent = new JsonObject();
		if (!parentItem.isLeaf()) {
			for (Entry<String, List<Item>> el : parentItem.children.entrySet()) {
				String property = el.getKey();
				List<Item> list = el.getValue();
				JsonElement element;
				if (list.size() > 1) {
					JsonArray array = new JsonArray();
					for (Item item : list) {
						JsonElement childJson = getJsonElement(item);
						if (childJson != null) {
							array.add(childJson);
						}
					}
					element = array;
				} else {
					Item item = list.get(0);
					element = getJsonElement(item);
				}
				if (element != null) {
					parent.add(property, element);
				}
			}
			if (parentItem.value != null) {
				parent.addProperty("#text", parentItem.value);
			}
		} else {
			JsonArray jArray = parseStringToJsonArray(parentItem.value);
			if (parentItem.name.equals("@value") && parentItem.bool && jArray != null
					&& jArray.size() > 0) {
				return jArray;
			}
			return new JsonPrimitive(
					parentItem.value != null && !parentItem.value.isEmpty() ? parentItem.value
							: "");
			// : "None");
		}
		return parent;
	}

	/**
	 * Функция, возврщающая результирующий JsonElement, преобразовать который в
	 * стороковый вид затем можно с помощью метода toString().
	 * 
	 * @return resultElement
	 */

	public JsonElement getResult() {
		JsonElement resultElement = getJsonElement(result);
		return resultElement;
	}
}
