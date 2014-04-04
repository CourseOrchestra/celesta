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

		public Item(final String sName) {
			this.name = sName;
		}

		public Item(final String sName, final String sValue) {
			this.name = sName;
			this.value = sValue;
		}

		private Item add(final String sName, final String sValue) {
			Item item = new Item(sName, sValue);
			add(item);
			return item;
		}

		private void add(final Item item) {
			if (children == null) {
				children = new HashMap<String, List<Item>>();
			}
			List<Item> list = children.get(item.name);
			if (list == null) {
				list = new LinkedList<Item>();
				children.put(item.name, list);
			}
			list.add(item);
		}

		public boolean isLeaf() {
			return children == null;
		}

	}

	private final Stack<Item> stack;
	private final Item result;
	private String tagValue;

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
					item.add("@" + attrName, attrs.getValue(i));
				}
			}
		}

		Item head = stack.peek();
		if (isSorted) {
			Item itemSorted = new Item("#sorted");
			itemSorted.add(item);
			head.add(itemSorted);
		} else {
			head.add(item);
		}
		stack.push(item);
	}

	@Override
	public void characters(final char[] ch, final int start, final int length) {
		tagValue = new String(ch, start, length);
		if (tagValue != null && !tagValue.isEmpty()) {
			tagValue = tagValue.replaceAll("\\t|\\r", "");
			tagValue = tagValue.trim();
		}
		if (tagValue.isEmpty()) {
			tagValue = null;
		}
	}

	@Override
	public void endElement(final String uri, final String localName, final String qName)
			throws SAXException {
		Item item = stack.pop();
		if (tagValue != null) {
			item.value = tagValue;
			tagValue = null;
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
			return new JsonPrimitive(
					parentItem.value != null && !parentItem.value.isEmpty() ? parentItem.value
							: "None");
		}
		return parent;
	}

	public JsonElement getResult() {
		JsonElement resultElement = getJsonElement(result);
		return resultElement;
	}
}
