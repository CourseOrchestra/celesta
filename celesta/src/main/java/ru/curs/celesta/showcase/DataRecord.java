package ru.curs.celesta.showcase;

import java.io.Serializable;
import java.util.*;

/**
 * Запись данных для селектора.
 */
public class DataRecord implements Serializable {
	/**
	 * serialVersionUID.
	 */
	private static final long serialVersionUID = -2237996754718724140L;

	/**
	 * id.
	 * 
	 */
	private String id;
	/**
	 * name.
	 * 
	 */
	private String name;
	/**
	 * parameters.
	 * 
	 */
	private Map<String, String> parameters;

	/**
	 * Идентификатор.
	 * 
	 * @return id
	 */
	public String getId() {
		return id;
	}

	/**
	 * Устанавливает идентификатор.
	 * 
	 * @param id1
	 *            индентификатор
	 */
	public void setId(final String id1) {
		this.id = id1;
	}

	/**
	 * Имя для пользователя (по которому идёт поиск).
	 * 
	 * @return name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Устанавливает имя для пользователя.
	 * 
	 * @param name1
	 *            имя
	 */
	public void setName(final String name1) {
		this.name = name1;
	}

	/**
	 * Возвращает набор произвольных полей.
	 * 
	 * @return parameters
	 */
	public Map<String, String> getParameters() {
		return parameters;
	}

	/**
	 * Устанавливает набор произвольных полей.
	 * 
	 * @param parameters1
	 *            набор произвольных полей
	 */
	public void setParameters(final Map<String, String> parameters1) {
		this.parameters = parameters1;
	}

	/**
	 * Добавляет произвольное поле.
	 * 
	 * @param name1
	 *            имя поля
	 * @param value
	 *            значение поля
	 */
	public void addParameter(final String name1, final String value) {
		if (parameters == null) {
			parameters = new HashMap<String, String>();
		}
		parameters.put(name1, value);
	}

}
