package ru.curs.celesta.showcase;

/**
 * DTO класс с сырыми данными для элементов Showcase: навигатора, инф. панели
 * или ее элементов. Данные передаются в виде строк.
 * 
 * 
 */
public final class JythonDTO {
	/**
	 * Данные (в формате HTML или XML).
	 */
	private String data;

	/**
	 * Массив с данными. Используется вместо data.
	 */
	private String[] dataArray;
	/**
	 * Настройки элемента в формате XML.
	 */
	private String settings;

	/**
	 * Основное назначение -- выдача "ok"-сообщений.
	 */
	private UserMessage userMessage = null;

	public JythonDTO(final String aData, final String aSettings) {
		super();
		data = aData;
		settings = aSettings;
	}

	public JythonDTO(final String aData, final String aSettings, final UserMessage aUserMessage) {
		this(aData, aSettings);
		userMessage = aUserMessage;
	}

	public JythonDTO(final String[] aData, final String aSettings) {
		super();
		dataArray = aData;
		settings = aSettings;
	}

	public JythonDTO(final String[] aData, final String aSettings, final UserMessage aUserMessage) {
		this(aData, aSettings);
		userMessage = aUserMessage;
	}

	public JythonDTO(final String[] aData) {
		super();
		dataArray = aData;
	}

	public JythonDTO(final String[] aData, final UserMessage aUserMessage) {
		this(aData);
		userMessage = aUserMessage;
	}

	public JythonDTO(final String aData) {
		super();
		data = aData;
	}

	public JythonDTO(final String aData, final UserMessage aUserMessage) {
		this(aData);
		userMessage = aUserMessage;
	}

	/**
	 * Функция-getter для строкового объекта data.
	 * 
	 * @return data
	 */
	public String getData() {
		return data;
	}

	/**
	 * Функция-setter для строкового объекта data.
	 * 
	 * @param aData
	 *            - входная строка
	 */
	public void setData(final String aData) {
		data = aData;
	}

	/**
	 * Функция-getter для строкового объекта settings.
	 * 
	 * @return settings
	 */
	public String getSettings() {
		return settings;
	}

	/**
	 * Функция-setter для строкового объекта settings.
	 * 
	 * @param aSettings
	 *            - входная строка
	 */
	public void setSettings(final String aSettings) {
		settings = aSettings;
	}

	/**
	 * Функция-getter для строкового массива dataArray.
	 * 
	 * @return dataArray
	 */
	public String[] getDataArray() {
		return dataArray;
	}

	/**
	 * Функция-setter для строкового массива dataArray.
	 * 
	 * @param aDataArray
	 *            - входной строковый массив
	 */
	public void setDataArray(final String[] aDataArray) {
		dataArray = aDataArray;
	}

	public UserMessage getUserMessage() {
		return userMessage;
	}

}
