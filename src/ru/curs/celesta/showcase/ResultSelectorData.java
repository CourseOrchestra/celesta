package ru.curs.celesta.showcase;

import java.util.*;

/**
 * Результат получения данных селектора.
 * 
 * @author anlug
 * 
 */

public class ResultSelectorData {
	private final List<DataRecord> dataRecordList;
	private final int count;

	public ResultSelectorData() {
		super();
		this.dataRecordList = new ArrayList<>();
		this.count = 0;
	}

	public ResultSelectorData(final List<DataRecord> aDataRecordList, final int aCount) {
		super();
		this.dataRecordList = aDataRecordList;
		this.count = aCount;
	}

	public ResultSelectorData(final DataRecord[] aDataRecordArray, final int aCount) {
		super();
		if (aDataRecordArray != null) {
			this.dataRecordList = Arrays.asList(aDataRecordArray);
		} else {
			this.dataRecordList = null;
		}
		this.count = aCount;
	}

	/**
	 * Функция-getter для списочного параметра dataRecordList.
	 * 
	 * @return dataRecordList
	 */
	public List<DataRecord> getDataRecordList() {
		return dataRecordList;
	}

	/**
	 * Функция-getter для целочисленного параметра count.
	 * 
	 * @return count
	 */

	public int getCount() {
		return count;
	}

}
