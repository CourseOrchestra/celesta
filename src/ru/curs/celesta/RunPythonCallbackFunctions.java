package ru.curs.celesta;

import org.python.core.PyObject;

/**
 * Интерфейс, определяющий функции обратного вызова при запуске питоновского
 * скрипта.
 * 
 */
public interface RunPythonCallbackFunctions {
	/**
	 * Определяет, надо ли откатывать транзакцию.
	 * 
	 * @param pyObj
	 *            - PyObject, который возвратил питоновкий интерпретатор.
	 */
	boolean needRollbackTransaction(PyObject pyObj);
}
